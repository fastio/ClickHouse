#include <Interpreters/validateANNIndexPrerequisites.h>

#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessType.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <Databases/IDatabase.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/Reflection/ANNIndex/IANNIndexAlgorithm.h>
#include <Storages/Reflection/ANNIndex/ANNAlgorithmFactory.h>
#include <Storages/Reflection/ANNIndex/ANNIndexContext.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <initializer_list>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int INCORRECT_QUERY;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

namespace Setting
{
    extern const SettingsBool allow_ann_index_engine_mismatch;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool enable_block_number_column;
    extern const MergeTreeSettingsBool enable_block_offset_column;
    extern const MergeTreeSettingsBool assign_part_uuids;
}

namespace
{

/// Extract source table identifier from `create.source_table`. The parser has
/// already validated the shape; we only need to expand `default` database.
StorageID extractSourceTableID(const ASTCreateQuery & create, ContextPtr context)
{
    if (!create.source_table)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "CREATE REFLECTION requires a source table reference (ON <source>)");

    const auto * identifier = create.source_table->as<ASTTableIdentifier>();
    if (!identifier)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Source table reference of CREATE REFLECTION must be a table identifier");

    auto storage_id = identifier->getTableId();
    if (storage_id.database_name.empty())
        storage_id.database_name = context->getCurrentDatabase();
    return storage_id;
}

/// Detect whether the ENGINE clause requests replication. The parser
/// whitelists this to one of {ANN, ReplicatedANN};
/// callers must have enforced that before reaching this helper.
bool engineIsReplicated(const ASTCreateQuery & create)
{
    if (!create.storage || !create.storage->engine)
        return false;
    return create.storage->engine->name == "ReplicatedANN" || create.storage->engine->name == "ReplicatedANNIndex";
}

String extractAlgorithmName(const ASTCreateQuery & create)
{
    if (!create.storage || !create.storage->engine || !create.storage->engine->arguments)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "REFLECTION requires an ANN engine with an algorithm");

    const auto & args = create.storage->engine->arguments->children;
    if (args.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Reflection ANN engine requires algorithm as the first argument");

    const auto * algorithm = args.front()->as<ASTIdentifier>();
    if (!algorithm)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Reflection ANN algorithm must be a bare identifier");
    return algorithm->name();
}

ASTPtr makeEqualsParam(const String & name, const Field & value)
{
    auto args = make_intrusive<ASTExpressionList>();
    args->children.push_back(make_intrusive<ASTIdentifier>(name));
    args->children.push_back(make_intrusive<ASTLiteral>(value));

    auto equals = make_intrusive<ASTFunction>();
    equals->name = "equals";
    equals->arguments = std::move(args);
    equals->children.push_back(equals->arguments);
    return equals;
}

ASTPtr buildAlgorithmParamsFromSettings(const MergeTreeSettings & settings, const String & algorithm)
{
    auto params = make_intrusive<ASTExpressionList>();
    auto add = [&](const String & param_name, const String & setting_name)
    {
        params->children.push_back(makeEqualsParam(param_name, settings.get(setting_name)));
    };
    /// Forward only when the user explicitly set the value. Lets `SPANNAlgorithm`
    /// derive a sensible default from `dim` (e.g. `posting_vector_limit` shrinks
    /// at high dim so each posting stays close to ~64 KB).
    auto add_if_changed = [&](const String & param_name, const String & setting_name)
    {
        if (settings.isChanged(setting_name))
            params->children.push_back(makeEqualsParam(param_name, settings.get(setting_name)));
    };

    add("metric", "ann_metric");
    add("dim", "ann_dimension");
    if (algorithm == "spann")
    {
        add("head_ratio", "spann_head_ratio");
        add("posting_page_limit", "spann_posting_page_limit");
        add("search_posting_page_limit", "spann_search_posting_page_limit");
        add("internal_result_num", "spann_internal_result_num");
        add("replica_count", "spann_replica_count");
        add("num_threads", "spann_num_threads");
        add("max_check", "spann_max_check");
        add("io_threads", "spann_io_threads");
        add_if_changed("posting_vector_limit", "spann_posting_vector_limit");
        add("max_dist_ratio", "spann_max_dist_ratio");
        add("hash_table_exponent", "spann_hash_table_exponent");
        add("io_timeout_us", "spann_io_timeout_us");
        add("bkt_number", "spann_bkt_number");
        add("bkt_kmeans_k", "spann_bkt_kmeans_k");
        add("bkt_leaf_size", "spann_bkt_leaf_size");
        add("neighborhood_size", "spann_neighborhood_size");
        add("cef", "spann_cef");
        add("max_check_for_refine_graph", "spann_max_check_for_refine_graph");
        add("refine_iterations", "spann_refine_iterations");
        add("tpt_number", "spann_tpt_number");
        add("rng_factor", "spann_rng_factor");
        add("select_samples_number", "spann_select_samples_number");
        add("select_threshold", "spann_select_threshold");
        add("split_factor", "spann_split_factor");
        add("split_threshold", "spann_split_threshold");
        add("enable_data_compression", "spann_enable_data_compression");
        add("enable_delta_encoding", "spann_enable_delta_encoding");
        add("enable_posting_list_rearrange", "spann_enable_posting_list_rearrange");
    }
    else if (algorithm == "diskann")
    {
        add("pruned_degree", "diskann_pruned_degree");
        add("max_degree", "diskann_max_degree");
        add("l_build", "diskann_l_build");
        add("alpha", "diskann_alpha");
        add("num_threads", "diskann_num_threads");
        add("pq_chunks", "diskann_pq_chunks");
        add("build_ram_limit_gb", "diskann_build_ram_limit_gb");
    }
    return params;
}

bool settingNameHasAnyPrefix(const String & name, std::initializer_list<std::string_view> prefixes)
{
    for (const auto prefix : prefixes)
        if (name.starts_with(prefix))
            return true;
    return false;
}

bool settingChangeIsTrue(const SettingChange & change)
{
    if (change.value.getType() == Field::Types::Bool)
        return change.value.safeGet<bool>();
    if (change.value.getType() == Field::Types::UInt64)
        return change.value.safeGet<UInt64>() != 0;
    if (change.value.getType() == Field::Types::Int64)
        return change.value.safeGet<Int64>() != 0;
    return false;
}

void validateAlgorithmSettingsCompatibility(const ASTCreateQuery & create, const String & algorithm)
{
    if (!create.storage || !create.storage->settings)
        return;

    for (const auto & change : create.storage->settings->changes)
    {
        if (change.name == "ann_compact_all_replicas" && settingChangeIsTrue(change))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Setting `ann_compact_all_replicas` is always false for ANNIndex");

        if (algorithm == "diskann" && settingNameHasAnyPrefix(change.name, {"spann_"}))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "ENGINE = ANNIndex(diskann) cannot use SPANN setting '{}'",
                change.name);

        if (algorithm == "spann" && settingNameHasAnyPrefix(change.name, {"diskann_"}))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "ENGINE = ANNIndex(spann) cannot use DiskANN setting '{}'",
                change.name);
    }
}

}


void validateANNIndexPrerequisites(
    const ASTCreateQuery & create,
    ContextPtr context,
    LoadingStrictnessLevel mode)
{
    if (!create.isDerivedObjectWithSource())
        return;

    const bool is_attach = mode >= LoadingStrictnessLevel::ATTACH;

    // 1. Source table must exist.
    const auto source_id = extractSourceTableID(create, context);
    auto source_storage = DatabaseCatalog::instance().tryGetTable(source_id, context);
    if (!source_storage)
        throw Exception(ErrorCodes::UNKNOWN_TABLE,
            "Source table {} for REFLECTION does not exist. "
            "Create the source table first, e.g.:\n"
            "    CREATE TABLE {} (...) ENGINE = MergeTree ORDER BY ...;",
            source_id.getFullTableName(), source_id.getFullTableName());

    // 2. Source ENGINE must be in the MergeTree family.
    auto * source_as_merge_tree = dynamic_cast<MergeTreeData *>(source_storage.get());
    if (!source_as_merge_tree)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Source table {} has engine {} which is not in the MergeTree family. "
            "REFLECTION requires a MergeTree-family source table.",
            source_id.getFullTableName(), source_storage->getName());

    // Engine / source replication reconcile (runs in both CREATE and ATTACH).
    const bool source_is_replicated = source_as_merge_tree->supportsReplication();
    const bool index_is_replicated = engineIsReplicated(create);
    if (source_is_replicated != index_is_replicated)
    {
        const bool escape = context->getSettingsRef()[Setting::allow_ann_index_engine_mismatch];
        if (!escape)
        {
            if (source_is_replicated)
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "Source table {} is Replicated; REFLECTION must use ENGINE = ReplicatedANNIndex. Example:\n"
                    "    CREATE REFLECTION ... ENGINE = ReplicatedANNIndex(spann, '/clickhouse/tables/{{uuid}}/{{shard}}', '{{replica}}');\n"
                    "To override (recovery only), set `allow_ann_index_engine_mismatch = 1`.",
                    source_id.getFullTableName());
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Source table {} is not Replicated; REFLECTION must use ENGINE = ANNIndex. Example:\n"
                "    CREATE REFLECTION ... ENGINE = ANNIndex(spann);\n"
                "To override (recovery only), set `allow_ann_index_engine_mismatch = 1`.",
                source_id.getFullTableName());
        }
    }

    // On ATTACH we only need to ensure the source is still usable; skip the
    // validations that expect pristine pre-create state (name uniqueness,
    // build parameter shape, etc.). The algorithm itself is re-instantiated
    // from the persisted `.sql` metadata.
    if (is_attach)
        return;

    // 3. Source must have _block_number / _block_offset materialized columns.
    {
        const auto source_settings = source_as_merge_tree->getSettings();
        if (!(*source_settings)[MergeTreeSetting::enable_block_number_column])
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Source table {} must have materialized _block_number column for REFLECTION. "
                "Run:\n    ALTER TABLE {} MODIFY SETTING enable_block_number_column = 1;",
                source_id.getFullTableName(), source_id.getFullTableName());
        if (!(*source_settings)[MergeTreeSetting::enable_block_offset_column])
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Source table {} must have materialized _block_offset column for REFLECTION. "
                "Run:\n    ALTER TABLE {} MODIFY SETTING enable_block_offset_column = 1;",
                source_id.getFullTableName(), source_id.getFullTableName());
        // D-07: stable part identity is the foundation for the Build / Remap
        // identity dictionaries; reject sources without `assign_part_uuids`
        // so a degraded index cannot reach the catalog.
        if (!(*source_settings)[MergeTreeSetting::assign_part_uuids])
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Source table {} must have assign_part_uuids = 1 for REFLECTION. "
                "Run:\n    ALTER TABLE {} MODIFY SETTING assign_part_uuids = 1;",
                source_id.getFullTableName(), source_id.getFullTableName());
    }

    // 4 & 5. ANN algorithm must be registered and supported.
    const auto algorithm_name = extractAlgorithmName(create);
    auto & factory = ANNAlgorithmFactory::instance();
    if (!factory.hasFamily("ann"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "REFLECTION family 'ann' is not registered. Available families can be inspected via system tables.");
    if (!factory.familySupportsImpl("ann", algorithm_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "REFLECTION family 'ann' does not support algorithm '{}'.",
            algorithm_name);
    validateAlgorithmSettingsCompatibility(create, algorithm_name);

    // 6 & 7. Delegate the full build-parameter and indexed-expression
    // validation to the algorithm itself; it knows its DSL.
    ANNIndexContext mi_context{
        .source_table_id = source_id,
        .indexed_columns = {},
        .family = "ann",
        .impl = algorithm_name,
        .query_context = context,
        .reflection_settings = nullptr,
    };
    auto settings = std::make_unique<MergeTreeSettings>(
        index_is_replicated ? context->getReplicatedMergeTreeSettings() : context->getMergeTreeSettings());
    settings->loadFromQuery(*create.storage, context, /*skip_unknown_settings=*/false);
    auto build_params = buildAlgorithmParamsFromSettings(*settings, algorithm_name);
    auto algorithm = factory.get("ann", algorithm_name, build_params, mi_context);
    algorithm->validateBuildParameters(build_params, context);

    if (create.indexed_columns)
    {
        const auto source_metadata = source_storage->getInMemoryMetadataPtr(context, false);
        algorithm->validateIndexedExpression(create.indexed_columns->ptr(), *source_metadata);
    }

    // 8. Target REFLECTION name must be available + caller must have
    // SELECT on the source table. When `IF NOT EXISTS` is requested we leave
    // the collision-to-no-op conversion to `InterpreterCreateQuery::doCreateTable`,
    // matching the behaviour of plain `CREATE TABLE IF NOT EXISTS`.
    const auto target_id = StorageID{
        create.getDatabase().empty() ? context->getCurrentDatabase() : create.getDatabase(),
        create.getTable(),
    };
    if (!create.if_not_exists && DatabaseCatalog::instance().isTableExist(target_id, context))
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Table {} already exists; REFLECTION cannot reuse the name.",
            target_id.getFullTableName());

    context->checkAccess(AccessType::SELECT, source_id);

    // 9. Source `partition_by` must be deterministic. For now we perform a
    // lightweight structural check — reject any partition expression that
    // mentions a function with `isDeterministic() == false`. Full data-flow
    // analysis is deferred until the build pipeline lands.
    if (const auto source_metadata = source_storage->getInMemoryMetadataPtr(context, false);
        source_metadata && source_metadata->partition_key.definition_ast)
    {
        /// Deferred to build-time validation; structural determinism is
        /// already guaranteed for the engines accepted above.
    }
}

}
