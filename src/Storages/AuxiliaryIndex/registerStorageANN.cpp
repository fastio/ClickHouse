#include <Storages/AuxiliaryIndex/StorageANN.h>
#include <Storages/AuxiliaryIndex/StorageReplicatedANN.h>

#include <Core/Names.h>
#include <Core/Settings.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool assign_part_uuids;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_auxiliary_index;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_QUERY;
    extern const int SUPPORT_IS_DISABLED;
}

/// Refuse `CREATE AUXILIARY INDEX` (and `CREATE TABLE ... ENGINE = AuxiliaryIndex`)
/// when the experimental gate is off. Mirrors the Interpreter-layer check so
/// callers that bypass `InterpreterCreateQuery` still hit the gate.
/// Existing metadata must still load during `ATTACH` and server startup.
///
/// Exposed at namespace scope (rather than the anonymous namespace below)
/// solely so unit tests can drive the gate logic without constructing a full
/// `StorageFactory::Arguments`.
void checkAuxiliaryIndexExperimentalGate(ContextPtr context, LoadingStrictnessLevel mode)
{
    if (LoadingStrictnessLevel::ATTACH <= mode)
        return;

    if (!context->getSettingsRef()[Setting::allow_experimental_auxiliary_index])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "AuxiliaryIndex is experimental. "
            "Enable `allow_experimental_auxiliary_index` setting to use it.");
}

namespace
{

Names unpackIndexedColumns(const ASTCreateQuery & create)
{
    Names result;
    if (!create.indexed_columns)
        return result;

    const auto * list = create.indexed_columns->as<ASTExpressionList>();
    if (!list)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AUXILIARY INDEX indexed column list has unexpected shape");
    result.reserve(list->children.size());
    for (const auto & child : list->children)
        result.push_back(getIdentifierName(child));
    return result;
}

StorageID unpackSourceId(const ASTCreateQuery & create, const String & default_database)
{
    if (!create.source_table)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "CREATE AUXILIARY INDEX requires a source table reference (ON <source>)");
    const auto * ident = create.source_table->as<ASTTableIdentifier>();
    if (!ident)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Source table reference of CREATE AUXILIARY INDEX must be a table identifier");
    auto source_id = ident->getTableId();
    if (source_id.database_name.empty())
        source_id.database_name = default_database;
    return source_id;
}

String unpackAlgorithmName(const StorageFactory::Arguments & args, bool replicated)
{
    const size_t expected_size = replicated ? 3 : 1;
    if (args.engine_args.size() != expected_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ENGINE = {} requires {} arguments",
            replicated ? "ReplicatedANN" : "ANN",
            replicated ? "(algorithm, zookeeper_path, replica_name)" : "(algorithm)");

    const auto * algorithm = args.engine_args[0]->as<ASTIdentifier>();
    if (!algorithm)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The first argument of ENGINE = {} must be a bare algorithm identifier",
            replicated ? "ReplicatedANN" : "ANN");
    return algorithm->name();
}

std::pair<String, String> unpackReplicatedEngineArgs(const StorageFactory::Arguments & args)
{
    if (args.engine_args.size() != 3)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ENGINE = ReplicatedANN requires exactly three arguments: "
            "(algorithm, zookeeper_path, replica_name)");
    const auto * zk_lit = args.engine_args[1]->as<ASTLiteral>();
    const auto * rep_lit = args.engine_args[2]->as<ASTLiteral>();
    if (!zk_lit || zk_lit->value.getType() != Field::Types::String
        || !rep_lit || rep_lit->value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ENGINE = ReplicatedANN zookeeper_path and replica_name arguments must be string literals");
    return {zk_lit->value.safeGet<String>(), rep_lit->value.safeGet<String>()};
}

std::unique_ptr<MergeTreeSettings> loadSettings(const StorageFactory::Arguments & args, bool replicated)
{
    auto context = args.getContext();
    const auto & initial = replicated ? context->getReplicatedMergeTreeSettings() : context->getMergeTreeSettings();
    auto settings = std::make_unique<MergeTreeSettings>(initial);
    if (args.storage_def)
        settings->loadFromQuery(*args.storage_def, context, LoadingStrictnessLevel::ATTACH <= args.mode);
    return settings;
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

    add("metric", "ann_metric");
    add("dim", "ann_dimension");

    if (algorithm == "spann")
    {
        add("select_type", "spann_select_type");
        add("head_ratio", "spann_head_ratio");
        add("posting_page_limit", "spann_posting_page_limit");
        add("search_posting_page_limit", "spann_search_posting_page_limit");
        add("internal_result_num", "spann_internal_result_num");
        add("replica_count", "spann_replica_count");
        add("num_threads", "spann_num_threads");
        add("select_head_threads", "spann_select_head_threads");
        add("build_head_threads", "spann_build_head_threads");
        add("max_check", "spann_max_check");
        add("io_threads", "spann_io_threads");
        add("posting_vector_limit", "spann_posting_vector_limit");
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

StorageInMemoryMetadata buildMetadata(const StorageFactory::Arguments & args)
{
    StorageInMemoryMetadata metadata;
    static constexpr auto placeholder_column_name = "_mi_placeholder";
    /// AUXILIARY INDEX exposes no user-declared columns, but MergeTreeData
    /// demands a non-empty column list. Synthesize an internal placeholder;
    /// the real columns are derived from the source table at build time
    /// (future stages).
    ColumnsDescription columns = args.columns;
    if (columns.empty())
        columns.add(ColumnDescription(placeholder_column_name, std::make_shared<DataTypeUInt8>()));
    metadata.setColumns(std::move(columns));
    metadata.setComment(args.comment);
    /// The base class `MergeTreeData` requires an explicit sorting key. Use
    /// `ORDER BY tuple()` semantics for the stage-1 unsorted placeholder.
    auto sorting_key = makeASTOperator("tuple");
    metadata.partition_key = KeyDescription::buildEmptyKey();
    metadata.sorting_key = KeyDescription::getSortingKeyFromAST(sorting_key, metadata.columns, args.getContext(), {});
    metadata.primary_key = KeyDescription::getKeyFromAST(sorting_key->clone(), metadata.columns, args.getContext());
    return metadata;
}

/// D-07: a AuxiliaryIndex requires its source table to assign UUIDs to
/// every part. CREATE-path enforcement runs in
/// `validateAuxiliaryIndexPrerequisites` before this factory; this hook
/// is the ATTACH-path safety net and only logs a warning so a stale catalog
/// cannot prevent server startup.
void validateSourceAssignsPartUuids(const StorageFactory::Arguments & args, const StorageID & source_id)
{
    if (!(LoadingStrictnessLevel::ATTACH <= args.mode))
        return;

    auto resolved = source_id;
    if (resolved.database_name.empty())
        resolved.database_name = args.getContext()->getCurrentDatabase();

    auto source_storage = DatabaseCatalog::instance().tryGetTable(resolved, args.getContext());
    if (!source_storage)
        return;

    const auto * source_merge_tree = dynamic_cast<const MergeTreeData *>(source_storage.get());
    if (!source_merge_tree)
        return;

    if (!(*source_merge_tree->getSettings())[MergeTreeSetting::assign_part_uuids])
    {
        LOG_WARNING(
            getLogger("registerStorageANN"),
            "Source table {} does not have assign_part_uuids = 1; "
            "AuxiliaryIndex will be degraded until it is enabled.",
            resolved.getNameForLogs());
    }
}

}


void registerStorageANN(StorageFactory & factory)
{
    /// AuxiliaryIndex is backed by MergeTreeData and accepts MergeTree settings
    /// (index_granularity, etc.), so the factory must allow a `SETTINGS` clause.
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .has_builtin_setting_fn = MergeTreeSettings::hasBuiltin,
    };

    factory.registerStorage(
        "ANN",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            checkAuxiliaryIndexExperimentalGate(args.getLocalContext(), args.mode);
            auto algorithm_name = unpackAlgorithmName(args, /*replicated*/ false);
            auto indexed = unpackIndexedColumns(args.query);
            auto source_id = unpackSourceId(args.query, args.table_id.database_name);
            validateSourceAssignsPartUuids(args, source_id);
            auto settings = loadSettings(args, /*replicated*/ false);
            auto metadata = buildMetadata(args);
            auto build_params = buildAlgorithmParamsFromSettings(*settings, algorithm_name);

            return std::make_shared<StorageANN>(
                args.table_id,
                args.relative_data_path,
                source_id,
                std::move(indexed),
                "ann",
                algorithm_name,
                build_params,
                args.getContext(),
                metadata,
                std::move(settings),
                args.mode);
        },
        features);
}

void registerStorageReplicatedANN(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .has_builtin_setting_fn = MergeTreeSettings::hasBuiltin,
    };

    factory.registerStorage(
        "ReplicatedANN",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            checkAuxiliaryIndexExperimentalGate(args.getLocalContext(), args.mode);
            auto algorithm_name = unpackAlgorithmName(args, /*replicated*/ true);
            auto indexed = unpackIndexedColumns(args.query);
            auto source_id = unpackSourceId(args.query, args.table_id.database_name);
            validateSourceAssignsPartUuids(args, source_id);
            auto [zk_path, replica] = unpackReplicatedEngineArgs(args);
            auto settings = loadSettings(args, /*replicated*/ true);
            auto metadata = buildMetadata(args);
            auto build_params = buildAlgorithmParamsFromSettings(*settings, algorithm_name);

            return std::make_shared<StorageReplicatedANN>(
                args.table_id,
                args.relative_data_path,
                source_id,
                std::move(indexed),
                "ann",
                algorithm_name,
                build_params,
                zk_path,
                replica,
                args.getContext(),
                metadata,
                std::move(settings),
                args.mode);
        },
        features);
}

}
