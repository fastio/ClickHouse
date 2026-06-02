#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>
#include <Storages/Reflection/ANNIndex/ReflectionReplicatedANNIndex.h>

#include <Core/Names.h>
#include <Core/Settings.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <initializer_list>
#include <unordered_set>


namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool assign_part_uuids;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_ann_index;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_QUERY;
    extern const int SUPPORT_IS_DISABLED;
}

using ANNIndexBuildParamSetting = std::pair<std::string_view, std::string_view>;

const std::vector<ANNIndexBuildParamSetting> & getCommonANNIndexBuildParamSettings()
{
    static const std::vector<ANNIndexBuildParamSetting> settings{
        {"metric", "ann_metric"},
        {"dim", "ann_dimension"},
    };
    return settings;
}

const std::vector<ANNIndexBuildParamSetting> & getSPANNBuildParamSettings()
{
    static const std::vector<ANNIndexBuildParamSetting> settings{
        {"select_type", "spann_select_type"},
        {"head_ratio", "spann_head_ratio"},
        {"posting_page_limit", "spann_posting_page_limit"},
        {"search_posting_page_limit", "spann_search_posting_page_limit"},
        {"internal_result_num", "spann_internal_result_num"},
        {"replica_count", "spann_replica_count"},
        {"num_threads", "spann_num_threads"},
        {"select_head_threads", "spann_select_head_threads"},
        {"build_head_threads", "spann_build_head_threads"},
        {"max_check", "spann_max_check"},
        {"io_threads", "spann_io_threads"},
        {"posting_vector_limit", "spann_posting_vector_limit"},
        {"max_dist_ratio", "spann_max_dist_ratio"},
        {"hash_table_exponent", "spann_hash_table_exponent"},
        {"io_timeout_us", "spann_io_timeout_us"},
        {"bkt_number", "spann_bkt_number"},
        {"bkt_kmeans_k", "spann_bkt_kmeans_k"},
        {"bkt_leaf_size", "spann_bkt_leaf_size"},
        {"neighborhood_size", "spann_neighborhood_size"},
        {"cef", "spann_cef"},
        {"max_check_for_refine_graph", "spann_max_check_for_refine_graph"},
        {"refine_iterations", "spann_refine_iterations"},
        {"tpt_number", "spann_tpt_number"},
        {"rng_factor", "spann_rng_factor"},
        {"select_samples_number", "spann_select_samples_number"},
        {"select_threshold", "spann_select_threshold"},
        {"split_factor", "spann_split_factor"},
        {"split_threshold", "spann_split_threshold"},
        {"enable_data_compression", "spann_enable_data_compression"},
        {"enable_delta_encoding", "spann_enable_delta_encoding"},
        {"enable_posting_list_rearrange", "spann_enable_posting_list_rearrange"},
    };
    return settings;
}

const std::vector<ANNIndexBuildParamSetting> & getDiskANNBuildParamSettings()
{
    static const std::vector<ANNIndexBuildParamSetting> settings{
        {"pruned_degree", "diskann_pruned_degree"},
        {"max_degree", "diskann_max_degree"},
        {"l_build", "diskann_l_build"},
        {"alpha", "diskann_alpha"},
        {"num_threads", "diskann_num_threads"},
        {"pq_chunks", "diskann_pq_chunks"},
        {"build_ram_limit_gb", "diskann_build_ram_limit_gb"},
    };
    return settings;
}

const std::unordered_set<std::string_view> & getANNIndexBuildAffectingSettings()
{
    static const std::unordered_set<std::string_view> settings = []
    {
        std::unordered_set<std::string_view> result;
        for (const auto & [_, setting_name] : getCommonANNIndexBuildParamSettings())
            result.insert(setting_name);
        for (const auto & [_, setting_name] : getSPANNBuildParamSettings())
            result.insert(setting_name);
        for (const auto & [_, setting_name] : getDiskANNBuildParamSettings())
            result.insert(setting_name);
        return result;
    }();
    return settings;
}

/// Refuse `CREATE REFLECTION` (and `CREATE TABLE ... ENGINE = ANNIndex`)
/// when the experimental gate is off. Mirrors the Interpreter-layer check so
/// callers that bypass `InterpreterCreateQuery` still hit the gate.
/// Existing metadata must still load during `ATTACH` and server startup.
///
/// Exposed at namespace scope (rather than the anonymous namespace below)
/// solely so unit tests can drive the gate logic without constructing a full
/// `StorageFactory::Arguments`.
void checkANNIndexExperimentalGate(ContextPtr context, LoadingStrictnessLevel mode)
{
    if (LoadingStrictnessLevel::ATTACH <= mode)
        return;

    if (!context->getSettingsRef()[Setting::allow_experimental_ann_index])
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "ANNIndex is experimental. "
            "Enable `allow_experimental_ann_index` setting to use it.");
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
            "REFLECTION indexed column list has unexpected shape");
    result.reserve(list->children.size());
    for (const auto & child : list->children)
        result.push_back(getIdentifierName(child));
    return result;
}

StorageID unpackSourceId(const ASTCreateQuery & create, const String & default_database)
{
    if (!create.source_table)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "CREATE REFLECTION requires a source table reference (ON <source>)");
    const auto * ident = create.source_table->as<ASTTableIdentifier>();
    if (!ident)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Source table reference of CREATE REFLECTION must be a table identifier");
    auto source_id = ident->getTableId();
    if (source_id.database_name.empty())
        source_id.database_name = default_database;
    return source_id;
}

String unpackAlgorithmName(const StorageFactory::Arguments & args, bool replicated, std::string_view engine_name)
{
    const size_t expected_size = replicated ? 3 : 1;
    if (args.engine_args.size() != expected_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ENGINE = {} requires {} arguments",
            engine_name,
            replicated ? "(algorithm, zookeeper_path, replica_name)" : "(algorithm)");

    const auto * algorithm = args.engine_args[0]->as<ASTIdentifier>();
    if (!algorithm)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The first argument of ENGINE = {} must be a bare algorithm identifier",
            engine_name);
    return algorithm->name();
}

std::pair<String, String> unpackReplicatedEngineArgs(const StorageFactory::Arguments & args, std::string_view engine_name)
{
    if (args.engine_args.size() != 3)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ENGINE = {} requires exactly three arguments: "
            "(algorithm, zookeeper_path, replica_name)",
            engine_name);
    const auto * zk_lit = args.engine_args[1]->as<ASTLiteral>();
    const auto * rep_lit = args.engine_args[2]->as<ASTLiteral>();
    if (!zk_lit || zk_lit->value.getType() != Field::Types::String
        || !rep_lit || rep_lit->value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ENGINE = {} zookeeper_path and replica_name arguments must be string literals",
            engine_name);
    return {zk_lit->value.safeGet<String>(), rep_lit->value.safeGet<String>()};
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

void validateAlgorithmSettingsCompatibility(const StorageFactory::Arguments & args, const String & algorithm)
{
    if (!args.storage_def || !args.storage_def->settings)
        return;

    for (const auto & change : args.storage_def->settings->changes)
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
    auto add = [&](std::string_view param_name, std::string_view setting_name)
    {
        String param_name_str(param_name);
        String setting_name_str(setting_name);
        params->children.push_back(makeEqualsParam(param_name_str, settings.get(setting_name_str)));
    };
    /// Forward only when the user explicitly set the value. Lets `SPANNAlgorithm`
    /// derive a sensible default from `dim` (e.g. `posting_vector_limit` shrinks
    /// at high dim so each posting stays close to ~64 KB).
    auto add_if_changed = [&](std::string_view param_name, std::string_view setting_name)
    {
        if (settings.isChanged(setting_name))
            add(param_name, setting_name);
    };

    for (const auto & [param_name, setting_name] : getCommonANNIndexBuildParamSettings())
        add(param_name, setting_name);

    if (algorithm == "spann")
    {
        for (const auto & [param_name, setting_name] : getSPANNBuildParamSettings())
        {
            if (param_name == "posting_vector_limit")
                add_if_changed(param_name, setting_name);
            else
                add(param_name, setting_name);
        }
    }
    else if (algorithm == "diskann")
    {
        for (const auto & [param_name, setting_name] : getDiskANNBuildParamSettings())
            add(param_name, setting_name);
    }

    return params;
}

StorageInMemoryMetadata buildMetadata(const StorageFactory::Arguments & args)
{
    StorageInMemoryMetadata metadata;
    /// REFLECTION exposes no user-declared columns. An ANN-index part is a
    /// standard Wide MergeTree part whose 4 physical columns form the row
    /// locator mapping an algorithm-internal id (= part-local row number) back
    /// to a source-table row. The part is unsorted (`internal_id` is the row
    /// number), hence `ORDER BY tuple()`.
    ColumnsDescription columns = args.columns;
    if (columns.empty())
        columns = parseColumnsListFromString(
            "source_uuid UUID CODEC(ZSTD), "
            "part_offset UInt64 CODEC(DoubleDelta, LZ4), "
            "block_number UInt64 CODEC(DoubleDelta, LZ4), "
            "block_offset UInt64 CODEC(DoubleDelta, LZ4)",
            args.getContext());
    metadata.setColumns(std::move(columns));
    metadata.setComment(args.comment);
    /// The base class `MergeTreeData` requires an explicit sorting key. The
    /// locator is unsorted, so use `ORDER BY tuple()`.
    auto sorting_key = makeASTOperator("tuple");
    metadata.partition_key = KeyDescription::buildEmptyKey();
    metadata.sorting_key = KeyDescription::getKeyFromAST(sorting_key, metadata.columns, /*virtuals=*/{}, args.getContext());
    metadata.primary_key = KeyDescription::getKeyFromAST(sorting_key->clone(), metadata.columns, /*virtuals=*/{}, args.getContext());
    return metadata;
}

/// D-07: a ANNIndex requires its source table to assign UUIDs to
/// every part. CREATE-path enforcement runs in
/// `validateANNIndexPrerequisites` before this factory; this hook
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
            getLogger("registerStorageANNIndex"),
            "Source table {} does not have assign_part_uuids = 1; "
            "ANNIndex will be degraded until it is enabled.",
            resolved.getNameForLogs());
    }
}

}


void registerStorageANNIndex(StorageFactory & factory)
{
    /// ANNIndex is backed by MergeTreeData and accepts MergeTree settings
    /// (index_granularity, etc.), so the factory must allow a `SETTINGS` clause.
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .has_builtin_setting_fn = MergeTreeSettings::hasBuiltin,
    };

    factory.registerStorage(
        "ANN",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            checkANNIndexExperimentalGate(args.getLocalContext(), args.mode);
            auto algorithm_name = unpackAlgorithmName(args, /*replicated*/ false, "ANN");
            validateAlgorithmSettingsCompatibility(args, algorithm_name);
            auto indexed = unpackIndexedColumns(args.query);
            auto source_id = unpackSourceId(args.query, args.table_id.database_name);
            validateSourceAssignsPartUuids(args, source_id);
            auto settings = loadSettings(args, /*replicated*/ false);
            auto metadata = buildMetadata(args);
            auto build_params = buildAlgorithmParamsFromSettings(*settings, algorithm_name);

            return std::make_shared<ReflectionANNIndex>(
                args.table_id,
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

    factory.registerStorage(
        "ANNIndex",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            checkANNIndexExperimentalGate(args.getLocalContext(), args.mode);
            auto algorithm_name = unpackAlgorithmName(args, /*replicated*/ false, "ANNIndex");
            validateAlgorithmSettingsCompatibility(args, algorithm_name);
            auto indexed = unpackIndexedColumns(args.query);
            auto source_id = unpackSourceId(args.query, args.table_id.database_name);
            validateSourceAssignsPartUuids(args, source_id);
            auto settings = loadSettings(args, /*replicated*/ false);
            auto metadata = buildMetadata(args);
            auto build_params = buildAlgorithmParamsFromSettings(*settings, algorithm_name);

            return std::make_shared<ReflectionANNIndex>(
                args.table_id,
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

void registerStorageReplicatedANNIndex(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .has_builtin_setting_fn = MergeTreeSettings::hasBuiltin,
    };

    factory.registerStorage(
        "ReplicatedANN",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            checkANNIndexExperimentalGate(args.getLocalContext(), args.mode);
            auto algorithm_name = unpackAlgorithmName(args, /*replicated*/ true, "ReplicatedANN");
            validateAlgorithmSettingsCompatibility(args, algorithm_name);
            auto indexed = unpackIndexedColumns(args.query);
            auto source_id = unpackSourceId(args.query, args.table_id.database_name);
            validateSourceAssignsPartUuids(args, source_id);
            auto [zk_path, replica] = unpackReplicatedEngineArgs(args, "ReplicatedANN");
            auto settings = loadSettings(args, /*replicated*/ true);
            auto metadata = buildMetadata(args);
            auto build_params = buildAlgorithmParamsFromSettings(*settings, algorithm_name);

            return std::make_shared<ReflectionReplicatedANNIndex>(
                args.table_id,
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

    factory.registerStorage(
        "ReplicatedANNIndex",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            checkANNIndexExperimentalGate(args.getLocalContext(), args.mode);
            auto algorithm_name = unpackAlgorithmName(args, /*replicated*/ true, "ReplicatedANNIndex");
            validateAlgorithmSettingsCompatibility(args, algorithm_name);
            auto indexed = unpackIndexedColumns(args.query);
            auto source_id = unpackSourceId(args.query, args.table_id.database_name);
            validateSourceAssignsPartUuids(args, source_id);
            auto [zk_path, replica] = unpackReplicatedEngineArgs(args, "ReplicatedANNIndex");
            auto settings = loadSettings(args, /*replicated*/ true);
            auto metadata = buildMetadata(args);
            auto build_params = buildAlgorithmParamsFromSettings(*settings, algorithm_name);

            return std::make_shared<ReflectionReplicatedANNIndex>(
                args.table_id,
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
