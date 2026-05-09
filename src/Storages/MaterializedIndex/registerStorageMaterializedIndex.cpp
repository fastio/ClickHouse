#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>
#include <Storages/MaterializedIndex/StorageReplicatedMaterializedIndex.h>

#include <Core/Names.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTMaterializedIndexDeclaration.h>
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

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_QUERY;
}


namespace
{

/// Pull the `(family, impl, build_params)` triple out of the AST built by
/// `ParserCreateMaterializedIndexQuery`; the declaration pointer is always
/// set by the time we reach the factory.
const ASTMaterializedIndexDeclaration & unpackTypeDeclaration(const ASTCreateQuery & create)
{
    if (!create.materialized_index_type)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MATERIALIZED INDEX factory invoked without a TYPE declaration");
    const auto * decl = create.materialized_index_type->as<ASTMaterializedIndexDeclaration>();
    if (!decl)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MATERIALIZED INDEX type clause has unexpected shape");
    return *decl;
}

Names unpackIndexedColumns(const ASTCreateQuery & create)
{
    Names result;
    if (!create.indexed_columns)
        return result;

    const auto * list = create.indexed_columns->as<ASTExpressionList>();
    if (!list)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MATERIALIZED INDEX indexed column list has unexpected shape");
    result.reserve(list->children.size());
    for (const auto & child : list->children)
        result.push_back(getIdentifierName(child));
    return result;
}

StorageID unpackSourceId(const ASTCreateQuery & create)
{
    if (!create.source_table)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "CREATE MATERIALIZED INDEX requires a source table reference (ON <source>)");
    const auto * ident = create.source_table->as<ASTTableIdentifier>();
    if (!ident)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Source table reference of CREATE MATERIALIZED INDEX must be a table identifier");
    return ident->getTableId();
}

std::pair<String, String> unpackReplicatedEngineArgs(const StorageFactory::Arguments & args)
{
    if (args.engine_args.size() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ENGINE = ReplicatedMaterializedIndex requires exactly two literal arguments: "
            "(zookeeper_path, replica_name)");
    const auto * zk_lit = args.engine_args[0]->as<ASTLiteral>();
    const auto * rep_lit = args.engine_args[1]->as<ASTLiteral>();
    if (!zk_lit || zk_lit->value.getType() != Field::Types::String
        || !rep_lit || rep_lit->value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ENGINE = ReplicatedMaterializedIndex arguments must be string literals");
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

StorageInMemoryMetadata buildMetadata(const StorageFactory::Arguments & args)
{
    StorageInMemoryMetadata metadata;
    static constexpr auto placeholder_column_name = "_mi_placeholder";
    /// MATERIALIZED INDEX exposes no user-declared columns, but MergeTreeData
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

/// D-07: a MaterializedIndex requires its source table to assign UUIDs to
/// every part. CREATE-path enforcement runs in
/// `validateMaterializedIndexPrerequisites` before this factory; this hook
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
            getLogger("registerStorageMaterializedIndex"),
            "Source table {} does not have assign_part_uuids = 1; "
            "MaterializedIndex will be degraded until it is enabled.",
            resolved.getNameForLogs());
    }
}

}


void registerStorageMaterializedIndex(StorageFactory & factory)
{
    /// MaterializedIndex is backed by MergeTreeData and accepts MergeTree settings
    /// (index_granularity, etc.), so the factory must allow a `SETTINGS` clause.
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .has_builtin_setting_fn = MergeTreeSettings::hasBuiltin,
    };

    factory.registerStorage(
        "MaterializedIndex",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            const auto & decl = unpackTypeDeclaration(args.query);
            auto indexed = unpackIndexedColumns(args.query);
            auto source_id = unpackSourceId(args.query);
            validateSourceAssignsPartUuids(args, source_id);
            auto settings = loadSettings(args, /*replicated*/ false);
            auto metadata = buildMetadata(args);
            auto build_params = decl.getBuildParams();

            return std::make_shared<StorageMaterializedIndex>(
                args.table_id,
                args.relative_data_path,
                source_id,
                std::move(indexed),
                decl.family,
                decl.impl,
                build_params,
                args.getContext(),
                metadata,
                std::move(settings),
                args.mode);
        },
        features);
}

void registerStorageReplicatedMaterializedIndex(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .has_builtin_setting_fn = MergeTreeSettings::hasBuiltin,
    };

    factory.registerStorage(
        "ReplicatedMaterializedIndex",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            const auto & decl = unpackTypeDeclaration(args.query);
            auto indexed = unpackIndexedColumns(args.query);
            auto source_id = unpackSourceId(args.query);
            validateSourceAssignsPartUuids(args, source_id);
            auto [zk_path, replica] = unpackReplicatedEngineArgs(args);
            auto settings = loadSettings(args, /*replicated*/ true);
            auto metadata = buildMetadata(args);
            auto build_params = decl.getBuildParams();

            return std::make_shared<StorageReplicatedMaterializedIndex>(
                args.table_id,
                args.relative_data_path,
                source_id,
                std::move(indexed),
                decl.family,
                decl.impl,
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
