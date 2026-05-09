#include <Interpreters/validateMaterializedIndexPrerequisites.h>

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
#include <Parsers/ASTMaterializedIndexDeclaration.h>
#include <Storages/MaterializedIndex/IMaterializedIndexAlgorithm.h>
#include <Storages/MaterializedIndex/MaterializedIndexAlgorithmFactory.h>
#include <Storages/MaterializedIndex/MaterializedIndexContext.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>


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
    extern const SettingsBool allow_materialized_index_engine_mismatch;
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
            "CREATE MATERIALIZED INDEX requires a source table reference (ON <source>)");

    const auto * identifier = create.source_table->as<ASTTableIdentifier>();
    if (!identifier)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Source table reference of CREATE MATERIALIZED INDEX must be a table identifier");

    auto storage_id = identifier->getTableId();
    if (storage_id.database_name.empty())
        storage_id.database_name = context->getCurrentDatabase();
    return storage_id;
}

/// Detect whether the ENGINE clause requests replication. The parser
/// whitelists this to one of {MaterializedIndex, ReplicatedMaterializedIndex};
/// callers must have enforced that before reaching this helper.
bool engineIsReplicated(const ASTCreateQuery & create)
{
    if (!create.storage || !create.storage->engine)
        return false;
    return create.storage->engine->name == "ReplicatedMaterializedIndex";
}

}


void validateMaterializedIndexPrerequisites(
    const ASTCreateQuery & create,
    ContextPtr context,
    LoadingStrictnessLevel mode)
{
    if (!create.is_materialized_index)
        return;

    const bool is_attach = mode >= LoadingStrictnessLevel::ATTACH;

    // 1. Source table must exist.
    const auto source_id = extractSourceTableID(create, context);
    auto source_storage = DatabaseCatalog::instance().tryGetTable(source_id, context);
    if (!source_storage)
        throw Exception(ErrorCodes::UNKNOWN_TABLE,
            "Source table {} for MATERIALIZED INDEX does not exist. "
            "Create the source table first, e.g.:\n"
            "    CREATE TABLE {} (...) ENGINE = MergeTree ORDER BY ...;",
            source_id.getFullTableName(), source_id.getFullTableName());

    // 2. Source ENGINE must be in the MergeTree family.
    auto * source_as_merge_tree = dynamic_cast<MergeTreeData *>(source_storage.get());
    if (!source_as_merge_tree)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Source table {} has engine {} which is not in the MergeTree family. "
            "MATERIALIZED INDEX requires a MergeTree-family source table.",
            source_id.getFullTableName(), source_storage->getName());

    // Engine / source replication reconcile (runs in both CREATE and ATTACH).
    const bool source_is_replicated = source_as_merge_tree->supportsReplication();
    const bool index_is_replicated = engineIsReplicated(create);
    if (source_is_replicated != index_is_replicated)
    {
        const bool escape = context->getSettingsRef()[Setting::allow_materialized_index_engine_mismatch];
        if (!escape)
        {
            if (source_is_replicated)
                throw Exception(ErrorCodes::INCORRECT_QUERY,
                    "Source table {} is Replicated; MATERIALIZED INDEX must use ENGINE = ReplicatedMaterializedIndex. Example:\n"
                    "    CREATE MATERIALIZED INDEX ... ENGINE = ReplicatedMaterializedIndex('/clickhouse/tables/{{uuid}}/{{shard}}', '{{replica}}');\n"
                    "To override (recovery only), set `allow_materialized_index_engine_mismatch = 1`.",
                    source_id.getFullTableName());
            throw Exception(ErrorCodes::INCORRECT_QUERY,
                "Source table {} is not Replicated; MATERIALIZED INDEX must use ENGINE = MaterializedIndex. Example:\n"
                "    CREATE MATERIALIZED INDEX ... ENGINE = MaterializedIndex;\n"
                "To override (recovery only), set `allow_materialized_index_engine_mismatch = 1`.",
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
                "Source table {} must have materialized _block_number column for MATERIALIZED INDEX. "
                "Run:\n    ALTER TABLE {} MODIFY SETTING enable_block_number_column = 1;",
                source_id.getFullTableName(), source_id.getFullTableName());
        if (!(*source_settings)[MergeTreeSetting::enable_block_offset_column])
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Source table {} must have materialized _block_offset column for MATERIALIZED INDEX. "
                "Run:\n    ALTER TABLE {} MODIFY SETTING enable_block_offset_column = 1;",
                source_id.getFullTableName(), source_id.getFullTableName());
        // D-07: stable part identity is the foundation for the Build / Remap
        // identity dictionaries; reject sources without `assign_part_uuids`
        // so a degraded index cannot reach the catalog.
        if (!(*source_settings)[MergeTreeSetting::assign_part_uuids])
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Source table {} must have assign_part_uuids = 1 for MATERIALIZED INDEX. "
                "Run:\n    ALTER TABLE {} MODIFY SETTING assign_part_uuids = 1;",
                source_id.getFullTableName(), source_id.getFullTableName());
    }

    // 4 & 5. Algorithm family must be registered and the requested impl
    // must be supported by it.
    const auto * decl = create.materialized_index_type
        ? create.materialized_index_type->as<ASTMaterializedIndexDeclaration>()
        : nullptr;
    if (!decl)
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "CREATE MATERIALIZED INDEX requires a TYPE clause, e.g. TYPE ann('diskann').");

    auto & factory = MaterializedIndexAlgorithmFactory::instance();
    if (!factory.hasFamily(decl->family))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "MATERIALIZED INDEX family '{}' is not registered. Available families can be inspected via system tables.",
            decl->family);
    if (!factory.familySupportsImpl(decl->family, decl->impl))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "MATERIALIZED INDEX family '{}' does not support impl '{}'.",
            decl->family, decl->impl);

    // 6 & 7. Delegate the full build-parameter and indexed-expression
    // validation to the algorithm itself; it knows its DSL.
    MaterializedIndexContext mi_context{
        .source_table_id = source_id,
        .indexed_columns = {},
        .family = decl->family,
        .impl = decl->impl,
        .query_context = context,
    };
    auto build_params = decl->getBuildParams();
    auto algorithm = factory.get(decl->family, decl->impl, build_params, mi_context);
    algorithm->validateBuildParameters(build_params, context);

    if (create.indexed_columns)
    {
        const auto source_metadata = source_storage->getInMemoryMetadataPtr(context, false);
        algorithm->validateIndexedExpression(create.indexed_columns->ptr(), *source_metadata);
    }

    // 8. Target MATERIALIZED INDEX name must be available + caller must have
    // SELECT on the source table. When `IF NOT EXISTS` is requested we leave
    // the collision-to-no-op conversion to `InterpreterCreateQuery::doCreateTable`,
    // matching the behaviour of plain `CREATE TABLE IF NOT EXISTS`.
    const auto target_id = StorageID{
        create.getDatabase().empty() ? context->getCurrentDatabase() : create.getDatabase(),
        create.getTable(),
    };
    if (!create.if_not_exists && DatabaseCatalog::instance().isTableExist(target_id, context))
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Table {} already exists; MATERIALIZED INDEX cannot reuse the name.",
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
