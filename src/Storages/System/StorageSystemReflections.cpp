#include <Storages/System/StorageSystemReflections.h>

#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/Reflection/IReflection.h>

namespace DB
{

StorageSystemReflections::StorageSystemReflections(const StorageID & storage_id_, ColumnsDescription columns_description_)
    : IStorageSystemOneBlock(storage_id_, std::move(columns_description_))
{
}

ColumnsDescription StorageSystemReflections::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Database of the reflection."},
        {"name", std::make_shared<DataTypeString>(), "Name of the reflection."},
        {"uuid", std::make_shared<DataTypeUUID>(), "UUID of the reflection."},
        {"source_database", std::make_shared<DataTypeString>(), "Database of the source table."},
        {"source_table", std::make_shared<DataTypeString>(), "Name of the source table."},
        {"family", std::make_shared<DataTypeString>(), "Reflection engine family."},
        {"impl", std::make_shared<DataTypeString>(), "Reflection engine implementation."},
        {"engine", std::make_shared<DataTypeString>(), "Storage engine backing the reflection."},
        {"internal_storage", std::make_shared<DataTypeString>(), "Internal storage table name, when exposed by the engine."},
        {"state", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Lifecycle state. NULL until an engine reports a framework state."},
        {"coverage_ratio", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()), "Fraction of source rows covered by this reflection."},
        {"part_count", std::make_shared<DataTypeUInt64>(), "Number of active reflection parts."},
        {"pending_task_count", std::make_shared<DataTypeUInt64>(), "Number of background tasks reserved or running."},
        {"backlog_parts", std::make_shared<DataTypeUInt64>(), "Source parts waiting for background reflection work. Matches the `backlog_parts` field reported by `SYSTEM SYNC REFLECTION` on timeout."},
        {"backlog_bytes", std::make_shared<DataTypeUInt64>(), "Bytes of source data waiting for background reflection work."},
        {"backlog_rows", std::make_shared<DataTypeUInt64>(), "Rows waiting for background reflection work."},
        {"retry_count", std::make_shared<DataTypeUInt64>(), "Number of consecutive postponed tasks."},
        {"next_retry_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Next time postponed work may be retried."},
        {"last_error", std::make_shared<DataTypeString>(), "Last scheduler error or backoff reason."},
    };
}

void StorageSystemReflections::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    const auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false});
    for (const auto & [database_name, database] : databases)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE || database->isExternal())
            continue;

        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);
        for (auto tables_it = database->getTablesIterator(context); tables_it->isValid(); tables_it->next())
        {
            const auto table_name = tables_it->name();
            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                continue;

            const auto table = tables_it->table();
            const auto * reflection = dynamic_cast<const IReflection *>(table.get());
            if (!reflection)
                continue;

            const auto source_id = reflection->getReflectionSourceTableID();
            const auto storage_id = table->getStorageID();
            const auto inner_table = reflection->getReflectionInnerTable();
            const auto observability = reflection->getReflectionObservabilitySnapshot();

            size_t col = 0;
            res_columns[col++]->insert(database_name);
            res_columns[col++]->insert(table_name);
            res_columns[col++]->insert(storage_id.uuid);
            res_columns[col++]->insert(source_id.database_name);
            res_columns[col++]->insert(source_id.table_name);
            res_columns[col++]->insert(reflection->getReflectionFamily());
            res_columns[col++]->insert(reflection->getReflectionImpl());
            res_columns[col++]->insert(reflection->getReflectionEngineName());
            res_columns[col++]->insert(inner_table ? inner_table->getStorageID().getFullTableName() : String{});
            res_columns[col++]->insertDefault();
            res_columns[col++]->insertDefault();
            res_columns[col++]->insert(observability.ready_part_count);
            res_columns[col++]->insert(observability.pending_task_count);
            res_columns[col++]->insert(observability.backlog_parts);
            res_columns[col++]->insert(observability.backlog_bytes);
            res_columns[col++]->insert(observability.backlog_rows);
            res_columns[col++]->insert(observability.retry_count);
            if (observability.next_retry_time == std::chrono::system_clock::time_point{})
                res_columns[col++]->insertDefault();
            else
                res_columns[col++]->insert(std::chrono::system_clock::to_time_t(observability.next_retry_time));
            res_columns[col++]->insert(observability.last_error);
        }
    }
}

}
