#include <Storages/System/StorageSystemANNIndexes.h>

#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>
#include <Storages/Reflection/IReflection.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <chrono>


namespace DB
{

StorageSystemANNIndexes::StorageSystemANNIndexes(const StorageID & storage_id_, ColumnsDescription columns_description_)
    : IStorageSystemOneBlock(storage_id_, std::move(columns_description_))
{
}

ColumnsDescription StorageSystemANNIndexes::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Database of the ann index."},
        {"name", std::make_shared<DataTypeString>(), "Name of the ann index."},
        {"uuid", std::make_shared<DataTypeUUID>(), "UUID of the ann index (only for Atomic databases)."},
        {"source_database", std::make_shared<DataTypeString>(), "Database of the source table the index is built on."},
        {"source_table", std::make_shared<DataTypeString>(), "Name of the source table the index is built on."},
        {"family", std::make_shared<DataTypeString>(), "Algorithm family declared in the TYPE clause (e.g. `ann`)."},
        {"impl", std::make_shared<DataTypeString>(), "Algorithm implementation declared in the TYPE clause (e.g. `mock`)."},
        {"engine", std::make_shared<DataTypeString>(), "Storage engine backing the index (`ANN` or `ReplicatedANN`)."},
        {"state", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Lifecycle state of the index. Always NULL placeholder; will be populated once the engine reports a real state."},
        {"coverage_ratio", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()), "Fraction of source rows covered by the index. Always NULL placeholder; will be populated once coverage tracking is wired up."},
        {"ann_index_part_count", std::make_shared<DataTypeUInt64>(), "Number of Active materialized-index-parts persisted for the index."},
        {"total_rows", std::make_shared<DataTypeUInt64>(), "Number of rows across Active materialized-index-parts."},
        {"total_bytes_on_disk", std::make_shared<DataTypeUInt64>(), "Disk footprint of Active materialized-index-parts in bytes."},
        {"consecutive_remap_count", std::make_shared<DataTypeUInt64>(), "Number of consecutive Remap cycles since the last Build (Q-E starvation counter)."},
        {"backlog_rows", std::make_shared<DataTypeUInt64>(), "Rows in uncovered source parts waiting for a ANNIndex BuildBatch."},
        {"backlog_bytes", std::make_shared<DataTypeUInt64>(), "Bytes in uncovered source parts waiting for a ANNIndex BuildBatch."},
        {"backlog_parts", std::make_shared<DataTypeUInt64>(), "Number of uncovered source parts waiting for a ANNIndex BuildBatch."},
        {"pending_task_count", std::make_shared<DataTypeUInt64>(), "Number of ANNIndex background tasks currently reserved or running."},
        {"ready_ann_index_part_count", std::make_shared<DataTypeUInt64>(), "Number of ready ANNIndex parts in scheduler state."},
        {"repeated_failure_count", std::make_shared<DataTypeUInt64>(), "Number of ANNIndex task keys currently in retry backoff or quarantine."},
        {"tombstone_rows", std::make_shared<DataTypeUInt64>(), "Total tombstone locator rows across active ANNIndex parts."},
        {"tombstone_ratio", std::make_shared<DataTypeFloat64>(), "Ratio of tombstone locator rows to rows across active ANNIndex parts."},
        {"retry_count", std::make_shared<DataTypeUInt64>(), "Number of consecutive ANNIndex resource/backoff postponements."},
        {"next_retry_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Next time a postponed ANNIndex task may be retried."},
        {"last_error", std::make_shared<DataTypeString>(), "Last ANNIndex scheduler resource/backoff reason."},
        {"comment", std::make_shared<DataTypeString>(), "User-provided comment from CREATE."},
        {"creation_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "When the index was created. Always NULL placeholder; will be populated once the metadata exposes a creation timestamp."},
        {"last_refresh_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Last time the background pipeline refreshed the index (placeholder, always NULL)."},
    };
}

void StorageSystemANNIndexes::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    const auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false});
    for (const auto & [database_name, database] : databases)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue;
        if (database->isExternal())
            continue;

        const bool check_access_for_tables = check_access_for_databases
            && !access->isGranted(AccessType::SHOW_TABLES, database_name);

        for (auto tables_it = database->getTablesIterator(context); tables_it->isValid(); tables_it->next())
        {
            const auto table_name = tables_it->name();
            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                continue;

            const auto table = tables_it->table();
            auto * reflection = dynamic_cast<IReflection *>(table.get());
            auto * ann_index = dynamic_cast<ReflectionANNIndex *>(table.get());
            if (!reflection || reflection->getReflectionFamily() != "ann")
                continue;

            const auto & source_id = reflection->getReflectionSourceTableID();
            const auto & storage_id = table->getStorageID();

            UInt64 ann_index_part_count = 0;
            UInt64 total_rows = 0;
            UInt64 total_bytes_on_disk = 0;
            try
            {
                if (ann_index)
                {
                    const auto active_parts = ann_index->getAccessPathPartsVectorForInternalUsage();
                    ann_index_part_count = active_parts.size();
                    for (const auto & part : active_parts)
                    {
                        total_rows += part->rows_count;
                        total_bytes_on_disk += part->getBytesOnDisk();
                    }
                }
            }
            catch (...)
            {
                /// Best-effort aggregation: if the storage rejects the
                /// snapshot (e.g. mid-shutdown), keep the row but expose
                /// zero counters rather than failing the whole query.
                tryLogCurrentException(getLogger("StorageSystemANNIndexes"));
            }

            size_t col = 0;
            res_columns[col++]->insert(database_name);
            res_columns[col++]->insert(table_name);
            res_columns[col++]->insert(storage_id.uuid);
            res_columns[col++]->insert(source_id.database_name);
            res_columns[col++]->insert(source_id.table_name);
            res_columns[col++]->insert(reflection->getReflectionFamily());
            res_columns[col++]->insert(reflection->getReflectionImpl());
            res_columns[col++]->insert(reflection->getReflectionEngineName());
            res_columns[col++]->insertDefault();
            res_columns[col++]->insertDefault();
            res_columns[col++]->insert(ann_index_part_count);
            res_columns[col++]->insert(total_rows);
            res_columns[col++]->insert(total_bytes_on_disk);
            res_columns[col++]->insert(static_cast<UInt64>(ann_index ? ann_index->getConsecutiveRemapCount() : 0));
            auto observability = reflection->getReflectionObservabilitySnapshot();
            res_columns[col++]->insert(observability.backlog_rows);
            res_columns[col++]->insert(observability.backlog_bytes);
            res_columns[col++]->insert(observability.backlog_parts);
            res_columns[col++]->insert(observability.pending_task_count);
            res_columns[col++]->insert(observability.ready_part_count);
            res_columns[col++]->insert(observability.repeated_failure_count);
            res_columns[col++]->insert(observability.tombstone_rows);
            res_columns[col++]->insert(observability.tombstone_ratio);
            res_columns[col++]->insert(observability.retry_count);
            if (observability.next_retry_time == std::chrono::system_clock::time_point{})
                res_columns[col++]->insertDefault();
            else
                res_columns[col++]->insert(std::chrono::system_clock::to_time_t(observability.next_retry_time));
            res_columns[col++]->insert(observability.last_error);

            String comment;
            if (auto metadata = table->getInMemoryMetadataPtr(context, false))
                comment = metadata->comment;
            res_columns[col++]->insert(comment);

            res_columns[col++]->insertDefault();
            res_columns[col++]->insertDefault();
        }
    }
}

}
