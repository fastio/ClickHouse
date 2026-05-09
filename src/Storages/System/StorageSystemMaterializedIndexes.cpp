#include <Storages/System/StorageSystemMaterializedIndexes.h>

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
#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace DB
{

StorageSystemMaterializedIndexes::StorageSystemMaterializedIndexes(const StorageID & storage_id_, ColumnsDescription columns_description_)
    : IStorageSystemOneBlock(storage_id_, std::move(columns_description_))
{
}

ColumnsDescription StorageSystemMaterializedIndexes::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Database of the materialized index."},
        {"name", std::make_shared<DataTypeString>(), "Name of the materialized index."},
        {"uuid", std::make_shared<DataTypeUUID>(), "UUID of the materialized index (only for Atomic databases)."},
        {"source_database", std::make_shared<DataTypeString>(), "Database of the source table the index is built on."},
        {"source_table", std::make_shared<DataTypeString>(), "Name of the source table the index is built on."},
        {"family", std::make_shared<DataTypeString>(), "Algorithm family declared in the TYPE clause (e.g. `ann`)."},
        {"impl", std::make_shared<DataTypeString>(), "Algorithm implementation declared in the TYPE clause (e.g. `mock`)."},
        {"engine", std::make_shared<DataTypeString>(), "Storage engine backing the index (MaterializedIndex or ReplicatedMaterializedIndex)."},
        {"state", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Lifecycle state of the index. Always NULL placeholder; will be populated once the engine reports a real state."},
        {"coverage_ratio", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()), "Fraction of source rows covered by the index. Always NULL placeholder; will be populated once coverage tracking is wired up."},
        {"mi_part_count", std::make_shared<DataTypeUInt64>(), "Number of Active mi-parts persisted for the index."},
        {"total_rows", std::make_shared<DataTypeUInt64>(), "Number of rows across Active mi-parts."},
        {"total_bytes_on_disk", std::make_shared<DataTypeUInt64>(), "Disk footprint of Active mi-parts in bytes."},
        {"consecutive_remap_count", std::make_shared<DataTypeUInt64>(), "Number of consecutive Remap cycles since the last Build (Q-E starvation counter)."},
        {"comment", std::make_shared<DataTypeString>(), "User-provided comment from CREATE."},
        {"creation_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "When the index was created. Always NULL placeholder; will be populated once the metadata exposes a creation timestamp."},
        {"last_refresh_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Last time the background pipeline refreshed the index (placeholder, always NULL)."},
    };
}

void StorageSystemMaterializedIndexes::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
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
            auto * mi = dynamic_cast<StorageMaterializedIndex *>(table.get());
            if (!mi)
                continue;

            const auto & source_id = mi->getSourceTableID();
            const auto & storage_id = mi->getStorageID();

            UInt64 mi_part_count = 0;
            UInt64 total_rows = 0;
            UInt64 total_bytes_on_disk = 0;
            try
            {
                const auto active_parts = mi->getAccessPathPartsVectorForInternalUsage();
                mi_part_count = active_parts.size();
                for (const auto & part : active_parts)
                {
                    total_rows += part->rows_count;
                    total_bytes_on_disk += part->getBytesOnDisk();
                }
            }
            catch (...)
            {
                /// Best-effort aggregation: if the storage rejects the
                /// snapshot (e.g. mid-shutdown), keep the row but expose
                /// zero counters rather than failing the whole query.
                tryLogCurrentException(getLogger("StorageSystemMaterializedIndexes"));
            }

            size_t col = 0;
            res_columns[col++]->insert(database_name);
            res_columns[col++]->insert(table_name);
            res_columns[col++]->insert(storage_id.uuid);
            res_columns[col++]->insert(source_id.database_name);
            res_columns[col++]->insert(source_id.table_name);
            res_columns[col++]->insert(mi->getFamily());
            res_columns[col++]->insert(mi->getImpl());
            res_columns[col++]->insert(mi->getName());
            res_columns[col++]->insertDefault();
            res_columns[col++]->insertDefault();
            res_columns[col++]->insert(mi_part_count);
            res_columns[col++]->insert(total_rows);
            res_columns[col++]->insert(total_bytes_on_disk);
            res_columns[col++]->insert(static_cast<UInt64>(mi->getConsecutiveRemapCount()));

            String comment;
            if (auto metadata = mi->getInMemoryMetadataPtr(context, false))
                comment = metadata->comment;
            res_columns[col++]->insert(comment);

            res_columns[col++]->insertDefault();
            res_columns[col++]->insertDefault();
        }
    }
}

}
