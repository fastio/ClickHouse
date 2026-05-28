#include <Storages/System/StorageSystemReflectionParts.h>

#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>
#include <Storages/Reflection/IReflection.h>

namespace DB
{

StorageSystemReflectionParts::StorageSystemReflectionParts(const StorageID & storage_id_, ColumnsDescription columns_description_)
    : IStorageSystemOneBlock(storage_id_, std::move(columns_description_))
{
}

ColumnsDescription StorageSystemReflectionParts::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Database of the reflection."},
        {"reflection_name", std::make_shared<DataTypeString>(), "Name of the reflection that owns this part."},
        {"family", std::make_shared<DataTypeString>(), "Reflection engine family."},
        {"impl", std::make_shared<DataTypeString>(), "Reflection engine implementation."},
        {"part_name", std::make_shared<DataTypeString>(), "On-disk name of the reflection part."},
        {"part_uuid", std::make_shared<DataTypeUUID>(), "UUID of the reflection part."},
        {"physical_partition_id", std::make_shared<DataTypeString>(), "Partition id encoded in the part name."},
        {"min_block", std::make_shared<DataTypeInt64>(), "Reflection-part min_block from the part name."},
        {"max_block", std::make_shared<DataTypeInt64>(), "Reflection-part max_block from the part name."},
        {"level", std::make_shared<DataTypeUInt32>(), "Reflection-part level from the part name."},
        {"rows", std::make_shared<DataTypeUInt64>(), "Number of rows represented by this reflection part."},
        {"bytes_on_disk", std::make_shared<DataTypeUInt64>(), "Disk footprint of the reflection part in bytes."},
        {"active", std::make_shared<DataTypeUInt8>(), "Whether the part is in the Active state. Currently this table only exposes Active parts."},
    };
}

void StorageSystemReflectionParts::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
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
            const auto * ann = dynamic_cast<const ReflectionANNIndex *>(table.get());
            if (!reflection || !ann)
                continue;

            MergeTreeData::DataPartsVector active_parts;
            try
            {
                active_parts = ann->getAccessPathPartsVectorForInternalUsage();
            }
            catch (...)
            {
                tryLogCurrentException(getLogger("StorageSystemReflectionParts"));
                continue;
            }

            for (const auto & part : active_parts)
            {
                if (!part)
                    continue;

                size_t col = 0;
                res_columns[col++]->insert(database_name);
                res_columns[col++]->insert(table_name);
                res_columns[col++]->insert(reflection->getReflectionFamily());
                res_columns[col++]->insert(reflection->getReflectionImpl());
                res_columns[col++]->insert(part->name);
                res_columns[col++]->insert(part->uuid);
                res_columns[col++]->insert(part->info.getPartitionId());
                res_columns[col++]->insert(part->info.min_block);
                res_columns[col++]->insert(part->info.max_block);
                res_columns[col++]->insert(static_cast<UInt64>(part->info.level));
                res_columns[col++]->insert(part->rows_count);
                res_columns[col++]->insert(part->getBytesOnDisk());
                res_columns[col++]->insert(static_cast<UInt8>(1));
            }
        }
    }
}

}
