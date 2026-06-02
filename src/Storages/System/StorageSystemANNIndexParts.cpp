#include <Storages/System/StorageSystemANNIndexParts.h>

#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>


namespace DB
{

namespace
{

/// Provenance fields persisted to `header.json` by `ANNIndex::BuildTaskImpl::FinalizeMetadataStage`.
/// Held in an `optional<>` because a part that has been hand-attached or
/// crash-recovered before the writer flushed may legitimately miss them.
struct PartProvenance
{
    String source_partition_id;
    Int64 source_min_block = 0;
    Int64 source_max_block = 0;
    UInt64 tombstone_rows = 0;
};

std::optional<PartProvenance> readProvenanceFromHeader(const IDataPartStorage & part_storage)
{
    if (!part_storage.existsFile("header.json"))
        return std::nullopt;

    auto reader = part_storage.readFile("header.json", ReadSettings{}, std::nullopt);
    String header_text;
    readStringUntilEOF(header_text, *reader);

    Poco::JSON::Parser parser;
    auto parsed = parser.parse(header_text);
    const auto & obj = parsed.extract<Poco::JSON::Object::Ptr>();
    if (!obj)
        return std::nullopt;

    PartProvenance result;
    if (obj->has("source_partition_id"))
        result.source_partition_id = obj->getValue<std::string>("source_partition_id");
    if (obj->has("source_min_block"))
        result.source_min_block = obj->getValue<Int64>("source_min_block");
    if (obj->has("source_max_block"))
        result.source_max_block = obj->getValue<Int64>("source_max_block");
    if (obj->has("tombstone_rows"))
        result.tombstone_rows = obj->getValue<UInt64>("tombstone_rows");
    return result;
}

}

StorageSystemANNIndexParts::StorageSystemANNIndexParts(const StorageID & storage_id_, ColumnsDescription columns_description_)
    : IStorageSystemOneBlock(storage_id_, std::move(columns_description_))
{
}

ColumnsDescription StorageSystemANNIndexParts::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Database of the ann index that owns this part."},
        {"index_name", std::make_shared<DataTypeString>(), "Name of the ann index that owns this part."},
        {"part_name", std::make_shared<DataTypeString>(), "On-disk name of the materialized-index part."},
        {"part_uuid", std::make_shared<DataTypeUUID>(), "UUID of the materialized-index part."},
        {"physical_partition_id", std::make_shared<DataTypeString>(), "Partition id encoded in the part name. Stable hash of the source partition id derived via `MergeTreePartition::getID`."},
        {"source_partition_id", std::make_shared<DataTypeString>(), "Source-table partition id this part covers, read from `header.json`."},
        {"source_min_block", std::make_shared<DataTypeInt64>(), "Lowest source-part block number covered by this part, read from `header.json`."},
        {"source_max_block", std::make_shared<DataTypeInt64>(), "Highest source-part block number covered by this part, read from `header.json`."},
        {"min_block", std::make_shared<DataTypeInt64>(), "Materialized-index-part min_block from the part name."},
        {"max_block", std::make_shared<DataTypeInt64>(), "Materialized-index-part max_block from the part name."},
        {"level", std::make_shared<DataTypeUInt32>(), "Materialized-index-part level from the part name."},
        {"rows", std::make_shared<DataTypeUInt64>(), "Number of source rows indexed by this part."},
        {"tombstone_rows", std::make_shared<DataTypeUInt64>(), "Number of tombstone locator rows recorded in this ANNIndex part."},
        {"tombstone_ratio", std::make_shared<DataTypeFloat64>(), "Ratio of tombstone locator rows to rows in this ANNIndex part."},
        {"bytes_on_disk", std::make_shared<DataTypeUInt64>(), "Disk footprint of the materialized-index part in bytes."},
        {"active", std::make_shared<DataTypeUInt8>(), "Whether the part is in the Active state. Currently this table only exposes Active parts."},
    };
}

void StorageSystemANNIndexParts::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    const auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_remote_databases = false});
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
            auto * ann_index = dynamic_cast<ReflectionANNIndex *>(table.get());
            if (!ann_index)
                continue;

            MergeTreeData::DataPartsVector active_parts;
            try
            {
                active_parts = ann_index->getAccessPathPartsVectorForInternalUsage();
            }
            catch (...)
            {
                /// Mirrors system.ann_indexes: a storage that rejects
                /// the snapshot mid-shutdown should not fail the whole query.
                tryLogCurrentException(getLogger("StorageSystemANNIndexParts"));
                continue;
            }

            for (const auto & part : active_parts)
            {
                if (!part)
                    continue;

                std::optional<PartProvenance> provenance;
                try
                {
                    provenance = readProvenanceFromHeader(part->getDataPartStorage());
                }
                catch (...)
                {
                    tryLogCurrentException(getLogger("StorageSystemANNIndexParts"));
                }

                size_t col = 0;
                res_columns[col++]->insert(database_name);
                res_columns[col++]->insert(table_name);
                res_columns[col++]->insert(part->name);
                res_columns[col++]->insert(part->uuid);
                res_columns[col++]->insert(part->info.getPartitionId());
                res_columns[col++]->insert(provenance ? provenance->source_partition_id : String{});
                res_columns[col++]->insert(provenance ? provenance->source_min_block : Int64{0});
                res_columns[col++]->insert(provenance ? provenance->source_max_block : Int64{0});
                res_columns[col++]->insert(part->info.min_block);
                res_columns[col++]->insert(part->info.max_block);
                res_columns[col++]->insert(static_cast<UInt64>(part->info.level));
                res_columns[col++]->insert(part->rows_count);
                const UInt64 tombstone_rows = provenance ? provenance->tombstone_rows : UInt64{0};
                res_columns[col++]->insert(tombstone_rows);
                res_columns[col++]->insert(part->rows_count == 0 ? 0.0 : static_cast<double>(tombstone_rows) / static_cast<double>(part->rows_count));
                res_columns[col++]->insert(part->getBytesOnDisk());
                res_columns[col++]->insert(static_cast<UInt8>(1));
            }
        }
    }
}

}
