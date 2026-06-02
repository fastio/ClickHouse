#include <Storages/System/StorageSystemReflectionJobs.h>

#include <Access/ContextAccess.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>

namespace DB
{

namespace
{

DataTypeEnum8::Values getTaskKindEnumValues()
{
    return {
        {"BuildBatch", static_cast<Int8>(ANNIndexSchedulerState::TaskKind::BuildBatch)},
        {"RemapLineage", static_cast<Int8>(ANNIndexSchedulerState::TaskKind::RemapLineage)},
        {"CompactMerge", static_cast<Int8>(ANNIndexSchedulerState::TaskKind::CompactMerge)},
        {"CompactRebuild", static_cast<Int8>(ANNIndexSchedulerState::TaskKind::CompactRebuild)},
    };
}

DataTypeEnum8::Values getTaskStateEnumValues()
{
    return {
        {"Scheduled", static_cast<Int8>(ANNIndexSchedulerState::TaskLifecycle::Scheduled)},
        {"Running", static_cast<Int8>(ANNIndexSchedulerState::TaskLifecycle::Running)},
        {"Committing", static_cast<Int8>(ANNIndexSchedulerState::TaskLifecycle::Committing)},
        {"Finished", static_cast<Int8>(ANNIndexSchedulerState::TaskLifecycle::Finished)},
        {"Failed", static_cast<Int8>(ANNIndexSchedulerState::TaskLifecycle::Failed)},
    };
}

Array makeUuidArray(const std::vector<UUID> & uuids)
{
    Array result;
    result.reserve(uuids.size());
    for (const auto & uuid : uuids)
        result.push_back(uuid);
    return result;
}

void insertNullableDateTime(MutableColumnPtr & column, std::chrono::system_clock::time_point value)
{
    if (value == std::chrono::system_clock::time_point{})
        column->insertDefault();
    else
        column->insert(std::chrono::system_clock::to_time_t(value));
}

}

StorageSystemReflectionJobs::StorageSystemReflectionJobs(const StorageID & storage_id_, ColumnsDescription columns_description_)
    : IStorageSystemOneBlock(storage_id_, std::move(columns_description_))
{
}

ColumnsDescription StorageSystemReflectionJobs::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Database of the reflection."},
        {"reflection_name", std::make_shared<DataTypeString>(), "Name of the reflection."},
        {"family", std::make_shared<DataTypeString>(), "Reflection engine family."},
        {"impl", std::make_shared<DataTypeString>(), "Reflection engine implementation."},
        {"task_id", std::make_shared<DataTypeString>(), "Scheduler task id."},
        {"kind", std::make_shared<DataTypeEnum8>(getTaskKindEnumValues()), "Scheduler task kind."},
        {"state", std::make_shared<DataTypeEnum8>(getTaskStateEnumValues()), "Scheduler task lifecycle state."},
        {"input_source_uuids", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUUID>()), "Source part UUIDs reserved by the task."},
        {"input_ann_index_part_uuids", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUUID>()), "ANNIndex part UUIDs reserved by the task."},
        {"output_ann_index_part_uuid", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUUID>()), "Output ANNIndex part UUID, if assigned."},
        {"retry_count", std::make_shared<DataTypeUInt64>(), "Task retry count."},
        {"next_retry_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Next retry time, if backoff is active."},
        {"last_error", std::make_shared<DataTypeString>(), "Last task error."},
        {"quarantined", std::make_shared<DataTypeUInt8>(), "Whether the task key is quarantined."},
        {"created_at", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Task creation time."},
        {"started_at", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Task start time."},
        {"finished_at", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "Task finish time."},
    };
}

void StorageSystemReflectionJobs::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    const auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_remote_databases = false});
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
            const auto * ann = dynamic_cast<const ReflectionANNIndex *>(table.get());
            if (!ann)
                continue;

            for (const auto & task : ann->getSchedulerTasksSnapshot())
            {
                size_t col = 0;
                res_columns[col++]->insert(database_name);
                res_columns[col++]->insert(table_name);
                res_columns[col++]->insert(ann->getFamily());
                res_columns[col++]->insert(ann->getImpl());
                res_columns[col++]->insert(task.task_id);
                res_columns[col++]->insert(static_cast<Int8>(task.kind));
                res_columns[col++]->insert(static_cast<Int8>(task.state));
                res_columns[col++]->insert(makeUuidArray(task.input_source_uuids));
                res_columns[col++]->insert(makeUuidArray(task.input_ann_index_part_uuids));
                if (task.output_ann_index_part_uuid == UUID{})
                    res_columns[col++]->insertDefault();
                else
                    res_columns[col++]->insert(task.output_ann_index_part_uuid);
                res_columns[col++]->insert(task.retry_count);
                insertNullableDateTime(res_columns[col++], task.next_retry_time);
                res_columns[col++]->insert(task.last_error);
                res_columns[col++]->insert(static_cast<UInt8>(task.quarantined));
                insertNullableDateTime(res_columns[col++], task.created_at);
                insertNullableDateTime(res_columns[col++], task.started_at);
                insertNullableDateTime(res_columns[col++], task.finished_at);
            }
        }
    }
}

}
