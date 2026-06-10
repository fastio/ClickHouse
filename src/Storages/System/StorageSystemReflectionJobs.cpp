#include <Storages/System/StorageSystemReflectionJobs.h>

#include <Access/ContextAccess.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>

#include <map>

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

Map makeUInt64Map(const std::map<String, UInt64> & values)
{
    Map result;
    result.reserve(values.size());
    for (const auto & [key, value] : values)
        result.push_back(Tuple{key, value});
    return result;
}

Map makeStringMap(const std::map<String, String> & values)
{
    Map result;
    result.reserve(values.size());
    for (const auto & [key, value] : values)
        result.push_back(Tuple{key, value});
    return result;
}

void insertNullableDateTime(MutableColumnPtr & column, std::chrono::system_clock::time_point value)
{
    if (value == std::chrono::system_clock::time_point{})
        column->insertDefault();
    else
        column->insert(std::chrono::system_clock::to_time_t(value));
}

double getDurationSeconds(
    std::chrono::system_clock::time_point started_at,
    std::chrono::system_clock::time_point finished_at,
    std::chrono::system_clock::time_point now)
{
    if (started_at == std::chrono::system_clock::time_point{})
        return 0.0;

    const auto end_time = finished_at == std::chrono::system_clock::time_point{} ? now : finished_at;
    if (end_time <= started_at)
        return 0.0;

    const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - started_at);
    return static_cast<double>(duration.count()) * 1e-9;
}

}

StorageSystemReflectionJobs::StorageSystemReflectionJobs(const StorageID & storage_id_, ColumnsDescription columns_description_)
    : IStorageSystemOneBlock(storage_id_, std::move(columns_description_))
{
}

ColumnsDescription StorageSystemReflectionJobs::getColumnsDescription()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    auto description = ColumnsDescription
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
        {"duration_seconds", std::make_shared<DataTypeFloat64>(), "Seconds elapsed since task start. Zero if the task has not started; total execution time if it has finished."},
        {"build_stage", std::make_shared<DataTypeString>(), "Algorithm build stage reported by the running task, if available."},
        {"build_next_stage", std::make_shared<DataTypeString>(), "Next algorithm build stage reported by the running task, if available."},
        {"build_progress", std::make_shared<DataTypeUInt64>(), "Algorithm-specific build progress offset/count reported by the running task."},
        {"build_stage_progress", std::make_shared<DataTypeUInt64>(), "Algorithm-specific progress within the current build stage."},
        {"build_stage_progress_total", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Known total units for build_stage_progress, if available."},
        {"build_current_shard", std::make_shared<DataTypeUInt64>(), "Current build shard reported by the running task, if available."},
        {"build_num_shards", std::make_shared<DataTypeUInt64>(), "Known build shard count reported by the running task, if available."},
        {"build_error", std::make_shared<DataTypeString>(), "Algorithm build error reported by the running task, if available."},
        {"settings", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeString>()), "Common algorithm settings reported by the running DiskANN or SPANN task instance."},
        {"BuildProfileEvents", std::make_shared<DataTypeMap>(low_cardinality_string, std::make_shared<DataTypeUInt64>()), "Algorithm-specific build profile events for the running task."},
    };

    description.setAliases({
        {"settings.Names", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())}, "mapKeys(settings)"},
        {"settings.Values", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())}, "mapValues(settings)"},
        {"BuildProfileEvents.Names", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())}, "mapKeys(BuildProfileEvents)"},
        {"BuildProfileEvents.Values", {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())}, "mapValues(BuildProfileEvents)"},
    });

    return description;
}

void StorageSystemReflectionJobs::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    const auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_remote_databases = false});
    const auto now = std::chrono::system_clock::now();
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
                res_columns[col++]->insert(getDurationSeconds(task.started_at, task.finished_at, now));
                res_columns[col++]->insert(task.build_stage);
                res_columns[col++]->insert(task.build_next_stage);
                res_columns[col++]->insert(task.build_progress);
                res_columns[col++]->insert(task.build_stage_progress);
                if (task.build_stage_progress_total)
                    res_columns[col++]->insert(*task.build_stage_progress_total);
                else
                    res_columns[col++]->insertDefault();
                res_columns[col++]->insert(task.build_current_shard);
                res_columns[col++]->insert(task.build_num_shards);
                res_columns[col++]->insert(task.build_error);
                res_columns[col++]->insert(makeStringMap(task.settings));
                res_columns[col++]->insert(makeUInt64Map(task.build_profile_events));
            }
        }
    }
}

}
