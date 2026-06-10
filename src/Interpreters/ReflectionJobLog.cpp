#include <Interpreters/ReflectionJobLog.h>

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


namespace DB
{
namespace
{

DataTypeEnum8::Values getTaskKindEnumValues()
{
    return {
        {"BuildBatch", 0},
        {"RemapLineage", 1},
        {"CompactMerge", 2},
        {"CompactRebuild", 3},
    };
}

DataTypeEnum8::Values getTaskStateEnumValues()
{
    return {
        {"Scheduled", 0},
        {"Running", 1},
        {"Committing", 2},
        {"Finished", 3},
        {"Failed", 4},
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

Map makeStringMap(const std::map<String, String> & values)
{
    Map result;
    result.reserve(values.size());
    for (const auto & [key, value] : values)
        result.push_back(Tuple{key, value});
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

void insertNullableDateTime(MutableColumnPtr & column, std::chrono::system_clock::time_point value)
{
    if (value == std::chrono::system_clock::time_point{})
        column->insertDefault();
    else
        column->insert(std::chrono::system_clock::to_time_t(value));
}

}

ColumnsDescription ReflectionJobLogElement::getColumnsDescription()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

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
}

NamesAndAliases ReflectionJobLogElement::getNamesAndAliases()
{
    return {
        {"settings.Names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "mapKeys(settings)"},
        {"settings.Values", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "mapValues(settings)"},
        {"BuildProfileEvents.Names", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "mapKeys(BuildProfileEvents)"},
        {"BuildProfileEvents.Values", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "mapValues(BuildProfileEvents)"},
    };
}

void ReflectionJobLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(database);
    columns[i++]->insert(reflection_name);
    columns[i++]->insert(family);
    columns[i++]->insert(impl);
    columns[i++]->insert(task_id);
    columns[i++]->insert(kind);
    columns[i++]->insert(state);
    columns[i++]->insert(makeUuidArray(input_source_uuids));
    columns[i++]->insert(makeUuidArray(input_ann_index_part_uuids));
    if (output_ann_index_part_uuid == UUID{})
        columns[i++]->insertDefault();
    else
        columns[i++]->insert(output_ann_index_part_uuid);
    columns[i++]->insert(retry_count);
    insertNullableDateTime(columns[i++], next_retry_time);
    columns[i++]->insert(last_error);
    columns[i++]->insert(quarantined);
    insertNullableDateTime(columns[i++], created_at);
    insertNullableDateTime(columns[i++], started_at);
    insertNullableDateTime(columns[i++], finished_at);
    columns[i++]->insert(duration_seconds);
    columns[i++]->insert(build_stage);
    columns[i++]->insert(build_next_stage);
    columns[i++]->insert(build_progress);
    columns[i++]->insert(build_stage_progress);
    if (build_stage_progress_total)
        columns[i++]->insert(*build_stage_progress_total);
    else
        columns[i++]->insertDefault();
    columns[i++]->insert(build_current_shard);
    columns[i++]->insert(build_num_shards);
    columns[i++]->insert(build_error);
    columns[i++]->insert(makeStringMap(settings));
    columns[i++]->insert(makeUInt64Map(build_profile_events));
}

}
