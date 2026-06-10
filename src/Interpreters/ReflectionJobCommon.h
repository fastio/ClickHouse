#pragma once

#include <Columns/IColumn.h>
#include <Core/DecimalFunctions.h>
#include <Core/Field.h>
#include <Core/NamesAndAliases.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/ColumnsDescription.h>
#include <Common/DateLUTImpl.h>

#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <vector>

namespace DB
{
namespace ReflectionJob
{

enum RowType : int8_t
{
    TASK = 0,
    RESOURCE_BACKOFF = 1,
    REPEATED_FAILURE = 2,
    QUARANTINED_ROW = 3,
};

enum LiveTaskState : int8_t
{
    SCHEDULED = 0,
    RUNNING = 1,
    COMMITTING = 2,
    BLOCKED = 3,
    QUARANTINED_STATE = 4,
};

enum LogTaskState : int8_t
{
    LOG_SCHEDULED = 0,
    LOG_RUNNING = 1,
    LOG_COMMITTING = 2,
    LOG_FINISHED = 3,
    LOG_FAILED = 4,
    LOG_BLOCKED = 5,
    LOG_QUARANTINED = 6,
};

enum EventType : int8_t
{
    TASK_SCHEDULED = 0,
    TASK_STARTED = 1,
    TASK_COMMITTING = 2,
    TASK_FINISHED = 3,
    TASK_FAILED = 4,
    RETRY_SCHEDULED = 5,
    QUARANTINED_EVENT = 6,
    UNQUARANTINED = 7,
};

struct PartSnapshot
{
    UUID uuid;
    String name;
    UInt64 rows = 0;
    UInt64 bytes = 0;
};

struct CommonFields
{
    String database;
    String reflection_name;
    String source_database;
    String source_table;
    String family;
    String impl;

    Int8 task_kind = 0;
    String task_id;
    String failure_key;

    std::chrono::system_clock::time_point created_at{};
    std::chrono::system_clock::time_point started_at{};
    std::chrono::system_clock::time_point finished_at{};
    std::chrono::system_clock::time_point next_retry_time{};

    std::vector<PartSnapshot> input_source_parts;
    std::vector<UUID> input_reflection_part_uuids;
    UUID output_reflection_part_uuid = UUID{};

    UInt32 retry_count = 0;
    UInt8 is_quarantined = 0;
    String exception;
    String algorithm_exception;

    String stage;
    String next_stage;
    UInt64 progress = 0;
    UInt64 progress_total = 0;
    String progress_unit = "items";
    UInt64 stage_progress = 0;
    UInt64 stage_progress_total = 0;
    String stage_progress_unit = "items";
    UInt32 current_shard = 0;
    UInt32 total_shards = 0;
    UInt64 rows_processed = 0;
    UInt64 rows_total = 0;
    UInt64 bytes_processed = 0;
    UInt64 bytes_total = 0;

    std::map<String, String> settings;
    std::map<String, UInt64> profile_events;
};

struct LiveRow
{
    std::chrono::system_clock::time_point snapshot_time{};
    Int8 row_type = RowType::TASK;
    Int8 task_state = LiveTaskState::SCHEDULED;
    CommonFields common;
};

struct LogRow
{
    std::chrono::system_clock::time_point event_time{};
    Int8 event_type = EventType::TASK_SCHEDULED;
    Int8 task_state = LogTaskState::LOG_SCHEDULED;
    CommonFields common;
};

inline DataTypePtr lowCardinalityString()
{
    return std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
}

inline DataTypePtr arrayOf(DataTypePtr nested)
{
    return std::make_shared<DataTypeArray>(std::move(nested));
}

inline DataTypeEnum8::Values getTaskKindEnumValues()
{
    return {
        {"BuildBatch", 0},
        {"RemapLineage", 1},
        {"CompactMerge", 2},
        {"CompactRebuild", 3},
    };
}

inline DataTypeEnum8::Values getLiveTaskStateEnumValues()
{
    return {
        {"Scheduled", LiveTaskState::SCHEDULED},
        {"Running", LiveTaskState::RUNNING},
        {"Committing", LiveTaskState::COMMITTING},
        {"Blocked", LiveTaskState::BLOCKED},
        {"Quarantined", LiveTaskState::QUARANTINED_STATE},
    };
}

inline DataTypeEnum8::Values getLogTaskStateEnumValues()
{
    return {
        {"Scheduled", LogTaskState::LOG_SCHEDULED},
        {"Running", LogTaskState::LOG_RUNNING},
        {"Committing", LogTaskState::LOG_COMMITTING},
        {"Finished", LogTaskState::LOG_FINISHED},
        {"Failed", LogTaskState::LOG_FAILED},
        {"Blocked", LogTaskState::LOG_BLOCKED},
        {"Quarantined", LogTaskState::LOG_QUARANTINED},
    };
}

inline DataTypeEnum8::Values getRowTypeEnumValues()
{
    return {
        {"Task", RowType::TASK},
        {"ResourceBackoff", RowType::RESOURCE_BACKOFF},
        {"RepeatedFailure", RowType::REPEATED_FAILURE},
        {"Quarantined", RowType::QUARANTINED_ROW},
    };
}

inline DataTypeEnum8::Values getEventTypeEnumValues()
{
    return {
        {"TaskScheduled", EventType::TASK_SCHEDULED},
        {"TaskStarted", EventType::TASK_STARTED},
        {"TaskCommitting", EventType::TASK_COMMITTING},
        {"TaskFinished", EventType::TASK_FINISHED},
        {"TaskFailed", EventType::TASK_FAILED},
        {"RetryScheduled", EventType::RETRY_SCHEDULED},
        {"Quarantined", EventType::QUARANTINED_EVENT},
        {"Unquarantined", EventType::UNQUARANTINED},
    };
}

inline DataTypePtr taskKindType()
{
    return std::make_shared<DataTypeEnum8>(getTaskKindEnumValues());
}

inline DataTypePtr liveTaskStateType()
{
    return std::make_shared<DataTypeEnum8>(getLiveTaskStateEnumValues());
}

inline DataTypePtr logTaskStateType()
{
    return std::make_shared<DataTypeEnum8>(getLogTaskStateEnumValues());
}

inline DataTypePtr rowTypeType()
{
    return std::make_shared<DataTypeEnum8>(getRowTypeEnumValues());
}

inline DataTypePtr eventTypeType()
{
    return std::make_shared<DataTypeEnum8>(getEventTypeEnumValues());
}

inline DataTypePtr dateTime64Type()
{
    return std::make_shared<DataTypeDateTime64>(6);
}

inline DataTypePtr settingsType()
{
    return std::make_shared<DataTypeMap>(lowCardinalityString(), std::make_shared<DataTypeString>());
}

inline DataTypePtr profileEventsType()
{
    return std::make_shared<DataTypeMap>(lowCardinalityString(), std::make_shared<DataTypeUInt64>());
}

inline bool hasTime(std::chrono::system_clock::time_point value)
{
    return value != std::chrono::system_clock::time_point{}
        && value != std::chrono::system_clock::time_point::max();
}

inline DateTime64 toDateTime64Micros(std::chrono::system_clock::time_point value)
{
    if (!hasTime(value))
        return 0;
    return std::chrono::duration_cast<std::chrono::microseconds>(value.time_since_epoch()).count();
}

inline UInt64 elapsedMilliseconds(
    std::chrono::system_clock::time_point start,
    std::chrono::system_clock::time_point finish,
    std::chrono::system_clock::time_point now)
{
    if (!hasTime(start))
        return 0;
    const auto end = hasTime(finish) ? finish : now;
    if (end <= start)
        return 0;
    return static_cast<UInt64>(std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
}

inline UInt16 eventDate(std::chrono::system_clock::time_point value)
{
    const auto seconds = std::chrono::system_clock::to_time_t(hasTime(value) ? value : std::chrono::system_clock::now());
    return static_cast<UInt16>(DateLUT::instance().toDayNum(seconds).toUnderType());
}

inline Array makeUuidArray(const std::vector<UUID> & values)
{
    Array result;
    result.reserve(values.size());
    for (const auto & value : values)
        result.push_back(value);
    return result;
}

inline Array makeSourceUuidArray(const std::vector<PartSnapshot> & values)
{
    Array result;
    result.reserve(values.size());
    for (const auto & value : values)
        result.push_back(value.uuid);
    return result;
}

inline Array makeSourceNameArray(const std::vector<PartSnapshot> & values)
{
    Array result;
    result.reserve(values.size());
    for (const auto & value : values)
        result.push_back(value.name);
    return result;
}

inline Array makeSourceRowsArray(const std::vector<PartSnapshot> & values)
{
    Array result;
    result.reserve(values.size());
    for (const auto & value : values)
        result.push_back(value.rows);
    return result;
}

inline Array makeSourceBytesArray(const std::vector<PartSnapshot> & values)
{
    Array result;
    result.reserve(values.size());
    for (const auto & value : values)
        result.push_back(value.bytes);
    return result;
}

inline Map makeStringMap(const std::map<String, String> & values)
{
    Map result;
    result.reserve(values.size());
    for (const auto & [key, value] : values)
        result.push_back(Tuple{key, value});
    return result;
}

inline Map makeUInt64Map(const std::map<String, UInt64> & values)
{
    Map result;
    result.reserve(values.size());
    for (const auto & [key, value] : values)
        result.push_back(Tuple{key, value});
    return result;
}

inline void insertDateTime64(MutableColumnPtr & column, std::chrono::system_clock::time_point value)
{
    column->insert(DecimalField<DateTime64>(toDateTime64Micros(value), 6));
}

inline void appendIdentityFields(MutableColumns & columns, size_t & i, const CommonFields & row)
{
    columns[i++]->insert(row.database);
    columns[i++]->insert(row.reflection_name);
    columns[i++]->insert(row.source_database);
    columns[i++]->insert(row.source_table);
    columns[i++]->insert(row.family);
    columns[i++]->insert(row.impl);
}

inline void appendTaskKeyFields(MutableColumns & columns, size_t & i, const CommonFields & row)
{
    columns[i++]->insert(row.task_id);
    columns[i++]->insert(row.failure_key);
    columns[i++]->insert(row.task_kind);
}

inline void appendStartTimingFields(MutableColumns & columns, size_t & i, const CommonFields & row, std::chrono::system_clock::time_point now)
{
    insertDateTime64(columns[i++], row.created_at);
    columns[i++]->insert(static_cast<UInt8>(hasTime(row.created_at)));
    insertDateTime64(columns[i++], row.started_at);
    columns[i++]->insert(static_cast<UInt8>(hasTime(row.started_at)));
    columns[i++]->insert(elapsedMilliseconds(row.started_at, row.finished_at, now));
    insertDateTime64(columns[i++], row.next_retry_time);
    columns[i++]->insert(static_cast<UInt8>(hasTime(row.next_retry_time)));
}

inline void appendReservationFields(MutableColumns & columns, size_t & i, const CommonFields & row)
{
    columns[i++]->insert(makeSourceUuidArray(row.input_source_parts));
    columns[i++]->insert(makeSourceNameArray(row.input_source_parts));
    columns[i++]->insert(makeSourceRowsArray(row.input_source_parts));
    columns[i++]->insert(makeSourceBytesArray(row.input_source_parts));
    columns[i++]->insert(makeUuidArray(row.input_reflection_part_uuids));
    columns[i++]->insert(row.output_reflection_part_uuid);
    columns[i++]->insert(static_cast<UInt8>(row.output_reflection_part_uuid != UUID{}));
}

inline void appendFailureFields(MutableColumns & columns, size_t & i, const CommonFields & row)
{
    columns[i++]->insert(row.retry_count);
    columns[i++]->insert(row.is_quarantined);
    columns[i++]->insert(row.exception);
    columns[i++]->insert(row.algorithm_exception);
}

inline void appendProgressFields(MutableColumns & columns, size_t & i, const CommonFields & row)
{
    columns[i++]->insert(row.stage);
    columns[i++]->insert(row.next_stage);
    columns[i++]->insert(row.progress);
    columns[i++]->insert(row.progress_total);
    columns[i++]->insert(row.progress_unit);
    columns[i++]->insert(row.stage_progress);
    columns[i++]->insert(row.stage_progress_total);
    columns[i++]->insert(row.stage_progress_unit);
    columns[i++]->insert(row.current_shard);
    columns[i++]->insert(row.total_shards);
    columns[i++]->insert(row.rows_processed);
    columns[i++]->insert(row.rows_total);
    columns[i++]->insert(row.bytes_processed);
    columns[i++]->insert(row.bytes_total);
    columns[i++]->insert(makeStringMap(row.settings));
    columns[i++]->insert(makeUInt64Map(row.profile_events));
}

inline ColumnsDescription getLiveColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"snapshot_time", dateTime64Type(), "Time when the live reflection scheduler snapshot was read."},
        {"database", std::make_shared<DataTypeString>(), "Database of the reflection."},
        {"reflection_name", std::make_shared<DataTypeString>(), "Name of the reflection."},
        {"source_database", std::make_shared<DataTypeString>(), "Database of the source table."},
        {"source_table", std::make_shared<DataTypeString>(), "Name of the source table."},
        {"family", lowCardinalityString(), "Reflection engine family."},
        {"impl", lowCardinalityString(), "Reflection engine implementation."},
        {"row_type", rowTypeType(), "Kind of live observability row."},
        {"task_id", std::make_shared<DataTypeString>(), "Scheduler task id. Empty for scheduler-level blocker rows."},
        {"failure_key", std::make_shared<DataTypeString>(), "Repeated-failure key for blocker rows."},
        {"task_kind", taskKindType(), "Scheduler task kind."},
        {"task_state", liveTaskStateType(), "Live scheduler task or blocker state."},
        {"created_at", dateTime64Type(), "Task creation time, or zero if not applicable."},
        {"has_created_at", std::make_shared<DataTypeUInt8>(), "Whether created_at is present."},
        {"started_at", dateTime64Type(), "Task start time, or zero if not applicable."},
        {"has_started_at", std::make_shared<DataTypeUInt8>(), "Whether started_at is present."},
        {"elapsed_ms", std::make_shared<DataTypeUInt64>(), "Elapsed milliseconds since task start."},
        {"next_retry_time", dateTime64Type(), "Next retry time, or zero if no retry is scheduled."},
        {"has_next_retry_time", std::make_shared<DataTypeUInt8>(), "Whether next_retry_time is present."},
        {"input_source_part_uuids", arrayOf(std::make_shared<DataTypeUUID>()), "Source part UUIDs reserved by the task."},
        {"input_source_part_names", arrayOf(std::make_shared<DataTypeString>()), "Source part names reserved by the task."},
        {"input_source_part_rows", arrayOf(std::make_shared<DataTypeUInt64>()), "Rows in reserved source parts."},
        {"input_source_part_bytes", arrayOf(std::make_shared<DataTypeUInt64>()), "Bytes in reserved source parts."},
        {"input_reflection_part_uuids", arrayOf(std::make_shared<DataTypeUUID>()), "Reflection part UUIDs reserved by the task."},
        {"output_reflection_part_uuid", std::make_shared<DataTypeUUID>(), "Output reflection part UUID, or zero UUID if not assigned."},
        {"has_output_reflection_part", std::make_shared<DataTypeUInt8>(), "Whether output_reflection_part_uuid is assigned."},
        {"retry_count", std::make_shared<DataTypeUInt32>(), "Retry count for the active task or blocker."},
        {"is_quarantined", std::make_shared<DataTypeUInt8>(), "Whether the repeated-failure key is quarantined."},
        {"last_exception", std::make_shared<DataTypeString>(), "Last scheduler exception."},
        {"algorithm_exception", std::make_shared<DataTypeString>(), "Algorithm-specific exception, if available."},
        {"stage", lowCardinalityString(), "Current algorithm stage."},
        {"next_stage", lowCardinalityString(), "Next algorithm stage."},
        {"progress", std::make_shared<DataTypeUInt64>(), "Algorithm-specific progress value."},
        {"progress_total", std::make_shared<DataTypeUInt64>(), "Algorithm-specific progress total."},
        {"progress_unit", lowCardinalityString(), "Unit for progress/progress_total."},
        {"stage_progress", std::make_shared<DataTypeUInt64>(), "Progress within current stage."},
        {"stage_progress_total", std::make_shared<DataTypeUInt64>(), "Total units for current stage."},
        {"stage_progress_unit", lowCardinalityString(), "Unit for stage progress."},
        {"current_shard", std::make_shared<DataTypeUInt32>(), "Current build shard."},
        {"total_shards", std::make_shared<DataTypeUInt32>(), "Total build shards."},
        {"rows_processed", std::make_shared<DataTypeUInt64>(), "Rows processed by the algorithm."},
        {"rows_total", std::make_shared<DataTypeUInt64>(), "Total rows to process, if known."},
        {"bytes_processed", std::make_shared<DataTypeUInt64>(), "Bytes processed by the algorithm."},
        {"bytes_total", std::make_shared<DataTypeUInt64>(), "Total bytes to process, if known."},
        {"settings", settingsType(), "Algorithm settings for the task."},
        {"profile_events", profileEventsType(), "Algorithm profile events for the task."},
    };

    description.setAliases({
        {"kind", {taskKindType()}, "task_kind"},
        {"state", {liveTaskStateType()}, "task_state"},
        {"input_source_uuids", {arrayOf(std::make_shared<DataTypeUUID>())}, "input_source_part_uuids"},
        {"input_ann_index_part_uuids", {arrayOf(std::make_shared<DataTypeUUID>())}, "input_reflection_part_uuids"},
        {"output_ann_index_part_uuid", {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUUID>())}, "if(has_output_reflection_part, output_reflection_part_uuid, NULL)"},
        {"quarantined", {std::make_shared<DataTypeUInt8>()}, "is_quarantined"},
        {"last_error", {std::make_shared<DataTypeString>()}, "last_exception"},
        {"build_stage", {lowCardinalityString()}, "stage"},
        {"build_next_stage", {lowCardinalityString()}, "next_stage"},
        {"build_progress", {std::make_shared<DataTypeUInt64>()}, "progress"},
        {"build_stage_progress", {std::make_shared<DataTypeUInt64>()}, "stage_progress"},
        {"build_stage_progress_total", {std::make_shared<DataTypeUInt64>()}, "stage_progress_total"},
        {"build_current_shard", {std::make_shared<DataTypeUInt32>()}, "current_shard"},
        {"build_num_shards", {std::make_shared<DataTypeUInt32>()}, "total_shards"},
        {"build_error", {std::make_shared<DataTypeString>()}, "algorithm_exception"},
        {"BuildProfileEvents", {profileEventsType()}, "profile_events"},
        {"BuildProfileEvents.Names", {arrayOf(std::make_shared<DataTypeString>())}, "mapKeys(profile_events)"},
        {"BuildProfileEvents.Values", {arrayOf(std::make_shared<DataTypeUInt64>())}, "mapValues(profile_events)"},
        {"profile_events.Names", {arrayOf(std::make_shared<DataTypeString>())}, "mapKeys(profile_events)"},
        {"profile_events.Values", {arrayOf(std::make_shared<DataTypeUInt64>())}, "mapValues(profile_events)"},
        {"settings.Names", {arrayOf(std::make_shared<DataTypeString>())}, "mapKeys(settings)"},
        {"settings.Values", {arrayOf(std::make_shared<DataTypeString>())}, "mapValues(settings)"},
    });

    return description;
}

inline ColumnsDescription getLogColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"event_date", std::make_shared<DataTypeDate>(), "Date of the reflection scheduler event."},
        {"event_time", dateTime64Type(), "Time of the reflection scheduler event."},
        {"event_type", eventTypeType(), "Reflection scheduler event type."},
        {"database", std::make_shared<DataTypeString>(), "Database of the reflection."},
        {"reflection_name", std::make_shared<DataTypeString>(), "Name of the reflection."},
        {"source_database", std::make_shared<DataTypeString>(), "Database of the source table."},
        {"source_table", std::make_shared<DataTypeString>(), "Name of the source table."},
        {"family", lowCardinalityString(), "Reflection engine family."},
        {"impl", lowCardinalityString(), "Reflection engine implementation."},
        {"task_id", std::make_shared<DataTypeString>(), "Scheduler task id. Empty for scheduler-level events."},
        {"failure_key", std::make_shared<DataTypeString>(), "Repeated-failure key for retry/quarantine events."},
        {"task_kind", taskKindType(), "Scheduler task kind."},
        {"task_state", logTaskStateType(), "Scheduler task state captured by the event."},
        {"created_at", dateTime64Type(), "Task creation time, or zero if not applicable."},
        {"has_created_at", std::make_shared<DataTypeUInt8>(), "Whether created_at is present."},
        {"started_at", dateTime64Type(), "Task start time, or zero if not applicable."},
        {"has_started_at", std::make_shared<DataTypeUInt8>(), "Whether started_at is present."},
        {"finished_at", dateTime64Type(), "Task finish time, or zero if not applicable."},
        {"has_finished_at", std::make_shared<DataTypeUInt8>(), "Whether finished_at is present."},
        {"duration_ms", std::make_shared<DataTypeUInt64>(), "Task duration in milliseconds."},
        {"next_retry_time", dateTime64Type(), "Next retry time, or zero if no retry is scheduled."},
        {"has_next_retry_time", std::make_shared<DataTypeUInt8>(), "Whether next_retry_time is present."},
        {"input_source_part_uuids", arrayOf(std::make_shared<DataTypeUUID>()), "Source part UUIDs reserved by the task."},
        {"input_source_part_names", arrayOf(std::make_shared<DataTypeString>()), "Source part names reserved by the task."},
        {"input_source_part_rows", arrayOf(std::make_shared<DataTypeUInt64>()), "Rows in reserved source parts."},
        {"input_source_part_bytes", arrayOf(std::make_shared<DataTypeUInt64>()), "Bytes in reserved source parts."},
        {"input_reflection_part_uuids", arrayOf(std::make_shared<DataTypeUUID>()), "Reflection part UUIDs reserved by the task."},
        {"output_reflection_part_uuid", std::make_shared<DataTypeUUID>(), "Output reflection part UUID, or zero UUID if not assigned."},
        {"has_output_reflection_part", std::make_shared<DataTypeUInt8>(), "Whether output_reflection_part_uuid is assigned."},
        {"retry_count", std::make_shared<DataTypeUInt32>(), "Retry count for the task or failure key."},
        {"is_quarantined", std::make_shared<DataTypeUInt8>(), "Whether the repeated-failure key is quarantined."},
        {"exception", std::make_shared<DataTypeString>(), "Scheduler exception captured by the event."},
        {"algorithm_exception", std::make_shared<DataTypeString>(), "Algorithm-specific exception, if available."},
        {"stage", lowCardinalityString(), "Algorithm stage captured by the event."},
        {"next_stage", lowCardinalityString(), "Next algorithm stage captured by the event."},
        {"progress", std::make_shared<DataTypeUInt64>(), "Algorithm-specific progress value."},
        {"progress_total", std::make_shared<DataTypeUInt64>(), "Algorithm-specific progress total."},
        {"progress_unit", lowCardinalityString(), "Unit for progress/progress_total."},
        {"stage_progress", std::make_shared<DataTypeUInt64>(), "Progress within current stage."},
        {"stage_progress_total", std::make_shared<DataTypeUInt64>(), "Total units for current stage."},
        {"stage_progress_unit", lowCardinalityString(), "Unit for stage progress."},
        {"current_shard", std::make_shared<DataTypeUInt32>(), "Current build shard."},
        {"total_shards", std::make_shared<DataTypeUInt32>(), "Total build shards."},
        {"rows_processed", std::make_shared<DataTypeUInt64>(), "Rows processed by the algorithm."},
        {"rows_total", std::make_shared<DataTypeUInt64>(), "Total rows to process, if known."},
        {"bytes_processed", std::make_shared<DataTypeUInt64>(), "Bytes processed by the algorithm."},
        {"bytes_total", std::make_shared<DataTypeUInt64>(), "Total bytes to process, if known."},
        {"settings", settingsType(), "Algorithm settings for the task."},
        {"profile_events", profileEventsType(), "Algorithm profile events for the task."},
    };
    return description;
}

inline NamesAndAliases getCommonAliasesForLog()
{
    return {
        {"kind", taskKindType(), "task_kind"},
        {"state", logTaskStateType(), "task_state"},
        {"input_source_uuids", arrayOf(std::make_shared<DataTypeUUID>()), "input_source_part_uuids"},
        {"input_ann_index_part_uuids", arrayOf(std::make_shared<DataTypeUUID>()), "input_reflection_part_uuids"},
        {"output_ann_index_part_uuid", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUUID>()), "if(has_output_reflection_part, output_reflection_part_uuid, NULL)"},
        {"last_error", std::make_shared<DataTypeString>(), "exception"},
        {"quarantined", std::make_shared<DataTypeUInt8>(), "is_quarantined"},
        {"duration_seconds", std::make_shared<DataTypeFloat64>(), "duration_ms / 1000."},
        {"build_stage", lowCardinalityString(), "stage"},
        {"build_next_stage", lowCardinalityString(), "next_stage"},
        {"build_progress", std::make_shared<DataTypeUInt64>(), "progress"},
        {"build_stage_progress", std::make_shared<DataTypeUInt64>(), "stage_progress"},
        {"build_stage_progress_total", std::make_shared<DataTypeUInt64>(), "stage_progress_total"},
        {"build_current_shard", std::make_shared<DataTypeUInt32>(), "current_shard"},
        {"build_num_shards", std::make_shared<DataTypeUInt32>(), "total_shards"},
        {"build_error", std::make_shared<DataTypeString>(), "algorithm_exception"},
        {"BuildProfileEvents", profileEventsType(), "profile_events"},
        {"BuildProfileEvents.Names", arrayOf(std::make_shared<DataTypeString>()), "mapKeys(profile_events)"},
        {"BuildProfileEvents.Values", arrayOf(std::make_shared<DataTypeUInt64>()), "mapValues(profile_events)"},
        {"profile_events.Names", arrayOf(std::make_shared<DataTypeString>()), "mapKeys(profile_events)"},
        {"profile_events.Values", arrayOf(std::make_shared<DataTypeUInt64>()), "mapValues(profile_events)"},
        {"settings.Names", arrayOf(std::make_shared<DataTypeString>()), "mapKeys(settings)"},
        {"settings.Values", arrayOf(std::make_shared<DataTypeString>()), "mapValues(settings)"},
    };
}

inline void appendLiveRow(MutableColumns & columns, const LiveRow & row)
{
    size_t i = 0;
    const auto now = row.snapshot_time == std::chrono::system_clock::time_point{} ? std::chrono::system_clock::now() : row.snapshot_time;
    insertDateTime64(columns[i++], now);
    appendIdentityFields(columns, i, row.common);
    columns[i++]->insert(row.row_type);
    appendTaskKeyFields(columns, i, row.common);
    columns[i++]->insert(row.task_state);
    appendStartTimingFields(columns, i, row.common, now);
    appendReservationFields(columns, i, row.common);
    appendFailureFields(columns, i, row.common);
    appendProgressFields(columns, i, row.common);
}

inline void appendLogRow(MutableColumns & columns, const LogRow & row)
{
    size_t i = 0;
    const auto event_time = hasTime(row.event_time) ? row.event_time : std::chrono::system_clock::now();
    columns[i++]->insert(eventDate(event_time));
    insertDateTime64(columns[i++], event_time);
    columns[i++]->insert(row.event_type);
    appendIdentityFields(columns, i, row.common);
    appendTaskKeyFields(columns, i, row.common);
    columns[i++]->insert(row.task_state);
    insertDateTime64(columns[i++], row.common.created_at);
    columns[i++]->insert(static_cast<UInt8>(hasTime(row.common.created_at)));
    insertDateTime64(columns[i++], row.common.started_at);
    columns[i++]->insert(static_cast<UInt8>(hasTime(row.common.started_at)));
    insertDateTime64(columns[i++], row.common.finished_at);
    columns[i++]->insert(static_cast<UInt8>(hasTime(row.common.finished_at)));
    columns[i++]->insert(elapsedMilliseconds(row.common.started_at, row.common.finished_at, event_time));
    insertDateTime64(columns[i++], row.common.next_retry_time);
    columns[i++]->insert(static_cast<UInt8>(hasTime(row.common.next_retry_time)));
    appendReservationFields(columns, i, row.common);
    appendFailureFields(columns, i, row.common);
    appendProgressFields(columns, i, row.common);
}

}
}
