#include <Interpreters/AuxiliaryIndexLog.h>

#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

namespace
{

Array toArray(const std::vector<String> & values)
{
    Array array;
    array.reserve(values.size());
    for (const auto & value : values)
        array.push_back(value);
    return array;
}

}

ColumnsDescription AuxiliaryIndexLogElement::getColumnsDescription()
{
    auto type_enum = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values
        {
            {"BuildStart",    static_cast<int8_t>(Type::BUILD_START)},
            {"BuildFinish",   static_cast<int8_t>(Type::BUILD_FINISH)},
            {"RefreshStart",  static_cast<int8_t>(Type::REFRESH_START)},
            {"RefreshFinish", static_cast<int8_t>(Type::REFRESH_FINISH)},
            {"Cleanup",       static_cast<int8_t>(Type::CLEANUP)},
            {"Error",         static_cast<int8_t>(Type::ERROR)},
        });

    return ColumnsDescription
    {
        {"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server that produced the event."},
        {"event_type", std::move(type_enum), "Kind of background event."},
        {"event_date", std::make_shared<DataTypeDate>(), "Date of the event."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Time of the event."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Time of the event with microsecond precision."},
        {"database", std::make_shared<DataTypeString>(), "Database of the auxiliary index."},
        {"name", std::make_shared<DataTypeString>(), "Name of the auxiliary index."},
        {"uuid", std::make_shared<DataTypeUUID>(), "UUID of the auxiliary index."},
        {"source_database", std::make_shared<DataTypeString>(), "Database of the source table."},
        {"source_table", std::make_shared<DataTypeString>(), "Name of the source table."},
        {"family", std::make_shared<DataTypeString>(), "Algorithm family."},
        {"impl", std::make_shared<DataTypeString>(), "Algorithm implementation."},
        {"state_before", std::make_shared<DataTypeString>(), "Lifecycle state before the event."},
        {"state_after", std::make_shared<DataTypeString>(), "Lifecycle state after the event."},
        {"task_id", std::make_shared<DataTypeString>(), "Identifier of the background task."},
        {"task_kind", std::make_shared<DataTypeString>(), "Kind of background task."},
        {"input_source_parts", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Source part names consumed by the task."},
        {"input_auxiliary_index_parts", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Materialized index part names consumed by the task."},
        {"stage", std::make_shared<DataTypeString>(), "Task stage that produced the event."},
        {"rows_added", std::make_shared<DataTypeUInt64>(), "Number of index rows added by the event."},
        {"bytes_added", std::make_shared<DataTypeUInt64>(), "Number of bytes added by the event."},
        {"duration_ms", std::make_shared<DataTypeFloat64>(), "Duration of the event in milliseconds."},
        {"error_code", std::make_shared<DataTypeInt32>(), "Error code for failed events, zero otherwise."},
        {"error_message", std::make_shared<DataTypeString>(), "Error message for failed events, empty otherwise."},
        {"error", std::make_shared<DataTypeString>(), "Error message for failed events, empty otherwise."},
        {"retry_count", std::make_shared<DataTypeUInt64>(), "Retry count known when the event was written."},
        {"next_retry_time", std::make_shared<DataTypeDateTime>(), "Next retry time, or zero when no retry is scheduled."},
    };
}

void AuxiliaryIndexLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;
    columns[i++]->insert(getFQDNOrHostName());
    columns[i++]->insert(static_cast<Int8>(type));
    columns[i++]->insert(DateLUT::instance().toDayNum(std::chrono::system_clock::to_time_t(event_time)).toUnderType());
    columns[i++]->insert(std::chrono::system_clock::to_time_t(event_time));
    columns[i++]->insert(event_time_usec);
    columns[i++]->insert(database);
    columns[i++]->insert(index_name);
    columns[i++]->insert(uuid);
    columns[i++]->insert(source_database);
    columns[i++]->insert(source_table);
    columns[i++]->insert(family);
    columns[i++]->insert(impl);
    columns[i++]->insert(state_before);
    columns[i++]->insert(state_after);
    columns[i++]->insert(task_id);
    columns[i++]->insert(task_kind);
    columns[i++]->insert(toArray(input_source_parts));
    columns[i++]->insert(toArray(input_auxiliary_index_parts));
    columns[i++]->insert(stage);
    columns[i++]->insert(rows_added);
    columns[i++]->insert(bytes_added);
    columns[i++]->insert(duration_ms);
    columns[i++]->insert(error_code);
    columns[i++]->insert(error_message);
    columns[i++]->insert(error);
    columns[i++]->insert(retry_count);
    columns[i++]->insert(next_retry_time);
}

}
