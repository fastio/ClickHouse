#include <Interpreters/MaterializedIndexLog.h>

#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
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

ColumnsDescription MaterializedIndexLogElement::getColumnsDescription()
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
        {"database", std::make_shared<DataTypeString>(), "Database of the materialized index."},
        {"name", std::make_shared<DataTypeString>(), "Name of the materialized index."},
        {"uuid", std::make_shared<DataTypeUUID>(), "UUID of the materialized index."},
        {"source_database", std::make_shared<DataTypeString>(), "Database of the source table."},
        {"source_table", std::make_shared<DataTypeString>(), "Name of the source table."},
        {"family", std::make_shared<DataTypeString>(), "Algorithm family."},
        {"impl", std::make_shared<DataTypeString>(), "Algorithm implementation."},
        {"state_before", std::make_shared<DataTypeString>(), "Lifecycle state before the event."},
        {"state_after", std::make_shared<DataTypeString>(), "Lifecycle state after the event."},
        {"rows_added", std::make_shared<DataTypeUInt64>(), "Number of index rows added by the event."},
        {"bytes_added", std::make_shared<DataTypeUInt64>(), "Number of bytes added by the event."},
        {"duration_ms", std::make_shared<DataTypeFloat64>(), "Duration of the event in milliseconds."},
        {"error", std::make_shared<DataTypeString>(), "Error message for failed events, empty otherwise."},
    };
}

void MaterializedIndexLogElement::appendToBlock(MutableColumns & columns) const
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
    columns[i++]->insert(rows_added);
    columns[i++]->insert(bytes_added);
    columns[i++]->insert(duration_ms);
    columns[i++]->insert(error);
}

}
