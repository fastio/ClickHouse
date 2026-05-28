#pragma once

#include <chrono>
#include <vector>

#include <Core/NamesAndAliases.h>
#include <Core/Types.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/SystemLog.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

/// One entry per background build / refresh / cleanup event on a materialized
/// index. In this release nothing writes to it; the schema is defined up front
/// so later releases that introduce the background pipeline can start appending
/// without a breaking migration.
struct ANNIndexLogElement
{
    enum class Type : int8_t
    {
        BUILD_START = 1,
        BUILD_FINISH = 2,
        REFRESH_START = 3,
        REFRESH_FINISH = 4,
        CLEANUP = 5,
        ERROR = 6,
    };

    Type type = Type::BUILD_START;
    std::chrono::system_clock::time_point event_time;
    Decimal64 event_time_usec{};

    String database;
    String index_name;
    UUID uuid = UUIDHelpers::Nil;

    String source_database;
    String source_table;
    String family;
    String impl;

    String state_before;
    String state_after;

    String task_id;
    String task_kind;
    std::vector<String> input_source_parts;
    std::vector<String> input_ann_index_parts;
    String stage;

    UInt64 rows_added = 0;
    UInt64 bytes_added = 0;
    Float64 duration_ms = 0.0;

    Int32 error_code = 0;
    String error_message;
    String error;
    UInt64 retry_count = 0;
    time_t next_retry_time = 0;

    static std::string name() { return "ANNIndexLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class ANNIndexLog : public SystemLog<ANNIndexLogElement>
{
    using SystemLog<ANNIndexLogElement>::SystemLog;
};

}
