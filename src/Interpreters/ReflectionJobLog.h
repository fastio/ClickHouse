#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Core/NamesAndAliases.h>
#include <Interpreters/SystemLog.h>
#include <Storages/ColumnsDescription.h>

#include <chrono>
#include <map>
#include <optional>
#include <vector>


namespace DB
{

struct ReflectionJobLogElement
{
    String database;
    String reflection_name;
    String family;
    String impl;
    String task_id;
    Int8 kind = 0;
    Int8 state = 0;
    std::vector<UUID> input_source_uuids;
    std::vector<UUID> input_ann_index_part_uuids;
    UUID output_ann_index_part_uuid = UUID{};
    UInt64 retry_count = 0;
    std::chrono::system_clock::time_point next_retry_time{};
    String last_error;
    UInt8 quarantined = 0;
    std::chrono::system_clock::time_point created_at{};
    std::chrono::system_clock::time_point started_at{};
    std::chrono::system_clock::time_point finished_at{};
    Float64 duration_seconds = 0.0;
    String build_stage;
    String build_next_stage;
    UInt64 build_progress = 0;
    UInt64 build_stage_progress = 0;
    std::optional<UInt64> build_stage_progress_total;
    UInt64 build_current_shard = 0;
    UInt64 build_num_shards = 0;
    String build_error;
    std::map<String, String> settings;
    std::map<String, UInt64> build_profile_events;

    static std::string name() { return "ReflectionJobLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
};

class ReflectionJobLog : public SystemLog<ReflectionJobLogElement>
{
public:
    using SystemLog<ReflectionJobLogElement>::SystemLog;

    static const char * getDefaultPartitionBy() { return ""; }
    static const char * getDefaultOrderBy() { return "tuple()"; }
};

}
