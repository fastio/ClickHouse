#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Core/NamesAndAliases.h>
#include <Interpreters/ReflectionJobCommon.h>
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
    ReflectionJob::LogRow row;

    static std::string name() { return "ReflectionJobLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
};

class ReflectionJobLog : public SystemLog<ReflectionJobLogElement>
{
public:
    using SystemLog<ReflectionJobLogElement>::SystemLog;

    static const char * getDefaultPartitionBy() { return "toYYYYMM(event_date)"; }
    static const char * getDefaultOrderBy() { return "database, reflection_name, event_date, event_type, event_time, task_id"; }
};

}
