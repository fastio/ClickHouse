#pragma once

#include <Core/Types.h>
#include <Interpreters/StorageID.h>

namespace DB
{

class BackgroundJobsAssignee;

struct ReflectionSchedulerSnapshot
{
    StorageID source_table_id = StorageID::createEmpty();
    UInt64 source_part_count = 0;
    UInt64 reflection_part_count = 0;
};

/// Narrow framework hook for background processing. Concrete engines decide
/// task kinds and diff policy; the framework only needs a common entry point
/// to describe and trigger work for a source-derived object.
class IReflectionScheduler
{
public:
    virtual ~IReflectionScheduler() = default;

    virtual ReflectionSchedulerSnapshot getSchedulerSnapshot() const = 0;
    virtual bool scheduleReflectionJob(BackgroundJobsAssignee & assignee) = 0;
};

}
