#pragma once

#include <Storages/MergeTree/IMergeTreeCleanupThread.h>
#include <Common/Stopwatch.h>

namespace DB
{

class MergeTreeCleanupThread : public IMergeTreeCleanupThread
{
public:
    explicit MergeTreeCleanupThread(MergeTreeData & data_);

    /// Shadows IMergeTreeCleanupThread::start() to restart cleanup timers
    /// before activating the background task. This ensures the thread waits
    /// a full interval after the manual cleanup done in startup().
    void start();

private:
    AtomicStopwatch time_after_previous_cleanup_parts;
    AtomicStopwatch time_after_previous_cleanup_temporary_directories;

    /// Returns a number that is directly proportional to the number of cleaned up objects
    Float32 iterate() override;
};

}
