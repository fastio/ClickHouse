#pragma once

#include <Storages/Reflection/ReflectionScheduler.h>


namespace DB
{

class ReflectionANNIndex;
class BackgroundJobsAssignee;

/// ANN-specific implementation of `IReflectionScheduler`. Held as a member of
/// `ReflectionANNIndex` and exposed via `getReflectionScheduler()` so the storage no
/// longer inherits the scheduler interface itself.
///
/// Stage-1 shim: the heavy scheduling logic (~250 lines, deeply coupled to
/// `ReflectionANNIndex`'s coverage map / scheduler state / backlog) still lives on
/// `ReflectionANNIndex::scheduleDataProcessingJob`. This shim only owns the dispatch
/// indirection. A follow-up refactor may move the logic itself in here once
/// the dependent state has a clean ownership boundary.
class ANNIndexScheduler final : public IReflectionScheduler
{
public:
    explicit ANNIndexScheduler(ReflectionANNIndex & storage_);
    ~ANNIndexScheduler() override = default;

    ReflectionSchedulerSnapshot getSchedulerSnapshot() const override;
    bool scheduleReflectionJob(BackgroundJobsAssignee & assignee) override;

private:
    ReflectionANNIndex & storage;
};

}
