#include <Storages/Reflection/ANNIndex/ANNIndexScheduler.h>

#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>


namespace DB
{

ANNIndexScheduler::ANNIndexScheduler(ReflectionANNIndex & storage_)
    : storage(storage_)
{
}

ReflectionSchedulerSnapshot ANNIndexScheduler::getSchedulerSnapshot() const
{
    return storage.getSchedulerSnapshot();
}

bool ANNIndexScheduler::scheduleReflectionJob(BackgroundJobsAssignee & assignee)
{
    return storage.scheduleDataProcessingJob(assignee);
}

}
