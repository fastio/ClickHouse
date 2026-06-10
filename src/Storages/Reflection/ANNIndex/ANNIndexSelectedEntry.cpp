#include <Storages/Reflection/ANNIndex/ANNIndexSelectedEntry.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>
#include <Interpreters/ReflectionJobCommon.h>

#include <mutex>


namespace DB
{

CurrentlyBuildingANNIndexPartTagger::CurrentlyBuildingANNIndexPartTagger(
    FutureANNIndexPartPtr future_part_,
    ReflectionANNIndex & storage_)
    : future_part(std::move(future_part_))
    , storage(storage_)
{
    bool marked_running = false;
    {
        std::lock_guard lock(storage.currently_processing_in_background_mutex);
        storage.currently_building_ann_index_parts.insert(future_part->new_part_name);
        if (future_part->scheduler_reserved)
        {
            storage.scheduler_state.markTaskRunning(future_part->task_id);
            marked_running = true;
        }
    }
    if (marked_running)
        storage.writeReflectionJobLifecycleLog(*future_part, ReflectionJob::EventType::TASK_STARTED);
}

void CurrentlyBuildingANNIndexPartTagger::finalize()
{
    if (finalized)
        return;
    {
        std::lock_guard lock(storage.currently_processing_in_background_mutex);
        storage.currently_building_ann_index_parts.erase(future_part->new_part_name);
    }
    storage.writeReflectionJobLogAndReleaseSchedulerTask(*future_part);
    storage.releaseReplicatedLeaderLease(*future_part);
    storage.releaseReplicatedTaskReservation(*future_part);
    storage.releaseTaskResources(*future_part);
    future_part->inner_table_snapshot.reset();
    finalized = true;
}

CurrentlyBuildingANNIndexPartTagger::~CurrentlyBuildingANNIndexPartTagger()
{
    finalize();
}


ANNIndexBuildSelectedEntry::ANNIndexBuildSelectedEntry(
    FutureANNIndexPartPtr future_part_,
    CurrentlyBuildingANNIndexPartTaggerPtr tagger_)
    : future_part(std::move(future_part_))
    , tagger(std::move(tagger_))
{
}

void ANNIndexBuildSelectedEntry::finalize()
{
    if (finalized)
        return;
    if (tagger)
        tagger->finalize();
    finalized = true;
}

ANNIndexBuildSelectedEntry::~ANNIndexBuildSelectedEntry()
{
    finalize();
}


ANNIndexRemapSelectedEntry::ANNIndexRemapSelectedEntry(
    FutureANNIndexPartPtr future_part_,
    CurrentlyBuildingANNIndexPartTaggerPtr tagger_)
    : future_part(std::move(future_part_))
    , tagger(std::move(tagger_))
{
}

void ANNIndexRemapSelectedEntry::finalize()
{
    if (finalized)
        return;
    if (tagger)
        tagger->finalize();
    finalized = true;
}

ANNIndexRemapSelectedEntry::~ANNIndexRemapSelectedEntry()
{
    finalize();
}

}
