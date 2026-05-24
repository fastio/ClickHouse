#include <Storages/AuxiliaryIndex/AuxiliaryIndexSelectedEntry.h>
#include <Storages/AuxiliaryIndex/StorageANN.h>

#include <mutex>


namespace DB
{

CurrentlyBuildingAuxiliaryIndexPartTagger::CurrentlyBuildingAuxiliaryIndexPartTagger(
    FutureAuxiliaryIndexPartPtr future_part_,
    StorageANN & storage_)
    : future_part(std::move(future_part_))
    , storage(storage_)
{
    std::lock_guard lock(storage.currently_processing_in_background_mutex);
    storage.currently_building_auxiliary_index_parts.insert(future_part->new_part_name);
}

void CurrentlyBuildingAuxiliaryIndexPartTagger::finalize()
{
    if (finalized)
        return;
    {
        std::lock_guard lock(storage.currently_processing_in_background_mutex);
        storage.currently_building_auxiliary_index_parts.erase(future_part->new_part_name);
        if (future_part->scheduler_reserved)
        {
            storage.scheduler_state.releaseTask(future_part->task_id);
            future_part->scheduler_reserved = false;
        }
    }
    storage.releaseReplicatedLeaderLease(*future_part);
    storage.releaseReplicatedTaskReservation(*future_part);
    storage.releaseTaskResources(*future_part);
    future_part->inner_table_snapshot.reset();
    finalized = true;
}

CurrentlyBuildingAuxiliaryIndexPartTagger::~CurrentlyBuildingAuxiliaryIndexPartTagger()
{
    finalize();
}


AuxiliaryIndexBuildSelectedEntry::AuxiliaryIndexBuildSelectedEntry(
    FutureAuxiliaryIndexPartPtr future_part_,
    CurrentlyBuildingAuxiliaryIndexPartTaggerPtr tagger_)
    : future_part(std::move(future_part_))
    , tagger(std::move(tagger_))
{
}

void AuxiliaryIndexBuildSelectedEntry::finalize()
{
    if (finalized)
        return;
    if (tagger)
        tagger->finalize();
    finalized = true;
}

AuxiliaryIndexBuildSelectedEntry::~AuxiliaryIndexBuildSelectedEntry()
{
    finalize();
}


AuxiliaryIndexRemapSelectedEntry::AuxiliaryIndexRemapSelectedEntry(
    FutureAuxiliaryIndexPartPtr future_part_,
    CurrentlyBuildingAuxiliaryIndexPartTaggerPtr tagger_)
    : future_part(std::move(future_part_))
    , tagger(std::move(tagger_))
{
}

void AuxiliaryIndexRemapSelectedEntry::finalize()
{
    if (finalized)
        return;
    if (tagger)
        tagger->finalize();
    finalized = true;
}

AuxiliaryIndexRemapSelectedEntry::~AuxiliaryIndexRemapSelectedEntry()
{
    finalize();
}

}
