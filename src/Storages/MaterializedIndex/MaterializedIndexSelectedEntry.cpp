#include <Storages/MaterializedIndex/MaterializedIndexSelectedEntry.h>
#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>

#include <mutex>


namespace DB
{

CurrentlyBuildingMaterializedIndexPartTagger::CurrentlyBuildingMaterializedIndexPartTagger(
    FutureMaterializedIndexPartPtr future_part_,
    StorageMaterializedIndex & storage_)
    : future_part(std::move(future_part_))
    , storage(storage_)
{
    std::lock_guard lock(storage.currently_processing_in_background_mutex);
    storage.currently_building_mi_parts.insert(future_part->new_part_name);
}

void CurrentlyBuildingMaterializedIndexPartTagger::finalize()
{
    if (finalized)
        return;
    {
        std::lock_guard lock(storage.currently_processing_in_background_mutex);
        storage.currently_building_mi_parts.erase(future_part->new_part_name);
    }
    finalized = true;
}

CurrentlyBuildingMaterializedIndexPartTagger::~CurrentlyBuildingMaterializedIndexPartTagger()
{
    finalize();
}


MaterializedIndexBuildSelectedEntry::MaterializedIndexBuildSelectedEntry(
    FutureMaterializedIndexPartPtr future_part_,
    CurrentlyBuildingMaterializedIndexPartTaggerPtr tagger_)
    : future_part(std::move(future_part_))
    , tagger(std::move(tagger_))
{
}

void MaterializedIndexBuildSelectedEntry::finalize()
{
    if (finalized)
        return;
    if (tagger)
        tagger->finalize();
    finalized = true;
}

MaterializedIndexBuildSelectedEntry::~MaterializedIndexBuildSelectedEntry()
{
    finalize();
}


MaterializedIndexRemapSelectedEntry::MaterializedIndexRemapSelectedEntry(
    FutureMaterializedIndexPartPtr future_part_,
    CurrentlyBuildingMaterializedIndexPartTaggerPtr tagger_)
    : future_part(std::move(future_part_))
    , tagger(std::move(tagger_))
{
}

void MaterializedIndexRemapSelectedEntry::finalize()
{
    if (finalized)
        return;
    if (tagger)
        tagger->finalize();
    finalized = true;
}

MaterializedIndexRemapSelectedEntry::~MaterializedIndexRemapSelectedEntry()
{
    finalize();
}

}
