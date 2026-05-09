#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <memory>
#include <vector>


namespace DB
{

class StorageMaterializedIndex;

/// Future part descriptor passed to the top-level Build / Remap tasks.
/// Build path uses `source_parts_snapshot`; Remap path uses
/// `affected_mi_parts` plus `delta_in_source_parts` and
/// `delta_out_source_uuids`. The same struct serves both kinds so that the
/// Tagger and SelectedEntry types can be shared.
struct FutureMaterializedIndexPart
{
    enum class Kind
    {
        Build,
        Remap,
    };

    String new_part_name;
    UUID new_part_uuid;
    MergeTreeData::DataPartsVector source_parts_snapshot;
    MergeTreeData::DataPartsVector affected_mi_parts;
    MergeTreeData::DataPartsVector delta_in_source_parts;
    std::vector<UUID> delta_out_source_uuids;
    Kind kind = Kind::Build;
};

using FutureMaterializedIndexPartPtr = std::shared_ptr<FutureMaterializedIndexPart>;


/// Holds the storage-level reservation for a future MaterializedIndex part.
/// The constructor inserts the future part name into
/// `storage.currently_building_mi_parts` under
/// `storage.currently_processing_in_background_mutex`; `finalize` (idempotent)
/// erases it. The destructor calls `finalize` as a safety net so the set is
/// always cleaned up even if the owner forgets.
struct CurrentlyBuildingMaterializedIndexPartTagger
{
    FutureMaterializedIndexPartPtr future_part;
    StorageMaterializedIndex & storage;
    bool finalized = false;

    CurrentlyBuildingMaterializedIndexPartTagger(
        FutureMaterializedIndexPartPtr future_part_,
        StorageMaterializedIndex & storage_);

    void finalize();
    ~CurrentlyBuildingMaterializedIndexPartTagger();
};

using CurrentlyBuildingMaterializedIndexPartTaggerPtr
    = std::unique_ptr<CurrentlyBuildingMaterializedIndexPartTagger>;


/// Picked by the cycle for a Build round. Owns the tagger lifetime so the
/// reservation is released when the entry goes away.
struct MaterializedIndexBuildSelectedEntry
{
    FutureMaterializedIndexPartPtr future_part;
    CurrentlyBuildingMaterializedIndexPartTaggerPtr tagger;
    bool finalized = false;

    MaterializedIndexBuildSelectedEntry(
        FutureMaterializedIndexPartPtr future_part_,
        CurrentlyBuildingMaterializedIndexPartTaggerPtr tagger_);

    void finalize();
    ~MaterializedIndexBuildSelectedEntry();
};

using MaterializedIndexBuildSelectedEntryPtr
    = std::shared_ptr<MaterializedIndexBuildSelectedEntry>;


/// Picked by the cycle for a Remap round. Same shape as the Build entry; the
/// kind information is carried inside `future_part->kind`.
struct MaterializedIndexRemapSelectedEntry
{
    FutureMaterializedIndexPartPtr future_part;
    CurrentlyBuildingMaterializedIndexPartTaggerPtr tagger;
    bool finalized = false;

    MaterializedIndexRemapSelectedEntry(
        FutureMaterializedIndexPartPtr future_part_,
        CurrentlyBuildingMaterializedIndexPartTaggerPtr tagger_);

    void finalize();
    ~MaterializedIndexRemapSelectedEntry();
};

using MaterializedIndexRemapSelectedEntryPtr
    = std::shared_ptr<MaterializedIndexRemapSelectedEntry>;

}
