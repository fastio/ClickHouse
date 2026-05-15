#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Storages/MaterializedIndex/MaterializedIndexRemapKind.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <memory>
#include <vector>


namespace DB
{

class StorageMaterializedIndex;

/// Future part descriptor passed to the top-level Build / Remap tasks.
/// Build path uses `source_parts_snapshot`; Remap path uses
/// `affected_materialized_index_parts` plus `delta_in_source_parts` and
/// `delta_out_source_uuids`. The same struct serves both kinds so that the
/// Tagger and SelectedEntry types can be shared.
struct FutureMaterializedIndexPart
{
    enum class Kind
    {
        Build,
        Remap,
        Compact,
    };

    String new_part_name;
    UUID new_part_uuid;
    String task_id;
    bool scheduler_reserved = false;
    String replicated_leader_lease_path;
    String replicated_leader_lease_payload;
    String replicated_task_lock_path;
    String replicated_task_lock_payload;
    bool resource_accounted = false;
    String source_table_key;
    MergeTreeData::DataPartsVector source_parts_snapshot;
    MergeTreeData::DataPartsVector affected_materialized_index_parts;
    MergeTreeData::DataPartsVector delta_in_source_parts;
    std::vector<UUID> delta_out_source_uuids;
    Kind kind = Kind::Build;
    MaterializedIndexRemapKind remap_kind = MaterializedIndexRemapKind::None;
};

using FutureMaterializedIndexPartPtr = std::shared_ptr<FutureMaterializedIndexPart>;


/// Holds the storage-level reservation for a future MaterializedIndex part.
/// `StorageMaterializedIndex` reserves source / MI UUIDs before constructing
/// this tagger; the constructor inserts the future part name into
/// `currently_building_materialized_index_parts`, and `finalize` releases both reservations.
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
