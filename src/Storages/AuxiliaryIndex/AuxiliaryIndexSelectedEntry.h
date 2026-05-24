#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexRemapKind.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <memory>
#include <vector>


namespace DB
{

class StorageANN;

/// Future part descriptor passed to the top-level Build / Remap tasks.
/// Build path uses `source_parts_snapshot`; Remap path uses
/// `affected_auxiliary_index_parts` plus `delta_in_source_parts` and
/// `delta_out_source_uuids`. The same struct serves both kinds so that the
/// Tagger and SelectedEntry types can be shared.
struct FutureAuxiliaryIndexPart
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
    StoragePtr inner_table_snapshot;
    MergeTreeData::DataPartsVector source_parts_snapshot;
    MergeTreeData::DataPartsVector affected_auxiliary_index_parts;
    MergeTreeData::DataPartsVector delta_in_source_parts;
    std::vector<UUID> delta_out_source_uuids;
    Kind kind = Kind::Build;
    AuxiliaryIndexRemapKind remap_kind = AuxiliaryIndexRemapKind::None;
};

using FutureAuxiliaryIndexPartPtr = std::shared_ptr<FutureAuxiliaryIndexPart>;


/// Holds the storage-level reservation for a future AuxiliaryIndex part.
/// `StorageANN` reserves source / MI UUIDs before constructing
/// this tagger; the constructor inserts the future part name into
/// `currently_building_auxiliary_index_parts`, and `finalize` releases both reservations.
struct CurrentlyBuildingAuxiliaryIndexPartTagger
{
    FutureAuxiliaryIndexPartPtr future_part;
    StorageANN & storage;
    bool finalized = false;

    CurrentlyBuildingAuxiliaryIndexPartTagger(
        FutureAuxiliaryIndexPartPtr future_part_,
        StorageANN & storage_);

    void finalize();
    ~CurrentlyBuildingAuxiliaryIndexPartTagger();
};

using CurrentlyBuildingAuxiliaryIndexPartTaggerPtr
    = std::unique_ptr<CurrentlyBuildingAuxiliaryIndexPartTagger>;


/// Picked by the cycle for a Build round. Owns the tagger lifetime so the
/// reservation is released when the entry goes away.
struct AuxiliaryIndexBuildSelectedEntry
{
    FutureAuxiliaryIndexPartPtr future_part;
    CurrentlyBuildingAuxiliaryIndexPartTaggerPtr tagger;
    bool finalized = false;

    AuxiliaryIndexBuildSelectedEntry(
        FutureAuxiliaryIndexPartPtr future_part_,
        CurrentlyBuildingAuxiliaryIndexPartTaggerPtr tagger_);

    void finalize();
    ~AuxiliaryIndexBuildSelectedEntry();
};

using AuxiliaryIndexBuildSelectedEntryPtr
    = std::shared_ptr<AuxiliaryIndexBuildSelectedEntry>;


/// Picked by the cycle for a Remap round. Same shape as the Build entry; the
/// kind information is carried inside `future_part->kind`.
struct AuxiliaryIndexRemapSelectedEntry
{
    FutureAuxiliaryIndexPartPtr future_part;
    CurrentlyBuildingAuxiliaryIndexPartTaggerPtr tagger;
    bool finalized = false;

    AuxiliaryIndexRemapSelectedEntry(
        FutureAuxiliaryIndexPartPtr future_part_,
        CurrentlyBuildingAuxiliaryIndexPartTaggerPtr tagger_);

    void finalize();
    ~AuxiliaryIndexRemapSelectedEntry();
};

using AuxiliaryIndexRemapSelectedEntryPtr
    = std::shared_ptr<AuxiliaryIndexRemapSelectedEntry>;

}
