#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Storages/Reflection/ANNIndex/ANNIndexRemapKind.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <map>
#include <memory>
#include <vector>


namespace DB
{

class ReflectionANNIndex;

/// Future part descriptor passed to the top-level Build / Remap tasks.
/// Build path uses `source_parts_snapshot`; Remap path uses
/// `affected_ann_index_parts` plus `delta_in_source_parts` and
/// `delta_out_source_uuids`. The same struct serves both kinds so that the
/// Tagger and SelectedEntry types can be shared.
struct FutureANNIndexPart
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
    bool scheduler_task_succeeded = false;
    String scheduler_task_error;
    std::map<String, String> scheduler_task_settings;
    StoragePtr inner_table_snapshot;
    MergeTreeData::DataPartsVector source_parts_snapshot;
    MergeTreeData::DataPartsVector affected_ann_index_parts;
    MergeTreeData::DataPartsVector delta_in_source_parts;
    std::vector<UUID> delta_out_source_uuids;
    Kind kind = Kind::Build;
    ANNIndexRemapKind remap_kind = ANNIndexRemapKind::None;
};

using FutureANNIndexPartPtr = std::shared_ptr<FutureANNIndexPart>;


/// Holds the storage-level reservation for a future ANNIndex part.
/// `ReflectionANNIndex` reserves source / MI UUIDs before constructing
/// this tagger; the constructor inserts the future part name into
/// `currently_building_ann_index_parts`, and `finalize` releases both reservations.
struct CurrentlyBuildingANNIndexPartTagger
{
    FutureANNIndexPartPtr future_part;
    ReflectionANNIndex & storage;
    bool finalized = false;

    CurrentlyBuildingANNIndexPartTagger(
        FutureANNIndexPartPtr future_part_,
        ReflectionANNIndex & storage_);

    void finalize();
    ~CurrentlyBuildingANNIndexPartTagger();
};

using CurrentlyBuildingANNIndexPartTaggerPtr
    = std::unique_ptr<CurrentlyBuildingANNIndexPartTagger>;


/// Picked by the cycle for a Build round. Owns the tagger lifetime so the
/// reservation is released when the entry goes away.
struct ANNIndexBuildSelectedEntry
{
    FutureANNIndexPartPtr future_part;
    CurrentlyBuildingANNIndexPartTaggerPtr tagger;
    bool finalized = false;

    ANNIndexBuildSelectedEntry(
        FutureANNIndexPartPtr future_part_,
        CurrentlyBuildingANNIndexPartTaggerPtr tagger_);

    void finalize();
    ~ANNIndexBuildSelectedEntry();
};

using ANNIndexBuildSelectedEntryPtr
    = std::shared_ptr<ANNIndexBuildSelectedEntry>;


/// Picked by the cycle for a Remap round. Same shape as the Build entry; the
/// kind information is carried inside `future_part->kind`.
struct ANNIndexRemapSelectedEntry
{
    FutureANNIndexPartPtr future_part;
    CurrentlyBuildingANNIndexPartTaggerPtr tagger;
    bool finalized = false;

    ANNIndexRemapSelectedEntry(
        FutureANNIndexPartPtr future_part_,
        CurrentlyBuildingANNIndexPartTaggerPtr tagger_);

    void finalize();
    ~ANNIndexRemapSelectedEntry();
};

using ANNIndexRemapSelectedEntryPtr
    = std::shared_ptr<ANNIndexRemapSelectedEntry>;

}
