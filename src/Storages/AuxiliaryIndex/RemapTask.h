#pragma once

#include <Common/ProfileEvents.h>
#include <Core/UUID.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <array>
#include <atomic>
#include <future>
#include <functional>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace DB
{

class IDataPartStorage;
class StorageANN;
class WriteBufferFromFileBase;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;


/** Mid-layer background Remap task for a AuxiliaryIndex table.
  *
  * Mirrors the same `IStage` array pattern used by `BuildTask`
  * (and ultimately `MergeTask`). Remap covers the N-to-M atomic replacement
  * path: N active materialized-index-parts that reference an outgoing or incoming source
  * delta are rewritten into M new materialized-index-parts. The atomic swap itself lives in
  * the top-level `AuxiliaryIndexRemapTask::finish()` via `MergeTreeData::Transaction`; this
  * mid-layer produces Temporary-state parts only.
  *
  * Scope of this class (by design, I-BG-4):
  *   - Writes `tmp_auxiliary_index_remap_*` directories only. Does not touch
  *     `data_parts_indexes`, does not switch part states.
  *   - Zero algorithm calls. `algorithm_private_*` files are shared with the
  *     old parts via hardlinks so the algorithm is oblivious to Remap.
  *   - Produces `std::vector<MutableDataPartPtr>` via `getFuture()`; the
  *     top-level task threads the vector through
  *     `Transaction::addPart(p, need_rename=true) + renameParts + commit`.
  *
  * Stage 1 scans each affected materialized-index-part's `locator` files with
  * `header.json::part_uuid_table` to classify affected segments, then derives
  * M empty-shell new materialized-index-parts (N=M in the simple case: one new
  * per old, level bumped by one). Stage 2 hardlinks immutable files. Stage 3
  * rewrites `locator_<seg>.bin` for segments touched by the delta via a lookup
  * against incoming source rows. Stage 4 writes metadata and fulfils the promise.
  */
class RemapTask
{
public:
    static constexpr auto TEMP_DIRECTORY_PREFIX = "tmp_auxiliary_index_remap_";

    RemapTask(
        MergeTreeData::DataPartsVector affected_auxiliary_index_parts_,
        MergeTreeData::DataPartsVector delta_in_source_parts_,
        std::vector<UUID> delta_out_source_uuids_,
        StorageANN * storage_,
        StoragePtr inner_storage_holder_,
        const MergeTreeData * source_storage_,
        StorageSnapshotPtr source_snapshot_,
        ContextPtr context_,
        UInt64 memory_budget_bytes_,
        UUID first_new_part_uuid_ = {});

    /// Drives one stage per call, mirroring MergeTask::execute. Returns true
    /// while more work remains, false once the final stage has completed and
    /// the promise has been fulfilled.
    bool execute();

    /// Idempotent cancellation hook. Sets the cancel flag observed by stages
    /// and forwards to the current stage so stage-local cleanup can run.
    void cancel() noexcept;

    std::future<std::vector<MergeTreeData::MutableDataPartPtr>> getFuture();

    /// Returns whatever new materialized-index-parts have been constructed so far. Used by
    /// the top-level task on failure paths to clean up temporary storages.
    std::vector<MergeTreeData::MutableDataPartPtr> getUnfinishedParts();

private:
    struct IStage;
    using StagePtr = std::shared_ptr<IStage>;

    struct IStageRuntimeContext {};
    using StageRuntimeContextPtr = std::shared_ptr<IStageRuntimeContext>;

    struct IStage
    {
        virtual void setRuntimeContext(StageRuntimeContextPtr local, StageRuntimeContextPtr global) = 0;
        virtual StageRuntimeContextPtr getContextForNextStage() = 0;
        virtual ProfileEvents::Event getTotalTimeProfileEvent() const = 0;
        virtual bool execute() = 0;
        virtual void cancel() noexcept = 0;
        virtual ~IStage() = default;
    };

    /// Half-open segment range `[begin, end)`. Kept as a small POD so future
    /// multi-segment-per-part schemes can reuse it; stage 1 currently only
    /// populates the `affected_seg_ids` set below.
    struct SegmentRange
    {
        size_t begin = 0;
        size_t end = 0;
    };

    struct GlobalRuntimeContext : public IStageRuntimeContext
    {
        struct SourceRowId
        {
            UInt64 block_number = 0;
            UInt64 block_offset = 0;

            bool operator==(const SourceRowId & rhs) const
            {
                return block_number == rhs.block_number && block_offset == rhs.block_offset;
            }
        };

        struct SourceRowIdHash
        {
            size_t operator()(const SourceRowId & value) const
            {
                size_t seed = std::hash<UInt64>{}(value.block_number);
                seed ^= std::hash<UInt64>{}(value.block_offset) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
                return seed;
            }
        };

        /// Inputs passed by the caller (snapshot semantics).
        MergeTreeData::DataPartsVector affected_auxiliary_index_parts;
        MergeTreeData::DataPartsVector delta_in_source_parts;
        std::vector<UUID> delta_out_source_uuids;
        StorageANN * storage{nullptr};
        StoragePtr inner_storage_holder;

        /// Source-table plumbing used by stage 3 to spin up sequential scans
        /// over `delta_in_source_parts`. Only read inside stage 3 — stages 1,
        /// 2 and 4 operate on mid-layer on-disk files only.
        const MergeTreeData * source_storage{nullptr};
        StorageSnapshotPtr source_snapshot;
        ContextPtr context;

        UInt64 memory_budget_bytes{0};
        UUID first_new_part_uuid;

        /// Owned cancellation flag observed by long-running stages.
        std::atomic<bool> is_cancelled{false};

        /// Produced by stage 1; stage 2/3/4 mutate in place. One element per
        /// new materialized-index-part; in the simple N=M case the ordering mirrors
        /// `affected_auxiliary_index_parts`.
        std::vector<MergeTreeData::MutableDataPartPtr> new_auxiliary_index_parts;

        /// Inter-stage state. Keyed by `new_auxiliary_index_parts[i]->name`.
        std::unordered_map<String, MutableDataPartStoragePtr> tmp_storages;

        /// Per-new-part set of `locator/<seg>` indices that a delta row
        /// touched. Stage 2 skips these when hardlinking `locator/`;
        /// stage 3 rewrites them.
        std::vector<std::unordered_set<size_t>> affected_seg_ids_per_new_part;

        /// Stage 1 also records the per-new-part segment_count read back from
        /// each old materialized-index-part's header, so stage 2 / stage 3 can iterate
        /// `locator_0..N-1` without re-reading `header.json`.
        std::vector<size_t> segment_count_per_new_part;

        /// Pairing between `new_auxiliary_index_parts[i]` and its source `affected_auxiliary_index_parts`
        /// element. Simple N=M case stores identity (i -> i). Kept explicit so
        /// later N!=M schemes plug in without changing the stage contract.
        std::vector<size_t> old_index_per_new_part;
        std::vector<UInt64> tombstone_rows_per_new_part;

        /// Per-new-part UUID table copied from the old header and extended
        /// append-only when affected rows are remapped to an incoming source part.
        std::vector<std::vector<UUID>> part_uuid_table_per_new_part;

        /// Incoming lineage rows keyed by stable identity. Stage 3 uses this to
        /// turn outgoing source locators into live locators for the new source part.
        bool incoming_rows_loaded = false;
        std::unordered_map<SourceRowId, UInt64, SourceRowIdHash> incoming_part_offsets;
        UUID incoming_source_part_uuid;
        String incoming_source_partition_id;
        UInt64 incoming_source_rows = 0;
        std::vector<std::optional<UInt32>> incoming_part_uuid_id_per_new_part;

        /// Stage 2 cursor: index of the next new-materialized-index-part to hardlink on the
        /// following `execute()` call. When `>= new_auxiliary_index_parts.size()` stage 2
        /// reports completion by returning false.
        size_t stage2_cursor{0};

        /// Produced by stage 4; returned via `getFuture`.
        std::promise<std::vector<MergeTreeData::MutableDataPartPtr>> promise;
    };

    using GlobalRuntimeContextPtr = std::shared_ptr<GlobalRuntimeContext>;

    /// Stage 1: scan each old materialized-index-part's locator files with
    /// `part_uuid_table`, flag segments that intersect the in/out delta, and
    /// derive M empty-shell new materialized-index-parts (N=M, level+1).
    struct PlanAffectedSegmentsStage;

    /// Stage 2: for each new materialized-index-part, hardlink `algorithm_private_*`,
    /// all `source_row_id_*`, and `locator_<seg>` for non-affected segments
    /// from the paired old materialized-index-part. Fall back to copy on cross-disk
    /// hardlink failures.
    struct DeriveHardlinksStage;

    /// Stage 3: for each affected segment, match stable `source_row_id`
    /// entries against the incoming source rows and write 12-byte entries to
    /// the new `locator_<seg>.bin` with tombstones for misses.
    struct RewriteMutableSegmentsStage;

    /// Stage 4: write `header.json` (with `derive_from`), `coverage.json`,
    /// and the standard MergeTree part metadata envelope; fsync in the
    /// canonical order; set the promise value.
    struct FinalizeMetadataStage;

    GlobalRuntimeContextPtr global_ctx;

    using Stages = std::array<StagePtr, 4>;
    const Stages stages;

    Stages::const_iterator stages_iterator = stages.begin();

    /// Ensures the promise is fulfilled exactly once: stage 4 on success,
    /// execute() catch on exception, dtor as a last-resort safety net.
    bool promise_fulfilled{false};

    static Stages makeStages();
};

}
