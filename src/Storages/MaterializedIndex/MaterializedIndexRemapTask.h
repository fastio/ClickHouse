#pragma once

#include <Common/ProfileEvents.h>
#include <Core/UUID.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <array>
#include <atomic>
#include <future>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace DB
{

class IDataPartStorage;
class StorageMaterializedIndex;
class WriteBufferFromFileBase;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;


/** Mid-layer background Remap task for a MaterializedIndex table.
  *
  * Mirrors the same `IStage` array pattern used by `MaterializedIndexBuildTask`
  * (and ultimately `MergeTask`). Remap covers the N-to-M atomic replacement
  * path: N active mi-parts that reference an outgoing or incoming source
  * delta are rewritten into M new mi-parts. The atomic swap itself lives in
  * the top-level `RemapTask::finish()` via `MergeTreeData::Transaction`; this
  * mid-layer produces Temporary-state parts only.
  *
  * Scope of this class (by design, I-BG-4):
  *   - Writes `tmp_mi_remap_*` directories only. Does not touch
  *     `data_parts_indexes`, does not switch part states.
  *   - Zero algorithm calls. `algorithm_private/` is shared with the old
  *     parts via hardlinks so the algorithm is oblivious to Remap.
  *   - Produces `std::vector<MutableDataPartPtr>` via `getFuture()`; the
  *     top-level task threads the vector through
  *     `Transaction::addPart(p, need_rename=true) + renameParts + commit`.
  *
  * Stage 1 scans each affected mi-part's `stable_layer` to rebuild the
  * per-segment source part-uuid set (the on-disk `header.json` does not
  * persist this mapping; see the MaterializedIndex on-disk format
  * specification), then derives M empty-shell new mi-parts (N=M in the
  * simple case: one new per old, level bumped by one). Stage 2 hardlinks
  * the immutable files. Stage 3 rewrites `mutable_offset/<seg>.bin` for
  * segments touched by the delta via a sort-merge join against the incoming
  * source rows. Stage 4 writes metadata and fulfils the promise.
  */
class MaterializedIndexRemapTask
{
public:
    static constexpr auto TEMP_DIRECTORY_PREFIX = "tmp_mi_remap_";

    MaterializedIndexRemapTask(
        MergeTreeData::DataPartsVector affected_mi_parts_,
        MergeTreeData::DataPartsVector delta_in_source_parts_,
        std::vector<UUID> delta_out_source_uuids_,
        StorageMaterializedIndex * storage_,
        const MergeTreeData * source_storage_,
        StorageSnapshotPtr source_snapshot_,
        ContextPtr context_,
        UInt64 memory_budget_bytes_);

    ~MaterializedIndexRemapTask();

    /// Drives one stage per call, mirroring MergeTask::execute. Returns true
    /// while more work remains, false once the final stage has completed and
    /// the promise has been fulfilled.
    bool execute();

    /// Idempotent cancellation hook. Sets the cancel flag observed by stages
    /// and forwards to the current stage so stage-local cleanup can run.
    void cancel() noexcept;

    std::future<std::vector<MergeTreeData::MutableDataPartPtr>> getFuture();

    /// Returns whatever new mi-parts have been constructed so far. Used by
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
        /// Inputs passed by the caller (snapshot semantics).
        MergeTreeData::DataPartsVector affected_mi_parts;
        MergeTreeData::DataPartsVector delta_in_source_parts;
        std::vector<UUID> delta_out_source_uuids;
        StorageMaterializedIndex * storage{nullptr};

        /// Source-table plumbing used by stage 3 to spin up sequential scans
        /// over `delta_in_source_parts`. Only read inside stage 3 — stages 1,
        /// 2 and 4 operate on mid-layer on-disk files only.
        const MergeTreeData * source_storage{nullptr};
        StorageSnapshotPtr source_snapshot;
        ContextPtr context;

        UInt64 memory_budget_bytes{0};

        /// Owned cancellation flag observed by long-running stages.
        std::atomic<bool> is_cancelled{false};

        /// Produced by stage 1; stage 2/3/4 mutate in place. One element per
        /// new mi-part; in the simple N=M case the ordering mirrors
        /// `affected_mi_parts`.
        std::vector<MergeTreeData::MutableDataPartPtr> new_mi_parts;

        /// Inter-stage state. Keyed by `new_mi_parts[i]->name`.
        std::unordered_map<String, MutableDataPartStoragePtr> tmp_storages;

        /// Per-new-part set of `stable_layer/<seg>` indices that a delta row
        /// touched. Stage 2 skips these when hardlinking `mutable_offset/`;
        /// stage 3 rewrites them.
        std::vector<std::unordered_set<size_t>> affected_seg_ids_per_new_part;

        /// Stage 1 also records the per-new-part segment_count read back from
        /// each old mi-part's header, so stage 2 / stage 3 can iterate
        /// `mutable_offset/0..N-1` without re-reading `header.json`.
        std::vector<size_t> segment_count_per_new_part;

        /// Pairing between `new_mi_parts[i]` and its source `affected_mi_parts`
        /// element. Simple N=M case stores identity (i -> i). Kept explicit so
        /// later N!=M schemes plug in without changing the stage contract.
        std::vector<size_t> old_index_per_new_part;

        /// Stage 2 cursor: index of the next new-mi-part to hardlink on the
        /// following `execute()` call. When `>= new_mi_parts.size()` stage 2
        /// reports completion by returning false.
        size_t stage2_cursor{0};

        /// Produced by stage 4; returned via `getFuture`.
        std::promise<std::vector<MergeTreeData::MutableDataPartPtr>> promise;
    };

    using GlobalRuntimeContextPtr = std::shared_ptr<GlobalRuntimeContext>;

    /// Stage 1: scan each old mi-part's `stable_layer` + `part_uuid_dict` to
    /// rebuild seg -> source-uuid mapping; flag segments that intersect the
    /// in/out delta; derive M empty-shell new mi-parts (N=M, level+1).
    struct PlanAffectedSegmentsStage;

    /// Stage 2: for each new mi-part, hardlink `algorithm_private/*`,
    /// `stable_layer/*`, and `mutable_offset/<seg>` for non-affected
    /// segments from the paired old mi-part. Fall back to copy on cross-disk
    /// hardlink failures.
    struct DeriveHardlinksStage;

    /// Stage 3: for each affected segment, sort-merge join the old
    /// stable_layer against the incoming source rows; write 12-byte entries
    /// to the new `mutable_offset/<seg>.bin` with tombstones for misses.
    struct RewriteMutableSegmentsStage;

    /// Stage 4: write `header.json` (with `derive_from`), `coverage.txt`,
    /// `checksum.txt`, `txn_version.txt`; fsync in the canonical order; set
    /// the promise value.
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
