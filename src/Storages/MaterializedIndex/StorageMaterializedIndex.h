#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/MergeTree/MergeTreeCleanupThread.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MaterializedIndex/CoverageMap.h>
#include <Storages/MaterializedIndex/IMaterializedIndexAlgorithm.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <unordered_set>


namespace DB
{

/// EXPERIMENTAL: MaterializedIndex is gated behind
/// `allow_experimental_materialized_index`. API and on-disk format may change.
///
/// Stage-1 skeleton of a MaterializedIndex backing table. Inherits the full
/// MergeTreeData lifecycle for future reuse (merges, mutations, backup, ...)
/// but rejects reads / writes outright: only the catalog-side paths are
/// exercised at this point. Not marked final: ReplicatedMaterializedIndex
/// derives from this class.
class StorageMaterializedIndex : public MergeTreeData
{
public:
    StorageMaterializedIndex(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageID & source_table_id_,
        const Names & indexed_columns_,
        const String & family_,
        const String & impl_,
        const ASTPtr & build_params_,
        ContextMutablePtr context_,
        const StorageInMemoryMetadata & metadata_,
        std::unique_ptr<MergeTreeSettings> settings_,
        LoadingStrictnessLevel mode);

    std::string getName() const override { return "MaterializedIndex"; }

    bool supportsPrewhere() const override { return false; }
    bool supportsFinal() const override { return false; }
    bool supportsSubcolumns() const override { return false; }
    bool supportsLightweightDelete() const override { return false; }

    /// Stage-1 constraint C1: the table object is catalog-visible but not
    /// queryable or writable through the usual SQL surfaces.
    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    /// Background machinery wiring (stage-2):
    ///   * `startup` arms the cleanup thread and re-checks the source table.
    ///   * `shutdown(is_drop)` flips `shutdown_called` then stops the thread.
    ///   * `scheduleDataProcessingJob` is the per-tick cycle entry point;
    ///     it diffs the source / mi snapshots once via SnapshotDiffReconciler
    ///     and dispatches a Build or Remap top-level task.
    void startup() override;
    void shutdown(bool is_drop) override;
    bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) override;

    MutationCounters getMutationCounters() const override { return {}; }
    std::map<std::string, MutationCommands> getUnfinishedMutationCommands() const override { return {}; }
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override { return {}; }
    MutationsSnapshotPtr getMutationsSnapshot(const IMutationsSnapshot::Params & params) const override;

    void dropPartNoWaitNoThrow(const String & part_name) override;
    void dropPart(const String & part_name, bool detach, ContextPtr context) override;
    void dropPartition(const ASTPtr & partition, bool detach, ContextPtr context) override;
    PartitionCommandsResultInfo attachPartition(const PartitionCommand & command, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) override;
    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr context) override;
    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr context) override;

    bool partIsAssignedToBackgroundOperation(const DataPartPtr & part) const override;
    void attachRestoredParts(MutableDataPartsVector && parts) override;
    void startBackgroundMovesIfNeeded() override {}
    std::unique_ptr<MergeTreeSettings> getDefaultSettings() const override;
    std::optional<UInt64> totalRows(ContextPtr) const override { return UInt64{0}; }
    std::optional<UInt64> totalBytes(ContextPtr) const override { return UInt64{0}; }
    std::optional<UInt64> totalBytesUncompressed(const Settings &) const override { return UInt64{0}; }

    const StorageID & getSourceTableID() const { return source_table_id; }
    const Names & getIndexedColumns() const { return indexed_columns; }
    const String & getFamily() const { return family; }
    const String & getImpl() const { return impl; }
    IMaterializedIndexAlgorithm * getAlgorithm() const { return algorithm.get(); }

    /// Active mi-parts only. Used by the cycle to feed the reconciler and by
    /// `system.materialized_indexes` for aggregate counters.
    DataPartsVector getAccessPathPartsVectorForInternalUsage() const;

    size_t getConsecutiveRemapCount() const { return consecutive_remap_count.load(std::memory_order_relaxed); }

    /// Parse the `coverage.json` manifest of a single mi-part. Static so that
    /// `BuildTask::finish` and `RemapTask::finish` can call it without owning
    /// the storage. Throws on malformed JSON; returns empty list if the file
    /// is missing.
    static std::vector<CoverageEntry> parseCoverageJsonFromMiPart(const IMergeTreeDataPart & part);

    /// Block until `coverage_map` fully covers every active source part, or
    /// `timeout` elapses. Used by `SYSTEM SYNC MATERIALIZED INDEX`. Returns
    /// false on timeout, false if the source table has gone away (caller
    /// surfaces these as `TIMEOUT_EXCEEDED` for the user).
    bool waitForCoverageOfSourceOrTimeout(std::chrono::seconds timeout, ContextPtr context);

private:
    /// Walk every active mi-part on disk, parse its `coverage.json`, and feed
    /// the result into `coverage_map`. Called from `startup` so the
    /// reconciler observes the persisted coverage state across restarts.
    void loadCoverageFromActiveParts();

protected:
    StorageID source_table_id;
    Names indexed_columns;
    String family;
    String impl;
    ASTPtr build_params;
    MaterializedIndexAlgorithmPtr algorithm;

    /// Cleanup thread: drives Outdated mi-part removal + tmp_mi_* directory
    /// pruning. Constructed in the ctor init list (`cleanup_thread(*this)`)
    /// and started in `startup`, stopped in `shutdown`.
    MergeTreeCleanupThread cleanup_thread;

    /// Authoritative map of which source UUIDs each active mi-part covers.
    /// Loaded by `startup` from on-disk `coverage.json` manifests and updated
    /// by Build / Remap commits. The reconciler reads it every cycle to feed
    /// `SnapshotDiffReconciler::run`; SYSTEM SYNC waits on it.
public:
    CoverageMap coverage_map;
protected:

    /// Q-E starvation protection counter. Incremented per Remap, reset to 0
    /// on Build. Cycle reads it against `materialized_index_starvation_protection_cycles`.
    std::atomic<size_t> consecutive_remap_count{0};

    /// Atomic kill-switch read at the head of every cycle so the assignee
    /// stops handing us work after `shutdown` begins.
    std::atomic<bool> shutdown_called{false};

    /// Names of mi-parts currently reserved by an in-flight Build / Remap
    /// task. Guarded by `currently_processing_in_background_mutex`. Populated
    /// by `CurrentlyBuildingMaterializedIndexPartTagger`.
    std::unordered_set<String> currently_building_mi_parts;
    mutable std::mutex currently_processing_in_background_mutex;

    friend struct CurrentlyBuildingMaterializedIndexPartTagger;
};

}
