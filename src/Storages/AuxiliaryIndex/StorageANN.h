#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/MergeTree/MergeTreeCleanupThread.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/AuxiliaryIndex/CoverageMap.h>
#include <Storages/AuxiliaryIndex/IAuxiliaryIndexAlgorithm.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexSchedulerState.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>


namespace DB
{

struct FutureAuxiliaryIndexPart;

/// EXPERIMENTAL: AuxiliaryIndex is gated behind
/// `allow_experimental_auxiliary_index`. API and on-disk format may change.
///
/// Stage-1 skeleton of a AuxiliaryIndex backing table. Inherits the full
/// MergeTreeData lifecycle for future reuse (merges, mutations, backup, ...)
/// but rejects reads / writes outright: only the catalog-side paths are
/// exercised at this point. Not marked final: ReplicatedANN
/// derives from this class.
class StorageANN : public MergeTreeData
{
public:
    StorageANN(
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
        LoadingStrictnessLevel mode,
        const String & inner_zookeeper_path_ = {},
        const String & inner_replica_name_ = {});

    std::string getName() const override { return "ANN"; }

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
    ///     it diffs the source / auxiliary_index snapshots once via SnapshotDiffReconciler
    ///     and dispatches a Build or Remap top-level task.
    void startup() override;
    void shutdown(bool is_drop) override;
    bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) override;
    void renameInMemory(const StorageID & new_table_id) override;
    void drop() override;
    void dropInnerTableIfAny(bool sync, ContextPtr local_context) override;
    void backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;
    void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;
    bool supportsBackupPartition() const override;

    MutationCounters getMutationCounters() const override { return {}; }
    std::map<std::string, MutationCommands> getUnfinishedMutationCommands() const override { return {}; }
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override { return {}; }
    MutationsSnapshotPtr getMutationsSnapshot(const IMutationsSnapshot::Params & params) const override;

    void checkAlterPartitionIsPossible(
        const PartitionCommands & commands,
        const StorageMetadataPtr & metadata_snapshot,
        const Settings & settings,
        ContextPtr query_context) const override;
    Pipe alterPartition(
        const StorageMetadataPtr & metadata_snapshot,
        const PartitionCommands & commands,
        ContextPtr query_context) override;
    /// `MergeTreeData::clearEmptyParts` / background cleanup call this hook on the
    /// storage object. Materialized-index data parts live on the inner MergeTree;
    /// see the implementation for forwarding rules. User-facing removal must use
    /// `dropPart` / `dropPartition` (`ALTER TABLE ... DROP PART/PARTITION`).
    void dropPartNoWaitNoThrow(const String & part_name) override;
    void dropPart(const String & part_name, bool detach, ContextPtr query_context) override;
    void dropPartition(const ASTPtr & partition, bool detach, ContextPtr query_context) override;
    PartitionCommandsResultInfo attachPartition(const PartitionCommand & command, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) override;
    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr context) override;
    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr context) override;

    size_t clearEmptyParts() override;
    size_t clearUnusedPatchParts() override;

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
    IAuxiliaryIndexAlgorithm * getAlgorithm() const { return algorithm.get(); }
    StoragePtr getInnerTable() const;
    MergeTreeData & getInnerMergeTreeData(const StoragePtr & inner_table_snapshot) const;
    bool hasReplicatedInnerTable() const { return !inner_zookeeper_path.empty(); }
    static String generateInnerTableName(const StorageID & index_id);

    /// Active materialized-index-parts only. Used by the cycle to feed the reconciler and by
    /// `system.auxiliary_indexes` for aggregate counters.
    DataPartsVector getAccessPathPartsVectorForInternalUsage() const;

    size_t getConsecutiveRemapCount() const { return consecutive_remap_count.load(std::memory_order_relaxed); }

    /// True after `shutdown` has been called. Background tasks that block on
    /// long waits (e.g. `pauseFailPoint`) must check this after the wait
    /// returns and abort cleanly instead of committing — otherwise
    /// `DROP TABLE ... SYNC` would deadlock on `waitTableFinallyDropped`
    /// because the paused task still holds a `StoragePtr` to the storage.
    bool isShuttingDown() const { return shutdown_called.load(std::memory_order_relaxed); }

    /// Parse the `coverage.json` manifest of a single materialized-index-part. Static so that
    /// `AuxiliaryIndexBuildTask::finish` and `AuxiliaryIndexRemapTask::finish` can call it without owning
    /// the storage. Throws on malformed JSON; returns empty list if the file
    /// is missing.
    static std::vector<CoverageEntry> parseCoverageJsonFromMiPart(const IMergeTreeDataPart & part);

    /// Block until `coverage_map` fully covers every active source part, or
    /// `timeout` elapses. Used by `SYSTEM SYNC AUXILIARY INDEX`. Returns
    /// false on timeout, false if the source table has gone away (caller
    /// surfaces these as `TIMEOUT_EXCEEDED` for the user).
    bool waitForCoverageOfSourceOrTimeout(std::chrono::seconds timeout, ContextPtr context);

    struct ObservabilitySnapshot
    {
        UInt64 backlog_rows = 0;
        UInt64 backlog_bytes = 0;
        UInt64 backlog_parts = 0;
        UInt64 pending_task_count = 0;
        UInt64 ready_auxiliary_index_part_count = 0;
        UInt64 obsolete_ready_source_count = 0;
        UInt64 repeated_failure_count = 0;
        UInt64 tombstone_rows = 0;
        double tombstone_ratio = 0.0;
        UInt64 retry_count = 0;
        std::chrono::system_clock::time_point next_retry_time{};
        String last_error;
    };

    ObservabilitySnapshot getObservabilitySnapshot() const;

    void recordBuildCommit(UUID auxiliary_index_part_uuid, const std::vector<CoverageEntry> & entries);
    void recordRemapCommit(
        UUID new_auxiliary_index_part_uuid,
        UUID retired_auxiliary_index_part_uuid,
        const std::vector<CoverageEntry> & incoming,
        const std::vector<UUID> & outgoing_source_uuids);
    void recordRemapBatchCommit(std::vector<CoverageMap::RemapCommit> commits);
    void recordCompactCommit(
        UUID new_auxiliary_index_part_uuid,
        const std::vector<UUID> & retired_auxiliary_index_part_uuids,
        const std::vector<CoverageEntry> & incoming);
    bool shouldCommitBuildOrCompactOutput(
        const std::vector<CoverageEntry> & entries,
        const String & task_kind,
        String & reason) const;
    bool shouldCommitRemapOutput(const FutureAuxiliaryIndexPart & future_part, String & reason) const;
    void refreshCoverageFromActiveParts();
    void assertReplicatedTaskReservation(const FutureAuxiliaryIndexPart & future_part) const;

    void releaseTaskResources(FutureAuxiliaryIndexPart & future_part) noexcept;
    /// Releases every resource that may have been partially acquired by
    /// `tryAcquireTaskResources` and `tryReserveFuturePart` before a tagger
    /// takes ownership. Idempotent; safe to invoke unconditionally from a
    /// scope guard covering the construction window of a submit lambda.
    void rollbackUncommittedTaskReservation(FutureAuxiliaryIndexPart & future_part) noexcept;
    void postponeForResourceFailure(const String & reason);
    void recordTaskFailure(const FutureAuxiliaryIndexPart & future_part, const String & reason);
    void clearTaskFailure(const FutureAuxiliaryIndexPart & future_part);

private:
    struct UncoveredSourceBacklogEntry
    {
        DataPartPtr part;
        std::chrono::steady_clock::time_point first_seen;
        UInt64 rows = 0;
        UInt64 bytes = 0;
    };

    void refreshBuildBacklog(
        const DataPartsVector & uncovered_source_parts,
        const std::unordered_set<UUID> & covered_source_uuids);

    DataPartsVector selectBuildBatchFromBacklog(
        const DataPartsVector & candidate_source_parts,
        bool initial_build,
        bool force_build);

    bool tryReserveFuturePart(FutureAuxiliaryIndexPart & future_part);
    bool tryAcquireTaskResources(FutureAuxiliaryIndexPart & future_part, UInt64 input_rows, UInt64 input_bytes);
    bool isTaskFailureBackoffActive(const FutureAuxiliaryIndexPart & future_part);
    UInt64 getTaskMemoryBudgetBytes() const;
    UInt64 estimateBuildOutputBytes(UInt64 input_rows, UInt64 input_bytes) const;
    bool shouldScheduleCompactRebuild(
        const DataPartsVector & source_snapshot,
        const DataPartsVector & auxiliary_index_snapshot,
        const std::unordered_set<UUID> & covered_source_uuids) const;
    bool tryReserveReplicatedTask(FutureAuxiliaryIndexPart & future_part);
    void releaseReplicatedTaskReservation(FutureAuxiliaryIndexPart & future_part) noexcept;
    void releaseReplicatedLeaderLease(FutureAuxiliaryIndexPart & future_part) noexcept;
    void initializeInnerTable(
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        std::unique_ptr<MergeTreeSettings> settings_,
        LoadingStrictnessLevel mode);

protected:
    void setReplicatedLeaderLeaseForNextTask(String lease_path, String lease_payload, StoragePtr inner_table_snapshot);
    void releasePendingReplicatedLeaderLease() noexcept;

    StorageID source_table_id;
    Names indexed_columns;
    String family;
    String impl;
    ASTPtr build_params;
    AuxiliaryIndexAlgorithmPtr algorithm;
    /// Guarded by `currently_processing_in_background_mutex` after construction.
    /// Background tasks take their own `StoragePtr` snapshot before using it.
    StoragePtr inner_table;
    String inner_zookeeper_path;
    String inner_replica_name;

    /// Cleanup thread: drives Outdated materialized-index-part removal + tmp_auxiliary_index_* directory
    /// pruning. Constructed in the ctor init list (`cleanup_thread(*this)`)
    /// and started in `startup`, stopped in `shutdown`.
    MergeTreeCleanupThread cleanup_thread;

    /// Authoritative map of which source UUIDs each active materialized-index-part covers.
    /// Loaded by `startup` from on-disk `coverage.json` manifests and updated
    /// by Build / Remap / Compact commits. The reconciler reads it every cycle to feed
    /// `SnapshotDiffReconciler::run`; SYSTEM SYNC waits on it.
    CoverageMap coverage_map;

    /// Q-E starvation protection counter. Incremented per Remap, reset to 0
    /// on Build. Cycle reads it against `auxiliary_index_starvation_protection_cycles`.
    std::atomic<size_t> consecutive_remap_count{0};

    /// Uncovered active source parts observed by periodic reconcile. Incremental
    /// BuildBatch scheduling reads this backlog and waits for size/age
    /// thresholds instead of building every tiny source part immediately.
    std::unordered_map<UUID, UncoveredSourceBacklogEntry> uncovered_source_backlog;

    AuxiliaryIndexSchedulerState scheduler_state;

    /// Atomic kill-switch read at the head of every cycle so the assignee
    /// stops handing us work after `shutdown` begins.
    std::atomic<bool> shutdown_called{false};

    /// Names of materialized-index-parts currently reserved by an in-flight Build / Remap
    /// task. Guarded by `currently_processing_in_background_mutex`. Populated
    /// by `CurrentlyBuildingAuxiliaryIndexPartTagger`.
    std::unordered_set<String> currently_building_auxiliary_index_parts;
    String pending_replicated_leader_lease_path;
    String pending_replicated_leader_lease_payload;
    StoragePtr pending_replicated_leader_inner_table;
    mutable std::mutex currently_processing_in_background_mutex;

    friend struct CurrentlyBuildingAuxiliaryIndexPartTagger;
};

}
