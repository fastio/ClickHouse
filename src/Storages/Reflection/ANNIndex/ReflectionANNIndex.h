#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/Reflection/ANNIndex/ANNIndexMatcher.h>
#include <Storages/Reflection/ANNIndex/ANNIndexScheduler.h>
#include <Storages/Reflection/ANNIndex/ANNIndexSchedulerState.h>
#include <Storages/Reflection/ANNIndex/CoverageMap.h>
#include <Storages/Reflection/ANNIndex/IANNIndexAlgorithm.h>
#include <Storages/Reflection/IReflection.h>
#include <Common/Logger.h>

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>


namespace DB
{

struct FutureANNIndexPart;
const std::unordered_set<std::string_view> & getANNIndexBuildAffectingSettings();

/// EXPERIMENTAL: ANNIndex is gated behind
/// `allow_experimental_ann_index`. API and on-disk format may change.
///
/// `ReflectionANNIndex` inherits `IReflection` (which inherits `IStorage`).
/// MergeTree-family plumbing is reused by composition: the inner table held
/// behind `getInnerTable` is a `MergeTreeData`-derived storage created via
/// internal DDL. The Reflection object itself does not own any MergeTree
/// state; part operations, cleanup, format-version handling all live on the
/// inner table and are accessed through `getInnerMergeTreeData(snapshot)`.
class ReflectionANNIndex : public IReflection, public WithMutableContext
{
public:
    ReflectionANNIndex(
        const StorageID & table_id_,
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

    /// ANN-side tuning settings (the `ann_index_*` family). Stored on the
    /// Reflection itself, distinct from the inner table's own MergeTreeSettings.
    /// Kept as a `MergeTreeSettings` instance to reuse the existing setting
    /// parsing and serialization machinery.
    std::shared_ptr<const MergeTreeSettings> getSettings() const { return ann_settings; }

    std::string getName() const override { return "ANNIndex"; }

    /// Background machinery wiring (stage-2):
    ///   * `startup` arms the background assignee and re-checks the source table.
    ///   * `shutdown(is_drop)` flips `shutdown_called` then stops the assignee.
    ///   * `scheduleDataProcessingJob` is the per-tick cycle entry point;
    ///     it diffs the source / ann_index snapshots once via SnapshotDiffReconciler
    ///     and dispatches a Build or Remap top-level task.
    void startup() override;
    void shutdown(bool is_drop) override;
    bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) override;
    void renameInMemory(const StorageID & new_table_id) override;
    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;
    void drop() override;
    void dropInnerTableIfAny(bool sync, ContextPtr local_context) override;

    void checkAlterPartitionIsPossible(
        const PartitionCommands & commands,
        const StorageMetadataPtr & metadata_snapshot,
        const Settings & settings,
        ContextPtr query_context) const override;
    Pipe alterPartition(
        const StorageMetadataPtr & metadata_snapshot,
        const PartitionCommands & commands,
        ContextPtr query_context) override;

private:
    /// Private helpers consumed by `alterPartition`; not part of any virtual
    /// contract. They forward to the inner MergeTree and then refresh
    /// `coverage_map` so the scheduler observes the new state.
    void dropPart(const String & part_name, bool detach, ContextPtr query_context);
    void dropPartition(const ASTPtr & partition, bool detach, ContextPtr query_context);

public:

    const StorageID & getSourceTableID() const { return source_table_id; }
    const Names & getIndexedColumns() const { return indexed_columns; }
    const String & getFamily() const { return family; }
    const String & getImpl() const { return impl; }
    IANNIndexAlgorithm * getAlgorithm() const { return algorithm.get(); }
    StoragePtr getInnerTable() const;
    MergeTreeData & getInnerMergeTreeData(const StoragePtr & inner_table_snapshot) const;
    bool hasReplicatedInnerTable() const { return !inner_zookeeper_path.empty(); }
    static String generateInnerTableName(const StorageID & index_id);

    /// Active materialized-index-parts only. Used by the cycle to feed the reconciler and by
    /// `system.ann_indexes` for aggregate counters.
    MergeTreeData::DataPartsVector getAccessPathPartsVectorForInternalUsage() const;

    size_t getConsecutiveRemapCount() const { return consecutive_remap_count.load(std::memory_order_relaxed); }

    /// True after `shutdown` has been called. Background tasks that block on
    /// long waits (e.g. `pauseFailPoint`) must check this after the wait
    /// returns and abort cleanly instead of committing — otherwise
    /// `DROP TABLE ... SYNC` would deadlock on `waitTableFinallyDropped`
    /// because the paused task still holds a `StoragePtr` to the storage.
    bool isShuttingDown() const { return shutdown_called.load(std::memory_order_relaxed); }

    /// Parse the `coverage.json` manifest of a single materialized-index-part. Static so that
    /// `ANNIndexBuildTask::finish` and `ANNIndexRemapTask::finish` can call it without owning
    /// the storage. Throws on malformed JSON; returns empty list if the file
    /// is missing.
    static std::vector<CoverageEntry> parseCoverageJsonFromMiPart(const IMergeTreeDataPart & part);

    /// Block until `coverage_map` fully covers every active source part, or
    /// `timeout` elapses. Used by `SYSTEM SYNC REFLECTION`. Returns
    /// false on timeout, false if the source table has gone away (caller
    /// surfaces these as `TIMEOUT_EXCEEDED` for the user).
    bool waitForCoverageOfSourceOrTimeout(std::chrono::seconds timeout, ContextPtr context);

    struct ObservabilitySnapshot
    {
        UInt64 backlog_rows = 0;
        UInt64 backlog_bytes = 0;
        UInt64 backlog_parts = 0;
        UInt64 pending_task_count = 0;
        UInt64 ready_ann_index_part_count = 0;
        UInt64 obsolete_ready_source_count = 0;
        UInt64 repeated_failure_count = 0;
        UInt64 tombstone_rows = 0;
        double tombstone_ratio = 0.0;
        UInt64 retry_count = 0;
        std::chrono::system_clock::time_point next_retry_time{};
        String last_error;
    };

    ObservabilitySnapshot getObservabilitySnapshot() const;
    std::map<String, String> getAlgorithmObservabilityFields() const;

    struct SchedulerTaskSnapshot
    {
        String task_id;
        ANNIndexSchedulerState::TaskKind kind = ANNIndexSchedulerState::TaskKind::BuildBatch;
        std::vector<UUID> input_source_uuids;
        std::vector<UUID> input_ann_index_part_uuids;
        UUID output_ann_index_part_uuid;
        ANNIndexSchedulerState::TaskLifecycle state = ANNIndexSchedulerState::TaskLifecycle::Scheduled;
        UInt64 retry_count = 0;
        std::chrono::system_clock::time_point next_retry_time{};
        String last_error;
        bool quarantined = false;
        std::chrono::system_clock::time_point created_at{};
        std::chrono::system_clock::time_point started_at{};
        std::chrono::system_clock::time_point finished_at{};
    };

    std::vector<SchedulerTaskSnapshot> getSchedulerTasksSnapshot() const;
    void triggerSchedulerTick();
    void setBuildsEnabled(bool enabled);
    void setRemapsEnabled(bool enabled);

    /// `IReflection` plumbing.
    const StorageID & getReflectionSourceTableID() const override { return source_table_id; }
    const Names & getReflectionIndexedColumns() const override { return indexed_columns; }
    const String & getReflectionFamily() const override { return family; }
    const String & getReflectionImpl() const override { return impl; }
    String getReflectionEngineName() const override { return getName(); }
    StoragePtr getReflectionInnerTable() const override { return getInnerTable(); }
    ReflectionObservabilitySnapshot getReflectionObservabilitySnapshot() const override;

    /// Behaviour accessors — `matcher` and `scheduler` are held as private
    /// members below; the storage itself does not inherit their interfaces.
    IReflectionMatcher * getReflectionMatcher() override { return &matcher; }
    IReflectionScheduler * getReflectionScheduler() override { return &scheduler; }

    /// Public helpers consumed by `ANNIndexScheduler` (shim) and by internal
    /// `system.reflections` callers. Not part of any virtual contract.
    ReflectionSchedulerSnapshot getSchedulerSnapshot() const;

    void recordBuildCommit(UUID ann_index_part_uuid, const std::vector<CoverageEntry> & entries);
    void recordRemapCommit(
        UUID new_ann_index_part_uuid,
        UUID retired_ann_index_part_uuid,
        const std::vector<CoverageEntry> & incoming,
        const std::vector<UUID> & outgoing_source_uuids);
    void recordRemapBatchCommit(std::vector<CoverageMap::RemapCommit> commits);
    void recordCompactCommit(
        UUID new_ann_index_part_uuid,
        const std::vector<UUID> & retired_ann_index_part_uuids,
        const std::vector<CoverageEntry> & incoming);
    bool shouldCommitBuildOrCompactOutput(
        const std::vector<CoverageEntry> & entries,
        const String & task_kind,
        String & reason) const;
    bool shouldCommitRemapOutput(const FutureANNIndexPart & future_part, String & reason) const;
    void refreshCoverageFromActiveParts();
    void assertReplicatedTaskReservation(const FutureANNIndexPart & future_part) const;

    void releaseTaskResources(FutureANNIndexPart & future_part) noexcept;
    /// Releases every resource that may have been partially acquired by
    /// `tryAcquireTaskResources` and `tryReserveFuturePart` before a tagger
    /// takes ownership. Idempotent; safe to invoke unconditionally from a
    /// scope guard covering the construction window of a submit lambda.
    void rollbackUncommittedTaskReservation(FutureANNIndexPart & future_part) noexcept;
    void postponeForResourceFailure(const String & reason);
    void recordTaskFailure(const FutureANNIndexPart & future_part, const String & reason);
    void clearTaskFailure(const FutureANNIndexPart & future_part);

private:
    struct UncoveredSourceBacklogEntry
    {
        DataPartPtr part;
        std::chrono::steady_clock::time_point first_seen;
        UInt64 rows = 0;
        UInt64 bytes = 0;
    };

    void refreshBuildBacklog(
        const MergeTreeData::DataPartsVector &uncovered_source_parts,
        const std::unordered_set<UUID> & covered_source_uuids);

    MergeTreeData::DataPartsVector selectBuildBatchFromBacklog(
        const MergeTreeData::DataPartsVector &candidate_source_parts,
        bool initial_build,
        bool force_build);

    bool tryReserveFuturePart(FutureANNIndexPart & future_part);
    bool tryAcquireTaskResources(FutureANNIndexPart & future_part, UInt64 input_rows, UInt64 input_bytes);
    bool isTaskFailureBackoffActive(const FutureANNIndexPart & future_part);
    UInt64 getTaskMemoryBudgetBytes() const;
    UInt64 estimateBuildOutputBytes(UInt64 input_rows, UInt64 input_bytes) const;
    bool shouldScheduleCompactRebuild(
        const MergeTreeData::DataPartsVector &source_snapshot,
        const MergeTreeData::DataPartsVector &ann_index_snapshot,
        const std::unordered_set<UUID> & covered_source_uuids) const;
    bool tryReserveReplicatedTask(FutureANNIndexPart & future_part);
    void releaseReplicatedTaskReservation(FutureANNIndexPart & future_part) noexcept;
    void releaseReplicatedLeaderLease(FutureANNIndexPart & future_part) noexcept;
    void initializeInnerTable(LoadingStrictnessLevel mode);

protected:
    void setReplicatedLeaderLeaseForNextTask(String lease_path, String lease_payload, StoragePtr inner_table_snapshot);
    void releasePendingReplicatedLeaderLease() noexcept;

    StorageID source_table_id;
    Names indexed_columns;
    String family;
    String impl;
    ASTPtr build_params;
    ANNIndexAlgorithmPtr algorithm;
    /// Guarded by `currently_processing_in_background_mutex` after construction.
    /// Background tasks take their own `StoragePtr` snapshot before using it.
    StoragePtr inner_table;
    String inner_zookeeper_path;
    String inner_replica_name;

    /// ANN-side tuning settings; see `getSettings`.
    std::shared_ptr<const MergeTreeSettings> ann_settings;

    /// Background assignee that drives the Reflection cycle. Each tick calls
    /// `scheduleDataProcessingJob`, which polls source/ANN snapshots and
    /// dispatches Build / Remap top-level tasks. The inner table runs its own
    /// `MergeTreeCleanupThread`; this storage no longer needs one.
    BackgroundJobsAssignee background_operations_assignee;

    /// Authoritative map of which source UUIDs each active materialized-index-part covers.
    /// Loaded by `startup` from on-disk `coverage.json` manifests and updated
    /// by Build / Remap / Compact commits. The reconciler reads it every cycle to feed
    /// `SnapshotDiffReconciler::run`; SYSTEM SYNC waits on it.
    CoverageMap coverage_map;

    /// Q-E starvation protection counter. Incremented per Remap, reset to 0
    /// on Build. Cycle reads it against `ann_index_starvation_protection_cycles`.
    std::atomic<size_t> consecutive_remap_count{0};

    /// Uncovered active source parts observed by periodic reconcile. Incremental
    /// BuildBatch scheduling reads this backlog and waits for size/age
    /// thresholds instead of building every tiny source part immediately.
    std::unordered_map<UUID, UncoveredSourceBacklogEntry> uncovered_source_backlog;

    ANNIndexSchedulerState scheduler_state;

    /// Atomic kill-switch read at the head of every cycle so the assignee
    /// stops handing us work after `shutdown` begins.
    std::atomic<bool> shutdown_called{false};

    /// Names of materialized-index-parts currently reserved by an in-flight Build / Remap
    /// task. Guarded by `currently_processing_in_background_mutex`. Populated
    /// by `CurrentlyBuildingANNIndexPartTagger`.
    std::unordered_set<String> currently_building_ann_index_parts;
    String pending_replicated_leader_lease_path;
    String pending_replicated_leader_lease_payload;
    StoragePtr pending_replicated_leader_inner_table;
    mutable std::mutex currently_processing_in_background_mutex;

    /// Composed framework-facing dispatchers. `IReflection::getReflectionMatcher`
    /// / `getReflectionScheduler` return pointers to these members; the storage
    /// itself no longer implements `IReflectionMatcher` / `IReflectionScheduler`
    /// by inheritance.
    ANNIndexMatcher matcher;
    ANNIndexScheduler scheduler;

    LoggerPtr log;

    friend struct CurrentlyBuildingANNIndexPartTagger;
};

}
