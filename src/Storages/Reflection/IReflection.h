#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>

#include <chrono>

namespace DB
{

class IReflectionMatcher;
class IReflectionScheduler;

enum class ReflectionMatchKind : uint8_t
{
    PlanRewrite,
    ReadHint,
    CostHint,
};

struct ReflectionObservabilitySnapshot
{
    UInt64 backlog_rows = 0;
    UInt64 backlog_bytes = 0;
    UInt64 backlog_parts = 0;
    UInt64 pending_task_count = 0;
    UInt64 ready_part_count = 0;
    UInt64 obsolete_ready_source_count = 0;
    UInt64 repeated_failure_count = 0;
    UInt64 tombstone_rows = 0;
    double tombstone_ratio = 0.0;
    UInt64 retry_count = 0;
    std::chrono::system_clock::time_point next_retry_time{};
    String last_error;
};

/// Framework-level base for Reflection ENGINE objects.
///
/// `IReflection` inherits `IStorage` so that every Reflection ENGINE is a
/// catalog-visible storage in its own right: `DatabaseCatalog` / DDL /
/// `system.tables` / the query-plan optimizer operate on Reflections via the
/// same `StoragePtr` channel they use for ordinary tables, and Reflection
/// identity is recognised through `dynamic_cast<IReflection *>(storage.get())`.
///
/// `IReflection` deliberately does NOT inherit `MergeTreeData`. Whether a
/// concrete ENGINE reuses MergeTree-family plumbing (the part lifecycle, the
/// scheduler/assignee, replication coordination, ...) is expressed by
/// composition: the ENGINE holds an inner storage exposed via
/// `getReflectionInnerTable`, and forwards data-side concerns there. Framework
/// code never assumes a specific storage family for the inner table.
///
/// Two roles fused into one type:
///   1. Identity + read-only catalog metadata. `dynamic_cast<IReflection *>`
///      is the sole mechanism by which catalog / DDL / system tables / the
///      optimizer recognise and introspect a Reflection.
///   2. Behaviour accessor. The matcher and scheduler are not inherited; they
///      are held as private members of the concrete ENGINE and exposed via
///      `getReflectionMatcher` / `getReflectionScheduler`. This keeps the
///      storage's type identity focused on "what it is" (a Reflection) and
///      delegates "what it does" to composed objects.
///
/// Default `IStorage` semantics provided here:
///   * `read` / `write` / `truncate` / `alter` / `mutate` reject — a
///     Reflection is not a direct SQL surface.
///   * `backupData` / `restoreDataFromBackup` reject — a Reflection is
///     recomputable from its source; back up / restore the source table.
///   * `drop` cascades to the inner table.
///   * Stats / data-location queries forward to the inner table.
///   * Capability flags (`supportsXxx`, `isMergeTree`, ...) report Reflection
///     semantics (not directly queryable, not a MergeTree).
///
/// Concrete ENGINEs override only the methods whose semantics differ from
/// these defaults (e.g. `startup` / `shutdown` for the background pipeline,
/// `alterPartition` to allow `DROP PART`).
class IReflection : public IStorage, public IBackgroundOperation
{
public:
    using IStorage::IStorage;

    /// `IBackgroundOperation::scheduleDataProcessingJob` stays pure — each
    /// ENGINE drives its own cycle. Data-moving is not part of the Reflection
    /// background model (the inner table runs its own move scheduler if any).
    bool scheduleDataMovingJob(BackgroundJobsAssignee &) override { return false; }

    /// --- Identity + read-only metadata (catalog / DDL / system tables / optimizer) ---
    virtual const StorageID & getReflectionSourceTableID() const = 0;
    virtual const Names & getReflectionIndexedColumns() const = 0;
    virtual const String & getReflectionFamily() const = 0;
    virtual const String & getReflectionImpl() const = 0;
    virtual String getReflectionEngineName() const = 0;
    virtual StoragePtr getReflectionInnerTable() const = 0;
    virtual ReflectionObservabilitySnapshot getReflectionObservabilitySnapshot() const = 0;

    /// --- Behaviour accessors ---
    /// The ENGINE returns a pointer to a privately-owned matcher / scheduler
    /// implementation. The framework calls these through the returned pointer;
    /// the ENGINE itself does not implement the matcher / scheduler interfaces
    /// by inheritance.
    virtual IReflectionMatcher * getReflectionMatcher() = 0;
    virtual IReflectionScheduler * getReflectionScheduler() = 0;

    /// --- IStorage hooks: default Reflection-wide semantics ---

    /// A Reflection is not directly queryable / writable. Stage-1 Constraint C1:
    /// catalog-visible but not on the SQL read/write surface.
    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        bool async_insert) override;

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        TableExclusiveLockHolder & lock_holder) override;

    /// ALTER / mutation default: rejected. ENGINEs that legitimately accept a
    /// subset (e.g. `MODIFY SETTING` for tuning) override `checkAlterIsPossible`
    /// and `alter` selectively.
    void alter(const AlterCommands & commands, ContextPtr context, AlterLockHolder & alter_lock_holder) override;
    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;
    void checkMutationIsPossible(const MutationCommands & commands, const Settings & settings) const override;
    void mutate(const MutationCommands & commands, ContextPtr context) override;

    /// Backup default: rejected. A Reflection is recomputable from its source;
    /// backing it up would duplicate data and risks restoring an index that no
    /// longer matches the restored source. Back up / restore the source table.
    void backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;
    void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;
    bool supportsBackupPartition() const override { return false; }

    /// Lifecycle: DROP TABLE on a Reflection cascades to its inner table.
    /// ENGINEs that hold extra lifetime-coupled state (e.g. a replicated
    /// leader lease) override and call `IReflection::dropInnerTableIfAny`
    /// at the end.
    void drop() override;
    void dropInnerTableIfAny(bool sync, ContextPtr local_context) override;

    /// Catalog wiring: report the inner table as a dependent storage so that
    /// system views and DDL recognise the parent/child relationship.
    std::vector<StorageID> getInnerStorageIDs() const override;

    /// `dynamic_cast<MergeTreeData *>` on a Reflection must fail: a Reflection
    /// is not a MergeTree, even if its inner table happens to be one.
    bool isMergeTree() const override { return false; }

    /// Stats / data-location / health checks: forward to the inner table.
    StoragePolicyPtr getStoragePolicy() const override;
    Strings getDataPaths() const override;
    bool storesDataOnDisk() const override;
    ColumnSizeByName getColumnSizes() const override;
    IndexSizeByName getSecondaryIndexSizes() const override;
    std::optional<UInt64> totalRows(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override;
    std::optional<UInt64> totalBytesUncompressed(const Settings & settings) const override;

    /// Action locks: forward to the inner table so that
    /// `SYSTEM STOP <action>` on the Reflection has the expected effect on
    /// its data.
    ActionLock getActionLock(StorageActionBlockType action_type) override;
    void onActionLockRemove(StorageActionBlockType action_type) override;
};

}
