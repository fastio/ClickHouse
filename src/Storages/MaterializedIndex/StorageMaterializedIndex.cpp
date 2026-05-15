#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>
#include <Storages/MaterializedIndex/MaterializedIndexBuildTask.h>
#include <Storages/MaterializedIndex/MaterializedIndexAlgorithmFactory.h>
#include <Storages/MaterializedIndex/MaterializedIndexCompactTask.h>
#include <Storages/MaterializedIndex/MaterializedIndexContext.h>
#include <Storages/MaterializedIndex/MaterializedIndexPartName.h>
#include <Storages/MaterializedIndex/MaterializedIndexSchedulerPolicy.h>
#include <Storages/MaterializedIndex/MaterializedIndexSelectedEntry.h>
#include <Storages/MaterializedIndex/MaterializedIndexRemapTask.h>
#include <Storages/MaterializedIndex/SnapshotDiffReconciler.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTRenameQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageReplicatedMergeTree.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Common/SipHash.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <limits>
#include <optional>
#include <unordered_map>


namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool assign_part_uuids;
    extern const MergeTreeSettingsUInt64 materialized_index_build_min_bytes;
    extern const MergeTreeSettingsUInt64 materialized_index_build_min_parts;
    extern const MergeTreeSettingsUInt64 materialized_index_build_min_rows;
    extern const MergeTreeSettingsSeconds materialized_index_build_max_delay;
    extern const MergeTreeSettingsUInt64 materialized_index_compact_min_parts;
    extern const MergeTreeSettingsUInt64 materialized_index_max_background_tasks_per_source_table;
    extern const MergeTreeSettingsUInt64 materialized_index_max_global_background_tasks;
    extern const MergeTreeSettingsSeconds materialized_index_resource_failure_backoff;
    extern const MergeTreeSettingsUInt64 materialized_index_size_ratio_percent;
    extern const MergeTreeSettingsUInt64 materialized_index_starvation_protection_cycles;
    extern const MergeTreeSettingsUInt64 materialized_index_sync_timeout;
    extern const MergeTreeSettingsUInt64 materialized_index_task_max_input_bytes;
    extern const MergeTreeSettingsUInt64 materialized_index_task_max_input_rows;
    extern const MergeTreeSettingsUInt64 materialized_index_task_memory_budget_bytes;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}


namespace
{

String makeMaterializedIndexPartName(std::string_view suffix)
{
    static std::atomic<Int64> sequence{0};
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const auto base_block = std::chrono::duration_cast<std::chrono::microseconds>(now).count();
    const auto block = base_block + sequence.fetch_add(1, std::memory_order_relaxed);

    return String{MergeTreePartInfo::MATERIALIZED_INDEX_PART_PREFIX}
        + String{suffix}
        + "_"
        + std::to_string(block)
        + "_"
        + std::to_string(block)
        + "_0";
}

String makeMaterializedIndexCompactPartName(
    const MergeTreeData::DataPartsVector & materialized_index_parts,
    MergeTreeDataFormatVersion format_version)
{
    std::vector<MergeTreePartInfo> part_infos;
    part_infos.reserve(materialized_index_parts.size());
    for (const auto & part : materialized_index_parts)
    {
        if (!part)
            continue;
        part_infos.push_back(part->info);
    }
    return makeMaterializedIndexCompactPartNameFromInfos(part_infos, format_version);
}

/// Minimal concrete subclass of MutationsSnapshotBase so stage-1 can return
/// an "empty, read-only" snapshot without depending on StorageMergeTree's
/// private MutationsSnapshot layout. The three pure-virtual methods answer
/// with empties because MaterializedIndex never produces mutations in
/// stage-1.
struct EmptyMutationsSnapshot final : public MergeTreeData::MutationsSnapshotBase
{
    using Params = MergeTreeData::IMutationsSnapshot::Params;

    EmptyMutationsSnapshot(Params params_, MutationCounters counters_, DataPartsVector patches_)
        : MergeTreeData::MutationsSnapshotBase(std::move(params_), std::move(counters_), std::move(patches_))
    {
    }

    MutationCommands getOnFlyMutationCommandsForPart(const MergeTreeData::DataPartPtr & /*part*/) const override
    {
        return {};
    }

    std::shared_ptr<MergeTreeData::IMutationsSnapshot> cloneEmpty() const override
    {
        return std::make_shared<EmptyMutationsSnapshot>(Params{}, MutationCounters{}, DataPartsVector{});
    }

    NameSet getAllUpdatedColumns() const override
    {
        return {};
    }
};

std::vector<UUID> collectPartUuids(const MergeTreeData::DataPartsVector & parts)
{
    std::vector<UUID> uuids;
    uuids.reserve(parts.size());
    for (const auto & part : parts)
    {
        if (part)
            uuids.push_back(part->uuid);
    }
    return uuids;
}

std::pair<UInt64, UInt64> sumRowsAndBytes(const MergeTreeData::DataPartsVector & parts)
{
    UInt64 rows = 0;
    UInt64 bytes = 0;
    for (const auto & part : parts)
    {
        if (!part)
            continue;
        rows += part->rows_count;
        bytes += part->getBytesOnDisk();
    }
    return {rows, bytes};
}

void updateHashWithUuid(SipHash & hash, const UUID & uuid)
{
    hash.update(UUIDHelpers::getHighBytes(uuid));
    hash.update(UUIDHelpers::getLowBytes(uuid));
}

String makeReplicationTaskKey(const FutureMaterializedIndexPart & future_part, const String & family, const String & impl)
{
    SipHash hash;
    hash.update(family);
    hash.update(impl);
    hash.update(static_cast<UInt8>(future_part.kind));
    hash.update(static_cast<UInt8>(future_part.remap_kind));

    auto update_part_vector = [&hash](const MergeTreeData::DataPartsVector & parts)
    {
        std::vector<UUID> uuids = collectPartUuids(parts);
        std::sort(uuids.begin(), uuids.end());
        for (const auto & uuid : uuids)
            updateHashWithUuid(hash, uuid);
    };

    update_part_vector(future_part.source_parts_snapshot);
    update_part_vector(future_part.affected_materialized_index_parts);
    update_part_vector(future_part.delta_in_source_parts);

    auto delta_out = future_part.delta_out_source_uuids;
    std::sort(delta_out.begin(), delta_out.end());
    for (const auto & uuid : delta_out)
        updateHashWithUuid(hash, uuid);

    return getSipHash128AsHexString(hash);
}

std::string_view materializedIndexTaskKindName(FutureMaterializedIndexPart::Kind kind)
{
    switch (kind)
    {
        case FutureMaterializedIndexPart::Kind::Build:
            return "Build";
        case FutureMaterializedIndexPart::Kind::Remap:
            return "Remap";
        case FutureMaterializedIndexPart::Kind::Compact:
            return "Compact";
    }
    UNREACHABLE();
}

std::mutex materialized_index_task_counters_mutex;
UInt64 global_materialized_index_task_count = 0;
std::unordered_map<String, UInt64> materialized_index_tasks_by_source_table;

ASTPtr makeInnerColumnList()
{
    auto columns = make_intrusive<ASTColumns>();
    auto column_list = make_intrusive<ASTExpressionList>();
    auto marker = make_intrusive<ASTColumnDeclaration>();
    marker->name = "_index_marker";
    marker->setType(make_intrusive<ASTIdentifier>("UInt8"));
    marker->default_specifier = ColumnDefaultSpecifier::Default;
    marker->setDefaultExpression(make_intrusive<ASTLiteral>(UInt64{0}));
    column_list->children.push_back(marker);
    columns->set(columns->columns, column_list);
    return columns;
}

ASTPtr makeInnerStorageDefinition(const String & zookeeper_path, const String & replica_name)
{
    auto storage = make_intrusive<ASTStorage>();
    auto engine = zookeeper_path.empty()
        ? makeASTFunction("MergeTree")
        : makeASTFunction(
            "ReplicatedMergeTree",
            make_intrusive<ASTLiteral>(zookeeper_path),
            make_intrusive<ASTLiteral>(replica_name));
    engine->setKind(ASTFunction::Kind::TABLE_ENGINE);
    storage->set(storage->engine, engine);
    storage->set(storage->order_by, makeASTFunction("tuple"));
    storage->normalizeChildrenOrder();
    return storage;
}

}


String makeMaterializedIndexReplicationTaskKeyForTest(
    const FutureMaterializedIndexPart & future_part,
    const String & family,
    const String & impl)
{
    return makeReplicationTaskKey(future_part, family, impl);
}

StorageMaterializedIndex::StorageMaterializedIndex(
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
    const String & inner_zookeeper_path_,
    const String & inner_replica_name_)
    : MergeTreeData(
          table_id_,
          metadata_,
          context_,
          /*date_column_name=*/ String{},
          MergingParams{},
          std::move(settings_),
          /*require_part_metadata_=*/ true,
          mode)
    , source_table_id(source_table_id_)
    , indexed_columns(indexed_columns_)
    , family(family_)
    , impl(impl_)
    , build_params(build_params_)
    , inner_zookeeper_path(inner_zookeeper_path_)
    , inner_replica_name(inner_replica_name_)
    , cleanup_thread(*this)
{
    initializeDirectoriesAndFormatVersion(relative_data_path_, LoadingStrictnessLevel::ATTACH <= mode, /*date_column_name=*/ String{});
    initializeInnerTable(relative_data_path_, metadata_, nullptr, mode);

    MaterializedIndexContext ctx;
    ctx.source_table_id = source_table_id;
    ctx.indexed_columns = indexed_columns;
    ctx.family = family;
    ctx.impl = impl;
    ctx.query_context = context_;

    algorithm = MaterializedIndexAlgorithmFactory::instance().get(family, impl, build_params, ctx);
    if (algorithm)
        algorithm->initialize(ctx);
}

String StorageMaterializedIndex::generateInnerTableName(const StorageID & index_id)
{
    if (index_id.hasUUID())
        return ".inner_id." + toString(index_id.uuid);
    return ".inner." + index_id.getTableName();
}

void StorageMaterializedIndex::initializeInnerTable(
    const String & /*relative_data_path_*/,
    const StorageInMemoryMetadata & /*metadata_*/,
    std::unique_ptr<MergeTreeSettings> /*settings_*/,
    LoadingStrictnessLevel mode)
{
    const auto index_id = getStorageID();
    StorageID inner_id(index_id.database_name, generateInnerTableName(index_id));
    auto context = getContext();

    if (auto existing = DatabaseCatalog::instance().tryGetTable(inner_id, context))
        inner_table = existing;
    else if (LoadingStrictnessLevel::ATTACH <= mode)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Inner table {} for MaterializedIndex {} is not loaded",
            inner_id.getNameForLogs(),
            index_id.getNameForLogs());
    }
    else
    {
        auto create_context = Context::createCopy(context);
        auto create = make_intrusive<ASTCreateQuery>();
        create->setDatabase(inner_id.database_name);
        create->setTable(inner_id.table_name);
        create->set(create->columns_list, makeInnerColumnList());
        create->set(create->storage, makeInnerStorageDefinition(inner_zookeeper_path, inner_replica_name));

        InterpreterCreateQuery interpreter(create, create_context);
        interpreter.setInternal(true);
        interpreter.execute();
        inner_table = DatabaseCatalog::instance().getTable(inner_id, context);
    }

    inner_data = dynamic_cast<MergeTreeData *>(inner_table.get());
    if (!inner_data)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Inner table {} for MaterializedIndex {} is not MergeTree-family storage",
            inner_id.getNameForLogs(),
            index_id.getNameForLogs());
}

MergeTreeData & StorageMaterializedIndex::getInnerMergeTreeData() const
{
    if (!inner_data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MaterializedIndex {} has no inner MergeTree storage", getStorageID().getNameForLogs());
    return *inner_data;
}


void StorageMaterializedIndex::startup()
{
    /// D-07 defensive recheck: the source table's `assign_part_uuids` may
    /// have been turned off after CREATE. We do not throw here (would
    /// prevent server startup); a warning is enough — Build / Remap will
    /// still see the missing UUIDs and surface failures through the log.
    auto source_storage = DatabaseCatalog::instance().tryGetTable(source_table_id, getContext());
    if (source_storage)
    {
        const auto * source_mt = dynamic_cast<const MergeTreeData *>(source_storage.get());
        if (!source_mt || !(*source_mt->getSettings())[MergeTreeSetting::assign_part_uuids])
            LOG_ERROR(
                log,
                "Source table {} no longer has assign_part_uuids = 1; MaterializedIndex {} will be degraded.",
                source_table_id.getNameForLogs(),
                getStorageID().getNameForLogs());
    }

    getInnerMergeTreeData().clearOldTemporaryDirectories(
        /*custom_directories_lifetime_seconds=*/0,
        {"tmp_materialized_index_build_", "tmp_materialized_index_remap_"});

    refreshCoverageFromActiveParts();
    try
    {
        cleanup_thread.start();
        background_operations_assignee.start();
    }
    catch (...)
    {
        try
        {
            shutdown(/*is_drop=*/false);
        }
        catch (...)
        {
            std::terminate();
        }
        throw;
    }
}

void StorageMaterializedIndex::refreshCoverageFromActiveParts()
{
    /// Walk every active materialized-index-part and ingest its `coverage.json` manifest
    /// into the in-memory `CoverageMap`. Startup and replicated followers
    /// call this so the reconciler does not re-trigger Build / Remap for parts
    /// that already fully cover the source. A malformed manifest is logged but
    /// does not abort refresh — `cleanup_thread` will eventually GC truly
    /// broken parts; until then the reconciler may schedule extra work.
    std::vector<std::pair<UUID, std::vector<CoverageEntry>>> snapshot;
    auto materialized_index_parts = getAccessPathPartsVectorForInternalUsage();
    snapshot.reserve(materialized_index_parts.size());
    for (const auto & part : materialized_index_parts)
    {
        if (!part)
            continue;
        try
        {
            auto entries = parseCoverageJsonFromMiPart(*part);
            snapshot.emplace_back(part->uuid, std::move(entries));
        }
        catch (...)
        {
            tryLogCurrentException(
                log,
                fmt::format("Failed to load coverage.json for materialized-index-part {}", part->name));
        }
    }
    std::lock_guard lock(currently_processing_in_background_mutex);
    coverage_map.replaceAll(snapshot);
    scheduler_state.replaceReadyCoverage(snapshot);
}

void StorageMaterializedIndex::shutdown(bool is_drop)
{
    if (shutdown_called.exchange(true))
        return;
    background_operations_assignee.finish();
    cleanup_thread.stop();
    if (is_drop)
    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        coverage_map.clear();
        scheduler_state.clear();
    }
}

void StorageMaterializedIndex::renameInMemory(const StorageID & new_table_id)
{
    if (inner_table)
    {
        auto old_inner_id = inner_table->getStorageID();
        StorageID new_inner_id(new_table_id.database_name, generateInnerTableName(new_table_id));
        if (old_inner_id.database_name != new_inner_id.database_name || old_inner_id.table_name != new_inner_id.table_name)
        {
            auto rename = make_intrusive<ASTRenameQuery>();
            rename->addElement(
                old_inner_id.database_name,
                old_inner_id.table_name,
                new_inner_id.database_name,
                new_inner_id.table_name);
            InterpreterRenameQuery(rename, getContext()).execute();
            inner_table = DatabaseCatalog::instance().getTable(new_inner_id, getContext());
            inner_data = dynamic_cast<MergeTreeData *>(inner_table.get());
        }
    }

    MergeTreeData::renameInMemory(new_table_id);
}

void StorageMaterializedIndex::drop()
{
    dropInnerTableIfAny(/*sync=*/false, getContext());
}

void StorageMaterializedIndex::dropInnerTableIfAny(bool sync, ContextPtr local_context)
{
    if (!inner_table)
        return;

    /// Release our own reference to the inner storage before issuing the (possibly
    /// synchronous) DROP. Otherwise `waitTableFinallyDropped` keeps spinning because
    /// our `inner_table` shared_ptr keeps the storage alive after the catalog removes
    /// it, producing a self-deadlock that hangs `DROP TABLE mi_<index> SYNC`.
    auto inner_id = inner_table->getStorageID();
    inner_table.reset();
    inner_data = nullptr;

    if (DatabaseCatalog::instance().tryGetTable(inner_id, getContext()))
    {
        InterpreterDropQuery::executeDropQuery(
            ASTDropQuery::Kind::Drop,
            getContext(),
            local_context,
            inner_id,
            sync,
            /*ignore_sync_setting=*/true,
            /*need_ddl_guard=*/false);
    }
}

void StorageMaterializedIndex::backupData(
    BackupEntriesCollector & backup_entries_collector,
    const String & data_path_in_backup,
    const std::optional<ASTs> & partitions)
{
    if (inner_table)
        inner_table->backupData(backup_entries_collector, data_path_in_backup, partitions);
}

void StorageMaterializedIndex::restoreDataFromBackup(
    RestorerFromBackup & restorer,
    const String & data_path_in_backup,
    const std::optional<ASTs> & partitions)
{
    if (inner_table)
        inner_table->restoreDataFromBackup(restorer, data_path_in_backup, partitions);
}

bool StorageMaterializedIndex::supportsBackupPartition() const
{
    return inner_table && inner_table->supportsBackupPartition();
}

void StorageMaterializedIndex::recordBuildCommit(UUID materialized_index_part_uuid, const std::vector<CoverageEntry> & entries)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    coverage_map.appendFromBuild(materialized_index_part_uuid, entries);
    scheduler_state.appendReadyCoverage(materialized_index_part_uuid, entries);
}

void StorageMaterializedIndex::recordRemapCommit(
    UUID new_materialized_index_part_uuid,
    UUID retired_materialized_index_part_uuid,
    const std::vector<CoverageEntry> & incoming,
    const std::vector<UUID> & outgoing_source_uuids)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    coverage_map.applyRemap(new_materialized_index_part_uuid, retired_materialized_index_part_uuid, incoming, outgoing_source_uuids);
    scheduler_state.applyRemap(new_materialized_index_part_uuid, retired_materialized_index_part_uuid, incoming, outgoing_source_uuids);
}

void StorageMaterializedIndex::recordCompactCommit(
    UUID new_materialized_index_part_uuid,
    const std::vector<UUID> & retired_materialized_index_part_uuids,
    const std::vector<CoverageEntry> & incoming)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    coverage_map.applyCompact(new_materialized_index_part_uuid, retired_materialized_index_part_uuids, incoming);
    scheduler_state.applyCompact(new_materialized_index_part_uuid, retired_materialized_index_part_uuids, incoming);
}

void StorageMaterializedIndex::setReplicatedLeaderLeaseForNextTask(String lease_path, String lease_payload)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    pending_replicated_leader_lease_path = std::move(lease_path);
    pending_replicated_leader_lease_payload = std::move(lease_payload);
}

void StorageMaterializedIndex::releasePendingReplicatedLeaderLease() noexcept
{
    String lease_path;
    String lease_payload;
    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        lease_path.swap(pending_replicated_leader_lease_path);
        lease_payload.swap(pending_replicated_leader_lease_payload);
    }

    if (lease_path.empty() || lease_payload.empty())
        return;

    if (auto * replicated = dynamic_cast<StorageReplicatedMergeTree *>(inner_table.get()))
        replicated->releaseMaterializedIndexLeaderLease(lease_path, lease_payload);
}

StorageMaterializedIndex::ObservabilitySnapshot StorageMaterializedIndex::getObservabilitySnapshot() const
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    auto scheduler_snapshot = scheduler_state.getObservabilitySnapshot();

    ObservabilitySnapshot snapshot;
    snapshot.backlog_rows = scheduler_snapshot.backlog.rows;
    snapshot.backlog_bytes = scheduler_snapshot.backlog.bytes;
    snapshot.backlog_parts = scheduler_snapshot.backlog.parts;
    snapshot.pending_task_count = scheduler_snapshot.pending_task_count;
    snapshot.ready_materialized_index_part_count = scheduler_snapshot.ready_materialized_index_part_count;
    snapshot.obsolete_ready_source_count = scheduler_snapshot.obsolete_ready_source_count;
    snapshot.repeated_failure_count = scheduler_snapshot.repeated_failure_count;
    snapshot.retry_count = scheduler_snapshot.retry_count;
    snapshot.next_retry_time = scheduler_snapshot.next_retry_time;
    snapshot.last_error = scheduler_snapshot.last_error;
    return snapshot;
}

UInt64 StorageMaterializedIndex::getTaskMemoryBudgetBytes() const
{
    return (*getSettings())[MergeTreeSetting::materialized_index_task_memory_budget_bytes];
}

UInt64 StorageMaterializedIndex::estimateBuildOutputBytes(UInt64 input_rows, UInt64 input_bytes) const
{
    UInt64 estimate = 0;
    if (algorithm)
        estimate = algorithm->estimateBuildBytes(input_bytes, input_rows);
    if (estimate != 0)
        return estimate;

    const UInt64 ratio_percent = (*getSettings())[MergeTreeSetting::materialized_index_size_ratio_percent];
    if (ratio_percent == 0 || input_bytes == 0)
        return input_bytes;
    if (input_bytes > std::numeric_limits<UInt64>::max() / ratio_percent)
        return input_bytes;
    return std::max<UInt64>(1, input_bytes * ratio_percent / 100);
}

void StorageMaterializedIndex::postponeForResourceFailure(const String & reason)
{
    const UInt64 backoff_seconds = (*getSettings())[MergeTreeSetting::materialized_index_resource_failure_backoff].totalSeconds();
    std::lock_guard lock(currently_processing_in_background_mutex);
    scheduler_state.postponeForResourceFailure(reason, std::chrono::seconds(backoff_seconds));
}

bool StorageMaterializedIndex::tryAcquireTaskResources(FutureMaterializedIndexPart & future_part, UInt64 input_rows, UInt64 input_bytes)
{
    const auto settings = getSettings();
    const UInt64 max_rows = (*settings)[MergeTreeSetting::materialized_index_task_max_input_rows];
    const UInt64 max_bytes = (*settings)[MergeTreeSetting::materialized_index_task_max_input_bytes];
    if (max_rows != 0 && input_rows > max_rows)
    {
        postponeForResourceFailure(fmt::format("input rows {} exceed limit {}", input_rows, max_rows));
        return false;
    }
    if (max_bytes != 0 && input_bytes > max_bytes)
    {
        postponeForResourceFailure(fmt::format("input bytes {} exceed limit {}", input_bytes, max_bytes));
        return false;
    }

    const UInt64 max_global = (*settings)[MergeTreeSetting::materialized_index_max_global_background_tasks];
    const UInt64 max_per_source = (*settings)[MergeTreeSetting::materialized_index_max_background_tasks_per_source_table];
    const String source_key = source_table_id.getFullTableName();

    String failure_reason;
    {
        std::lock_guard counters_lock(materialized_index_task_counters_mutex);
        if (max_global != 0 && global_materialized_index_task_count >= max_global)
            failure_reason = fmt::format("global MaterializedIndex task limit {} reached", max_global);
        else
        {
            const UInt64 current_for_source = materialized_index_tasks_by_source_table[source_key];
            if (max_per_source != 0 && current_for_source >= max_per_source)
                failure_reason = fmt::format("MaterializedIndex task limit {} reached for source table {}", max_per_source, source_key);
            else
            {
                ++global_materialized_index_task_count;
                ++materialized_index_tasks_by_source_table[source_key];
                future_part.source_table_key = source_key;
                future_part.resource_accounted = true;
            }
        }
    }

    if (!failure_reason.empty())
    {
        postponeForResourceFailure(failure_reason);
        return false;
    }

    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        scheduler_state.clearResourceBackoff();
    }
    return true;
}

void StorageMaterializedIndex::releaseTaskResources(FutureMaterializedIndexPart & future_part) noexcept
{
    if (!future_part.resource_accounted)
        return;

    try
    {
        std::lock_guard lock(materialized_index_task_counters_mutex);
        if (global_materialized_index_task_count != 0)
            --global_materialized_index_task_count;

        auto it = materialized_index_tasks_by_source_table.find(future_part.source_table_key);
        if (it != materialized_index_tasks_by_source_table.end())
        {
            if (it->second != 0)
                --it->second;
            if (it->second == 0)
                materialized_index_tasks_by_source_table.erase(it);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to release MaterializedIndex resource counters");
    }

    future_part.resource_accounted = false;
}

bool StorageMaterializedIndex::tryReserveReplicatedTask(FutureMaterializedIndexPart & future_part)
{
    auto * replicated = dynamic_cast<StorageReplicatedMergeTree *>(inner_table.get());
    if (!replicated)
        return true;

    String payload = fmt::format(
        "task_id={}\npart={}\nreplica={}\n",
        future_part.task_id,
        future_part.new_part_name,
        replicated->getReplicaName());
    const String key = makeReplicationTaskKey(future_part, family, impl);
    if (replicated->tryReserveMaterializedIndexTask(key, payload, future_part.replicated_task_lock_path))
    {
        future_part.replicated_task_lock_payload = std::move(payload);
        return true;
    }

    LOG_TRACE(
        log,
        "Cannot reserve MaterializedIndex {} task {} for part {}: Keeper task {} already exists",
        materializedIndexTaskKindName(future_part.kind),
        future_part.task_id,
        future_part.new_part_name,
        key);
    future_part.replicated_task_lock_path.clear();
    future_part.replicated_task_lock_payload.clear();
    return false;
}

void StorageMaterializedIndex::releaseReplicatedTaskReservation(FutureMaterializedIndexPart & future_part) noexcept
{
    auto * replicated = dynamic_cast<StorageReplicatedMergeTree *>(inner_table.get());
    if (replicated)
        replicated->releaseMaterializedIndexTask(future_part.replicated_task_lock_path, future_part.replicated_task_lock_payload);
    future_part.replicated_task_lock_path.clear();
    future_part.replicated_task_lock_payload.clear();
}

void StorageMaterializedIndex::releaseReplicatedLeaderLease(FutureMaterializedIndexPart & future_part) noexcept
{
    auto * replicated = dynamic_cast<StorageReplicatedMergeTree *>(inner_table.get());
    if (replicated)
        replicated->releaseMaterializedIndexLeaderLease(
            future_part.replicated_leader_lease_path,
            future_part.replicated_leader_lease_payload);
    future_part.replicated_leader_lease_path.clear();
    future_part.replicated_leader_lease_payload.clear();
}

void StorageMaterializedIndex::assertReplicatedTaskReservation(const FutureMaterializedIndexPart & future_part) const
{
    auto * replicated = dynamic_cast<StorageReplicatedMergeTree *>(inner_table.get());
    if (!replicated)
        return;

    replicated->assertMaterializedIndexLeaderLease(
        future_part.replicated_leader_lease_path,
        future_part.replicated_leader_lease_payload);
    replicated->assertMaterializedIndexTaskReservation(
        future_part.replicated_task_lock_path,
        future_part.replicated_task_lock_payload);
}

void StorageMaterializedIndex::refreshBuildBacklog(
    const DataPartsVector & uncovered_source_parts,
    const std::unordered_set<UUID> & covered_source_uuids)
{
    std::unordered_set<UUID> active_uncovered_uuids;
    active_uncovered_uuids.reserve(uncovered_source_parts.size());

    const auto now = std::chrono::steady_clock::now();
    for (const auto & part : uncovered_source_parts)
    {
        if (!part)
            continue;

        active_uncovered_uuids.insert(part->uuid);
        UncoveredSourceBacklogEntry entry;
        entry.part = part;
        entry.first_seen = now;
        entry.rows = part->rows_count;
        entry.bytes = part->getBytesOnDisk();

        auto [it, inserted] = uncovered_source_backlog.emplace(
            part->uuid,
            std::move(entry));

        if (!inserted)
        {
            it->second.part = part;
            it->second.rows = part->rows_count;
            it->second.bytes = part->getBytesOnDisk();
        }
    }

    for (auto it = uncovered_source_backlog.begin(); it != uncovered_source_backlog.end();)
    {
        if (covered_source_uuids.contains(it->first) || !active_uncovered_uuids.contains(it->first))
            it = uncovered_source_backlog.erase(it);
        else
            ++it;
    }

    MaterializedIndexSchedulerState::BacklogStats stats;
    stats.parts = uncovered_source_backlog.size();
    for (const auto & [_, entry] : uncovered_source_backlog)
    {
        stats.rows += entry.rows;
        stats.bytes += entry.bytes;
    }

    std::lock_guard lock(currently_processing_in_background_mutex);
    scheduler_state.setBacklogStats(stats);
}

StorageMaterializedIndex::DataPartsVector StorageMaterializedIndex::selectBuildBatchFromBacklog(
    const DataPartsVector & candidate_source_parts,
    bool initial_build,
    bool force_build)
{
    if (candidate_source_parts.empty() || uncovered_source_backlog.empty())
        return {};

    DataPartsVector batch;
    batch.reserve(candidate_source_parts.size());

    UInt64 rows = 0;
    UInt64 bytes = 0;
    std::optional<std::chrono::steady_clock::time_point> oldest_seen;

    for (const auto & part : candidate_source_parts)
    {
        if (!part)
            continue;

        auto it = uncovered_source_backlog.find(part->uuid);
        if (it == uncovered_source_backlog.end())
            continue;

        batch.push_back(part);
        rows += it->second.rows;
        bytes += it->second.bytes;
        if (!oldest_seen || it->second.first_seen < *oldest_seen)
            oldest_seen = it->second.first_seen;
    }

    if (batch.empty())
        return {};

    if (initial_build || force_build)
        return batch;

    const auto settings = getSettings();
    const UInt64 min_rows = (*settings)[MergeTreeSetting::materialized_index_build_min_rows];
    const UInt64 min_bytes = (*settings)[MergeTreeSetting::materialized_index_build_min_bytes];
    const UInt64 min_parts = (*settings)[MergeTreeSetting::materialized_index_build_min_parts];
    const UInt64 max_delay_seconds = (*settings)[MergeTreeSetting::materialized_index_build_max_delay].totalSeconds();

    bool should_build = false;
    should_build |= min_rows != 0 && rows >= min_rows;
    should_build |= min_bytes != 0 && bytes >= min_bytes;
    should_build |= min_parts != 0 && batch.size() >= min_parts;

    if (!should_build && max_delay_seconds != 0 && oldest_seen)
    {
        const auto max_delay = std::chrono::seconds(max_delay_seconds);
        should_build = std::chrono::steady_clock::now() - *oldest_seen >= max_delay;
    }

    if (!should_build)
        return {};

    return batch;
}

bool StorageMaterializedIndex::tryReserveFuturePart(FutureMaterializedIndexPart & future_part)
{
    if (future_part.task_id.empty())
        future_part.task_id = toString(future_part.new_part_uuid);

    std::lock_guard lock(currently_processing_in_background_mutex);
    if (currently_building_materialized_index_parts.contains(future_part.new_part_name))
    {
        LOG_DEBUG(
            log,
            "Cannot reserve MaterializedIndex {} task {} for part {}: part name is already reserved",
            materializedIndexTaskKindName(future_part.kind),
            future_part.task_id,
            future_part.new_part_name);
        return false;
    }
    if (future_part.kind == FutureMaterializedIndexPart::Kind::Build)
    {
        if (scheduler_state.hasActiveTaskKind(MaterializedIndexSchedulerState::TaskKind::CompactRebuild)
            || scheduler_state.hasActiveTaskKind(MaterializedIndexSchedulerState::TaskKind::CompactMerge))
        {
            LOG_DEBUG(
                log,
                "Cannot reserve MaterializedIndex Build task {} for part {}: a Compact task is already active",
                future_part.task_id,
                future_part.new_part_name);
            return false;
        }
    }
    else if (future_part.kind == FutureMaterializedIndexPart::Kind::Remap)
    {
        if (scheduler_state.hasActiveTaskKind(MaterializedIndexSchedulerState::TaskKind::CompactRebuild)
            || scheduler_state.hasActiveTaskKind(MaterializedIndexSchedulerState::TaskKind::CompactMerge))
        {
            LOG_DEBUG(
                log,
                "Cannot reserve MaterializedIndex Remap task {} for part {}: a Compact task is already active",
                future_part.task_id,
                future_part.new_part_name);
            return false;
        }
    }
    else if (scheduler_state.hasActiveTasks())
    {
        LOG_DEBUG(
            log,
            "Cannot reserve MaterializedIndex {} task {} for part {}: another task is already active",
            materializedIndexTaskKindName(future_part.kind),
            future_part.task_id,
            future_part.new_part_name);
        return false;
    }

    bool reserved = false;
    if (future_part.kind == FutureMaterializedIndexPart::Kind::Build)
    {
        reserved = scheduler_state.reserveBuildBatch(
            future_part.task_id,
            collectPartUuids(future_part.source_parts_snapshot),
            future_part.new_part_uuid);
    }
    else if (future_part.kind == FutureMaterializedIndexPart::Kind::Compact)
    {
        reserved = scheduler_state.reserveCompactRebuild(
            future_part.task_id,
            collectPartUuids(future_part.affected_materialized_index_parts),
            collectPartUuids(future_part.source_parts_snapshot),
            future_part.new_part_uuid);
    }
    else
    {
        reserved = scheduler_state.reserveRemapLineage(
            future_part.task_id,
            collectPartUuids(future_part.affected_materialized_index_parts),
            future_part.delta_out_source_uuids,
            future_part.new_part_uuid);
    }

    if (!reserved)
    {
        LOG_DEBUG(
            log,
            "Cannot reserve MaterializedIndex {} task {} for part {}: scheduler state rejected the reservation "
            "(source_parts={}, materialized_index_parts={}, delta_in_parts={}, delta_out_sources={})",
            materializedIndexTaskKindName(future_part.kind),
            future_part.task_id,
            future_part.new_part_name,
            future_part.source_parts_snapshot.size(),
            future_part.affected_materialized_index_parts.size(),
            future_part.delta_in_source_parts.size(),
            future_part.delta_out_source_uuids.size());
        return false;
    }

    future_part.scheduler_reserved = true;
    if (!tryReserveReplicatedTask(future_part))
    {
        scheduler_state.releaseTask(future_part.task_id);
        future_part.scheduler_reserved = false;
        return false;
    }
    future_part.replicated_leader_lease_path.swap(pending_replicated_leader_lease_path);
    future_part.replicated_leader_lease_payload.swap(pending_replicated_leader_lease_payload);
    return reserved;
}

bool StorageMaterializedIndex::shouldScheduleCompactRebuild(
    const DataPartsVector & source_snapshot,
    const DataPartsVector & materialized_index_snapshot,
    const std::unordered_set<UUID> & covered_source_uuids) const
{
    const UInt64 min_parts = (*getSettings())[MergeTreeSetting::materialized_index_compact_min_parts];
    if (min_parts == 0 || materialized_index_snapshot.size() < min_parts)
        return false;

    if (source_snapshot.empty())
        return false;

    for (const auto & part : source_snapshot)
    {
        if (!part || !covered_source_uuids.contains(part->uuid))
            return false;
    }

    return true;
}

bool StorageMaterializedIndex::scheduleDataProcessingJob(BackgroundJobsAssignee & assignee)
{
    if (shutdown_called.load(std::memory_order_relaxed))
        return false;

    cleanup_thread.wakeupEarlierIfNeeded();

    auto context = getContext();
    auto source_storage = DatabaseCatalog::instance().tryGetTable(source_table_id, context);
    if (!source_storage)
        return false;
    const auto * source_mt = dynamic_cast<const MergeTreeData *>(source_storage.get());
    if (!source_mt)
        return false;

    /// I-BG-14: pull source / materialized_index snapshots once per cycle.
    auto source_snapshot = source_mt->getDataPartsVectorForInternalUsage();
    auto materialized_index_snapshot = getAccessPathPartsVectorForInternalUsage();

    std::unordered_map<UUID, CoverageEntry> coverage_by_source_uuid;
    std::unordered_map<UUID, std::vector<CoverageEntry>> coverage_by_materialized_index_part_uuid;
    std::unordered_set<UUID> active_build_source_uuids;
    bool has_active_tasks = false;
    {
        /// Build and Remap tasks can overlap: Build writes new MI parts while
        /// Remap replaces already-ready MI parts. Remap tasks can also
        /// overlap when their reserved MI/source UUIDs do not intersect.
        /// Compact still stays exclusive because it rewrites a whole ready MI part set.
        std::lock_guard lock(currently_processing_in_background_mutex);
        if (scheduler_state.isResourceBackoffActive()
            || scheduler_state.hasActiveTaskKind(MaterializedIndexSchedulerState::TaskKind::CompactRebuild)
            || scheduler_state.hasActiveTaskKind(MaterializedIndexSchedulerState::TaskKind::CompactMerge))
            return false;

        has_active_tasks = scheduler_state.hasActiveTasks() || !currently_building_materialized_index_parts.empty();

        scheduler_state.refreshSources(source_snapshot);
        active_build_source_uuids = scheduler_state.activeBuildSourceUuids();
        coverage_by_source_uuid = coverage_map.coverageEntriesBySourceUuid();
        coverage_by_materialized_index_part_uuid = coverage_map.coverageEntriesByMiPartUuid();
    }

    if (!active_build_source_uuids.empty())
    {
        auto & pending_build_entries = coverage_by_materialized_index_part_uuid[UUID{}];
        for (const auto & part : source_snapshot)
        {
            if (!part || !active_build_source_uuids.contains(part->uuid) || coverage_by_source_uuid.contains(part->uuid))
                continue;

            CoverageEntry entry;
            entry.source_part_uuid = part->uuid;
            entry.rows = part->rows_count;
            entry.source_part_name = part->name;
            entry.partition_id = part->info.getPartitionId();
            entry.min_block = part->info.min_block;
            entry.max_block = part->info.max_block;
            entry.level = part->info.level;
            entry.mutation = part->info.mutation;
            entry.has_part_info = true;
            coverage_by_source_uuid.emplace(entry.source_part_uuid, entry);
            pending_build_entries.push_back(std::move(entry));
        }
    }

    /// Scheduler coverage view: active MI manifests plus source UUIDs reserved
    /// by running Build tasks. Pending Build coverage is synthetic here so
    /// Remap can proceed concurrently; query-time coverage still comes only
    /// from committed `coverage.json` manifests.
    std::unordered_set<UUID> coverage;
    coverage.reserve(coverage_by_source_uuid.size());
    for (const auto & [uuid, _] : coverage_by_source_uuid)
        coverage.insert(uuid);

    auto reconciled = SnapshotDiffReconciler::run(source_snapshot, materialized_index_snapshot, coverage_by_materialized_index_part_uuid);
    auto build_candidates = reconciled.build_batch.source_parts;
    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        build_candidates.erase(
            std::remove_if(
                build_candidates.begin(),
                build_candidates.end(),
                [&](const auto & part)
                {
                    return !part || scheduler_state.isSourceReserved(part->uuid);
                }),
            build_candidates.end());
    }
    refreshBuildBacklog(build_candidates, coverage);

    const size_t starvation_threshold
        = (*getSettings())[MergeTreeSetting::materialized_index_starvation_protection_cycles];
    const bool force_build
        = starvation_threshold != 0
        && consecutive_remap_count.load(std::memory_order_relaxed) >= starvation_threshold
        && reconciled.candidate_kind == ReconcileCandidateKind::BuildBatch;
    const bool initial_build = materialized_index_snapshot.empty() && !source_snapshot.empty();
    auto build_batch = selectBuildBatchFromBacklog(
        build_candidates,
        initial_build,
        force_build);
    const bool compact_rebuild_candidate
        = !has_active_tasks
        && reconciled.candidate_kind == ReconcileCandidateKind::Nothing
        && shouldScheduleCompactRebuild(source_snapshot, materialized_index_snapshot, coverage);
    auto decision = MaterializedIndexSchedulerPolicy::choose(
        reconciled,
        std::move(build_batch),
        compact_rebuild_candidate,
        source_snapshot,
        materialized_index_snapshot);
    const bool compact_decision = decision.kind == MaterializedIndexSchedulerDecisionKind::RebuildSourcePart
        || decision.kind == MaterializedIndexSchedulerDecisionKind::CompactRebuild
        || decision.kind == MaterializedIndexSchedulerDecisionKind::CompactMerge;
    if (has_active_tasks && compact_decision)
        return false;

    auto build_callback = [](bool /*delay*/) {};
    auto metadata_snapshot = getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);
    auto storage_snapshot
        = source_mt->getStorageSnapshotWithoutData(
            source_mt->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false), context);

    auto submit_build = [&](DataPartsVector selected_source_parts)
    {
        auto fp = std::make_shared<FutureMaterializedIndexPart>();
        fp->kind = FutureMaterializedIndexPart::Kind::Build;
        fp->new_part_name = makeMaterializedIndexPartName("build");
        fp->new_part_uuid = UUIDHelpers::generateV4();
        fp->task_id = toString(fp->new_part_uuid);
        fp->source_parts_snapshot = selected_source_parts;

        const auto [input_rows, input_bytes] = sumRowsAndBytes(selected_source_parts);
        if (!tryAcquireTaskResources(*fp, input_rows, input_bytes))
            return false;

        if (!tryReserveFuturePart(*fp))
        {
            releaseTaskResources(*fp);
            return false;
        }

        auto tagger = std::make_unique<CurrentlyBuildingMaterializedIndexPartTagger>(fp, *this);
        auto entry = std::make_shared<MaterializedIndexBuildSelectedEntry>(fp, std::move(tagger));

        consecutive_remap_count.store(0, std::memory_order_relaxed);

        auto task = std::make_shared<MaterializedIndexBuildTask>(
            *this,
            std::move(entry),
            selected_source_parts,
            source_mt,
            storage_snapshot,
            metadata_snapshot,
            context,
            getTaskMemoryBudgetBytes(),
            estimateBuildOutputBytes(input_rows, input_bytes),
            build_callback);

        const bool scheduled = assignee.scheduleCommonTask(task, /*need_trigger=*/true);
        if (scheduled)
        {
            for (const auto & part : selected_source_parts)
            {
                if (part)
                    uncovered_source_backlog.erase(part->uuid);
            }
        }
        return scheduled;
    };

    auto submit_remap = [&](
        MaterializedIndexRemapKind remap_kind,
        DataPartsVector affected_materialized_index_parts,
        DataPartsVector delta_in_source_parts,
        std::vector<UUID> delta_out_source_uuids)
    {
        auto fp = std::make_shared<FutureMaterializedIndexPart>();
        fp->kind = FutureMaterializedIndexPart::Kind::Remap;
        fp->remap_kind = remap_kind;
        fp->new_part_name = makeMaterializedIndexPartName("remap");
        fp->new_part_uuid = UUIDHelpers::generateV4();
        fp->task_id = toString(fp->new_part_uuid);
        fp->affected_materialized_index_parts = affected_materialized_index_parts;
        fp->delta_in_source_parts = delta_in_source_parts;
        fp->delta_out_source_uuids = delta_out_source_uuids;

        const auto [input_rows, input_bytes] = sumRowsAndBytes(fp->affected_materialized_index_parts);
        if (!tryAcquireTaskResources(*fp, input_rows, input_bytes))
            return false;

        if (!tryReserveFuturePart(*fp))
        {
            releaseTaskResources(*fp);
            return false;
        }

        auto tagger = std::make_unique<CurrentlyBuildingMaterializedIndexPartTagger>(fp, *this);
        auto entry = std::make_shared<MaterializedIndexRemapSelectedEntry>(fp, std::move(tagger));

        consecutive_remap_count.fetch_add(1, std::memory_order_relaxed);

        auto task = std::make_shared<MaterializedIndexRemapTask>(
            *this,
            std::move(entry),
            fp->affected_materialized_index_parts,
            fp->delta_in_source_parts,
            fp->delta_out_source_uuids,
            fp->remap_kind,
            source_mt,
            storage_snapshot,
            context,
            getTaskMemoryBudgetBytes(),
            build_callback);

        return assignee.scheduleCommonTask(task, /*need_trigger=*/true);
    };

    auto submit_compact = [&](DataPartsVector selected_source_parts, DataPartsVector affected_materialized_index_parts)
    {
        /// Defensive: a compact requires at least one materialized-index part to
        /// fold into the new compact name. Upstream paths (Reconciler /
        /// SchedulerPolicy) should never hand us an empty set, but if they ever
        /// do, decline rather than tripping `makeMaterializedIndexCompactPartName`'s
        /// LOGICAL_ERROR contract and aborting the server.
        if (affected_materialized_index_parts.empty())
            return false;

        auto fp = std::make_shared<FutureMaterializedIndexPart>();
        fp->kind = FutureMaterializedIndexPart::Kind::Compact;
        fp->source_parts_snapshot = selected_source_parts;
        fp->affected_materialized_index_parts = affected_materialized_index_parts;
        fp->new_part_name = makeMaterializedIndexCompactPartName(
            fp->affected_materialized_index_parts,
            getInnerMergeTreeData().format_version);
        fp->new_part_uuid = UUIDHelpers::generateV4();
        fp->task_id = toString(fp->new_part_uuid);

        const auto [input_rows, input_bytes] = sumRowsAndBytes(fp->source_parts_snapshot);
        if (!tryAcquireTaskResources(*fp, input_rows, input_bytes))
            return false;

        if (!tryReserveFuturePart(*fp))
        {
            releaseTaskResources(*fp);
            return false;
        }

        auto tagger = std::make_unique<CurrentlyBuildingMaterializedIndexPartTagger>(fp, *this);
        auto entry = std::make_shared<MaterializedIndexBuildSelectedEntry>(fp, std::move(tagger));

        auto task = std::make_shared<MaterializedIndexCompactTask>(
            *this,
            std::move(entry),
            fp->source_parts_snapshot,
            fp->affected_materialized_index_parts,
            source_mt,
            storage_snapshot,
            metadata_snapshot,
            context,
            getTaskMemoryBudgetBytes(),
            estimateBuildOutputBytes(input_rows, input_bytes),
            build_callback);

        return assignee.scheduleCommonTask(task, /*need_trigger=*/true);
    };

    switch (decision.kind)
    {
        case MaterializedIndexSchedulerDecisionKind::BuildBatch:
            return submit_build(std::move(decision.source_parts));
        case MaterializedIndexSchedulerDecisionKind::RemapLineage:
        case MaterializedIndexSchedulerDecisionKind::ObsoleteCoverageCleanup:
            return submit_remap(
                decision.remap_kind,
                std::move(decision.materialized_index_parts),
                std::move(decision.source_parts),
                std::move(decision.delta_out_source_uuids));
        case MaterializedIndexSchedulerDecisionKind::RebuildSourcePart:
        case MaterializedIndexSchedulerDecisionKind::CompactRebuild:
        case MaterializedIndexSchedulerDecisionKind::CompactMerge:
            return submit_compact(
                std::move(decision.source_parts),
                std::move(decision.materialized_index_parts));
        case MaterializedIndexSchedulerDecisionKind::Nothing:
            return false;
    }

    return false;
}

bool StorageMaterializedIndex::partIsAssignedToBackgroundOperation(const DataPartPtr & part) const
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    return currently_building_materialized_index_parts.contains(part->name);
}

DataPartsVector StorageMaterializedIndex::getAccessPathPartsVectorForInternalUsage() const
{
    return getInnerMergeTreeData().getDataPartsVectorForInternalUsage(
        {DataPartState::Active},
        {MergeTreePartInfo::Kind::MaterializedIndex});
}

void StorageMaterializedIndex::read(
    QueryPlan & /*query_plan*/,
    const Names & /*column_names*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Direct SELECT from a MaterializedIndex table is not supported");
}

SinkToStoragePtr StorageMaterializedIndex::write(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/, bool /*async_insert*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Direct INSERT into a MaterializedIndex table is not supported");
}

StorageMaterializedIndex::MutationsSnapshotPtr StorageMaterializedIndex::getMutationsSnapshot(const IMutationsSnapshot::Params & params) const
{
    // An empty snapshot is enough: stage-1 never produces mutations for a
    // MaterializedIndex. The concrete EmptyMutationsSnapshot subclass below
    // stubs the three pure-virtual methods with empty returns.
    return std::make_shared<EmptyMutationsSnapshot>(params, MutationCounters{}, DataPartsVector{});
}

void StorageMaterializedIndex::dropPartNoWaitNoThrow(const String & /*part_name*/)
{
}

void StorageMaterializedIndex::dropPart(const String & /*part_name*/, bool /*detach*/, ContextPtr /*context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DROP PART is not supported for MaterializedIndex yet");
}

void StorageMaterializedIndex::dropPartition(const ASTPtr & /*partition*/, bool /*detach*/, ContextPtr /*context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "DROP PARTITION is not supported for MaterializedIndex yet");
}

PartitionCommandsResultInfo StorageMaterializedIndex::attachPartition(const PartitionCommand & /*command*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*query_context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ATTACH PARTITION is not supported for MaterializedIndex yet");
}

void StorageMaterializedIndex::replacePartitionFrom(const StoragePtr & /*source_table*/, const ASTPtr & /*partition*/, bool /*replace*/, ContextPtr /*context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "REPLACE PARTITION is not supported for MaterializedIndex yet");
}

void StorageMaterializedIndex::movePartitionToTable(const StoragePtr & /*dest_table*/, const ASTPtr & /*partition*/, ContextPtr /*context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MOVE PARTITION is not supported for MaterializedIndex yet");
}

void StorageMaterializedIndex::attachRestoredParts(MutableDataPartsVector && /*parts*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Restoring parts is not supported for MaterializedIndex yet");
}

std::unique_ptr<MergeTreeSettings> StorageMaterializedIndex::getDefaultSettings() const
{
    return std::make_unique<MergeTreeSettings>(getContext()->getMergeTreeSettings());
}

std::vector<CoverageEntry> StorageMaterializedIndex::parseCoverageJsonFromMiPart(const IMergeTreeDataPart & part)
{
    const auto & part_storage = part.getDataPartStorage();
    if (!part_storage.existsFile("coverage.json"))
        return {};

    auto reader = part_storage.readFile("coverage.json", ReadSettings{}, /*read_hint=*/std::nullopt);
    String body;
    readStringUntilEOF(body, *reader);

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var parsed;
    try
    {
        parsed = parser.parse(body);
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Failed to parse coverage.json for materialized-index-part {}: {}", part.name, e.displayText());
    }

    auto root = parsed.extract<Poco::JSON::Object::Ptr>();
    if (!root)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "coverage.json for materialized-index-part {} is not a JSON object", part.name);

    if (root->getValue<int>("format_version") != 1)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Unsupported coverage.json format_version for materialized-index-part {}", part.name);

    std::vector<CoverageEntry> result;
    auto covered = root->getArray("covered");
    if (!covered)
        return result;

    result.reserve(covered->size());
    for (size_t i = 0; i < covered->size(); ++i)
    {
        auto item = covered->getObject(static_cast<unsigned int>(i));
        if (!item)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "coverage.json[{}] for materialized-index-part {} is not an object", i, part.name);

        const auto uuid_text = item->getValue<std::string>("source_part_uuid");
        UUID uuid;
        if (!tryParse(uuid, uuid_text))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "coverage.json[{}].source_part_uuid is not a valid UUID for materialized-index-part {}", i, part.name);

        CoverageEntry entry;
        entry.source_part_uuid = uuid;
        entry.rows = static_cast<UInt64>(item->getValue<Int64>("rows"));
        if (item->has("source_part_name"))
            entry.source_part_name = item->getValue<std::string>("source_part_name");

        if (item->has("partition_id")
            && item->has("min_block")
            && item->has("max_block")
            && item->has("level")
            && item->has("mutation"))
        {
            entry.partition_id = item->getValue<std::string>("partition_id");
            entry.min_block = item->getValue<Int64>("min_block");
            entry.max_block = item->getValue<Int64>("max_block");
            entry.level = item->getValue<UInt32>("level");
            entry.mutation = item->getValue<Int64>("mutation");
            entry.has_part_info = true;
        }

        result.push_back(std::move(entry));
    }
    return result;
}

bool StorageMaterializedIndex::waitForCoverageOfSourceOrTimeout(std::chrono::seconds timeout, ContextPtr query_context)
{
    refreshCoverageFromActiveParts();

    auto source_storage = DatabaseCatalog::instance().tryGetTable(source_table_id, query_context);
    if (!source_storage)
        return false;

    const auto * source_mt = dynamic_cast<const MergeTreeData *>(source_storage.get());
    if (!source_mt)
        return false;

    auto source_snapshot = source_mt->getDataPartsVectorForInternalUsage();
    std::unordered_set<UUID> active_uuids;
    active_uuids.reserve(source_snapshot.size());
    for (const auto & part : source_snapshot)
    {
        if (part)
            active_uuids.insert(part->uuid);
    }

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    constexpr auto refresh_interval = std::chrono::seconds{1};
    while (true)
    {
        if (coverage_map.isFullyCovering(active_uuids))
            return true;

        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline)
            return false;

        const auto wait_time = std::min(
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now),
            std::chrono::duration_cast<std::chrono::milliseconds>(refresh_interval));
        if (coverage_map.waitForFullCoverage(active_uuids, wait_time))
            return true;

        refreshCoverageFromActiveParts();
    }
}

}
