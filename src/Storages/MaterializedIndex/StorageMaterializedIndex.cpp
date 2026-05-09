#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>
#include <Storages/MaterializedIndex/BuildTask.h>
#include <Storages/MaterializedIndex/MaterializedIndexAlgorithmFactory.h>
#include <Storages/MaterializedIndex/MaterializedIndexContext.h>
#include <Storages/MaterializedIndex/MaterializedIndexSelectedEntry.h>
#include <Storages/MaterializedIndex/RemapTask.h>
#include <Storages/MaterializedIndex/SnapshotDiffReconciler.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeSettings.h>


namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool assign_part_uuids;
    extern const MergeTreeSettingsUInt64 materialized_index_starvation_protection_cycles;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


namespace
{

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
    LoadingStrictnessLevel mode)
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
    , cleanup_thread(*this)
{
    initializeDirectoriesAndFormatVersion(relative_data_path_, LoadingStrictnessLevel::ATTACH <= mode, /*date_column_name=*/ String{});

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

    cleanup_thread.start();
}

void StorageMaterializedIndex::shutdown(bool /*is_drop*/)
{
    if (shutdown_called.exchange(true))
        return;
    cleanup_thread.stop();
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

    /// I-BG-14: pull source / mi snapshots once per cycle.
    auto source_snapshot = source_mt->getDataPartsVectorForInternalUsage();
    auto mi_snapshot = getAccessPathPartsVectorForInternalUsage();

    /// Stage-2 simplification: aggregated coverage is left empty; the
    /// algorithm-side coverage layer is the source of truth and lands in a
    /// later stage (see plan.md §coverage_ratio degradation).
    std::unordered_set<UUID> coverage;

    auto reconciled = SnapshotDiffReconciler::run(source_snapshot, mi_snapshot, coverage);

    const size_t starvation_threshold
        = (*getSettings())[MergeTreeSetting::materialized_index_starvation_protection_cycles];
    const bool force_build
        = consecutive_remap_count.load(std::memory_order_relaxed) >= starvation_threshold
        && reconciled.has_build_candidate;

    auto build_callback = [](bool /*delay*/) {};
    auto metadata_snapshot = getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);
    auto storage_snapshot
        = source_mt->getStorageSnapshotWithoutData(
            source_mt->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false), context);

    auto submit_build = [&]()
    {
        auto fp = std::make_shared<FutureMaterializedIndexPart>();
        fp->kind = FutureMaterializedIndexPart::Kind::Build;
        fp->new_part_name = "mi-build-" + getStorageID().getShortName();
        fp->new_part_uuid = UUIDHelpers::generateV4();
        fp->source_parts_snapshot = source_snapshot;

        auto tagger = std::make_unique<CurrentlyBuildingMaterializedIndexPartTagger>(fp, *this);
        auto entry = std::make_shared<MaterializedIndexBuildSelectedEntry>(fp, std::move(tagger));

        consecutive_remap_count.store(0, std::memory_order_relaxed);

        auto task = std::make_shared<BuildTask>(
            *this,
            std::move(entry),
            source_snapshot,
            source_mt,
            storage_snapshot,
            metadata_snapshot,
            context,
            /*memory_budget_bytes=*/0,
            build_callback);

        return assignee.scheduleCommonTask(task, /*need_trigger=*/true);
    };

    auto submit_remap = [&]()
    {
        auto fp = std::make_shared<FutureMaterializedIndexPart>();
        fp->kind = FutureMaterializedIndexPart::Kind::Remap;
        fp->new_part_name = "mi-remap-" + getStorageID().getShortName();
        fp->new_part_uuid = UUIDHelpers::generateV4();
        fp->affected_mi_parts = mi_snapshot;
        fp->delta_in_source_parts = reconciled.delta_in;
        fp->delta_out_source_uuids = reconciled.delta_out;

        auto tagger = std::make_unique<CurrentlyBuildingMaterializedIndexPartTagger>(fp, *this);
        auto entry = std::make_shared<MaterializedIndexRemapSelectedEntry>(fp, std::move(tagger));

        consecutive_remap_count.fetch_add(1, std::memory_order_relaxed);

        auto task = std::make_shared<RemapTask>(
            *this,
            std::move(entry),
            mi_snapshot,
            reconciled.delta_in,
            reconciled.delta_out,
            source_mt,
            storage_snapshot,
            context,
            /*memory_budget_bytes=*/0,
            build_callback);

        return assignee.scheduleCommonTask(task, /*need_trigger=*/true);
    };

    if (force_build)
        return submit_build();
    if (reconciled.has_remap_target)
        return submit_remap();
    if (reconciled.has_build_candidate)
        return submit_build();

    return false;
}

bool StorageMaterializedIndex::partIsAssignedToBackgroundOperation(const DataPartPtr & part) const
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    return currently_building_mi_parts.contains(part->name);
}

DataPartsVector StorageMaterializedIndex::getAccessPathPartsVectorForInternalUsage() const
{
    return getDataPartsVectorForInternalUsage(
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

}
