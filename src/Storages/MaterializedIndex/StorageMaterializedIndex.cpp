#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>
#include <Storages/MaterializedIndex/MaterializedIndexAlgorithmFactory.h>
#include <Storages/MaterializedIndex/MaterializedIndexContext.h>

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/MergeTreeSettings.h>


namespace DB
{

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


void StorageMaterializedIndex::shutdown(bool /*is_drop*/)
{
}

bool StorageMaterializedIndex::scheduleDataProcessingJob(BackgroundJobsAssignee & /*assignee*/)
{
    // No background jobs in stage-1; merges / mutations / refresh are wired
    // up in later stages.
    return false;
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
