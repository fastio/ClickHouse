#include <Storages/Reflection/ANNIndex/ReflectionReplicatedANNIndex.h>

#include <Core/UUID.h>
#include <Common/logger_useful.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageReplicatedMergeTree.h>

#include <fmt/format.h>

namespace DB
{

ReflectionReplicatedANNIndex::ReflectionReplicatedANNIndex(
    const StorageID & table_id_,
    const StorageID & source_table_id_,
    const Names & indexed_columns_,
    const String & family_,
    const String & impl_,
    const ASTPtr & build_params_,
    const String & zookeeper_path_,
    const String & replica_name_,
    ContextMutablePtr context_,
    const StorageInMemoryMetadata & metadata_,
    std::unique_ptr<MergeTreeSettings> settings_,
    LoadingStrictnessLevel mode)
    : ReflectionANNIndex(
          table_id_,
          source_table_id_,
          indexed_columns_,
          family_,
          impl_,
          build_params_,
          context_,
          metadata_,
          std::move(settings_),
          mode,
          zookeeper_path_,
          replica_name_)
    , zookeeper_path(zookeeper_path_)
    , replica_name(replica_name_)
{
    // The ctor only records parsed literals; scheduling opens Keeper lazily
    // through the inner ReplicatedMergeTree when it tries to acquire leadership.
}

bool ReflectionReplicatedANNIndex::scheduleDataProcessingJob(BackgroundJobsAssignee & assignee)
{
    auto inner_table_snapshot = getInnerTable();
    auto * replicated = dynamic_cast<StorageReplicatedMergeTree *>(inner_table_snapshot.get());
    if (!replicated)
        return ReflectionANNIndex::scheduleDataProcessingJob(assignee);

    String lease_path;
    String lease_payload = fmt::format(
        "index={}\nreplica={}\nlease_id={}\n",
        getStorageID().getNameForLogs(),
        replica_name,
        toString(UUIDHelpers::generateV4()));

    if (!replicated->tryAcquireANNIndexLeaderLease(lease_payload, lease_path))
    {
        refreshCoverageFromActiveParts();
        return false;
    }

    setReplicatedLeaderLeaseForNextTask(lease_path, lease_payload, inner_table_snapshot);
    try
    {
        const bool scheduled = ReflectionANNIndex::scheduleDataProcessingJob(assignee);
        if (!scheduled)
            releasePendingReplicatedLeaderLease();
        return scheduled;
    }
    catch (...)
    {
        releasePendingReplicatedLeaderLease();
        throw;
    }
}

}
