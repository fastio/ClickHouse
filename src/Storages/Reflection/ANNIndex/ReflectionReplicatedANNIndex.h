#pragma once

#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>


namespace DB
{

class BackgroundJobsAssignee;

/// Replicated variant: stores the ZooKeeper path / replica name parsed from
/// `ENGINE = ReplicatedANNIndex(...)` and gates the base background
/// scheduler with a per-index Keeper leader lease.
class ReflectionReplicatedANNIndex final : public ReflectionANNIndex
{
public:
    ReflectionReplicatedANNIndex(
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
        LoadingStrictnessLevel mode);

    std::string getName() const override { return "ReplicatedANNIndex"; }
    bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) override;

    const String & getZooKeeperPath() const { return zookeeper_path; }
    const String & getReplicaName() const { return replica_name; }

private:
    String zookeeper_path;
    String replica_name;
};

}
