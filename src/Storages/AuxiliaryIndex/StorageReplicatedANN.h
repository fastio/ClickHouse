#pragma once

#include <Storages/AuxiliaryIndex/StorageANN.h>


namespace DB
{

class BackgroundJobsAssignee;

/// Replicated variant: stores the ZooKeeper path / replica name parsed from
/// `ENGINE = ReplicatedANN(...)` and gates the base background
/// scheduler with a per-index Keeper leader lease.
class StorageReplicatedANN final : public StorageANN
{
public:
    StorageReplicatedANN(
        const StorageID & table_id_,
        const String & relative_data_path_,
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

    std::string getName() const override { return "ReplicatedANN"; }
    bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) override;

    const String & getZooKeeperPath() const { return zookeeper_path; }
    const String & getReplicaName() const { return replica_name; }

private:
    String zookeeper_path;
    String replica_name;
};

}
