#pragma once

#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>


namespace DB
{

class BackgroundJobsAssignee;

/// Stage-1 skeleton for the replicated variant: stores the ZooKeeper path /
/// replica name that CREATE parses out of `ENGINE = ReplicatedMaterializedIndex(...)`
/// but does not open any ZK session. Replication wires into the base in
/// stage-4.
class StorageReplicatedMaterializedIndex final : public StorageMaterializedIndex
{
public:
    StorageReplicatedMaterializedIndex(
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

    std::string getName() const override { return "ReplicatedMaterializedIndex"; }
    bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) override;

    const String & getZooKeeperPath() const { return zookeeper_path; }
    const String & getReplicaName() const { return replica_name; }

private:
    String zookeeper_path;
    String replica_name;
};

}
