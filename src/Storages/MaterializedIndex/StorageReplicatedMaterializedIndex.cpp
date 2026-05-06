#include <Storages/MaterializedIndex/StorageReplicatedMaterializedIndex.h>

#include <Storages/MergeTree/MergeTreeSettings.h>


namespace DB
{

StorageReplicatedMaterializedIndex::StorageReplicatedMaterializedIndex(
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
    LoadingStrictnessLevel mode)
    : StorageMaterializedIndex(
          table_id_,
          relative_data_path_,
          source_table_id_,
          indexed_columns_,
          family_,
          impl_,
          build_params_,
          context_,
          metadata_,
          std::move(settings_),
          mode)
    , zookeeper_path(zookeeper_path_)
    , replica_name(replica_name_)
{
    // Intentionally empty: ZooKeeper I/O for replication is wired up in a
    // later stage. The ctor only records the parsed literals so metadata
    // round-trips cleanly across server restarts.
}

}
