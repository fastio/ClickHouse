#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MaterializedIndex/IMaterializedIndexAlgorithm.h>

#include <memory>


namespace DB
{

/// Stage-1 skeleton of a MaterializedIndex backing table. Inherits the full
/// MergeTreeData lifecycle for future reuse (merges, mutations, backup, ...)
/// but rejects reads / writes outright: only the catalog-side paths are
/// exercised at this point. Not marked final: ReplicatedMaterializedIndex
/// derives from this class.
class StorageMaterializedIndex : public MergeTreeData
{
public:
    StorageMaterializedIndex(
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
        LoadingStrictnessLevel mode);

    std::string getName() const override { return "MaterializedIndex"; }

    bool supportsPrewhere() const override { return false; }
    bool supportsFinal() const override { return false; }
    bool supportsSubcolumns() const override { return false; }
    bool supportsLightweightDelete() const override { return false; }

    /// Stage-1 constraint C1: the table object is catalog-visible but not
    /// queryable or writable through the usual SQL surfaces.
    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    /// Background machinery is off in stage-1.
    void startup() override {}
    void shutdown(bool is_drop) override;

    /// No background data processing is scheduled until stage-2 wires up
    /// the build / refresh tasks.
    bool scheduleDataProcessingJob(BackgroundJobsAssignee & assignee) override;

    MutationCounters getMutationCounters() const override { return {}; }
    std::map<std::string, MutationCommands> getUnfinishedMutationCommands() const override { return {}; }
    std::vector<MergeTreeMutationStatus> getMutationsStatus() const override { return {}; }
    MutationsSnapshotPtr getMutationsSnapshot(const IMutationsSnapshot::Params & params) const override;

    void dropPartNoWaitNoThrow(const String & part_name) override;
    void dropPart(const String & part_name, bool detach, ContextPtr context) override;
    void dropPartition(const ASTPtr & partition, bool detach, ContextPtr context) override;
    PartitionCommandsResultInfo attachPartition(const PartitionCommand & command, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) override;
    void replacePartitionFrom(const StoragePtr & source_table, const ASTPtr & partition, bool replace, ContextPtr context) override;
    void movePartitionToTable(const StoragePtr & dest_table, const ASTPtr & partition, ContextPtr context) override;

    bool partIsAssignedToBackgroundOperation(const DataPartPtr & /*part*/) const override { return false; }
    void attachRestoredParts(MutableDataPartsVector && parts) override;
    void startBackgroundMovesIfNeeded() override {}
    std::unique_ptr<MergeTreeSettings> getDefaultSettings() const override;

    const StorageID & getSourceTableID() const { return source_table_id; }
    const Names & getIndexedColumns() const { return indexed_columns; }
    const String & getFamily() const { return family; }
    const String & getImpl() const { return impl; }

protected:
    StorageID source_table_id;
    Names indexed_columns;
    String family;
    String impl;
    ASTPtr build_params;
    MaterializedIndexAlgorithmPtr algorithm;
};

}
