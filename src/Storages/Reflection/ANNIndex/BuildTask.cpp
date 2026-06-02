#include <Storages/Reflection/ANNIndex/BuildTask.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypeUUID.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartMetadata.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartName.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartWriter.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityAdaptive.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/StorageSnapshot.h>

#include <Compression/CompressionFactory.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/UUID.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Common/TransactionID.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>

#include <algorithm>
#include <ctime>
#include <exception>
#include <limits>
#include <sstream>


namespace ProfileEvents
{
    extern const Event ANNIndexPayloadV1Used;
    extern const Event ANNIndexPayloadV2Used;
}


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::ANNIndex
{

namespace ErrorCodes = DB::ErrorCodes;

namespace
{

constexpr UInt64 PARTITIONING_FORMAT_VERSION = 1;

}


/// Stage 1: read source blocks, write locator/source_row_id entries, feed
/// algorithm->prepareBuild per block.
struct BuildTaskImpl::ReadColumnsWriteLocatorAndPrepareStage : public IStage
{
    void setRuntimeContext(StageRuntimeContextPtr local, StageRuntimeContextPtr global) override
    {
        local_ctx = std::move(local);
        global_ctx = std::static_pointer_cast<GlobalRuntimeContext>(std::move(global));
    }

    StageRuntimeContextPtr getContextForNextStage() override
    {
        return global_ctx;
    }

    ProfileEvents::Event getTotalTimeProfileEvent() const override
    {
        return ProfileEvents::end();
    }

    bool execute() override
    {
        /// Phase A: no more source parts left — publish total rows to build_ctx.
        if (global_ctx->current_source_part_index >= global_ctx->source_parts.size())
            return finalizeStage();

        /// Phase B: need a pipeline for the current source part. Spin one up
        /// lazily so a cancelled task that never runs saves the allocation.
        if (!global_ctx->current_part_executor)
            openPipelineForCurrentPart();

        /// Phase C: pull one block from the current part's pipeline.
        Block block;
        if (!global_ctx->current_part_executor->pull(block))
        {
            global_ctx->current_part_executor.reset();
            global_ctx->current_pipeline.reset();
            ++global_ctx->current_source_part_index;
            return true;
        }

        processBlock(block);
        return true;
    }

    void cancel() noexcept override
    {
    }

    void openPipelineForCurrentPart()
    {
        auto data_part = global_ctx->source_parts[global_ctx->current_source_part_index];

        const Names & indexed_columns = global_ctx->storage
            ? global_ctx->storage->getIndexedColumns()
            : Names{};

        Names columns_to_read;
        columns_to_read.reserve(indexed_columns.size() + 3);
        for (const auto & col : indexed_columns)
            columns_to_read.emplace_back(col);
        columns_to_read.emplace_back(BlockNumberColumn::name);
        columns_to_read.emplace_back(BlockOffsetColumn::name);
        columns_to_read.emplace_back("_part_offset");

        QueryPlan plan;
        createReadFromPartStep(
            MergeTreeSequentialSourceType::Merge,
            plan,
            *global_ctx->source_storage,
            global_ctx->source_snapshot,
            RangesInDataPart(data_part),
            /*alter_conversions=*/std::make_shared<AlterConversions>(),
            /*merged_part_offsets=*/nullptr,
            std::move(columns_to_read),
            /*filtered_rows_count=*/nullptr,
            /*apply_deleted_mask=*/true,
            /*filter=*/std::nullopt,
            /*read_with_direct_io=*/false,
            /*prefetch=*/false,
            global_ctx->context,
            getLogger("BuildTaskImpl"));

        auto builder = plan.buildQueryPipeline(
            QueryPlanOptimizationSettings(global_ctx->context),
            BuildQueryPipelineSettings(global_ctx->context));

        global_ctx->current_pipeline = std::make_unique<QueryPipeline>(
            QueryPipelineBuilder::getPipeline(std::move(*builder)));
        global_ctx->current_part_executor = std::make_unique<PullingPipelineExecutor>(
            *global_ctx->current_pipeline);
    }

    /// Lazily creates the Wide part and its `MergedBlockOutputStream` on the
    /// first non-empty block. A build with zero source rows never calls this;
    /// the finalize stage then materializes an empty part instead.
    void ensureOutputStream()
    {
        if (global_ctx->output_stream || !global_ctx->storage)
            return;

        auto & inner = global_ctx->storage->getInnerMergeTreeData(global_ctx->inner_storage_holder);
        auto bundle = openAnnPartStream(
            inner,
            global_ctx->new_part_name,
            global_ctx->new_part_uuid,
            global_ctx->source_partition_id,
            global_ctx->output_storage);

        global_ctx->inner_metadata = bundle.inner_metadata;
        global_ctx->part_columns_list = bundle.columns;
        global_ctx->minmax_idx = bundle.minmax_idx;
        global_ctx->new_ann_index_part = bundle.part;
        global_ctx->output_stream = std::move(bundle.stream);
    }

    /// Assembles the 5-column locator block for `block`'s rows. `source_uuid`
    /// is constant within a source block; `part_offset` / `block_number` /
    /// `block_offset` come straight from the source virtual columns.
    Block makeLocatorBlock(const UUID & source_uuid, const Block & block) const
    {
        const size_t num_rows = block.rows();
        const auto & block_number_col = block.getByName(BlockNumberColumn::name).column;
        const auto & block_offset_col = block.getByName(BlockOffsetColumn::name).column;
        const auto & part_offset_col = block.getByName("_part_offset").column;

        Block result;
        for (const auto & name_type : global_ctx->part_columns_list)
        {
            ColumnPtr col;
            if (name_type.name == ANN_INDEX_SOURCE_PARTITION_ID_COLUMN)
                col = name_type.type->createColumnConst(num_rows, Field(global_ctx->source_partition_id))
                    ->convertToFullColumnIfConst();
            else if (name_type.name == ANN_INDEX_LOCATOR_SOURCE_UUID_COLUMN)
                col = name_type.type->createColumnConst(num_rows, Field(source_uuid))
                    ->convertToFullColumnIfConst();
            else if (name_type.name == ANN_INDEX_LOCATOR_PART_OFFSET_COLUMN)
                col = part_offset_col;
            else if (name_type.name == ANN_INDEX_LOCATOR_BLOCK_NUMBER_COLUMN)
                col = block_number_col;
            else if (name_type.name == ANN_INDEX_LOCATOR_BLOCK_OFFSET_COLUMN)
                col = block_offset_col;
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Unexpected ANN-index locator column '{}'", name_type.name);

            result.insert(ColumnWithTypeAndName(col, name_type.type, name_type.name));
        }
        return result;
    }

    void processBlock(const Block & block)
    {
        const size_t num_rows = block.rows();
        if (num_rows == 0)
            return;

        if (global_ctx->is_cancelled.load(std::memory_order_relaxed))
            return;

        ensureOutputStream();

        const auto data_part = global_ctx->source_parts[global_ctx->current_source_part_index];

        if (global_ctx->output_stream)
        {
            Block locator_block = makeLocatorBlock(data_part->uuid, block);
            global_ctx->output_stream->write(locator_block);
        }

        global_ctx->internal_id_cursor += num_rows;

        /// Algorithm integration: feed the registered algorithm a sub-block
        /// containing only the indexed columns (in declared order), so that
        /// the algorithm never sees the framework's bookkeeping columns
        /// (`_block_number`, `_block_offset`, `_part_offset`).
        if (global_ctx->algorithm)
        {
            Block indexed_only;
            if (global_ctx->storage)
            {
                for (const auto & col : global_ctx->storage->getIndexedColumns())
                    indexed_only.insert(block.getByName(col));
            }
            global_ctx->algorithm->prepareBuild(global_ctx->build_ctx, indexed_only);

            /// Optional: feed the algorithm the per-vector locator payload
            /// (source UUID + `_part_offset`) when it opts in via
            /// `supportsPerVectorPayload`. Aligned 1:1 with `indexed_only`.
            /// `internal_id_cursor` was just advanced by `num_rows`, so the
            /// first row of this block had internal_id `cursor - num_rows`.
            if (global_ctx->algorithm->supportsPerVectorPayload())
            {
                Block locator_only;
                auto uuid_col = DataTypeUUID().createColumnConst(num_rows, Field(data_part->uuid))
                    ->convertToFullColumnIfConst();
                locator_only.insert(ColumnWithTypeAndName(
                    uuid_col,
                    std::make_shared<DataTypeUUID>(),
                    ANN_INDEX_LOCATOR_SOURCE_UUID_COLUMN));
                locator_only.insert(ColumnWithTypeAndName(
                    block.getByName("_part_offset").column,
                    block.getByName("_part_offset").type,
                    ANN_INDEX_LOCATOR_PART_OFFSET_COLUMN));

                const UInt64 internal_id_offset = global_ctx->internal_id_cursor - num_rows;
                global_ctx->algorithm->prepareBuildLocator(
                    global_ctx->build_ctx, locator_only, internal_id_offset);
            }
        }
    }

    bool finalizeStage()
    {
        /// Publish the row count for the algorithm's phase 2. Segment
        /// boundaries are no longer used by the framework's on-disk layout;
        /// algorithms that want them still expose `preferredSegmentBoundaries`.
        global_ctx->build_ctx.total_rows = global_ctx->internal_id_cursor;
        global_ctx->build_ctx.segment_boundaries = {};
        return false;
    }

    StageRuntimeContextPtr local_ctx;
    GlobalRuntimeContextPtr global_ctx;
};


/// Stage 3: exactly one algorithm->buildAlgorithmPrivate call.
struct BuildTaskImpl::BuildAlgorithmStage : public IStage
{
    void setRuntimeContext(StageRuntimeContextPtr local, StageRuntimeContextPtr global) override
    {
        local_ctx = std::move(local);
        global_ctx = std::static_pointer_cast<GlobalRuntimeContext>(std::move(global));
    }

    StageRuntimeContextPtr getContextForNextStage() override
    {
        return global_ctx;
    }

    ProfileEvents::Event getTotalTimeProfileEvent() const override
    {
        return ProfileEvents::end();
    }

    bool execute() override
    {
        /// One-shot algorithm invocation. Mid-layer does not poll cancel here
        /// — the algorithm is expected to observe `ctx.is_cancelled` during
        /// its own long-running work. Exceptions are logged with full context
        /// and rethrown for the outer execute() catch block to record on the
        /// promise.
        if (global_ctx->algorithm)
        {
            try
            {
                global_ctx->algorithm->buildAlgorithmPrivate(global_ctx->build_ctx);
            }
            catch (...)
            {
                tryLogCurrentException(getLogger("BuildTaskImpl"), __PRETTY_FUNCTION__);
                throw;
            }
        }
        return false;
    }

    void cancel() noexcept override
    {
    }

    StageRuntimeContextPtr local_ctx;
    GlobalRuntimeContextPtr global_ctx;
};


/// Stage 4: exactly one algorithm->finishBuild call.
struct BuildTaskImpl::FinishAlgorithmStage : public IStage
{
    void setRuntimeContext(StageRuntimeContextPtr local, StageRuntimeContextPtr global) override
    {
        local_ctx = std::move(local);
        global_ctx = std::static_pointer_cast<GlobalRuntimeContext>(std::move(global));
    }

    StageRuntimeContextPtr getContextForNextStage() override
    {
        return global_ctx;
    }

    ProfileEvents::Event getTotalTimeProfileEvent() const override
    {
        return ProfileEvents::end();
    }

    bool execute() override
    {
        if (global_ctx->algorithm)
            global_ctx->algorithm->finishBuild(global_ctx->build_ctx);
        return false;
    }

    void cancel() noexcept override
    {
    }

    StageRuntimeContextPtr local_ctx;
    GlobalRuntimeContextPtr global_ctx;
};


/// Stage 5: reclaim intermediate_storage.
struct BuildTaskImpl::CleanupIntermediateStage : public IStage
{
    void setRuntimeContext(StageRuntimeContextPtr local, StageRuntimeContextPtr global) override
    {
        local_ctx = std::move(local);
        global_ctx = std::static_pointer_cast<GlobalRuntimeContext>(std::move(global));
    }

    StageRuntimeContextPtr getContextForNextStage() override
    {
        return global_ctx;
    }

    ProfileEvents::Event getTotalTimeProfileEvent() const override
    {
        return ProfileEvents::end();
    }

    bool execute() override
    {
        /// Phase A: drop intermediate storage. Both the stage's own pointer
        /// and the algorithm-visible one are cleared so the algorithm cannot
        /// re-use a now-deleted scratch directory by mistake.
        if (global_ctx->intermediate_storage)
        {
            global_ctx->intermediate_storage->removeRecursive();
            global_ctx->intermediate_storage.reset();
            global_ctx->build_ctx.intermediate_storage.reset();
        }

        return false;
    }

    void cancel() noexcept override
    {
    }

    StageRuntimeContextPtr local_ctx;
    GlobalRuntimeContextPtr global_ctx;
};


/// Stage 6: finalize the columnar locator part, write the framework JSON
/// sidecars, fold every file into checksums.txt and pre-commit the storage
/// transaction. Produces the Temporary-state Wide part returned via getFuture.
struct BuildTaskImpl::FinalizeMetadataStage : public IStage
{
    void setRuntimeContext(StageRuntimeContextPtr local, StageRuntimeContextPtr global) override
    {
        local_ctx = std::move(local);
        global_ctx = std::static_pointer_cast<GlobalRuntimeContext>(std::move(global));
    }

    StageRuntimeContextPtr getContextForNextStage() override
    {
        return global_ctx;
    }

    ProfileEvents::Event getTotalTimeProfileEvent() const override
    {
        return ProfileEvents::end();
    }

    bool execute() override
    {
        auto & output_storage = global_ctx->output_storage;

        /// RAII guard queued up-front so fsync fires once the writes below
        /// finalize, before execute() returns (D-24).
        SyncGuardPtr sync_guard;
        if (output_storage)
            sync_guard = output_storage->getDirectorySyncGuard();

        /// Step 1: finalize the columnar locator part (writes `.bin`/`.mrk`,
        /// columns.txt, count.txt, partition.dat, minmax, primary.idx,
        /// metadata_version and the column-only checksums.txt).
        finalizeColumnarPart();

        /// Step 2: framework sidecars. `header.json` doubles as the layout
        /// marker that identifies the part as an ANN-index part on load.
        writeHeaderJson();
        writeCoverageJson();
        writeAnnFormatJson();
        writeAnnCoverageJson();

        /// Step 3: recompute checksums.txt over *every* file (locator columns +
        /// algorithm-private payload + JSON sidecars) so replicated fetch sees
        /// an identical checksum set on both ends.
        finalizeChecksums();

        if (output_storage)
            output_storage->precommitTransaction();

        /// sync_guard dtor here fsyncs the directory on scope exit.
        return false;
    }

    void cancel() noexcept override
    {
    }

    void finalizeColumnarPart() const
    {
        if (!global_ctx->storage || !global_ctx->output_storage)
            return;

        /// Zero-row build: no block ever opened the stream. Materialize an
        /// empty Wide part so the standard load / fetch path still works.
        if (!global_ctx->output_stream)
        {
            auto & inner = global_ctx->storage->getInnerMergeTreeData(global_ctx->inner_storage_holder);
            auto bundle = openAnnPartStream(
                inner,
                global_ctx->new_part_name,
                global_ctx->new_part_uuid,
                global_ctx->source_partition_id,
                global_ctx->output_storage);
            global_ctx->inner_metadata = bundle.inner_metadata;
            global_ctx->part_columns_list = bundle.columns;
            global_ctx->minmax_idx = bundle.minmax_idx;
            global_ctx->new_ann_index_part = bundle.part;
            global_ctx->output_stream = std::move(bundle.stream);
            global_ctx->output_stream->write(global_ctx->inner_metadata->getSampleBlock());
        }

        global_ctx->new_ann_index_part->rows_count = global_ctx->internal_id_cursor;
        global_ctx->output_stream->finalizeIndexGranularity();
        global_ctx->output_stream->finalizePart(
            global_ctx->new_ann_index_part, IMergedBlockOutputStream::GatheredData{}, /*sync=*/false);
    }

    void finalizeChecksums() const
    {
        if (!global_ctx->storage || !global_ctx->output_storage || !global_ctx->new_ann_index_part)
            return;

        auto & inner = global_ctx->storage->getInnerMergeTreeData(global_ctx->inner_storage_holder);
        auto checksums = finalizeANNIndexPartChecksums(*global_ctx->output_storage, &inner);
        global_ctx->new_ann_index_part->checksums = checksums;
        global_ctx->new_ann_index_part->setBytesOnDisk(checksums.getTotalSizeOnDisk());

        /// `loadColumnsChecksumsIndexes` re-reads the part we just wrote, and
        /// `loadIndexGranularity` *appends* marks to `index_granularity`. The
        /// write path already populated (and possibly compacted to constant)
        /// that member, so reset it to a fresh adaptive state derived from the
        /// on-disk marks first — otherwise the append trips `Cannot append mark
        /// after final`.
        global_ctx->new_ann_index_part->initializeIndexGranularityInfo(*inner.getSettings());
        global_ctx->new_ann_index_part->loadColumnsChecksumsIndexes(
            /*require_columns_checksums=*/true, /*check_consistency=*/false);
    }

    void writeHeaderJson() const
    {
        if (!global_ctx->output_storage)
            return;

        Poco::JSON::Object header_json;
        header_json.set("version", 1);
        if (global_ctx->algorithm)
        {
            header_json.set("algorithm_family", global_ctx->algorithm->getFamily());
            header_json.set("algorithm_impl", global_ctx->algorithm->getName());
        }
        else
        {
            header_json.set("algorithm_family", String{});
            header_json.set("algorithm_impl", String{});
        }
        header_json.set("total_rows", global_ctx->build_ctx.total_rows);
        header_json.set("tombstone_rows", 0);

        /// Snapshot the coverage-source count from `source_parts.size()`;
        /// the persisted coverage.json below carries the exact per-part data.
        header_json.set("coverage_source_part_count", global_ctx->source_parts.size());
        header_json.set("partitioning_format_version", static_cast<Int64>(PARTITIONING_FORMAT_VERSION));
        header_json.set("source_partition_id", global_ctx->source_partition_id);
        header_json.set("source_partition_hash", global_ctx->source_partition_hash);
        header_json.set("source_min_block", global_ctx->source_min_block);
        header_json.set("source_max_block", global_ctx->source_max_block);
        header_json.set("created_timestamp_seconds", static_cast<Int64>(std::time(nullptr)));

        auto writer = global_ctx->output_storage->writeFile("header.json", 4096, WriteSettings{});
        std::ostringstream oss;
        Poco::JSON::Stringifier::stringify(header_json, oss);
        const std::string body = oss.str();
        writer->write(body.data(), body.size());
        writer->finalize();
    }

    void writeCoverageJson() const
    {
        if (!global_ctx->output_storage)
            return;

        Poco::JSON::Object coverage_json;
        coverage_json.set("format_version", 1);
        coverage_json.set("tombstone_rows", 0);
        coverage_json.set("partitioning_format_version", static_cast<Int64>(PARTITIONING_FORMAT_VERSION));
        coverage_json.set("source_partition_id", global_ctx->source_partition_id);
        coverage_json.set("source_partition_hash", global_ctx->source_partition_hash);
        coverage_json.set("source_min_block", global_ctx->source_min_block);
        coverage_json.set("source_max_block", global_ctx->source_max_block);
        /// Compact payload format selected during ctor wiring; the per-entry
        /// `payload_part_id` below carries the dense ids the inline payload
        /// actually stores. The matcher needs both to decode hits.
        coverage_json.set("payload_format_version",
            static_cast<int>(global_ctx->payload_format_version));

        Poco::JSON::Array covered_arr;
        for (UInt32 idx = 0; idx < global_ctx->source_parts.size(); ++idx)
        {
            const auto & part = global_ctx->source_parts[idx];
            Poco::JSON::Object item;
            item.set("source_part_uuid", toString(part->uuid));
            item.set("source_part_name", part->name);
            item.set("partition_id", part->info.getPartitionId());
            item.set("min_block", part->info.min_block);
            item.set("max_block", part->info.max_block);
            item.set("level", part->info.level);
            item.set("mutation", part->info.mutation);
            /// Poco::JSON does not have a dedicated UInt64 setter; cast via
            /// Int64 is safe in practice — `rows_count` will not exceed 2^63.
            item.set("rows", static_cast<Int64>(part->rows_count));
            /// Dense local id: identical to the index in `source_parts`,
            /// matching the population order in the ctor.
            item.set("payload_part_id", static_cast<Int64>(idx));
            covered_arr.add(item);
        }
        coverage_json.set("covered", covered_arr);

        auto writer = global_ctx->output_storage->writeFile("coverage.json", 4096, WriteSettings{});
        std::ostringstream oss;
        Poco::JSON::Stringifier::stringify(coverage_json, oss);
        const std::string body = oss.str();
        writer->write(body.data(), body.size());
        writer->finalize();
    }

    void writeAnnFormatJson() const
    {
        if (!global_ctx->output_storage)
            return;

        Poco::JSON::Object format_json;
        format_json.set("framework_format_version", 1);
        format_json.set("engine", "ANNIndex");
        format_json.set("family", global_ctx->algorithm ? global_ctx->algorithm->getFamily() : String{});
        format_json.set("impl", global_ctx->algorithm ? global_ctx->algorithm->getName() : String{});
        format_json.set("algorithm_data_version", global_ctx->algorithm ? global_ctx->algorithm->getAlgorithmVersion() : String{});
        format_json.set("algorithm_parameter_fingerprint", global_ctx->algorithm ? global_ctx->algorithm->getBuildParamsHash() : String{});
        format_json.set("built_at_unix_ms", static_cast<Int64>(std::time(nullptr)) * 1000);

        auto writer = global_ctx->output_storage->writeFile("ann_format.json", 4096, WriteSettings{});
        std::ostringstream oss;
        Poco::JSON::Stringifier::stringify(format_json, oss);
        const std::string body = oss.str();
        writer->write(body.data(), body.size());
        writer->finalize();
    }

    void writeAnnCoverageJson() const
    {
        if (!global_ctx->output_storage)
            return;

        Poco::JSON::Object coverage_json;
        Poco::JSON::Array covered_arr;
        UInt64 total_rows = 0;
        for (const auto & part : global_ctx->source_parts)
        {
            Poco::JSON::Object item;
            item.set("uuid", toString(part->uuid));
            item.set("name", part->name);
            item.set("mutation_version", part->info.mutation);
            item.set("row_count", static_cast<Int64>(part->rows_count));
            covered_arr.add(item);
            total_rows += part->rows_count;
        }

        coverage_json.set("covered_source_parts", covered_arr);
        coverage_json.set("total_rows", static_cast<Int64>(total_rows));

        auto writer = global_ctx->output_storage->writeFile("ann_coverage.json", 4096, WriteSettings{});
        std::ostringstream oss;
        Poco::JSON::Stringifier::stringify(coverage_json, oss);
        const std::string body = oss.str();
        writer->write(body.data(), body.size());
        writer->finalize();
    }

    StageRuntimeContextPtr local_ctx;
    GlobalRuntimeContextPtr global_ctx;
};


BuildTaskImpl::Stages BuildTaskImpl::makeStages()
{
    return {
        std::make_shared<ReadColumnsWriteLocatorAndPrepareStage>(),
        std::make_shared<BuildAlgorithmStage>(),
        std::make_shared<FinishAlgorithmStage>(),
        std::make_shared<CleanupIntermediateStage>(),
        std::make_shared<FinalizeMetadataStage>(),
    };
}


BuildTaskImpl::BuildTaskImpl(
    MergeTreeData::DataPartsVector source_parts_,
    IANNIndexAlgorithm * algorithm_,
    ReflectionANNIndex * storage_,
    StoragePtr inner_storage_holder_,
    String new_part_name_,
    const MergeTreeData * source_storage_,
    StorageSnapshotPtr source_snapshot_,
    StorageMetadataPtr source_metadata_,
    ContextPtr context_,
    MutableDataPartStoragePtr output_storage_,
    MutableDataPartStoragePtr intermediate_storage_,
    UInt64 memory_budget_bytes_,
    UUID new_part_uuid_)
    : global_ctx(std::make_shared<GlobalRuntimeContext>())
    , stages(makeStages())
{
    global_ctx->source_parts = std::move(source_parts_);
    global_ctx->algorithm = algorithm_;
    global_ctx->storage = storage_;
    global_ctx->inner_storage_holder = std::move(inner_storage_holder_);
    global_ctx->new_part_name = std::move(new_part_name_);
    global_ctx->new_part_uuid = new_part_uuid_ == UUIDHelpers::Nil ? UUIDHelpers::generateV4() : new_part_uuid_;
    if (!global_ctx->source_parts.empty())
    {
        const auto source_partition_range = getANNIndexSourcePartitionRange(global_ctx->source_parts);
        global_ctx->source_partition_id = source_partition_range.source_partition_id;
        global_ctx->source_partition_hash = source_partition_range.source_partition_hash;
        global_ctx->source_min_block = source_partition_range.min_block;
        global_ctx->source_max_block = source_partition_range.max_block;

        const auto & inner_storage = global_ctx->storage->getInnerMergeTreeData(global_ctx->inner_storage_holder);
        const auto part_info = MergeTreePartInfo::fromPartName(global_ctx->new_part_name, inner_storage.format_version);
        if (part_info.getPartitionId() != getANNIndexPhysicalPartitionId(global_ctx->source_partition_id)
            || part_info.min_block != global_ctx->source_min_block
            || part_info.max_block != global_ctx->source_max_block)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Materialized-index part {} does not match source partition {} block range {}_{}",
                global_ctx->new_part_name,
                global_ctx->source_partition_id,
                global_ctx->source_min_block,
                global_ctx->source_max_block);
    }
    global_ctx->source_storage = source_storage_;
    global_ctx->source_snapshot = std::move(source_snapshot_);
    global_ctx->source_metadata = std::move(source_metadata_);
    global_ctx->context = std::move(context_);
    global_ctx->output_storage = std::move(output_storage_);
    global_ctx->intermediate_storage = std::move(intermediate_storage_);
    global_ctx->memory_budget_bytes = memory_budget_bytes_;

    /// Wire up AlgorithmBuildContext: the framework owns the atomic flag,
    /// the algorithm gets a bare pointer whose lifetime is bound to this task.
    global_ctx->build_ctx.output_storage = global_ctx->output_storage;
    global_ctx->build_ctx.intermediate_storage = global_ctx->intermediate_storage;
    global_ctx->build_ctx.memory_budget_bytes = global_ctx->memory_budget_bytes;
    global_ctx->build_ctx.is_cancelled = &global_ctx->is_cancelled;

    /// Compact-payload preparation. Must happen before stage 1 starts
    /// emitting locator blocks: the algorithm consults
    /// `payload_format_version` and `uuid_to_payload_part_id` from
    /// `prepareBuildLocator`. The dense local id is the position in
    /// `source_parts` (0..N-1); duplicates are impossible because the
    /// caller upstream already deduplicated by source UUID.
    UInt64 max_part_rows = 0;
    global_ctx->uuid_to_payload_part_id.reserve(global_ctx->source_parts.size());
    for (UInt32 idx = 0; idx < global_ctx->source_parts.size(); ++idx)
    {
        const auto & part = global_ctx->source_parts[idx];
        global_ctx->uuid_to_payload_part_id.emplace(part->uuid, idx);
        max_part_rows = std::max(max_part_rows, part->rows_count);
    }
    global_ctx->payload_format_version = ANNIndexPayloadCodec::chooseVersion(max_part_rows);
    global_ctx->build_ctx.payload_format_version = global_ctx->payload_format_version;
    global_ctx->build_ctx.uuid_to_payload_part_id = &global_ctx->uuid_to_payload_part_id;

    if (global_ctx->payload_format_version == ANNIndexPayloadCodec::Version::V1_OFFSET32)
        ProfileEvents::increment(ProfileEvents::ANNIndexPayloadV1Used);
    else
        ProfileEvents::increment(ProfileEvents::ANNIndexPayloadV2Used);

    stages_iterator = stages.begin();

    auto prepare_stage_ctx = std::make_shared<IStageRuntimeContext>();
    (*stages_iterator)->setRuntimeContext(std::move(prepare_stage_ctx), global_ctx);
}


bool BuildTaskImpl::execute()
{
    chassert(stages_iterator != stages.end());
    try
    {
        const auto & current_stage = *stages_iterator;

        if (current_stage->execute())
            return true;

        auto next_stage_context = current_stage->getContextForNextStage();

        ++stages_iterator;
        if (stages_iterator == stages.end())
        {
            /// All stages have completed. Fulfil the promise exactly once with
            /// the part produced by stage 6 (may be null while stage 6 is still
            /// a skeleton; that is intentional for early change packs).
            if (!promise_fulfilled)
            {
                global_ctx->promise.set_value(global_ctx->new_ann_index_part);
                promise_fulfilled = true;
            }
            return false;
        }

        (*stages_iterator)->setRuntimeContext(std::move(next_stage_context), global_ctx);
        return true;
    }
    catch (...)
    {
        if (!promise_fulfilled)
        {
            global_ctx->promise.set_exception(std::current_exception());
            promise_fulfilled = true;
        }

        stages_iterator = stages.end();
        return false;
    }
}


void BuildTaskImpl::cancel() noexcept
{
    global_ctx->is_cancelled.store(true, std::memory_order_relaxed);
    if (stages_iterator != stages.end())
        (*stages_iterator)->cancel();
}


std::future<MergeTreeData::MutableDataPartPtr> BuildTaskImpl::getFuture()
{
    return global_ctx->promise.get_future();
}


MergeTreeData::MutableDataPartPtr BuildTaskImpl::getUnfinishedPart()
{
    return global_ctx->new_ann_index_part;
}

}
