#include <Storages/Reflection/ANNIndex/BuildTask.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
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
#include <Storages/Reflection/ANNIndex/MergeTreeDataPartANNIndex.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartMetadata.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartName.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartReverseLookup.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/StorageSnapshot.h>

#include <Core/Block.h>
#include <Core/UUID.h>
#include <Columns/IColumn.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>

#include <algorithm>
#include <ctime>
#include <exception>
#include <limits>
#include <sstream>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 ann_index_segment_size_rows;
}


namespace
{

constexpr UInt64 PARTITIONING_FORMAT_VERSION = 1;

/// Stage 1 writes two fixed-width segment files:
///   * `source_row_id_<seg>.bin`: UInt64 _block_number | UInt64 _block_offset
///   * `locator_<seg>.bin`: UInt32 part_uuid_id | UInt64 _part_offset
/// The integer widths are fixed by the on-disk format contract and are
/// referenced directly by the query and Remap paths.

/// Strict per-row cancel poll cadence inside stage 1. Checked every
/// POLL_CANCEL_EVERY rows so tight per-row hot path is not weighed down.
constexpr UInt64 POLL_CANCEL_EVERY = 256;

/// UUID table width. The maximum UInt32 value is reserved for tombstones in
/// `locator`, so real table ids stop one id earlier.
/// If a build produces more the writer throws LOGICAL_ERROR so the caller
/// notices rather than silently truncating.
constexpr UInt32 MAX_PART_UUID_ID = ANNIndexPartReverseLookup::TOMBSTONE_PART_UUID_ID - 1;

}


/// Stage 1: read source blocks, write locator/source_row_id entries, feed
/// algorithm->prepareBuild per block.
struct BuildTask::ReadColumnsWriteLocatorAndPrepareStage : public IStage
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
        /// Phase A: no more source parts left — finalize writers, append the
        /// trailing segment boundary, publish boundaries to build_ctx.
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
            /// Source part exhausted — force a segment boundary on the part
            /// boundary (I-BG-8) and advance to the next part on the next
            /// tick.
            if (global_ctx->internal_id_cursor > global_ctx->current_part_start_internal_id
                && (global_ctx->segment_boundaries_buffer.empty()
                    || global_ctx->segment_boundaries_buffer.back() != global_ctx->internal_id_cursor))
            {
                closeCurrentMappingSegment();
                global_ctx->segment_boundaries_buffer.push_back(global_ctx->internal_id_cursor);
            }
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
            getLogger("BuildTask"));

        auto builder = plan.buildQueryPipeline(
            QueryPlanOptimizationSettings(global_ctx->context),
            BuildQueryPipelineSettings(global_ctx->context));

        global_ctx->current_pipeline = std::make_unique<QueryPipeline>(
            QueryPipelineBuilder::getPipeline(std::move(*builder)));
        global_ctx->current_part_executor = std::make_unique<PullingPipelineExecutor>(
            *global_ctx->current_pipeline);
        global_ctx->current_part_start_internal_id = global_ctx->internal_id_cursor;
    }

    void processBlock(const Block & block)
    {
        const size_t num_rows = block.rows();
        if (num_rows == 0)
            return;

        openMappingSegmentIfNeeded();

        const auto data_part = global_ctx->source_parts[global_ctx->current_source_part_index];
        const UUID source_part_uuid = data_part->uuid;
        const UInt32 part_uuid_id = internPartUuid(source_part_uuid);

        const auto & block_number_col = block.getByName(BlockNumberColumn::name).column;
        const auto & block_offset_col = block.getByName(BlockOffsetColumn::name).column;
        const auto & part_offset_col = block.getByName("_part_offset").column;

        const UInt64 segment_threshold = getSegmentSizeRows();

        for (size_t i = 0; i < num_rows; ++i)
        {
            if ((global_ctx->internal_id_cursor % POLL_CANCEL_EVERY) == 0
                && global_ctx->is_cancelled.load(std::memory_order_relaxed))
                return;

            if (segment_threshold > 0 && global_ctx->current_segment_row_count >= segment_threshold)
            {
                closeCurrentMappingSegment();
                global_ctx->segment_boundaries_buffer.push_back(global_ctx->internal_id_cursor);
                openMappingSegmentIfNeeded();
            }

            const UInt64 block_number = block_number_col->getUInt(i);
            const UInt64 block_offset = block_offset_col->getUInt(i);
            const UInt64 part_offset = part_offset_col->getUInt(i);

            writeBinaryLittleEndian(block_number, *global_ctx->current_source_row_id_writer);
            writeBinaryLittleEndian(block_offset, *global_ctx->current_source_row_id_writer);
            ANNIndexPartReverseLookup::writeLocatorEntry(
                ANNIndexPartReverseLookup::liveLocatorEntry(part_uuid_id, part_offset),
                *global_ctx->current_locator_writer);

            ++global_ctx->internal_id_cursor;
            ++global_ctx->current_segment_row_count;
        }

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
        }
    }

    bool finalizeStage()
    {
        if (global_ctx->current_locator_writer || global_ctx->current_source_row_id_writer)
            closeCurrentMappingSegment();

        /// Segment boundary buffer invariant: always starts at 0 and ends at
        /// total row count. Final append is idempotent (guarded against
        /// double-push on the part-exhaust path).
        if (global_ctx->segment_boundaries_buffer.empty())
            global_ctx->segment_boundaries_buffer.push_back(0);
        if (global_ctx->segment_boundaries_buffer.back() != global_ctx->internal_id_cursor)
            global_ctx->segment_boundaries_buffer.push_back(global_ctx->internal_id_cursor);

        /// Publish to build_ctx for the algorithm's phase 2.
        global_ctx->build_ctx.segment_boundaries = global_ctx->segment_boundaries_buffer;
        global_ctx->build_ctx.total_rows = global_ctx->internal_id_cursor;

        /// `preferredSegmentBoundaries` is consulted at stage-1 start; the
        /// soft threshold honours algorithm-supplied boundaries when the
        /// vector is non-empty (see D-17). The on-disk layout always follows
        /// `segment_boundaries_buffer`.
        return false;
    }

    UInt32 internPartUuid(const UUID & uuid)
    {
        auto [it, inserted] = global_ctx->part_uuid_id_by_uuid.try_emplace(uuid, 0);
        if (inserted)
        {
            const size_t id = global_ctx->part_uuid_table.size();
            if (id > MAX_PART_UUID_ID)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "ANNIndex part_uuid_table overflow: more than {} distinct UUIDs", MAX_PART_UUID_ID);
            it->second = static_cast<UInt32>(id);
            global_ctx->part_uuid_table.push_back(uuid);
        }
        return it->second;
    }

    void openMappingSegmentIfNeeded()
    {
        if (global_ctx->current_locator_writer && global_ctx->current_source_row_id_writer)
            return;

        if (global_ctx->segment_boundaries_buffer.empty())
            global_ctx->segment_boundaries_buffer.push_back(0);

        /// Segment index = number of completed segment boundaries. With
        /// boundaries `[0, s1, ...]`, segment 0 covers `[0, s1)`, segment 1
        /// covers `[s1, s2)`, and so on.
        const size_t segment_index = global_ctx->segment_boundaries_buffer.size() - 1;
        global_ctx->current_source_row_id_writer = global_ctx->output_storage->writeFile(
            fmt::format("source_row_id_{}.bin", segment_index), 4096, WriteSettings{});
        global_ctx->current_locator_writer = global_ctx->output_storage->writeFile(
            fmt::format("locator_{}.bin", segment_index), 4096, WriteSettings{});
        global_ctx->current_segment_row_count = 0;
    }

    void closeCurrentMappingSegment()
    {
        if (global_ctx->current_source_row_id_writer)
        {
            global_ctx->current_source_row_id_writer->finalize();
            global_ctx->current_source_row_id_writer.reset();
        }
        if (global_ctx->current_locator_writer)
        {
            global_ctx->current_locator_writer->finalize();
            global_ctx->current_locator_writer.reset();
        }
        global_ctx->current_segment_row_count = 0;
    }

    UInt64 getSegmentSizeRows() const
    {
        /// Algorithm-supplied boundaries take precedence. The mid-layer does
        /// not interpret them beyond "non-empty means don't apply the soft
        /// threshold"; concrete boundary enforcement is left to the algorithm
        /// itself (future work).
        if (global_ctx->algorithm && !global_ctx->algorithm->preferredSegmentBoundaries().empty())
            return 0;

        if (global_ctx->storage)
            return (*global_ctx->storage->getSettings())[MergeTreeSetting::ann_index_segment_size_rows];

        /// Fallback for tests that construct a task without a storage pointer.
        return 16ULL * 1024 * 1024;
    }

    StageRuntimeContextPtr local_ctx;
    GlobalRuntimeContextPtr global_ctx;
};


/// Stage 3: exactly one algorithm->buildAlgorithmPrivate call.
struct BuildTask::BuildAlgorithmStage : public IStage
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
                tryLogCurrentException(getLogger("BuildTask"), __PRETTY_FUNCTION__);
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
struct BuildTask::FinishAlgorithmStage : public IStage
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
struct BuildTask::CleanupIntermediateStage : public IStage
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

        /// Phase B: reclaim the hash table used only for stage-1 UUID dedup.
        /// `part_uuid_table` itself is kept until header.json is written.
        global_ctx->part_uuid_id_by_uuid.clear();
        global_ctx->part_uuid_id_by_uuid.rehash(0);

        return false;
    }

    void cancel() noexcept override
    {
    }

    StageRuntimeContextPtr local_ctx;
    GlobalRuntimeContextPtr global_ctx;
};


/// Stage 6: write metadata and construct the Temporary-state
/// MergeTreeDataPartANNIndex. The full implementation lands in a
/// later change pack; the skeleton keeps the 5-method contract so the driver
/// loop compiles and exercises the state machine end to end.
struct BuildTask::FinalizeMetadataStage : public IStage
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

        /// Step 1: header.json (D-20). Poco::JSON serialization matches the
        /// convention in the rest of the codebase (see e.g. RestCatalog).
        writeHeaderJson();

        /// Step 2: coverage.json. One object per source part with its UUID
        /// and row count. Read back by `ReflectionANNIndex::startup` to
        /// rebuild `CoverageMap` after process restart, and by `ANNIndexRemapTask` as
        /// the base manifest before applying delta_in / delta_out.
        writeCoverageJson();
        writeAnnFormatJson();
        writeAnnCoverageJson();

        /// Step 3: construct the Temporary-state part before writing the
        /// standard metadata envelope because `uuid.txt` and `checksums.txt`
        /// must reflect the final part identity.
        constructNewMiPart();

        /// Step 4: standard MergeTree metadata. Replicated fetch calls
        /// `loadColumnsChecksumsIndexes(true, false)`, so MI parts must carry
        /// the same metadata envelope as regular parts even though their
        /// payload is algorithm-private.
        writeStandardMetadata();

        /// sync_guard dtor here fsyncs the directory on scope exit.
        return false;
    }

    void cancel() noexcept override
    {
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
        ANNIndexPartReverseLookup::addLocatorHeaderFields(header_json);

        Poco::JSON::Array uuid_arr;
        for (const auto & uuid : global_ctx->part_uuid_table)
            uuid_arr.add(toString(uuid));
        header_json.set("part_uuid_table", uuid_arr);

        const size_t segment_count = global_ctx->segment_boundaries_buffer.size() >= 2
            ? global_ctx->segment_boundaries_buffer.size() - 1
            : 0;
        header_json.set("segment_count", segment_count);

        Poco::JSON::Array seg_arr;
        for (auto boundary : global_ctx->segment_boundaries_buffer)
            seg_arr.add(boundary);
        header_json.set("segment_boundaries", seg_arr);

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

        Poco::JSON::Array covered_arr;
        for (const auto & part : global_ctx->source_parts)
        {
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

    void writeStandardMetadata() const
    {
        if (!global_ctx->output_storage)
            return;

        if (global_ctx->new_ann_index_part)
        {
            auto & part = global_ctx->new_ann_index_part;
            auto & inner_storage = global_ctx->storage->getInnerMergeTreeData(global_ctx->inner_storage_holder);
            writeANNIndexPartMetadata(
                part->getDataPartStorage(),
                &inner_storage,
                part->rows_count,
                global_ctx->source_partition_id,
                part->uuid);
            part->loadColumnsChecksumsIndexes(/*require_columns_checksums=*/true, /*check_consistency=*/false);
            return;
        }

        writeANNIndexPartMetadata(
            *global_ctx->output_storage,
            nullptr,
            global_ctx->build_ctx.total_rows,
            global_ctx->source_partition_id,
            global_ctx->new_part_uuid);
    }

    void constructNewMiPart() const
    {
        if (!global_ctx->storage || !global_ctx->output_storage)
            return;

        auto & inner_storage = global_ctx->storage->getInnerMergeTreeData(global_ctx->inner_storage_holder);
        auto part_info = MergeTreePartInfo::fromPartName(
            global_ctx->new_part_name, inner_storage.format_version);
        part_info = markAsANNIndexPartInfo(std::move(part_info));

        auto settings = inner_storage.getSettings();
        auto new_part = std::make_shared<MergeTreeDataPartANNIndex>(
            inner_storage,
            *settings,
            global_ctx->new_part_name,
            part_info,
            global_ctx->output_storage,
            /*parent_part_=*/nullptr);
        new_part->is_temp = true;
        new_part->uuid = global_ctx->new_part_uuid;
        new_part->rows_count = global_ctx->build_ctx.total_rows;
        global_ctx->new_ann_index_part = std::move(new_part);
    }

    StageRuntimeContextPtr local_ctx;
    GlobalRuntimeContextPtr global_ctx;
};


BuildTask::Stages BuildTask::makeStages()
{
    return {
        std::make_shared<ReadColumnsWriteLocatorAndPrepareStage>(),
        std::make_shared<BuildAlgorithmStage>(),
        std::make_shared<FinishAlgorithmStage>(),
        std::make_shared<CleanupIntermediateStage>(),
        std::make_shared<FinalizeMetadataStage>(),
    };
}


BuildTask::BuildTask(
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

    stages_iterator = stages.begin();

    auto prepare_stage_ctx = std::make_shared<IStageRuntimeContext>();
    (*stages_iterator)->setRuntimeContext(std::move(prepare_stage_ctx), global_ctx);
}


bool BuildTask::execute()
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


void BuildTask::cancel() noexcept
{
    global_ctx->is_cancelled.store(true, std::memory_order_relaxed);
    if (stages_iterator != stages.end())
        (*stages_iterator)->cancel();
}


std::future<MergeTreeData::MutableDataPartPtr> BuildTask::getFuture()
{
    return global_ctx->promise.get_future();
}


MergeTreeData::MutableDataPartPtr BuildTask::getUnfinishedPart()
{
    return global_ctx->new_ann_index_part;
}

}
