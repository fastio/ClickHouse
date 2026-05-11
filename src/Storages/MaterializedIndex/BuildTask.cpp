#include <Storages/MaterializedIndex/BuildTask.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
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
#include <Storages/MaterializedIndex/MergeTreeDataPartMaterializedIndex.h>
#include <Storages/MaterializedIndex/MaterializedIndexPartReverseLookup.h>
#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>
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
    extern const MergeTreeSettingsUInt64 materialized_index_segment_size_rows;
}


namespace
{

/// Stage-1 writes fixed 24-byte little-endian entries to each stable_layer
/// segment file: UInt32 part_uuid_dict_id | UInt32 partition_dict_id |
/// UInt64 _block_number | UInt64 _block_offset. Stage-2 emits fixed 12-byte
/// entries to each mutable_offset segment file: UInt32 part_uuid_dict_id |
/// UInt64 part_offset. The integer widths are fixed by the on-disk format
/// contract and are referenced directly by the read-side (future query
/// path).

/// Strict per-row cancel poll cadence inside stage 1. Checked every
/// POLL_CANCEL_EVERY rows so tight per-row hot path is not weighed down.
constexpr UInt64 POLL_CANCEL_EVERY = 256;

/// Dictionary widths (see D-16). The maximum UInt32 value is reserved for
/// tombstones in `mutable_offset`, so real dictionaries stop one id earlier.
/// If a build produces more the writer throws LOGICAL_ERROR so the caller
/// notices rather than silently truncating.
constexpr UInt32 MAX_DICT_ID = MaterializedIndexPartReverseLookup::TOMBSTONE_DICT_ID - 1;


/// Look up `key` in `dict_by_key`; if absent, append its encoded bytes to
/// `dict_writer` and record the freshly assigned id (monotonic from 0).
template <typename Key, typename Encode>
UInt32 internDictionary(
    std::unordered_map<Key, UInt32> & dict_by_key,
    WriteBufferFromFileBase & dict_writer,
    const Key & key,
    Encode && encode)
{
    auto [it, inserted] = dict_by_key.try_emplace(key, 0);
    if (inserted)
    {
        const size_t id = dict_by_key.size() - 1;
        if (id > MAX_DICT_ID)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "MaterializedIndex dictionary overflow: more than {} distinct keys", MAX_DICT_ID);
        it->second = static_cast<UInt32>(id);
        encode(key, dict_writer);
    }
    return it->second;
}

inline void writeUuidDictEntry(const UUID & uuid, WriteBufferFromFileBase & out)
{
    /// UUID is stored as two little-endian UInt64 halves for a total of 16
    /// bytes. The query side parses the same layout.
    writeBinaryLittleEndian(UUIDHelpers::getHighBytes(uuid), out);
    writeBinaryLittleEndian(UUIDHelpers::getLowBytes(uuid), out);
}

inline void writePartitionDictEntry(const String & pid, WriteBufferFromFileBase & out)
{
    /// Variable-length partition_id: UInt32 length prefix + UTF-8 bytes.
    writeBinaryLittleEndian(static_cast<UInt32>(pid.size()), out);
    out.write(pid.data(), pid.size());
}

}


/// Stage 1: read source blocks, write stable_layer entries + dictionary
/// appends, feed algorithm->prepareBuild per block.
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
                closeCurrentStableLayerSegment();
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

        Names columns_to_read;
        columns_to_read.reserve(3);
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
            /*alter_conversions=*/nullptr,
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

        /// Lazy-open dictionary writers on the first row of the first block.
        if (!global_ctx->part_uuid_dict_writer)
        {
            global_ctx->part_uuid_dict_writer = global_ctx->output_storage->writeFile(
                "part_uuid_dict.bin", 4096, WriteSettings{});
            global_ctx->partition_dict_writer = global_ctx->output_storage->writeFile(
                "partition_dict.bin", 4096, WriteSettings{});
        }

        openStableLayerSegmentIfNeeded();

        const auto data_part = global_ctx->source_parts[global_ctx->current_source_part_index];
        const UUID source_part_uuid = data_part->uuid;
        const String partition_id = data_part->info.getPartitionId();

        const UInt32 part_uuid_dict_id = internDictionary(
            global_ctx->part_uuid_dict_by_key,
            *global_ctx->part_uuid_dict_writer,
            source_part_uuid,
            writeUuidDictEntry);

        const UInt32 partition_dict_id = internDictionary(
            global_ctx->partition_dict_by_key,
            *global_ctx->partition_dict_writer,
            partition_id,
            writePartitionDictEntry);

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
                closeCurrentStableLayerSegment();
                global_ctx->segment_boundaries_buffer.push_back(global_ctx->internal_id_cursor);
                openStableLayerSegmentIfNeeded();
            }

            const UInt64 block_number = block_number_col->getUInt(i);
            const UInt64 block_offset = block_offset_col->getUInt(i);
            const UInt64 part_offset = part_offset_col->getUInt(i);

            writeBinaryLittleEndian(part_uuid_dict_id, *global_ctx->current_stable_layer_writer);
            writeBinaryLittleEndian(partition_dict_id, *global_ctx->current_stable_layer_writer);
            writeBinaryLittleEndian(block_number, *global_ctx->current_stable_layer_writer);
            writeBinaryLittleEndian(block_offset, *global_ctx->current_stable_layer_writer);

            global_ctx->stable_layer_part_uuid_ids.push_back(part_uuid_dict_id);
            global_ctx->stable_layer_part_offsets.push_back(part_offset);

            ++global_ctx->internal_id_cursor;
            ++global_ctx->current_segment_row_count;
        }

        /// Algorithm integration: feed the block to the registered algorithm
        /// once per block. The algorithm itself decides whether to process
        /// row-at-a-time or batch — either way the framework only pays one
        /// virtual call per block.
        if (global_ctx->algorithm)
            global_ctx->algorithm->prepareBuild(global_ctx->build_ctx, block);
    }

    bool finalizeStage()
    {
        if (global_ctx->current_stable_layer_writer)
            closeCurrentStableLayerSegment();

        if (global_ctx->part_uuid_dict_writer)
        {
            global_ctx->part_uuid_dict_writer->finalize();
            global_ctx->part_uuid_dict_writer.reset();
        }
        if (global_ctx->partition_dict_writer)
        {
            global_ctx->partition_dict_writer->finalize();
            global_ctx->partition_dict_writer.reset();
        }

        /// Segment boundary buffer invariant: always starts at 0 and ends at
        /// total row count. Final append is idempotent (guarded against
        /// double-push on the part-exhaust path).
        if (global_ctx->segment_boundaries_buffer.empty())
            global_ctx->segment_boundaries_buffer.push_back(0);
        if (global_ctx->segment_boundaries_buffer.back() != global_ctx->internal_id_cursor)
            global_ctx->segment_boundaries_buffer.push_back(global_ctx->internal_id_cursor);

        /// Publish to build_ctx for stage 2 and the algorithm's phase 2.
        global_ctx->build_ctx.segment_boundaries = global_ctx->segment_boundaries_buffer;
        global_ctx->build_ctx.total_rows = global_ctx->internal_id_cursor;

        /// `preferredSegmentBoundaries` is consulted at stage-1 start; the
        /// soft threshold honours algorithm-supplied boundaries when the
        /// vector is non-empty (see D-17). For stage-2 purposes the on-disk
        /// layout always follows `segment_boundaries_buffer`.
        return false;
    }

    void openStableLayerSegmentIfNeeded()
    {
        if (global_ctx->current_stable_layer_writer)
            return;

        if (global_ctx->segment_boundaries_buffer.empty())
            global_ctx->segment_boundaries_buffer.push_back(0);

        /// Segment index = number of completed segment boundaries. With
        /// boundaries `[0, s1, ...]`, segment 0 covers `[0, s1)`, segment 1
        /// covers `[s1, s2)`, and so on.
        const size_t segment_index = global_ctx->segment_boundaries_buffer.size() - 1;
        const String segment_path = fmt::format("stable_layer/{}.bin", segment_index);
        global_ctx->current_stable_layer_writer = global_ctx->output_storage->writeFile(
            segment_path, 4096, WriteSettings{});
        global_ctx->current_segment_row_count = 0;
    }

    void closeCurrentStableLayerSegment()
    {
        if (!global_ctx->current_stable_layer_writer)
            return;
        global_ctx->current_stable_layer_writer->finalize();
        global_ctx->current_stable_layer_writer.reset();
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
            return (*global_ctx->storage->getSettings())[MergeTreeSetting::materialized_index_segment_size_rows];

        /// Fallback for tests that construct a task without a storage pointer.
        return 16ULL * 1024 * 1024;
    }

    StageRuntimeContextPtr local_ctx;
    GlobalRuntimeContextPtr global_ctx;
};


/// Stage 2: fill mutable_offset files per segment boundary.
struct BuildTask::WriteMutableLayerStage : public IStage
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
        const auto & boundaries = global_ctx->segment_boundaries_buffer;

        /// A valid boundaries buffer always has the form [0, s1, s2, ...,
        /// total_rows]; the number of segments is boundaries.size() - 1.
        if (boundaries.size() < 2)
            return false;
        if (current_segment_index + 1 >= boundaries.size())
            return false;

        const UInt64 start = boundaries[current_segment_index];
        const UInt64 end = boundaries[current_segment_index + 1];

        const String segment_path = fmt::format("mutable_offset/{}.bin", current_segment_index);
        auto writer = global_ctx->output_storage->writeFile(segment_path, 4096, WriteSettings{});

        for (UInt64 internal_id = start; internal_id < end; ++internal_id)
        {
            const UInt32 part_uuid_dict_id = global_ctx->stable_layer_part_uuid_ids[internal_id];
            const UInt64 source_part_offset = global_ctx->stable_layer_part_offsets[internal_id];
            MaterializedIndexPartReverseLookup::writeLocatorEntry(
                MaterializedIndexPartReverseLookup::liveLocatorEntry(part_uuid_dict_id, source_part_offset),
                *writer);
        }

        writer->finalize();
        ++current_segment_index;
        return true;
    }

    void cancel() noexcept override
    {
    }

    StageRuntimeContextPtr local_ctx;
    GlobalRuntimeContextPtr global_ctx;
    size_t current_segment_index{0};
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

        /// Phase B (D-18): reclaim the large stage-1 auxiliary structures.
        /// The dictionaries have already been flushed to disk; the
        /// stable_layer_part_uuid_ids vector was consumed in stage 2. Stage 6
        /// reads dictionaries back from disk when producing checksum entries,
        /// so nothing downstream depends on the in-memory copies. Shrink-to-
        /// fit so the capacity is actually freed (clear() alone keeps the
        /// buckets allocated).
        global_ctx->part_uuid_dict_by_key.clear();
        global_ctx->part_uuid_dict_by_key.rehash(0);
        global_ctx->partition_dict_by_key.clear();
        global_ctx->partition_dict_by_key.rehash(0);
        global_ctx->stable_layer_part_uuid_ids.clear();
        global_ctx->stable_layer_part_uuid_ids.shrink_to_fit();
        global_ctx->stable_layer_part_offsets.clear();
        global_ctx->stable_layer_part_offsets.shrink_to_fit();

        return false;
    }

    void cancel() noexcept override
    {
    }

    StageRuntimeContextPtr local_ctx;
    GlobalRuntimeContextPtr global_ctx;
};


/// Stage 6: write metadata and construct the Temporary-state
/// MergeTreeDataPartMaterializedIndex. The full implementation lands in a
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
        /// and row count. Read back by `StorageMaterializedIndex::startup` to
        /// rebuild `CoverageMap` after process restart, and by `MaterializedIndexRemapTask` as
        /// the base manifest before applying delta_in / delta_out.
        writeCoverageJson();

        /// Step 3: checksum.txt (D-22). SipHash128 over data files only; meta
        /// files (header / coverage / txn_version / checksum itself) are
        /// deliberately excluded to avoid a self-reference.
        writeChecksumTxt();

        /// Step 4: txn_version.txt (D-23). Reserved for future transactional
        /// build integration; stage-2 writes the literal "0\n".
        writeTxnVersionTxt();

        /// Step 5: construct the Temporary-state part. The top-level Build
        /// task commits it via `MergeTreeData::Transaction::commit` which
        /// flips `is_temp` off and inserts into `data_parts_indexes`.
        constructNewMiPart();

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
        MaterializedIndexPartReverseLookup::addLocatorHeaderFields(header_json);

        const size_t segment_count = global_ctx->segment_boundaries_buffer.size() >= 2
            ? global_ctx->segment_boundaries_buffer.size() - 1
            : 0;
        header_json.set("segment_count", segment_count);

        Poco::JSON::Array seg_arr;
        for (auto boundary : global_ctx->segment_boundaries_buffer)
            seg_arr.add(boundary);
        header_json.set("segment_boundaries", seg_arr);

        /// `part_uuid_dict_by_key` is cleared by stage 5, so snapshot the
        /// coverage-source count from `source_parts.size()` instead — the two
        /// are equal by construction (first-seen dedup with one entry per
        /// source part).
        header_json.set("coverage_source_part_count", global_ctx->source_parts.size());
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

        Poco::JSON::Array covered_arr;
        for (const auto & part : global_ctx->source_parts)
        {
            Poco::JSON::Object item;
            item.set("source_part_uuid", toString(part->uuid));
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

    void writeChecksumTxt() const
    {
        if (!global_ctx->output_storage)
            return;

        /// Deviation from D-22: the mid-layer checksums only the files it
        /// wrote itself (stable_layer / mutable_offset / dictionaries). The
        /// `algorithm_private/` subtree is opaque at this layer; the
        /// algorithm is responsible for its own integrity via a private
        /// fingerprint inside its artefacts. Rationale: IDataPartStorage
        /// does not expose a recursive iterator, so listing the subtree from
        /// here would require peeking at the underlying Disk, which breaks
        /// the storage abstraction. Meta files (header / coverage /
        /// txn_version / checksum) are excluded to avoid self-reference.
        std::vector<String> data_files;
        collectMidLayerDataFiles(*global_ctx, data_files);
        std::sort(data_files.begin(), data_files.end());

        auto writer = global_ctx->output_storage->writeFile("checksum.txt", 4096, WriteSettings{});
        for (const auto & rel_path : data_files)
        {
            if (!global_ctx->output_storage->existsFile(rel_path))
                continue;

            SipHash hasher;
            auto reader = global_ctx->output_storage->readFile(rel_path, ReadSettings{}, std::nullopt);
            constexpr size_t chunk_size = 4096;
            std::array<char, chunk_size> buffer{};
            while (!reader->eof())
            {
                const size_t n = reader->readBig(buffer.data(), buffer.size());
                if (n == 0)
                    break;
                hasher.update(buffer.data(), n);
            }
            const UInt128 digest = hasher.get128();
            const UInt64 lo = digest.items[UInt128::_impl::little(0)];
            const UInt64 hi = digest.items[UInt128::_impl::little(1)];

            const String line = fmt::format("{:016x}{:016x} {}\n", lo, hi, rel_path);
            writer->write(line.data(), line.size());
        }
        writer->finalize();
    }

    static void collectMidLayerDataFiles(const GlobalRuntimeContext & ctx, std::vector<String> & out)
    {
        const size_t segment_count = ctx.segment_boundaries_buffer.size() >= 2
            ? ctx.segment_boundaries_buffer.size() - 1
            : 0;
        for (size_t i = 0; i < segment_count; ++i)
        {
            out.push_back(fmt::format("stable_layer/{}.bin", i));
            out.push_back(fmt::format("mutable_offset/{}.bin", i));
        }
        /// The dictionaries are opened lazily on the first row; skipped
        /// entirely when the Build task covers zero rows.
        if (ctx.internal_id_cursor > 0)
        {
            out.push_back("part_uuid_dict.bin");
            out.push_back("partition_dict.bin");
        }
    }

    void writeTxnVersionTxt() const
    {
        if (!global_ctx->output_storage)
            return;
        auto writer = global_ctx->output_storage->writeFile("txn_version.txt", 4096, WriteSettings{});
        const std::string_view payload{"0\n"};
        writer->write(payload.data(), payload.size());
        writer->finalize();
    }

    void constructNewMiPart() const
    {
        if (!global_ctx->storage || !global_ctx->output_storage)
            return;

        auto part_info = MergeTreePartInfo::fromPartName(
            global_ctx->new_part_name, global_ctx->storage->format_version);
        chassert(part_info.getKind() == MergeTreePartInfo::Kind::MaterializedIndex);

        auto settings = global_ctx->storage->getSettings();
        auto new_part = std::make_shared<MergeTreeDataPartMaterializedIndex>(
            *global_ctx->storage,
            *settings,
            global_ctx->new_part_name,
            part_info,
            global_ctx->output_storage,
            /*parent_part_=*/nullptr);
        new_part->is_temp = true;
        global_ctx->new_mi_part = std::move(new_part);
    }

    StageRuntimeContextPtr local_ctx;
    GlobalRuntimeContextPtr global_ctx;
};


BuildTask::Stages BuildTask::makeStages()
{
    return {
        std::make_shared<ReadColumnsWriteLocatorAndPrepareStage>(),
        std::make_shared<WriteMutableLayerStage>(),
        std::make_shared<BuildAlgorithmStage>(),
        std::make_shared<FinishAlgorithmStage>(),
        std::make_shared<CleanupIntermediateStage>(),
        std::make_shared<FinalizeMetadataStage>(),
    };
}


BuildTask::BuildTask(
    MergeTreeData::DataPartsVector source_parts_,
    IMaterializedIndexAlgorithm * algorithm_,
    StorageMaterializedIndex * storage_,
    String new_part_name_,
    const MergeTreeData * source_storage_,
    StorageSnapshotPtr source_snapshot_,
    StorageMetadataPtr source_metadata_,
    ContextPtr context_,
    MutableDataPartStoragePtr output_storage_,
    MutableDataPartStoragePtr intermediate_storage_,
    UInt64 memory_budget_bytes_)
    : global_ctx(std::make_shared<GlobalRuntimeContext>())
    , stages(makeStages())
{
    global_ctx->source_parts = std::move(source_parts_);
    global_ctx->algorithm = algorithm_;
    global_ctx->storage = storage_;
    global_ctx->new_part_name = std::move(new_part_name_);
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


BuildTask::~BuildTask()
{
    /// Best-effort: if the task is destroyed before the promise was fulfilled
    /// (cancellation, exception during stage construction) wake up any waiter
    /// on getFuture().get() so the caller does not block indefinitely.
    if (promise_fulfilled)
        return;

    try
    {
        global_ctx->promise.set_exception(std::make_exception_ptr(
            Exception(ErrorCodes::LOGICAL_ERROR, "BuildTask destroyed before completion")));
    }
    catch (const std::future_error &)
    {
        /// Promise may have been satisfied on a racing code path; swallow to
        /// keep the destructor noexcept-friendly.
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


bool BuildTask::execute()
try
{
    chassert(stages_iterator != stages.end());
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
            global_ctx->promise.set_value(global_ctx->new_mi_part);
            promise_fulfilled = true;
        }
        return false;
    }

    (*stages_iterator)->setRuntimeContext(std::move(next_stage_context), global_ctx);
    return true;
}
catch (...)
{
    /// Propagate the exception to any thread waiting on getFuture().get()
    /// and rethrow so the top-level scheduler can record the failure.
    if (!promise_fulfilled)
    {
        global_ctx->promise.set_exception(std::current_exception());
        promise_fulfilled = true;
    }
    throw;
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
    return global_ctx->new_mi_part;
}

}
