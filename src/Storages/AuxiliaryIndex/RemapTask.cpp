#include <Storages/AuxiliaryIndex/RemapTask.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Core/UUID.h>
#include <Disks/IDisk.h>
#include <Disks/SingleDiskVolume.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/AuxiliaryIndex/MergeTreeDataPartAuxiliaryIndex.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexPartMetadata.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexPartName.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexPartReverseLookup.h>
#include <Storages/AuxiliaryIndex/StorageANN.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/StorageSnapshot.h>

#include <Core/Block.h>
#include <Columns/IColumn.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <fmt/format.h>

#include <exception>
#include <filesystem>
#include <future>
#include <utility>


namespace fs = std::filesystem;


namespace DB
{

/// -----------------------------------------------------------------------------
/// Helpers shared by stages 1-4.
/// -----------------------------------------------------------------------------

namespace
{

constexpr UInt64 PARTITIONING_FORMAT_VERSION = 1;

struct PartLayoutHeader
{
    size_t segment_count = 0;
    std::vector<UInt64> segment_boundaries;
    std::vector<UUID> part_uuid_table;
};

PartLayoutHeader readPartLayoutHeader(const IDataPartStorage & storage)
{
    if (!storage.existsFile("header.json"))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Materialized-index part {} does not have header.json", storage.getRelativePath());

    auto header_reader = storage.readFile("header.json", ReadSettings{}, std::nullopt);
    String header_text;
    readStringUntilEOF(header_text, *header_reader);

    Poco::JSON::Parser parser;
    auto parsed = parser.parse(header_text);
    auto obj = parsed.extract<Poco::JSON::Object::Ptr>();
    if (!obj)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Materialized-index part {} header.json is not a JSON object", storage.getRelativePath());

    PartLayoutHeader header;
    if (obj->has("segment_count"))
        header.segment_count = obj->getValue<size_t>("segment_count");

    auto boundaries_arr = obj->getArray("segment_boundaries");
    if (!boundaries_arr)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Materialized-index part {} header.json missing segment_boundaries", storage.getRelativePath());
    header.segment_boundaries.reserve(boundaries_arr->size());
    for (size_t i = 0; i < boundaries_arr->size(); ++i)
        header.segment_boundaries.push_back(boundaries_arr->getElement<UInt64>(static_cast<unsigned int>(i)));

    if (header.segment_boundaries.empty() || header.segment_boundaries.front() != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Materialized-index part {} has invalid segment_boundaries", storage.getRelativePath());
    if (header.segment_count == 0 && header.segment_boundaries.size() > 1)
        header.segment_count = header.segment_boundaries.size() - 1;
    if (header.segment_boundaries.size() != header.segment_count + 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Materialized-index part {} has segment_count {} but {} boundaries",
            storage.getRelativePath(), header.segment_count, header.segment_boundaries.size());

    auto uuid_arr = obj->getArray("part_uuid_table");
    if (!uuid_arr)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Materialized-index part {} header.json missing part_uuid_table", storage.getRelativePath());
    header.part_uuid_table.reserve(uuid_arr->size());
    for (size_t i = 0; i < uuid_arr->size(); ++i)
    {
        UUID uuid;
        const auto uuid_text = uuid_arr->getElement<std::string>(static_cast<unsigned int>(i));
        if (!tryParse(uuid, uuid_text))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Materialized-index part {} cannot parse part_uuid_table[{}] as UUID: {}",
                storage.getRelativePath(), i, uuid_text);
        header.part_uuid_table.push_back(uuid);
    }

    return header;
}

/// Scan one `locator_<seg>.bin` and return `true` as soon as a row's
/// `part_uuid_id` resolves to a UUID present in either delta set. The
/// scan stops at the first hit (affected-segment detection is a boolean
/// classification, not an accumulation).
bool segmentIntersectsDelta(
    const IDataPartStorage & storage,
    size_t segment_index,
    const std::vector<UUID> & part_uuid_table,
    const std::unordered_set<UUID> & delta_uuids)
{
    const String segment_path = fmt::format("locator_{}.bin", segment_index);
    if (!storage.existsFile(segment_path))
        return false;
    if (part_uuid_table.empty() || delta_uuids.empty())
        return false;

    auto reader = storage.readFile(segment_path, ReadSettings{}, std::nullopt);
    while (!reader->eof())
    {
        auto locator = AuxiliaryIndexPartReverseLookup::readLocatorEntry(*reader);
        if (locator.isTombstone())
            continue;
        if (locator.part_uuid_id < part_uuid_table.size() && delta_uuids.contains(part_uuid_table[locator.part_uuid_id]))
            return true;
    }
    return false;
}

/// Parse a materialized-index part name, bump `level` by one and rebuild the
/// canonical string.
String bumpLevelInPartName(const String & old_name, MergeTreeDataFormatVersion format_version)
{
    auto info = MergeTreePartInfo::fromPartName(old_name, format_version);
    info.level += 1;
    return info.getPartNameAndCheckFormat(format_version);
}

/// Derive a `tmp_auxiliary_index_remap_<new_part_name>` storage that lives on the same
/// disk as `source_storage`. The new directory is not created on disk here;
/// stage 2 does that via `createDirectories()`.
MutableDataPartStoragePtr makeRemapTmpStorage(
    const IDataPartStorage & source_storage,
    const MergeTreeData & storage_for_relative_path,
    const String & new_part_name)
{
    const auto & disk_base = dynamic_cast<const DataPartStorageOnDiskBase &>(source_storage);
    const auto disk = disk_base.getDisk();
    const String relative_data_path = storage_for_relative_path.getRelativeDataPath();

    auto volume = std::make_shared<SingleDiskVolume>(
        "remap_tmp_volume_" + new_part_name, disk, /*max_data_part_size=*/0);

    const String tmp_dir_name = String{RemapTask::TEMP_DIRECTORY_PREFIX} + new_part_name;
    return std::make_shared<DataPartStorageOnDiskFull>(std::move(volume), relative_data_path, tmp_dir_name);
}

String joinDiskPath(const String & lhs, const String & rhs)
{
    if (lhs.empty())
        return rhs;
    if (rhs.empty())
        return lhs;
    if (lhs.ends_with('/'))
        return lhs + rhs;
    return lhs + "/" + rhs;
}

void validateAlgorithmPrivatePath(const AlgorithmPrivatePath & private_path)
{
    if (private_path.path.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AuxiliaryIndex algorithm returned an empty private path");

    const fs::path path(private_path.path);
    if (path.is_absolute())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AuxiliaryIndex algorithm returned absolute private path {}", private_path.path);

    for (const auto & component : path)
    {
        if (component == "..")
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "AuxiliaryIndex algorithm returned private path with parent traversal: {}", private_path.path);
    }
}

String relativeToPartRoot(const String & part_root, const String & disk_path)
{
    const String prefix = part_root.ends_with('/') ? part_root : part_root + "/";
    if (!disk_path.starts_with(prefix))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot map disk path {} below materialized-index part root {}", disk_path, part_root);
    return disk_path.substr(prefix.size());
}

void createParentDirectories(IDataPartStorage & dest_storage, const String & rel_path)
{
    const String parent = fs::path(rel_path).parent_path().generic_string();
    if (parent.empty())
        return;

    const auto & dest_base = dynamic_cast<const DataPartStorageOnDiskBase &>(dest_storage);
    dest_base.getDisk()->createDirectories(joinDiskPath(dest_storage.getRelativePath(), parent));
}

void createDirectoryInDest(IDataPartStorage & dest_storage, const String & rel_dir)
{
    dest_storage.createDirectories();
    const auto & dest_base = dynamic_cast<const DataPartStorageOnDiskBase &>(dest_storage);
    dest_base.getDisk()->createDirectories(joinDiskPath(dest_storage.getRelativePath(), rel_dir));
}

void hardlinkOrCopyFile(
    const IDataPartStorage & source_storage,
    IDataPartStorage & dest_storage,
    const String & rel_path,
    LoggerPtr log)
{
    dest_storage.createDirectories();
    createParentDirectories(dest_storage, rel_path);

    try
    {
        dest_storage.createHardLinkFrom(source_storage, rel_path, rel_path);
    }
    catch (...)
    {
        /// Cross-disk (or zero-copy) hardlink failures fall back to
        /// physical copy; log once per failed file rather than throwing
        /// so a single cross-disk part does not abort the whole remap.
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        dest_storage.copyFileFrom(source_storage, rel_path, rel_path);
    }
}

void hardlinkOrCopyRecursiveDirectory(
    const IDataPartStorage & source_storage,
    IDataPartStorage & dest_storage,
    const String & rel_dir,
    LoggerPtr log)
{
    const auto & src_base = dynamic_cast<const DataPartStorageOnDiskBase &>(source_storage);
    const auto disk = src_base.getDisk();
    const String part_root = source_storage.getRelativePath();

    std::vector<String> dirs{joinDiskPath(part_root, rel_dir)};
    while (!dirs.empty())
    {
        String current = std::move(dirs.back());
        dirs.pop_back();

        createDirectoryInDest(dest_storage, relativeToPartRoot(part_root, current));

        for (auto it = disk->iterateDirectory(current); it->isValid(); it->next())
        {
            const String entry_path = it->path();
            if (disk->existsFile(entry_path))
            {
                hardlinkOrCopyFile(source_storage, dest_storage, relativeToPartRoot(part_root, entry_path), log);
                continue;
            }

            if (disk->existsDirectory(entry_path))
                dirs.push_back(entry_path);
        }
    }
}

void hardlinkOrCopyAlgorithmPrivatePath(
    const IDataPartStorage & source_storage,
    IDataPartStorage & dest_storage,
    const AlgorithmPrivatePath & private_path,
    LoggerPtr log)
{
    validateAlgorithmPrivatePath(private_path);

    if (private_path.recursive)
    {
        if (!source_storage.existsDirectory(private_path.path))
        {
            if (private_path.required)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Required AuxiliaryIndex algorithm private directory {} is missing in part {}",
                    private_path.path,
                    source_storage.getRelativePath());
            return;
        }

        hardlinkOrCopyRecursiveDirectory(source_storage, dest_storage, private_path.path, log);
        return;
    }

    if (!source_storage.existsFile(private_path.path))
    {
        if (private_path.required)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Required AuxiliaryIndex algorithm private file {} is missing in part {}",
                private_path.path,
                source_storage.getRelativePath());
        return;
    }

    hardlinkOrCopyFile(source_storage, dest_storage, private_path.path, log);
}

}


/// -----------------------------------------------------------------------------
/// Pack 1 skeleton stages. Every stage returns false on the first execute()
/// call; the outer driver advances the state machine across all four stages
/// and fulfils the promise with the (empty) new-parts vector. Real data-path
/// implementations land in Pack 2-4.
/// -----------------------------------------------------------------------------

struct RemapTask::PlanAffectedSegmentsStage : public IStage
{
    void setRuntimeContext(StageRuntimeContextPtr, StageRuntimeContextPtr global) override
    {
        global_ctx = std::static_pointer_cast<GlobalRuntimeContext>(global);
    }

    StageRuntimeContextPtr getContextForNextStage() override { return global_ctx; }
    ProfileEvents::Event getTotalTimeProfileEvent() const override { return ProfileEvents::end(); }

    bool execute() override
    {
        auto & ctx = *global_ctx;
        if (ctx.affected_auxiliary_index_parts.empty())
            return false;
        if (!ctx.storage)
            return false;

        /// Build a single set with every UUID that could flag a segment as
        /// affected. `delta_out` is a raw UUID vector; `delta_in` is a part
        /// vector and we take `part->uuid` for each.
        std::unordered_set<UUID> delta_uuids;
        delta_uuids.reserve(ctx.delta_out_source_uuids.size() + ctx.delta_in_source_parts.size());
        for (const auto & uuid : ctx.delta_out_source_uuids)
            delta_uuids.insert(uuid);
        for (const auto & part : ctx.delta_in_source_parts)
            delta_uuids.insert(part->uuid);

        auto & inner_storage = ctx.storage->getInnerMergeTreeData(ctx.inner_storage_holder);
        const auto format_version = inner_storage.format_version;
        const size_t n = ctx.affected_auxiliary_index_parts.size();

        ctx.new_auxiliary_index_parts.resize(n);
        ctx.tmp_storages.reserve(n);
        ctx.affected_seg_ids_per_new_part.assign(n, {});
        ctx.segment_count_per_new_part.assign(n, 0);
        ctx.old_index_per_new_part.resize(n);
        ctx.incoming_part_uuid_id_per_new_part.assign(n, std::nullopt);
        ctx.part_uuid_table_per_new_part.assign(n, {});
        ctx.tombstone_rows_per_new_part.assign(n, 0);

        auto log = getLogger("RemapTask");

        for (size_t i = 0; i < n; ++i)
        {
            const auto & old_part = ctx.affected_auxiliary_index_parts[i];
            if (!old_part)
                continue;

            const auto & old_storage = old_part->getDataPartStorage();
            if (auto * algorithm = ctx.storage->getAlgorithm())
            {
                auto compatibility = algorithm->checkPartCompatibility(old_storage);
                if (!compatibility.compatible)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Cannot remap materialized-index-part {} with incompatible algorithm fingerprint: {}",
                        old_part->name,
                        compatibility.reason);
            }

            /// Layout data is recorded in header.json; keep the dependency
            /// between Remap and the Build on-disk spec localised to one helper.
            const auto layout = readPartLayoutHeader(old_storage);
            ctx.segment_count_per_new_part[i] = layout.segment_count;
            ctx.part_uuid_table_per_new_part[i] = layout.part_uuid_table;

            /// Scan every segment's locator file to classify it as affected.
            /// Short-circuit: `segmentIntersectsDelta` returns as soon as it
            /// sees one delta-referencing row (sampling is implicit — no
            /// need to scan the whole segment once classification is set).
            if (!delta_uuids.empty())
            {
                for (size_t seg = 0; seg < layout.segment_count; ++seg)
                {
                    if (segmentIntersectsDelta(old_storage, seg, layout.part_uuid_table, delta_uuids))
                        ctx.affected_seg_ids_per_new_part[i].insert(seg);
                }
            }

            /// N=M derivation: one new materialized-index-part per old, `level` bumped by one.
            const String new_part_name = bumpLevelInPartName(old_part->name, format_version);
            const auto new_part_info = markAsAuxiliaryIndexPartInfo(
                MergeTreePartInfo::fromPartName(new_part_name, format_version));

            auto new_tmp_storage = makeRemapTmpStorage(old_storage, inner_storage, new_part_name);
            ctx.tmp_storages[new_part_name] = new_tmp_storage;

            auto settings = inner_storage.getSettings();
            auto new_part = std::make_shared<MergeTreeDataPartAuxiliaryIndex>(
                inner_storage,
                *settings,
                new_part_name,
                new_part_info,
                new_tmp_storage,
                /*parent_part_=*/nullptr);
            new_part->is_temp = true;
            ctx.new_auxiliary_index_parts[i] = std::move(new_part);
            ctx.old_index_per_new_part[i] = i;
        }

        return false;
    }

    void cancel() noexcept override {}

    GlobalRuntimeContextPtr global_ctx;
};


struct RemapTask::DeriveHardlinksStage : public IStage
{
    void setRuntimeContext(StageRuntimeContextPtr, StageRuntimeContextPtr global) override
    {
        global_ctx = std::static_pointer_cast<GlobalRuntimeContext>(global);
    }

    StageRuntimeContextPtr getContextForNextStage() override { return global_ctx; }
    ProfileEvents::Event getTotalTimeProfileEvent() const override { return ProfileEvents::end(); }

    bool execute() override
    {
        auto & ctx = *global_ctx;
        if (ctx.stage2_cursor >= ctx.new_auxiliary_index_parts.size())
            return false;
        if (ctx.is_cancelled.load(std::memory_order_relaxed))
            return false;

        const size_t i = ctx.stage2_cursor;
        const auto & new_part = ctx.new_auxiliary_index_parts[i];
        if (!new_part)
        {
            ++ctx.stage2_cursor;
            return ctx.stage2_cursor < ctx.new_auxiliary_index_parts.size();
        }

        const size_t old_idx = ctx.old_index_per_new_part[i];
        const auto & old_part = ctx.affected_auxiliary_index_parts[old_idx];
        const auto & old_storage = old_part->getDataPartStorage();
        auto & dest_storage = new_part->getDataPartStorage();

        auto log = getLogger("RemapTask");

        auto * algorithm = ctx.storage ? ctx.storage->getAlgorithm() : nullptr;
        if (!algorithm)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cannot remap materialized-index-part {} without an AuxiliaryIndex algorithm private-path provider",
                old_part->name);

        /// Algorithm-private files are opaque to the framework and remain
        /// valid across remap because internal ids stay in the same order.
        for (const auto & private_path : algorithm->getAlgorithmPrivatePaths(old_storage))
            hardlinkOrCopyAlgorithmPrivatePath(old_storage, dest_storage, private_path, log);

        /// `source_row_id` is immutable across remaps, so it is always linked.
        /// `locator` is linked only for non-affected segments; stage 3 rewrites
        /// it for affected segments.
        const size_t segment_count = ctx.segment_count_per_new_part[i];
        const auto & affected = ctx.affected_seg_ids_per_new_part[i];
        if (segment_count > 0)
        {
            dest_storage.createDirectories();
            for (size_t seg = 0; seg < segment_count; ++seg)
            {
                for (const auto & rel : {
                    fmt::format("source_row_id_{}.bin", seg),
                    fmt::format("locator_{}.bin", seg)})
                {
                    if (affected.contains(seg) && rel.starts_with("locator_"))
                        continue;
                    if (!old_storage.existsFile(rel))
                        continue;
                    try
                    {
                        dest_storage.createHardLinkFrom(old_storage, rel, rel);
                    }
                    catch (...)
                    {
                        tryLogCurrentException(log, __PRETTY_FUNCTION__);
                        dest_storage.copyFileFrom(old_storage, rel, rel);
                    }
                }
            }
        }

        ++ctx.stage2_cursor;
        return ctx.stage2_cursor < ctx.new_auxiliary_index_parts.size();
    }

    void cancel() noexcept override {}

    GlobalRuntimeContextPtr global_ctx;
};


struct RemapTask::RewriteMutableSegmentsStage : public IStage
{
    void setRuntimeContext(StageRuntimeContextPtr, StageRuntimeContextPtr global) override
    {
        global_ctx = std::static_pointer_cast<GlobalRuntimeContext>(global);
    }

    StageRuntimeContextPtr getContextForNextStage() override { return global_ctx; }
    ProfileEvents::Event getTotalTimeProfileEvent() const override { return ProfileEvents::end(); }

    void loadIncomingRowsIfNeeded()
    {
        auto & ctx = *global_ctx;
        if (ctx.incoming_rows_loaded)
            return;

        ctx.incoming_rows_loaded = true;
        if (ctx.delta_in_source_parts.size() != 1)
            return;

        const auto & data_part = ctx.delta_in_source_parts.front();
        if (!data_part)
            return;

        ctx.incoming_source_part_uuid = data_part->uuid;
        ctx.incoming_source_partition_id = data_part->info.getPartitionId();
        ctx.incoming_source_rows = data_part->rows_count;

        if (!ctx.source_storage || !ctx.source_snapshot || !ctx.context)
            return;

        QueryPlan plan;
        Names columns_to_read{
            BlockNumberColumn::name,
            BlockOffsetColumn::name,
            "_part_offset",
        };
        createReadFromPartStep(
            MergeTreeSequentialSourceType::Merge,
            plan,
            *ctx.source_storage,
            ctx.source_snapshot,
            RangesInDataPart(data_part),
            /*alter_conversions=*/std::make_shared<AlterConversions>(),
            /*merged_part_offsets=*/nullptr,
            std::move(columns_to_read),
            /*filtered_rows_count=*/nullptr,
            /*apply_deleted_mask=*/true,
            /*filter=*/std::nullopt,
            /*read_with_direct_io=*/false,
            /*prefetch=*/false,
            ctx.context,
            getLogger("RemapTask"));

        auto builder = plan.buildQueryPipeline(
            QueryPlanOptimizationSettings(ctx.context),
            BuildQueryPipelineSettings(ctx.context));
        QueryPipeline pipeline(QueryPipelineBuilder::getPipeline(std::move(*builder)));
        PullingPipelineExecutor executor(pipeline);

        Block block;
        while (executor.pull(block))
        {
            const auto & block_number_col = block.getByName(BlockNumberColumn::name).column;
            const auto & block_offset_col = block.getByName(BlockOffsetColumn::name).column;
            const auto & part_offset_col = block.getByName("_part_offset").column;
            for (size_t i = 0; i < block.rows(); ++i)
            {
                GlobalRuntimeContext::SourceRowId identity{
                    block_number_col->getUInt(i),
                    block_offset_col->getUInt(i),
                };
                ctx.incoming_part_offsets.emplace(identity, part_offset_col->getUInt(i));
            }
        }
    }

    UInt32 ensureIncomingPartUuidId(size_t new_part_index)
    {
        auto & ctx = *global_ctx;
        auto & cached = ctx.incoming_part_uuid_id_per_new_part[new_part_index];
        if (cached)
            return *cached;

        auto & table = ctx.part_uuid_table_per_new_part[new_part_index];
        for (size_t i = 0; i < table.size(); ++i)
        {
            if (table[i] == ctx.incoming_source_part_uuid)
            {
                cached = static_cast<UInt32>(i);
                return *cached;
            }
        }

        if (table.size() > AuxiliaryIndexPartReverseLookup::TOMBSTONE_PART_UUID_ID - 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "AuxiliaryIndex part_uuid_table overflow: more than {} distinct UUIDs",
                AuxiliaryIndexPartReverseLookup::TOMBSTONE_PART_UUID_ID - 1);
        const auto id = static_cast<UInt32>(table.size());
        table.push_back(ctx.incoming_source_part_uuid);
        cached = id;
        return id;
    }

    bool execute() override
    {
        auto & ctx = *global_ctx;
        loadIncomingRowsIfNeeded();

        if (ctx.new_auxiliary_index_parts.empty())
            return false;

        /// Advance the part/segment cursor.
        if (cursor_initialised)
        {
            ++segment_cursor;
        }
        else
        {
            cursor_initialised = true;
            part_cursor = 0;
            segment_cursor = 0;
        }

        while (true)
        {
            if (part_cursor >= ctx.new_auxiliary_index_parts.size())
                return false;
            if (ctx.is_cancelled.load(std::memory_order_relaxed))
                return false;

            const auto & affected = ctx.affected_seg_ids_per_new_part[part_cursor];
            while (segment_cursor < ctx.segment_count_per_new_part[part_cursor]
                && !affected.contains(segment_cursor))
                ++segment_cursor;

            if (segment_cursor >= ctx.segment_count_per_new_part[part_cursor])
            {
                ++part_cursor;
                segment_cursor = 0;
                continue;
            }
            break;
        }

        /// At this point (part_cursor, segment_cursor) points to an affected
        /// segment that needs rewriting.
        const size_t i = part_cursor;
        const auto & new_part = ctx.new_auxiliary_index_parts[i];
        if (!new_part)
        {
            /// Force advancement to the next part on the next invocation.
            ++part_cursor;
            segment_cursor = 0;
            cursor_initialised = false;
            return part_cursor < ctx.new_auxiliary_index_parts.size();
        }

        const size_t old_idx = ctx.old_index_per_new_part[i];
        const auto & old_part = ctx.affected_auxiliary_index_parts[old_idx];
        const auto & old_storage = old_part->getDataPartStorage();
        auto & dest_storage = new_part->getDataPartStorage();

        auto log = getLogger("RemapTask");

        /// `locator` carries the mutable source UUID id + `_part_offset`;
        /// `source_row_id` carries the stable `(block_number, block_offset)`
        /// identity used to match outgoing rows against incoming source rows.
        const auto layout = readPartLayoutHeader(old_storage);
        const auto & part_uuid_table = layout.part_uuid_table;
        std::unordered_set<UUID> delta_out_set(
            ctx.delta_out_source_uuids.begin(), ctx.delta_out_source_uuids.end());

        const String source_row_id_rel = fmt::format("source_row_id_{}.bin", segment_cursor);
        const String locator_rel = fmt::format("locator_{}.bin", segment_cursor);

        if (!old_storage.existsFile(locator_rel))
        {
            /// Nothing to rewrite; advance and continue on next call.
            return true;
        }

        if (segment_cursor + 1 >= layout.segment_boundaries.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cannot rewrite materialized-index segment {} for part {}: missing segment boundary",
                segment_cursor, old_part->name);

        const UInt64 segment_rows = layout.segment_boundaries[segment_cursor + 1] - layout.segment_boundaries[segment_cursor];
        auto source_row_id_reader = old_storage.readFile(source_row_id_rel, ReadSettings{}, std::nullopt);
        auto locator_reader = old_storage.readFile(locator_rel, ReadSettings{}, std::nullopt);
        auto writer = dest_storage.writeFile(locator_rel, 4096, WriteSettings{});

        size_t tombstones = 0;
        size_t survivors = 0;
        for (UInt64 row = 0; row < segment_rows; ++row)
        {
            if ((row & 0xFF) == 0
                && ctx.is_cancelled.load(std::memory_order_relaxed))
            {
                /// Cooperative cancel: finalize whatever has been written so
                /// the caller can observe partial progress via iterate().
                writer->finalize();
                return false;
            }

            UInt64 block_number = 0;
            UInt64 block_offset = 0;
            if (source_row_id_reader->eof())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "AuxiliaryIndex source_row_id segment {} truncated at row {} (expected {})",
                    segment_cursor, row, segment_rows);
            readBinaryLittleEndian(block_number, *source_row_id_reader);
            readBinaryLittleEndian(block_offset, *source_row_id_reader);

            if (locator_reader->eof())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "AuxiliaryIndex locator segment {} truncated at row {} (expected {})",
                    segment_cursor, row, segment_rows);
            auto old_locator = AuxiliaryIndexPartReverseLookup::readLocatorEntry(*locator_reader);

            const bool outgoing = !old_locator.isTombstone()
                && old_locator.part_uuid_id < part_uuid_table.size()
                && delta_out_set.contains(part_uuid_table[old_locator.part_uuid_id]);

            AuxiliaryIndexPartReverseLookup::LocatorEntry out_locator;
            if (outgoing)
            {
                GlobalRuntimeContext::SourceRowId identity{block_number, block_offset};
                auto incoming_it = ctx.incoming_part_offsets.find(identity);
                if (incoming_it != ctx.incoming_part_offsets.end())
                {
                    const UInt32 incoming_part_uuid_id = ensureIncomingPartUuidId(i);
                    out_locator = AuxiliaryIndexPartReverseLookup::liveLocatorEntry(
                        incoming_part_uuid_id,
                        incoming_it->second);
                    ++survivors;
                }
                else
                {
                    out_locator = AuxiliaryIndexPartReverseLookup::tombstoneLocatorEntry();
                    ++tombstones;
                }
            }
            else
            {
                out_locator = old_locator;
                ++survivors;
            }

            AuxiliaryIndexPartReverseLookup::writeLocatorEntry(out_locator, *writer);
        }
        assertEOF(*source_row_id_reader);
        assertEOF(*locator_reader);
        writer->finalize();
        ctx.tombstone_rows_per_new_part[i] += tombstones;

        if (tombstones == 0 && survivors == segment_rows)
            LOG_TRACE(log, "Rewrote segment {} for part {}: {} live rows, 0 tombstones",
                segment_cursor, new_part->name, survivors);
        else
            LOG_DEBUG(log, "Rewrote segment {} for part {}: {} live rows, {} tombstones",
                segment_cursor, new_part->name, survivors, tombstones);

        return true;
    }

    void cancel() noexcept override {}

    GlobalRuntimeContextPtr global_ctx;
    bool cursor_initialised{false};
    size_t part_cursor{0};
    size_t segment_cursor{0};
};


struct RemapTask::FinalizeMetadataStage : public IStage
{
    void setRuntimeContext(StageRuntimeContextPtr, StageRuntimeContextPtr global) override
    {
        global_ctx = std::static_pointer_cast<GlobalRuntimeContext>(global);
    }

    StageRuntimeContextPtr getContextForNextStage() override { return global_ctx; }
    ProfileEvents::Event getTotalTimeProfileEvent() const override { return ProfileEvents::end(); }

    bool execute() override
    {
        auto & ctx = *global_ctx;
        if (ctx.new_auxiliary_index_parts.empty())
            return false;

        std::unordered_set<UUID> delta_out_set(
            ctx.delta_out_source_uuids.begin(), ctx.delta_out_source_uuids.end());

        for (size_t i = 0; i < ctx.new_auxiliary_index_parts.size(); ++i)
        {
            const auto & new_part = ctx.new_auxiliary_index_parts[i];
            if (!new_part)
                continue;

            const size_t old_idx = ctx.old_index_per_new_part[i];
            const auto & old_part = ctx.affected_auxiliary_index_parts[old_idx];
            const auto & old_storage = old_part->getDataPartStorage();
            auto & dest_storage = new_part->getDataPartStorage();

            SyncGuardPtr sync_guard = dest_storage.getDirectorySyncGuard();

            const MergeTreeData::DataPartPtr incoming_source_part
                = ctx.delta_in_source_parts.size() == 1 ? ctx.delta_in_source_parts.front() : nullptr;
            String header_source_partition_id;
            const UInt64 total_rows = writeHeaderJson(
                old_storage,
                dest_storage,
                old_part->uuid,
                delta_out_set,
                incoming_source_part,
                ctx.part_uuid_table_per_new_part[i],
                ctx.tombstone_rows_per_new_part[i],
                header_source_partition_id);
            const String coverage_source_partition_id = writeCoverageJson(
                old_storage,
                dest_storage,
                delta_out_set,
                incoming_source_part,
                ctx.tombstone_rows_per_new_part[i]);
            if (!coverage_source_partition_id.empty() && coverage_source_partition_id != header_source_partition_id)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Remapped materialized-index part {} has inconsistent source partition ids in header ({}) and coverage ({})",
                    new_part->name,
                    header_source_partition_id,
                    coverage_source_partition_id);
            if (header_source_partition_id.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot derive source partition id for remapped materialized-index part {}", new_part->name);

            if (new_part->uuid == UUIDHelpers::Nil)
            {
                if (i == 0 && ctx.first_new_part_uuid != UUIDHelpers::Nil)
                    new_part->uuid = ctx.first_new_part_uuid;
                else
                    new_part->uuid = UUIDHelpers::generateV4();
            }
            new_part->rows_count = total_rows;
            auto & inner_storage = ctx.storage->getInnerMergeTreeData(ctx.inner_storage_holder);
            writeAuxiliaryIndexPartMetadata(
                dest_storage,
                &inner_storage,
                new_part->rows_count,
                header_source_partition_id,
                new_part->uuid);
            new_part->loadColumnsChecksumsIndexes(/*require_columns_checksums=*/true, /*check_consistency=*/false);

            /// sync_guard dtor here fsyncs the directory on scope exit.
        }

        return false;
    }

    void cancel() noexcept override {}

    static UInt64 writeHeaderJson(
        const IDataPartStorage & old_storage,
        IDataPartStorage & dest_storage,
        const UUID & derive_from,
        const std::unordered_set<UUID> & delta_out_set,
        const MergeTreeData::DataPartPtr & incoming_source_part,
        const std::vector<UUID> & part_uuid_table,
        UInt64 tombstone_rows,
        String & out_source_partition_id)
    {
        /// Read the old header, copy forward the stable fields, and
        /// recompute `coverage_source_part_count` by counting `coverage.json`
        /// entries that are NOT in the outgoing delta set. Absence of an old
        /// header reduces the new header to an empty-defaults document; the
        /// Build-time schema version is preserved.
        Poco::JSON::Object header_json;
        header_json.set("version", 1);

        String algorithm_family;
        String algorithm_impl;
        UInt64 total_rows = 0;
        size_t segment_count = 0;
        String source_partition_id;
        Int64 source_min_block = 0;
        Int64 source_max_block = 0;
        bool has_source_range = false;
        Poco::JSON::Array segment_boundaries_arr;

        if (old_storage.existsFile("header.json"))
        {
            auto header_reader = old_storage.readFile("header.json", ReadSettings{}, std::nullopt);
            String header_text;
            readStringUntilEOF(header_text, *header_reader);
            try
            {
                Poco::JSON::Parser parser;
                auto parsed = parser.parse(header_text);
                auto obj = parsed.extract<Poco::JSON::Object::Ptr>();
                if (obj)
                {
                    if (obj->has("algorithm_family"))
                        algorithm_family = obj->getValue<std::string>("algorithm_family");
                    if (obj->has("algorithm_impl"))
                        algorithm_impl = obj->getValue<std::string>("algorithm_impl");
                    if (obj->has("total_rows"))
                        total_rows = obj->getValue<UInt64>("total_rows");
                    if (obj->has("segment_count"))
                        segment_count = obj->getValue<size_t>("segment_count");
                    if (obj->has("source_partition_id"))
                        source_partition_id = obj->getValue<std::string>("source_partition_id");
                    if (obj->has("source_min_block") && obj->has("source_max_block"))
                    {
                        source_min_block = obj->getValue<Int64>("source_min_block");
                        source_max_block = obj->getValue<Int64>("source_max_block");
                        has_source_range = true;
                    }
                    if (obj->has("segment_boundaries"))
                    {
                        auto arr = obj->getArray("segment_boundaries");
                        if (arr)
                        {
                            for (size_t j = 0; j < arr->size(); ++j)
                                segment_boundaries_arr.add(arr->getElement<UInt64>(static_cast<unsigned int>(j)));
                        }
                    }
                }
            }
            catch (...)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot parse header.json while remapping materialized-index-part: {}",
                    getCurrentExceptionMessage(false));
            }
        }

        header_json.set("algorithm_family", algorithm_family);
        header_json.set("algorithm_impl", algorithm_impl);
        header_json.set("total_rows", total_rows);
        header_json.set("tombstone_rows", static_cast<Int64>(tombstone_rows));
        AuxiliaryIndexPartReverseLookup::addLocatorHeaderFields(header_json);
        Poco::JSON::Array uuid_arr;
        for (const auto & uuid : part_uuid_table)
            uuid_arr.add(toString(uuid));
        header_json.set("part_uuid_table", uuid_arr);
        header_json.set("segment_count", segment_count);
        header_json.set("segment_boundaries", segment_boundaries_arr);

        /// `coverage_source_part_count` is the old count minus outgoing, plus
        /// the incoming lineage source part when the scheduler proved that the
        /// old MI part fully covers that lineage.
        size_t new_coverage = 0;
        auto account_partition = [&](const String & partition_id, Int64 min_block, Int64 max_block)
        {
            if (source_partition_id.empty())
                source_partition_id = partition_id;
            else if (source_partition_id != partition_id)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot remap one materialized-index part across source partitions {} and {}",
                    source_partition_id,
                    partition_id);

            if (!has_source_range)
            {
                source_min_block = min_block;
                source_max_block = max_block;
                has_source_range = true;
            }
            else
            {
                source_min_block = std::min(source_min_block, min_block);
                source_max_block = std::max(source_max_block, max_block);
            }
        };

        if (old_storage.existsFile("coverage.json"))
        {
            auto cov_reader = old_storage.readFile("coverage.json", ReadSettings{}, std::nullopt);
            String body;
            readStringUntilEOF(body, *cov_reader);
            try
            {
                Poco::JSON::Parser cov_parser;
                auto parsed_cov = cov_parser.parse(body);
                auto cov_obj = parsed_cov.extract<Poco::JSON::Object::Ptr>();
                if (cov_obj)
                {
                    auto covered_arr = cov_obj->getArray("covered");
                    if (covered_arr)
                    {
                        for (size_t j = 0; j < covered_arr->size(); ++j)
                        {
                            auto item = covered_arr->getObject(static_cast<unsigned int>(j));
                            if (!item || !item->has("source_part_uuid"))
                                continue;
                            UUID u;
                            if (tryParse(u, item->getValue<std::string>("source_part_uuid"))
                                && !delta_out_set.contains(u))
                            {
                                if (item->has("partition_id") && item->has("min_block") && item->has("max_block"))
                                    account_partition(
                                        item->getValue<std::string>("partition_id"),
                                        item->getValue<Int64>("min_block"),
                                        item->getValue<Int64>("max_block"));
                                ++new_coverage;
                            }
                        }
                    }
                }
            }
            catch (...)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot parse coverage.json while recomputing remap header: {}",
                    getCurrentExceptionMessage(false));
            }
        }
        if (incoming_source_part)
        {
            account_partition(
                incoming_source_part->info.getPartitionId(),
                incoming_source_part->info.min_block,
                incoming_source_part->info.max_block);
            ++new_coverage;
        }
        header_json.set("coverage_source_part_count", new_coverage);
        if (!source_partition_id.empty())
        {
            header_json.set("partitioning_format_version", static_cast<Int64>(PARTITIONING_FORMAT_VERSION));
            header_json.set("source_partition_id", source_partition_id);
            header_json.set("source_partition_hash", getAuxiliaryIndexSourcePartitionHash(source_partition_id));
            header_json.set("source_min_block", source_min_block);
            header_json.set("source_max_block", source_max_block);
        }
        header_json.set("created_timestamp_seconds", static_cast<Int64>(std::time(nullptr)));
        header_json.set("derive_from", toString(derive_from));

        auto writer = dest_storage.writeFile("header.json", 4096, WriteSettings{});
        std::ostringstream oss;
        Poco::JSON::Stringifier::stringify(header_json, oss);
        const std::string body = oss.str();
        writer->write(body.data(), body.size());
        writer->finalize();
        out_source_partition_id = source_partition_id;
        return total_rows;
    }

    static String writeCoverageJson(
        const IDataPartStorage & old_storage,
        IDataPartStorage & dest_storage,
        const std::unordered_set<UUID> & delta_out_set,
        const MergeTreeData::DataPartPtr & incoming_source_part,
        UInt64 tombstone_rows)
    {
        Poco::JSON::Object coverage_json;
        coverage_json.set("format_version", 1);
        coverage_json.set("tombstone_rows", static_cast<Int64>(tombstone_rows));

        Poco::JSON::Array covered_arr;
        String source_partition_id;
        Int64 source_min_block = 0;
        Int64 source_max_block = 0;
        bool has_source_range = false;
        auto account_partition = [&](const String & partition_id, Int64 min_block, Int64 max_block)
        {
            if (source_partition_id.empty())
                source_partition_id = partition_id;
            else if (source_partition_id != partition_id)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot write coverage for one materialized-index part across source partitions {} and {}",
                    source_partition_id,
                    partition_id);

            if (!has_source_range)
            {
                source_min_block = min_block;
                source_max_block = max_block;
                has_source_range = true;
            }
            else
            {
                source_min_block = std::min(source_min_block, min_block);
                source_max_block = std::max(source_max_block, max_block);
            }
        };

        if (old_storage.existsFile("coverage.json"))
        {
            auto cov_reader = old_storage.readFile("coverage.json", ReadSettings{}, std::nullopt);
            String body;
            readStringUntilEOF(body, *cov_reader);
            try
            {
                Poco::JSON::Parser parser;
                auto parsed = parser.parse(body);
                auto root = parsed.extract<Poco::JSON::Object::Ptr>();
                if (root)
                {
                    auto old_covered = root->getArray("covered");
                    if (old_covered)
                    {
                        for (size_t j = 0; j < old_covered->size(); ++j)
                        {
                            auto item = old_covered->getObject(static_cast<unsigned int>(j));
                            if (!item || !item->has("source_part_uuid"))
                                continue;
                            UUID u;
                            if (!tryParse(u, item->getValue<std::string>("source_part_uuid")))
                                continue;
                            if (delta_out_set.contains(u))
                                continue;
                            if (item->has("partition_id") && item->has("min_block") && item->has("max_block"))
                                account_partition(
                                    item->getValue<std::string>("partition_id"),
                                    item->getValue<Int64>("min_block"),
                                    item->getValue<Int64>("max_block"));
                            covered_arr.add(item);
                        }
                    }
                }
            }
            catch (...)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot parse coverage.json while remapping materialized-index-part: {}",
                    getCurrentExceptionMessage(false));
            }
        }
        if (incoming_source_part)
        {
            Poco::JSON::Object item;
            item.set("source_part_uuid", toString(incoming_source_part->uuid));
            item.set("rows", incoming_source_part->rows_count);
            item.set("source_part_name", incoming_source_part->name);
            item.set("partition_id", incoming_source_part->info.getPartitionId());
            item.set("min_block", incoming_source_part->info.min_block);
            item.set("max_block", incoming_source_part->info.max_block);
            item.set("level", incoming_source_part->info.level);
            item.set("mutation", incoming_source_part->info.mutation);
            account_partition(
                incoming_source_part->info.getPartitionId(),
                incoming_source_part->info.min_block,
                incoming_source_part->info.max_block);
            covered_arr.add(item);
        }
        if (!source_partition_id.empty())
        {
            coverage_json.set("partitioning_format_version", static_cast<Int64>(PARTITIONING_FORMAT_VERSION));
            coverage_json.set("source_partition_id", source_partition_id);
            coverage_json.set("source_partition_hash", getAuxiliaryIndexSourcePartitionHash(source_partition_id));
            coverage_json.set("source_min_block", source_min_block);
            coverage_json.set("source_max_block", source_max_block);
        }
        coverage_json.set("covered", covered_arr);

        auto writer = dest_storage.writeFile("coverage.json", 4096, WriteSettings{});
        std::ostringstream oss;
        Poco::JSON::Stringifier::stringify(coverage_json, oss);
        const std::string body = oss.str();
        writer->write(body.data(), body.size());
        writer->finalize();
        return source_partition_id;
    }

    GlobalRuntimeContextPtr global_ctx;
};


RemapTask::Stages RemapTask::makeStages()
{
    return {
        std::make_shared<PlanAffectedSegmentsStage>(),
        std::make_shared<DeriveHardlinksStage>(),
        std::make_shared<RewriteMutableSegmentsStage>(),
        std::make_shared<FinalizeMetadataStage>(),
    };
}


RemapTask::RemapTask(
    MergeTreeData::DataPartsVector affected_auxiliary_index_parts_,
    MergeTreeData::DataPartsVector delta_in_source_parts_,
    std::vector<UUID> delta_out_source_uuids_,
    StorageANN * storage_,
    StoragePtr inner_storage_holder_,
    const MergeTreeData * source_storage_,
    StorageSnapshotPtr source_snapshot_,
    ContextPtr context_,
    UInt64 memory_budget_bytes_,
    UUID first_new_part_uuid_)
    : global_ctx(std::make_shared<GlobalRuntimeContext>())
    , stages(makeStages())
{
    global_ctx->affected_auxiliary_index_parts = std::move(affected_auxiliary_index_parts_);
    global_ctx->delta_in_source_parts = std::move(delta_in_source_parts_);
    global_ctx->delta_out_source_uuids = std::move(delta_out_source_uuids_);
    global_ctx->storage = storage_;
    global_ctx->inner_storage_holder = std::move(inner_storage_holder_);
    global_ctx->source_storage = source_storage_;
    global_ctx->source_snapshot = std::move(source_snapshot_);
    global_ctx->context = std::move(context_);
    global_ctx->memory_budget_bytes = memory_budget_bytes_;
    global_ctx->first_new_part_uuid = first_new_part_uuid_;

    stages_iterator = stages.begin();

    auto prepare_stage_ctx = std::make_shared<IStageRuntimeContext>();
    (*stages_iterator)->setRuntimeContext(std::move(prepare_stage_ctx), global_ctx);
}




bool RemapTask::execute()
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
            /// the vector produced by stage 4 (empty while stages 1-4 are still
            /// skeletons; that is intentional for early change packs).
            if (!promise_fulfilled)
            {
                global_ctx->promise.set_value(std::move(global_ctx->new_auxiliary_index_parts));
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


void RemapTask::cancel() noexcept
{
    global_ctx->is_cancelled.store(true, std::memory_order_relaxed);
    if (stages_iterator != stages.end())
        (*stages_iterator)->cancel();
}


std::future<std::vector<MergeTreeData::MutableDataPartPtr>> RemapTask::getFuture()
{
    return global_ctx->promise.get_future();
}


std::vector<MergeTreeData::MutableDataPartPtr> RemapTask::getUnfinishedParts()
{
    return global_ctx->new_auxiliary_index_parts;
}

}
