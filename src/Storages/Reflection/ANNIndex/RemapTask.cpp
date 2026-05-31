#include <Storages/Reflection/ANNIndex/RemapTask.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Core/UUID.h>
#include <Columns/ColumnsNumber.h>
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
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartMetadata.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartName.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartWriter.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/StorageSnapshot.h>

#include <Core/Block.h>
#include <Columns/IColumn.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <fmt/format.h>

#include <ctime>
#include <exception>
#include <filesystem>
#include <future>
#include <sstream>
#include <utility>


namespace fs = std::filesystem;


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::ANNIndex
{

namespace ErrorCodes = DB::ErrorCodes;

/// -----------------------------------------------------------------------------
/// Helpers shared by the columnar remap stages.
/// -----------------------------------------------------------------------------

namespace
{

constexpr UInt64 PARTITIONING_FORMAT_VERSION = 1;

/// Framework JSON sidecars + checksums.txt are rewritten for every new part, so
/// they must never be hardlinked from the old part (a hardlink shares the inode
/// and would corrupt the source).
const std::unordered_set<String> & rewrittenPartFiles()
{
    static const std::unordered_set<String> files{
        "header.json", "coverage.json", "ann_format.json", "ann_coverage.json", "checksums.txt", IMergeTreeDataPart::UUID_FILE_NAME};
    return files;
}

/// Parse the set of covered source-part UUIDs from a part's `coverage.json`.
/// Used to decide whether a part is affected by the outgoing delta without
/// scanning the locator columns.
std::unordered_set<UUID> readCoverageSourceUuids(const IDataPartStorage & storage)
{
    std::unordered_set<UUID> uuids;
    if (!storage.existsFile("coverage.json"))
        return uuids;

    auto reader = storage.readFile("coverage.json", ReadSettings{}, std::nullopt);
    String body;
    readStringUntilEOF(body, *reader);

    Poco::JSON::Parser parser;
    auto parsed = parser.parse(body);
    auto root = parsed.extract<Poco::JSON::Object::Ptr>();
    if (!root)
        return uuids;

    auto covered = root->getArray("covered");
    if (!covered)
        return uuids;

    for (size_t j = 0; j < covered->size(); ++j)
    {
        auto item = covered->getObject(static_cast<unsigned int>(j));
        if (!item || !item->has("source_part_uuid"))
            continue;
        UUID u;
        if (tryParse(u, item->getValue<std::string>("source_part_uuid")))
            uuids.insert(u);
    }
    return uuids;
}

/// Read `source_partition_id` and `tombstone_rows` from a part's `header.json`.
void readHeaderBasics(const IDataPartStorage & storage, String & source_partition_id, UInt64 & tombstone_rows)
{
    source_partition_id.clear();
    tombstone_rows = 0;
    if (!storage.existsFile("header.json"))
        return;

    auto reader = storage.readFile("header.json", ReadSettings{}, std::nullopt);
    String body;
    readStringUntilEOF(body, *reader);

    Poco::JSON::Parser parser;
    auto parsed = parser.parse(body);
    auto obj = parsed.extract<Poco::JSON::Object::Ptr>();
    if (!obj)
        return;
    if (obj->has("source_partition_id"))
        source_partition_id = obj->getValue<std::string>("source_partition_id");
    if (obj->has("tombstone_rows"))
        tombstone_rows = obj->getValue<UInt64>("tombstone_rows");
}

/// Parse a materialized-index part name, bump `level` by one and rebuild the
/// canonical string.
String bumpLevelInPartName(const String & old_name, MergeTreeDataFormatVersion format_version)
{
    auto info = MergeTreePartInfo::fromPartName(old_name, format_version);
    info.level += 1;
    return info.getPartNameAndCheckFormat(format_version);
}

String makeRemapTmpDirName(const String & new_part_name)
{
    return String{RemapTaskImpl::TEMP_DIRECTORY_PREFIX} + new_part_name;
}

/// Derive a `tmp_ann_index_remap_<new_part_name>` storage that lives on the same
/// disk as `source_storage`.
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

    const String tmp_dir_name = makeRemapTmpDirName(new_part_name);
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndex algorithm returned an empty private path");

    const fs::path path(private_path.path);
    if (path.is_absolute())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ANNIndex algorithm returned absolute private path {}", private_path.path);

    for (const auto & component : path)
    {
        if (component == "..")
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "ANNIndex algorithm returned private path with parent traversal: {}", private_path.path);
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
        /// Cross-disk (or zero-copy) hardlink failures fall back to physical copy.
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
                    "Required ANNIndex algorithm private directory {} is missing in part {}",
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
                "Required ANNIndex algorithm private file {} is missing in part {}",
                private_path.path,
                source_storage.getRelativePath());
        return;
    }

    hardlinkOrCopyFile(source_storage, dest_storage, private_path.path, log);
}

/// Hardlink the entire old part directory into the destination, skipping the
/// per-part files that the remap rewrites (`excluded`, matched at the part
/// root only). Used for unaffected parts whose columnar payload is unchanged.
void hardlinkWholePartExcept(
    const IDataPartStorage & source_storage,
    IDataPartStorage & dest_storage,
    const std::unordered_set<String> & excluded,
    LoggerPtr log)
{
    dest_storage.createDirectories();

    const auto & src_base = dynamic_cast<const DataPartStorageOnDiskBase &>(source_storage);
    const auto disk = src_base.getDisk();
    const String part_root = source_storage.getRelativePath();

    std::vector<String> dirs{part_root};
    while (!dirs.empty())
    {
        String current = std::move(dirs.back());
        dirs.pop_back();

        if (current != part_root)
            createDirectoryInDest(dest_storage, relativeToPartRoot(part_root, current));

        for (auto it = disk->iterateDirectory(current); it->isValid(); it->next())
        {
            const String entry_path = it->path();
            if (disk->existsFile(entry_path))
            {
                const String rel = relativeToPartRoot(part_root, entry_path);
                /// Exclusions apply only to files at the part root.
                if (fs::path(rel).parent_path().empty() && excluded.contains(rel))
                    continue;
                hardlinkOrCopyFile(source_storage, dest_storage, rel, log);
                continue;
            }

            if (disk->existsDirectory(entry_path))
                dirs.push_back(entry_path);
        }
    }
}

/// Assemble the 5-column locator block (`_source_partition_id` + 4 locator
/// columns) in the inner-table physical column order.
Block buildLocatorOutputBlock(
    const NamesAndTypesList & cols,
    const String & source_partition_id,
    const ColumnPtr & source_uuid,
    const ColumnPtr & part_offset,
    const ColumnPtr & block_number,
    const ColumnPtr & block_offset)
{
    const size_t num_rows = source_uuid->size();
    Block result;
    for (const auto & nt : cols)
    {
        ColumnPtr col;
        if (nt.name == ANN_INDEX_SOURCE_PARTITION_ID_COLUMN)
            col = nt.type->createColumnConst(num_rows, Field(source_partition_id))->convertToFullColumnIfConst();
        else if (nt.name == ANN_INDEX_LOCATOR_SOURCE_UUID_COLUMN)
            col = source_uuid;
        else if (nt.name == ANN_INDEX_LOCATOR_PART_OFFSET_COLUMN)
            col = part_offset;
        else if (nt.name == ANN_INDEX_LOCATOR_BLOCK_NUMBER_COLUMN)
            col = block_number;
        else if (nt.name == ANN_INDEX_LOCATOR_BLOCK_OFFSET_COLUMN)
            col = block_offset;
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unexpected ANN-index locator column '{}'", nt.name);

        result.insert(ColumnWithTypeAndName(col, nt.type, nt.name));
    }
    return result;
}

}


/// -----------------------------------------------------------------------------
/// Stage 1: derive new parts and classify them as affected / unaffected.
/// -----------------------------------------------------------------------------

struct RemapTaskImpl::PlanAffectedSegmentsStage : public IStage
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
        if (ctx.affected_ann_index_parts.empty())
            return false;
        if (!ctx.storage)
            return false;

        std::unordered_set<UUID> delta_out_set(
            ctx.delta_out_source_uuids.begin(), ctx.delta_out_source_uuids.end());

        auto & inner_storage = ctx.storage->getInnerMergeTreeData(ctx.inner_storage_holder);
        const auto format_version = inner_storage.format_version;
        const size_t n = ctx.affected_ann_index_parts.size();

        ctx.new_ann_index_parts.resize(n);
        ctx.tmp_storages.reserve(n);
        ctx.temporary_directory_locks.reserve(n);
        ctx.old_index_per_new_part.resize(n);
        ctx.tombstone_rows_per_new_part.assign(n, 0);
        ctx.affected_per_new_part.assign(n, 0);
        ctx.source_partition_id_per_new_part.assign(n, String{});

        for (size_t i = 0; i < n; ++i)
        {
            const auto & old_part = ctx.affected_ann_index_parts[i];
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

            String source_partition_id;
            UInt64 old_tombstones = 0;
            readHeaderBasics(old_storage, source_partition_id, old_tombstones);
            ctx.source_partition_id_per_new_part[i] = source_partition_id;
            ctx.tombstone_rows_per_new_part[i] = old_tombstones;

            /// Affected iff the part's coverage intersects the outgoing delta.
            /// Such rows reference a source part that is going away and must be
            /// rewritten; otherwise the columnar payload is unchanged.
            const auto covered = readCoverageSourceUuids(old_storage);
            bool affected = false;
            for (const auto & u : covered)
            {
                if (delta_out_set.contains(u))
                {
                    affected = true;
                    break;
                }
            }
            ctx.affected_per_new_part[i] = affected ? 1 : 0;

            /// Current N=M derivation: one new materialized-index-part per old,
            /// with `level` bumped by one.
            const String new_part_name = bumpLevelInPartName(old_part->name, format_version);
            const auto new_part_info = markAsANNIndexPartInfo(
                MergeTreePartInfo::fromPartName(new_part_name, format_version));

            const String tmp_dir_name = makeRemapTmpDirName(new_part_name);
            ctx.temporary_directory_locks.push_back(inner_storage.getTemporaryPartDirectoryHolder(tmp_dir_name));
            auto new_tmp_storage = makeRemapTmpStorage(old_storage, inner_storage, new_part_name);
            new_tmp_storage->beginTransaction();
            ctx.tmp_storages[new_part_name] = new_tmp_storage;

            auto settings = inner_storage.getSettings();
            auto new_part = std::make_shared<MergeTreeDataPartWide>(
                inner_storage,
                *settings,
                new_part_name,
                new_part_info,
                new_tmp_storage,
                /*parent_part_=*/nullptr);
            new_part->is_temp = true;
            if (i == 0 && ctx.first_new_part_uuid != UUIDHelpers::Nil)
                new_part->uuid = ctx.first_new_part_uuid;
            else
                new_part->uuid = UUIDHelpers::generateV4();
            ctx.new_ann_index_parts[i] = std::move(new_part);
            ctx.old_index_per_new_part[i] = i;
        }

        return false;
    }

    void cancel() noexcept override {}

    GlobalRuntimeContextPtr global_ctx;
};


/// -----------------------------------------------------------------------------
/// Stage 2: hardlink the unchanged on-disk payload of unaffected parts.
/// -----------------------------------------------------------------------------

struct RemapTaskImpl::DeriveHardlinksStage : public IStage
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
        if (ctx.stage2_cursor >= ctx.new_ann_index_parts.size())
            return false;
        if (ctx.is_cancelled.load(std::memory_order_relaxed))
            return false;

        const size_t i = ctx.stage2_cursor;
        const auto & new_part = ctx.new_ann_index_parts[i];
        if (!new_part || ctx.affected_per_new_part[i])
        {
            /// Affected parts are produced by the rewrite stage (which also
            /// links their algorithm-private files); nothing to do here.
            ++ctx.stage2_cursor;
            return ctx.stage2_cursor < ctx.new_ann_index_parts.size();
        }

        const size_t old_idx = ctx.old_index_per_new_part[i];
        const auto & old_part = ctx.affected_ann_index_parts[old_idx];
        const auto & old_storage = old_part->getDataPartStorage();
        auto & dest_storage = new_part->getDataPartStorage();

        auto log = getLogger("RemapTaskImpl");
        hardlinkWholePartExcept(old_storage, dest_storage, rewrittenPartFiles(), log);

        ++ctx.stage2_cursor;
        return ctx.stage2_cursor < ctx.new_ann_index_parts.size();
    }

    void cancel() noexcept override {}

    GlobalRuntimeContextPtr global_ctx;
};


/// -----------------------------------------------------------------------------
/// Stage 3: rewrite the locator columns of affected parts.
/// -----------------------------------------------------------------------------

struct RemapTaskImpl::RewriteMutableSegmentsStage : public IStage
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
            getLogger("RemapTaskImpl"));

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

    /// Read the 4 locator columns of one old part, remap outgoing rows and
    /// stream the result into a fresh Wide part. Internal-id order (= row
    /// order) is preserved so the algorithm-private files stay valid.
    void rewriteOneAffectedPart(size_t i)
    {
        auto & ctx = *global_ctx;

        const size_t old_idx = ctx.old_index_per_new_part[i];
        const auto & old_part = ctx.affected_ann_index_parts[old_idx];
        const auto & new_part_shell = ctx.new_ann_index_parts[i];
        if (!old_part || !new_part_shell)
            return;

        auto log = getLogger("RemapTaskImpl");
        auto & inner_storage = ctx.storage->getInnerMergeTreeData(ctx.inner_storage_holder);
        const auto output_storage = ctx.tmp_storages.at(new_part_shell->name);

        std::unordered_set<UUID> delta_out_set(
            ctx.delta_out_source_uuids.begin(), ctx.delta_out_source_uuids.end());

        auto bundle = openAnnPartStream(
            inner_storage,
            new_part_shell->name,
            new_part_shell->uuid,
            ctx.source_partition_id_per_new_part[i],
            output_storage);

        auto snapshot = inner_storage.getStorageSnapshot(bundle.inner_metadata, ctx.context);

        Names columns_to_read{
            ANN_INDEX_LOCATOR_SOURCE_UUID_COLUMN,
            ANN_INDEX_LOCATOR_PART_OFFSET_COLUMN,
            ANN_INDEX_LOCATOR_BLOCK_NUMBER_COLUMN,
            ANN_INDEX_LOCATOR_BLOCK_OFFSET_COLUMN,
        };

        Pipe pipe = createMergeTreeSequentialSource(
            MergeTreeSequentialSourceType::Merge,
            inner_storage,
            snapshot,
            RangesInDataPart(old_part),
            /*alter_conversions=*/std::make_shared<AlterConversions>(),
            /*merged_part_offsets=*/nullptr,
            std::move(columns_to_read),
            /*mark_ranges=*/std::nullopt,
            /*filtered_rows_count=*/nullptr,
            /*apply_deleted_mask=*/false,
            /*read_with_direct_io=*/false,
            /*prefetch=*/false);

        QueryPipeline pipeline(std::move(pipe));
        PullingPipelineExecutor executor(pipeline);

        UInt64 total_rows = 0;
        UInt64 tombstones = 0;

        Block block;
        while (executor.pull(block))
        {
            const size_t rows = block.rows();
            if (!rows)
                continue;
            if (ctx.is_cancelled.load(std::memory_order_relaxed))
                return;

            const auto src_uuid_in = block.getByName(ANN_INDEX_LOCATOR_SOURCE_UUID_COLUMN).column;
            const auto part_off_in = block.getByName(ANN_INDEX_LOCATOR_PART_OFFSET_COLUMN).column;
            const auto block_number_in = block.getByName(ANN_INDEX_LOCATOR_BLOCK_NUMBER_COLUMN).column;
            const auto block_offset_in = block.getByName(ANN_INDEX_LOCATOR_BLOCK_OFFSET_COLUMN).column;

            const auto & uuid_data = assert_cast<const ColumnUUID &>(*src_uuid_in).getData();
            const auto & part_off_data = assert_cast<const ColumnUInt64 &>(*part_off_in).getData();

            auto out_uuid = ColumnUUID::create(rows);
            auto out_part_off = ColumnUInt64::create(rows);
            auto & out_uuid_data = out_uuid->getData();
            auto & out_part_off_data = out_part_off->getData();

            for (size_t r = 0; r < rows; ++r)
            {
                const UUID s = uuid_data[r];
                if (!delta_out_set.contains(s))
                {
                    out_uuid_data[r] = s;
                    out_part_off_data[r] = part_off_data[r];
                    continue;
                }

                GlobalRuntimeContext::SourceRowId identity{
                    block_number_in->getUInt(r),
                    block_offset_in->getUInt(r),
                };
                auto incoming_it = ctx.incoming_part_offsets.find(identity);
                if (incoming_it != ctx.incoming_part_offsets.end())
                {
                    out_uuid_data[r] = ctx.incoming_source_part_uuid;
                    out_part_off_data[r] = incoming_it->second;
                }
                else
                {
                    /// Source row was deleted: detach the locator from any live
                    /// source part and write the sentinel `part_offset` so the
                    /// query-side merge-join never matches it.
                    out_uuid_data[r] = UUIDHelpers::Nil;
                    out_part_off_data[r] = ANN_INDEX_LOCATOR_TOMBSTONE_PART_OFFSET;
                    ++tombstones;
                }
            }

            Block out_block = buildLocatorOutputBlock(
                bundle.columns,
                ctx.source_partition_id_per_new_part[i],
                std::move(out_uuid),
                std::move(out_part_off),
                block_number_in,
                block_offset_in);
            bundle.stream->write(out_block);
            total_rows += rows;
        }

        bundle.part->rows_count = total_rows;
        bundle.stream->finalizeIndexGranularity();
        bundle.stream->finalizePart(bundle.part, IMergedBlockOutputStream::GatheredData{}, /*sync=*/false);

        /// Algorithm-private payload is opaque and order-stable across remaps.
        if (auto * algorithm = ctx.storage->getAlgorithm())
        {
            auto & dest_storage = bundle.part->getDataPartStorage();
            for (const auto & private_path : algorithm->getAlgorithmPrivatePaths(old_part->getDataPartStorage()))
                hardlinkOrCopyAlgorithmPrivatePath(old_part->getDataPartStorage(), dest_storage, private_path, log);
        }

        ctx.new_ann_index_parts[i] = bundle.part;
        ctx.tombstone_rows_per_new_part[i] += tombstones;

        LOG_DEBUG(log, "Rewrote ANN-index part {}: {} rows, {} new tombstones",
            bundle.part->name, total_rows, tombstones);
    }

    bool execute() override
    {
        auto & ctx = *global_ctx;
        loadIncomingRowsIfNeeded();

        if (cursor >= ctx.new_ann_index_parts.size())
            return false;
        if (ctx.is_cancelled.load(std::memory_order_relaxed))
            return false;

        const size_t i = cursor;
        ++cursor;
        if (ctx.affected_per_new_part[i] && ctx.new_ann_index_parts[i])
            rewriteOneAffectedPart(i);

        return cursor < ctx.new_ann_index_parts.size();
    }

    void cancel() noexcept override {}

    GlobalRuntimeContextPtr global_ctx;
    size_t cursor{0};
};


/// -----------------------------------------------------------------------------
/// Stage 4: framework JSON sidecars + checksums; fulfil the promise.
/// -----------------------------------------------------------------------------

struct RemapTaskImpl::FinalizeMetadataStage : public IStage
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
        if (ctx.new_ann_index_parts.empty())
            return false;

        std::unordered_set<UUID> delta_out_set(
            ctx.delta_out_source_uuids.begin(), ctx.delta_out_source_uuids.end());

        auto & inner_storage = ctx.storage->getInnerMergeTreeData(ctx.inner_storage_holder);

        for (size_t i = 0; i < ctx.new_ann_index_parts.size(); ++i)
        {
            const auto & new_part = ctx.new_ann_index_parts[i];
            if (!new_part)
                continue;

            const size_t old_idx = ctx.old_index_per_new_part[i];
            const auto & old_part = ctx.affected_ann_index_parts[old_idx];
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
                ctx.tombstone_rows_per_new_part[i],
                header_source_partition_id);
            const String coverage_source_partition_id = writeCoverageJson(
                old_storage,
                dest_storage,
                delta_out_set,
                incoming_source_part,
                ctx.tombstone_rows_per_new_part[i]);
            writeAnnFormatJson(old_storage, dest_storage);
            if (!coverage_source_partition_id.empty() && coverage_source_partition_id != header_source_partition_id)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Remapped materialized-index part {} has inconsistent source partition ids in header ({}) and coverage ({})",
                    new_part->name,
                    header_source_partition_id,
                    coverage_source_partition_id);
            if (header_source_partition_id.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot derive source partition id for remapped materialized-index part {}", new_part->name);

            writeANNIndexPartUUIDFile(dest_storage, new_part->uuid, inner_storage.getContext()->getWriteSettings());

            /// Fold every file (locator columns + algorithm-private payload +
            /// JSON sidecars) into a fresh checksums.txt so replicated fetch
            /// sees an identical checksum set on both ends.
            auto checksums = finalizeANNIndexPartChecksums(dest_storage, &inner_storage);
            new_part->checksums = checksums;
            new_part->setBytesOnDisk(checksums.getTotalSizeOnDisk());

            /// Reset the granularity to a fresh adaptive state derived from the
            /// on-disk marks before reloading: `loadIndexGranularity` appends to
            /// `index_granularity`, which the write path already populated, so a
            /// stale (possibly constant-compacted) member would trip
            /// `Cannot append mark after final`.
            new_part->initializeIndexGranularityInfo(*inner_storage.getSettings());
            new_part->loadColumnsChecksumsIndexes(/*require_columns_checksums=*/true, /*check_consistency=*/false);

            /// `total_rows` from the old header is authoritative for the metadata
            /// envelope; the rewrite preserves the row count.
            (void)total_rows;

            if (dest_storage.hasActiveTransaction())
                dest_storage.precommitTransaction();

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
        UInt64 tombstone_rows,
        String & out_source_partition_id)
    {
        Poco::JSON::Object header_json;
        header_json.set("version", 1);

        String algorithm_family;
        String algorithm_impl;
        UInt64 total_rows = 0;
        String source_partition_id;
        Int64 source_min_block = 0;
        Int64 source_max_block = 0;
        bool has_source_range = false;

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
                    if (obj->has("source_partition_id"))
                        source_partition_id = obj->getValue<std::string>("source_partition_id");
                    if (obj->has("source_min_block") && obj->has("source_max_block"))
                    {
                        source_min_block = obj->getValue<Int64>("source_min_block");
                        source_max_block = obj->getValue<Int64>("source_max_block");
                        has_source_range = true;
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

        /// `coverage_source_part_count` is the old count minus outgoing, plus
        /// the incoming lineage source part when present.
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
            header_json.set("source_partition_hash", getANNIndexSourcePartitionHash(source_partition_id));
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
            coverage_json.set("source_partition_hash", getANNIndexSourcePartitionHash(source_partition_id));
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
        writeAnnCoverageJson(dest_storage, covered_arr);
        return source_partition_id;
    }

    static void writeAnnCoverageJson(IDataPartStorage & dest_storage, const Poco::JSON::Array & covered_arr)
    {
        Poco::JSON::Object ann_coverage_json;
        Poco::JSON::Array ann_covered_arr;
        UInt64 total_rows = 0;

        for (size_t j = 0; j < covered_arr.size(); ++j)
        {
            auto item = covered_arr.getObject(static_cast<unsigned int>(j));
            if (!item)
                continue;

            Poco::JSON::Object ann_item;
            if (item->has("source_part_uuid"))
                ann_item.set("uuid", item->getValue<std::string>("source_part_uuid"));
            if (item->has("source_part_name"))
                ann_item.set("name", item->getValue<std::string>("source_part_name"));
            if (item->has("mutation"))
                ann_item.set("mutation_version", item->getValue<Int64>("mutation"));
            if (item->has("rows"))
            {
                const auto rows = item->getValue<Int64>("rows");
                ann_item.set("row_count", rows);
                if (rows > 0)
                    total_rows += static_cast<UInt64>(rows);
            }
            ann_covered_arr.add(ann_item);
        }

        ann_coverage_json.set("covered_source_parts", ann_covered_arr);
        ann_coverage_json.set("total_rows", static_cast<Int64>(total_rows));

        auto writer = dest_storage.writeFile("ann_coverage.json", 4096, WriteSettings{});
        std::ostringstream oss;
        Poco::JSON::Stringifier::stringify(ann_coverage_json, oss);
        const std::string body = oss.str();
        writer->write(body.data(), body.size());
        writer->finalize();
    }

    static void writeAnnFormatJson(const IDataPartStorage & old_storage, IDataPartStorage & dest_storage)
    {
        if (old_storage.existsFile("ann_format.json"))
        {
            dest_storage.copyFileFrom(old_storage, "ann_format.json", "ann_format.json");
            return;
        }

        Poco::JSON::Object format_json;
        format_json.set("framework_format_version", 1);
        format_json.set("engine", "ANNIndex");
        format_json.set("family", String{});
        format_json.set("impl", String{});
        format_json.set("algorithm_data_version", String{});
        format_json.set("algorithm_parameter_fingerprint", String{});
        format_json.set("built_at_unix_ms", static_cast<Int64>(std::time(nullptr)) * 1000);

        auto writer = dest_storage.writeFile("ann_format.json", 4096, WriteSettings{});
        std::ostringstream oss;
        Poco::JSON::Stringifier::stringify(format_json, oss);
        const std::string body = oss.str();
        writer->write(body.data(), body.size());
        writer->finalize();
    }

    GlobalRuntimeContextPtr global_ctx;
};


RemapTaskImpl::Stages RemapTaskImpl::makeStages()
{
    return {
        std::make_shared<PlanAffectedSegmentsStage>(),
        std::make_shared<DeriveHardlinksStage>(),
        std::make_shared<RewriteMutableSegmentsStage>(),
        std::make_shared<FinalizeMetadataStage>(),
    };
}


RemapTaskImpl::RemapTaskImpl(
    MergeTreeData::DataPartsVector affected_ann_index_parts_,
    MergeTreeData::DataPartsVector delta_in_source_parts_,
    std::vector<UUID> delta_out_source_uuids_,
    ReflectionANNIndex * storage_,
    StoragePtr inner_storage_holder_,
    const MergeTreeData * source_storage_,
    StorageSnapshotPtr source_snapshot_,
    ContextPtr context_,
    UInt64 memory_budget_bytes_,
    UUID first_new_part_uuid_)
    : global_ctx(std::make_shared<GlobalRuntimeContext>())
    , stages(makeStages())
{
    global_ctx->affected_ann_index_parts = std::move(affected_ann_index_parts_);
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


bool RemapTaskImpl::execute()
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
            if (!promise_fulfilled)
            {
                global_ctx->promise.set_value(std::move(global_ctx->new_ann_index_parts));
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


void RemapTaskImpl::cancel() noexcept
{
    global_ctx->is_cancelled.store(true, std::memory_order_relaxed);
    if (stages_iterator != stages.end())
        (*stages_iterator)->cancel();
}


std::future<std::vector<MergeTreeData::MutableDataPartPtr>> RemapTaskImpl::getFuture()
{
    return global_ctx->promise.get_future();
}


std::vector<MergeTreeData::MutableDataPartPtr> RemapTaskImpl::getUnfinishedParts()
{
    return global_ctx->new_ann_index_parts;
}

}
