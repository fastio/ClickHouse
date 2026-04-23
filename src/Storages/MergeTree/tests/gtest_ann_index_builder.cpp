#include "config.h"
#if USE_DISKANN

#include <gtest/gtest.h>

#include <Storages/MergeTree/ANNIndex/ANNGroupStorageDiskFull.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexBuilder.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexGroup.h>
#include <Storages/MergeTree/DiskANNIndex.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartState.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/tests/MergeTreeTestHarness.h>
#include <Storages/StorageMergeTree.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/tests/gtest_global_context.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>

#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>

#include <Interpreters/Context.h>

#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <vector>

using namespace DB;

namespace
{
namespace fs = std::filesystem;

fs::path makeUniqueTempDir(const std::string & name)
{
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = fs::temp_directory_path() / ("clickhouse-annb-" + name + "-" + std::to_string(now));
    fs::create_directories(p);
    return p;
}

class TempDirScope
{
public:
    explicit TempDirScope(const std::string & name) : path(makeUniqueTempDir(name)) {}
    ~TempDirScope() { std::error_code ec; fs::remove_all(path, ec); }
    fs::path path;
};

class StorageScope
{
public:
    explicit StorageScope(MergeTreeTestHarness::TestStorage s_) : s(std::move(s_)) {}
    ~StorageScope() { if (s.storage) s.storage->flushAndShutdown(); }
    MergeTreeTestHarness::TestStorage & get() { return s; }
private:
    MergeTreeTestHarness::TestStorage s;
};

DataPartsVector activeParts(const StorageMergeTree & storage)
{
    return storage.getDataPartsVectorForInternalUsage({MergeTreeDataPartState::Active});
}

DiskANNBuildOptions smallBuildOpts()
{
    DiskANNBuildOptions opts;
    opts.pruned_degree = 16;
    opts.max_degree = 32;
    opts.l_build = 64;
    opts.alpha = 1.2f;
    opts.num_threads = 1;
    opts.pq_chunks = 4;
    opts.build_ram_limit_gb = 0.25;
    return opts;
}

DiskANNSearchOptions smallSearchOpts()
{
    DiskANNSearchOptions opts;
    opts.num_threads = 1;
    opts.search_io_limit = 4;
    opts.num_nodes_to_cache = 0;
    opts.default_search_list_size = 16;
    opts.default_beam_width = 4;
    return opts;
}

UInt64 sipHashPartitionId(const std::string & partition_id, UInt64 seed)
{
    return sipHash64Keyed(seed, 0, partition_id.data(), partition_id.size());
}

/// Read all rows (vec, _block_number, _block_offset, partition_id) from a single part by
/// mirroring the sequential source setup used inside `VectorStreamWriter`. This is deliberately
/// independent from the writer under test so that we validate the end-to-end mapping instead
/// of a tautology.
struct RowSample
{
    std::vector<float> vec;
    UInt64 block_number;
    UInt64 block_offset;
    UInt64 partition_hash;
};

std::vector<RowSample> readPartRows(
    const MergeTreeData & storage,
    StorageSnapshotPtr snapshot,
    DataPartPtr part,
    const std::string & vec_col_name,
    size_t dim,
    UInt64 hash_seed)
{
    Names columns_to_read = {vec_col_name, BlockNumberColumn::name, BlockOffsetColumn::name};
    auto alter_conversions = std::make_shared<AlterConversions>();
    MarkRanges full_ranges{MarkRange{0, part->getMarksCount()}};
    RangesInDataPart ranges(part, nullptr, 0, 0, full_ranges, {});

    Pipe pipe = createMergeTreeSequentialSource(
        MergeTreeSequentialSourceType::Merge,
        storage, snapshot, std::move(ranges), alter_conversions, nullptr,
        columns_to_read, std::nullopt, nullptr, true, false, false);

    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);

    const UInt64 partition_hash = sipHashPartitionId(part->info.getPartitionId(), hash_seed);

    std::vector<RowSample> out;
    Chunk chunk;
    while (executor.pull(chunk))
    {
        if (!chunk.hasRows())
            continue;
        const auto & header = executor.getHeader();
        const size_t vec_pos = header.getPositionByName(vec_col_name);
        const size_t bn_pos = header.getPositionByName(BlockNumberColumn::name);
        const size_t bo_pos = header.getPositionByName(BlockOffsetColumn::name);
        const auto & cols = chunk.getColumns();

        const auto & arr = assert_cast<const ColumnArray &>(*cols[vec_pos]);
        const auto & offsets = arr.getOffsets();
        const auto & floats = assert_cast<const ColumnFloat32 &>(arr.getData()).getData();
        const auto & bn = assert_cast<const ColumnUInt64 &>(*cols[bn_pos]).getData();
        const auto & bo = assert_cast<const ColumnUInt64 &>(*cols[bo_pos]).getData();

        for (size_t r = 0; r < chunk.getNumRows(); ++r)
        {
            RowSample rs;
            const size_t begin = r == 0 ? 0 : offsets[r - 1];
            rs.vec.assign(floats.begin() + begin, floats.begin() + offsets[r]);
            if (rs.vec.size() != dim)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected dim {}", rs.vec.size());
            rs.block_number = bn[r];
            rs.block_offset = bo[r];
            rs.partition_hash = partition_hash;
            out.push_back(std::move(rs));
        }
    }
    return out;
}

ANNGroupStoragePtr makeTmpGroupStorage(const fs::path & root, const std::string & group_name)
{
    fs::create_directories(root / group_name);
    auto disk = std::make_shared<DiskLocal>("annb_disk_" + group_name, root.string() + "/");
    auto volume = std::make_shared<SingleDiskVolume>("annb_vol_" + group_name, disk);
    return std::make_shared<ANNGroupStorageDiskFull>(volume, group_name);
}
}

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

TEST(ANNIndexBuilderTest, EndToEndMapping)
{
    TempDirScope disk("e2e-disk");
    TempDirScope tmp_group_root("e2e-tmpgroup");

    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk.path.string(), "store/annb_e2e", "emb", /*dim=*/ 8, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();

    /// Two inserts with different partition values → two parts with different partition ids,
    /// each 10 rows.
    auto make_vectors = [](size_t rows, size_t dim, float base) {
        std::vector<std::vector<float>> out(rows, std::vector<float>(dim, 0.f));
        for (size_t r = 0; r < rows; ++r)
            for (size_t c = 0; c < dim; ++c)
                out[r][c] = base + 0.1f * static_cast<float>(r) + 0.01f * static_cast<float>(c);
        return out;
    };

    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 10, /*partition_value=*/ 1, make_vectors(10, 8, 1.0f));
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 10, /*partition_value=*/ 2, make_vectors(10, 8, 100.0f));

    auto parts = activeParts(*setup.storage);
    ASSERT_EQ(parts.size(), 2u);

    auto context = Context::createCopy(getContext().context);
    auto metadata_snapshot = setup.storage->getInMemoryMetadataPtr(context, false);
    auto storage_snapshot = setup.storage->getStorageSnapshot(metadata_snapshot, context);

    const UInt64 hash_seed = 0xA1B2C3D4E5F60708ULL;

    /// Collect ground-truth rows via an independent sequential source reader.
    std::vector<RowSample> all_rows;
    for (const auto & part : parts)
    {
        auto rows = readPartRows(*setup.storage, storage_snapshot, part, setup.vec_column_name, 8, hash_seed);
        all_rows.insert(all_rows.end(), rows.begin(), rows.end());
    }
    ASSERT_EQ(all_rows.size(), 20u);

    /// Build the group.
    auto tmp_storage = makeTmpGroupStorage(tmp_group_root.path, "tmp_group_x");

    ANNBuildInput input;
    input.selected_parts = parts;
    input.vector_column_name = setup.vec_column_name;
    input.storage = setup.storage.get();
    input.storage_snapshot = storage_snapshot;
    input.shape.dim = 8;
    input.shape.metric = static_cast<UInt8>(DiskANNMetric::L2);
    input.shape.algorithm = "diskann";
    input.shape.params_hash = 0;
    input.hash_seed = hash_seed;
    input.build_options = smallBuildOpts();
    input.search_defaults = smallSearchOpts();

    auto group = ANNIndexBuilder::build(input, tmp_storage, getLogger("ANNIndexBuilderTest"));

    ASSERT_NE(group, nullptr);
    EXPECT_EQ(group->numPoints(), 20u);
    EXPECT_EQ(group->getCoverage().partitionCount(), 2u);
    EXPECT_FALSE(tmp_storage->existsFile(std::string(ANNIndexBuilder::FBIN_FILE_NAME)));
    EXPECT_TRUE(tmp_storage->existsFile(std::string(ANNIndexGroup::META_FILE_NAME)));

    /// R-01 validation: every row must be recoverable via `search(vec, k=1) → lookup(id)` and
    /// the resulting `PartRowId` must strictly match the ground-truth row.
    for (size_t i = 0; i < all_rows.size(); ++i)
    {
        const auto & rs = all_rows[i];
        auto hits = group->search(rs.vec.data(), rs.vec.size(), /*k=*/ 1);
        ASSERT_EQ(hits.size(), 1u) << "row " << i;
        const auto got = group->lookup(hits[0].internal_id);
        EXPECT_EQ(got.partition_hash, rs.partition_hash) << "row " << i;
        EXPECT_EQ(got.block_number, rs.block_number) << "row " << i;
        EXPECT_EQ(got.block_offset, rs.block_offset) << "row " << i;
    }
}

#endif
