#include "config.h"

#if USE_DISKANN

#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <Disks/SingleDiskVolume.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MaterializedIndex/DiskANNAlgorithm.h>
#include <Storages/MaterializedIndex/IMaterializedIndexAlgorithm.h>
#include <Storages/MaterializedIndex/MaterializedIndexAlgorithmFactory.h>
#include <Storages/MaterializedIndex/MaterializedIndexContext.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>

#include <atomic>
#include <filesystem>
#include <random>
#include <string>
#include <utility>


using namespace DB;


namespace DB::ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int EXTERNAL_LIBRARY_ERROR;
}


namespace
{

ASTPtr makeKwarg(const String & name, Field value)
{
    auto eq = make_intrusive<ASTFunction>();
    eq->name = "equals";
    eq->arguments = make_intrusive<ASTExpressionList>();
    eq->arguments->children.push_back(make_intrusive<ASTIdentifier>(name));
    eq->arguments->children.push_back(make_intrusive<ASTLiteral>(std::move(value)));
    eq->children.push_back(eq->arguments);
    return eq;
}

/// Build an ASTExpressionList of `name=value` kwargs suitable for
/// `validateBuildParameters`. The caller specifies an explicit subset; when
/// `with_metric` / `with_dim` is false the corresponding key is omitted to
/// produce a malformed request.
struct KwargBuild
{
    bool with_metric = true;
    String metric_value = "L2";
    bool with_dim = true;
    UInt64 dim_value = 128;
    bool with_unknown = false;
};

ASTPtr buildKwargList(const KwargBuild & b)
{
    auto list = make_intrusive<ASTExpressionList>();
    if (b.with_metric)
        list->children.push_back(makeKwarg("metric", b.metric_value));
    if (b.with_dim)
        list->children.push_back(makeKwarg("dim", b.dim_value));
    list->children.push_back(makeKwarg("pruned_degree", static_cast<UInt64>(32)));
    list->children.push_back(makeKwarg("max_degree", static_cast<UInt64>(64)));
    list->children.push_back(makeKwarg("l_build", static_cast<UInt64>(128)));
    list->children.push_back(makeKwarg("alpha", 1.2));
    list->children.push_back(makeKwarg("num_threads", static_cast<UInt64>(4)));
    list->children.push_back(makeKwarg("pq_chunks", static_cast<UInt64>(4)));
    list->children.push_back(makeKwarg("build_ram_limit_gb", 1.0));
    if (b.with_unknown)
        list->children.push_back(makeKwarg("unknown_param", static_cast<UInt64>(1)));
    return list;
}


/// Per-test temporary working tree under `./build/test-state/diskann/<name>/`.
class DiskANNAlgorithmTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        const auto * info = ::testing::UnitTest::GetInstance()->current_test_info();
        const std::string root_str = std::string{"./build/test-state/diskann/"} + info->name();
        std::filesystem::remove_all(root_str);
        std::filesystem::create_directories(root_str);
        std::filesystem::create_directories(root_str + "/output/part");
        std::filesystem::create_directories(root_str + "/intermediate/part");

        const std::string root = std::filesystem::absolute(root_str).string();

        disk = std::make_shared<DiskLocal>("diskann_test_disk", root);
        volume = std::make_shared<SingleDiskVolume>("diskann_test_vol", disk, 0);

        output_storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "output", "part");
        intermediate_storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "intermediate", "part");
    }

    void TearDown() override
    {
        output_storage.reset();
        intermediate_storage.reset();
        volume.reset();
        disk.reset();
    }

    std::shared_ptr<DiskLocal> disk;
    VolumePtr volume;
    MutableDataPartStoragePtr output_storage;
    MutableDataPartStoragePtr intermediate_storage;
};


/// Build a Block whose only column is an Array(Float32) of `dim`-wide rows
/// with values drawn from std::mt19937 with a fixed seed.
Block makeRandomEmbeddingBlock(size_t rows, UInt32 dim, uint32_t seed)
{
    std::mt19937 rng(seed);
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);

    auto inner = ColumnVector<Float32>::create();
    auto & inner_data = inner->getData();
    inner_data.reserve(rows * dim);

    auto offsets = ColumnArray::ColumnOffsets::create();
    auto & offsets_data = offsets->getData();
    offsets_data.reserve(rows);

    UInt64 acc = 0;
    for (size_t r = 0; r < rows; ++r)
    {
        for (UInt32 d = 0; d < dim; ++d)
            inner_data.push_back(dist(rng));
        acc += dim;
        offsets_data.push_back(acc);
    }

    auto array_col = ColumnArray::create(std::move(inner), std::move(offsets));
    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());

    Block block;
    block.insert({std::move(array_col), array_type, "embedding"});
    return block;
}

}


TEST_F(DiskANNAlgorithmTest, ValidateBuildParamsAccepts)
{
    DiskANNAlgorithm algo;
    KwargBuild b{};
    EXPECT_NO_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr));
}

TEST_F(DiskANNAlgorithmTest, ValidateBuildParamsRejectsUnknown)
{
    DiskANNAlgorithm algo;
    KwargBuild b{};
    b.with_unknown = true;
    try
    {
        algo.validateBuildParameters(buildKwargList(b), nullptr);
        FAIL() << "expected DB::Exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
    }
}

TEST_F(DiskANNAlgorithmTest, ValidateBuildParamsRejectsMissingDim)
{
    DiskANNAlgorithm algo;
    KwargBuild b{};
    b.with_dim = false;
    try
    {
        algo.validateBuildParameters(buildKwargList(b), nullptr);
        FAIL() << "expected DB::Exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
    }
}

TEST_F(DiskANNAlgorithmTest, ValidateBuildParamsRejectsBadMetric)
{
    DiskANNAlgorithm algo;
    KwargBuild b{};
    b.metric_value = "cosine_x";
    try
    {
        algo.validateBuildParameters(buildKwargList(b), nullptr);
        FAIL() << "expected DB::Exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
    }
}


/// Builds a `StorageInMemoryMetadata` whose only column is an
/// `embedding Array(<inner_type>)` with the given inner element type.
static StorageInMemoryMetadata makeMetadataWithArrayColumn(DataTypePtr inner)
{
    StorageInMemoryMetadata metadata;
    ColumnsDescription columns;
    columns.add(ColumnDescription("embedding", std::make_shared<DataTypeArray>(std::move(inner))));
    metadata.setColumns(std::move(columns));
    return metadata;
}

TEST_F(DiskANNAlgorithmTest, ValidateIndexedExprAcceptsFloatArray)
{
    DiskANNAlgorithm algo;
    auto metadata = makeMetadataWithArrayColumn(std::make_shared<DataTypeFloat32>());
    auto expr = make_intrusive<ASTIdentifier>("embedding");
    EXPECT_NO_THROW(algo.validateIndexedExpression(expr, metadata));
}

TEST_F(DiskANNAlgorithmTest, ValidateIndexedExprRejectsNonFloat32)
{
    DiskANNAlgorithm algo;
    auto metadata = makeMetadataWithArrayColumn(std::make_shared<DataTypeFloat64>());
    auto expr = make_intrusive<ASTIdentifier>("embedding");
    try
    {
        algo.validateIndexedExpression(expr, metadata);
        FAIL() << "expected DB::Exception";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::BAD_ARGUMENTS);
    }
}


TEST_F(DiskANNAlgorithmTest, BuildThenSearchSmoke)
{
    constexpr UInt32 dim = 128;
    constexpr size_t rows = 1000;

    DiskANNAlgorithm algo;
    KwargBuild b{};
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeRandomEmbeddingBlock(rows, dim, /*seed=*/42);

    std::atomic<bool> cancelled{false};
    AlgorithmBuildContext ctx;
    ctx.output_storage = output_storage;
    ctx.intermediate_storage = intermediate_storage;
    ctx.is_cancelled = &cancelled;
    ctx.total_rows = rows;

    ASSERT_NO_THROW(algo.prepareBuild(ctx, block));
    ASSERT_NO_THROW(algo.buildAlgorithmPrivate(ctx));
    ASSERT_NO_THROW(algo.finishBuild(ctx));

    /// At least one DiskANN artefact exists under algorithm_private.
    bool any_index_file = false;
    static constexpr std::string_view candidate_suffixes[] = {
        "_disk.index",
        "_disk.index_pq_compressed.bin",
        "_disk.index_pq_pivots.bin",
        ".index",
    };
    for (auto suffix : candidate_suffixes)
    {
        const String rel = "algorithm_private/diskann" + std::string{suffix};
        if (output_storage->existsFile(rel))
        {
            any_index_file = true;
            break;
        }
    }
    EXPECT_TRUE(any_index_file)
        << "no DiskANN-produced index file found under algorithm_private/";
}


TEST_F(DiskANNAlgorithmTest, FingerprintContents)
{
    constexpr UInt32 dim = 128;
    constexpr size_t rows = 1000;

    DiskANNAlgorithm algo;
    KwargBuild b{};
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeRandomEmbeddingBlock(rows, dim, /*seed=*/42);

    std::atomic<bool> cancelled{false};
    AlgorithmBuildContext ctx;
    ctx.output_storage = output_storage;
    ctx.intermediate_storage = intermediate_storage;
    ctx.is_cancelled = &cancelled;
    ctx.total_rows = rows;

    ASSERT_NO_THROW(algo.prepareBuild(ctx, block));
    ASSERT_NO_THROW(algo.buildAlgorithmPrivate(ctx));
    ASSERT_NO_THROW(algo.finishBuild(ctx));

    ASSERT_TRUE(output_storage->existsFile("algorithm_private/fingerprint.json"));

    auto buf = output_storage->readFile("algorithm_private/fingerprint.json", ReadSettings{}, std::nullopt);
    std::string body;
    readStringUntilEOF(body, *buf);

    Poco::JSON::Parser parser;
    auto parsed = parser.parse(body);
    auto obj = parsed.extract<Poco::JSON::Object::Ptr>();   // NOLINT(performance-unnecessary-copy-initialization)
    ASSERT_TRUE(obj);

    EXPECT_TRUE(obj->has("algorithm_version"));
    EXPECT_TRUE(obj->has("params_hash"));
    EXPECT_TRUE(obj->has("num_points"));
    EXPECT_TRUE(obj->has("files"));

    EXPECT_FALSE(obj->getValue<std::string>("algorithm_version").empty());
    EXPECT_EQ(obj->getValue<Int64>("num_points"), static_cast<Int64>(rows));

    auto files_var = obj->get("files");
    auto files_arr = files_var.extract<Poco::JSON::Array::Ptr>();   // NOLINT(performance-unnecessary-copy-initialization)
    ASSERT_TRUE(files_arr);
    ASSERT_GT(files_arr->size(), 0u);

    for (UInt32 i = 0; i < files_arr->size(); ++i)
    {
        auto entry_var = files_arr->get(i);
        auto entry = entry_var.extract<Poco::JSON::Object::Ptr>();   // NOLINT(performance-unnecessary-copy-initialization)
        ASSERT_TRUE(entry);
        EXPECT_TRUE(entry->has("name"));
        EXPECT_TRUE(entry->has("size"));
        EXPECT_TRUE(entry->has("sipHash128"));
        const auto hash = entry->getValue<std::string>("sipHash128");
        EXPECT_EQ(hash.size(), 32u) << "expected 128-bit hex hash";
    }
}


TEST_F(DiskANNAlgorithmTest, FfiErrorMapsToException)
{
    /// Drive the FFI error path by calling builder.setDataPath with a
    /// non-existent file. We talk to the FFI directly here rather than
    /// going through `prepareBuild` so the test stays focused on the
    /// translation layer between FFI return codes and `DB::Exception`.
    constexpr UInt32 dim = 128;
    DiskANNBuilderHandle builder(
        dim,
        DISKANN_METRIC_L2,
        /*pruned_degree=*/32,
        /*max_degree=*/64,
        /*l_build=*/128,
        /*alpha=*/1.2f,
        /*num_threads=*/4,
        /*pq_chunks=*/4,
        /*build_ram_limit_gb=*/1.0);

    const std::string missing = std::filesystem::absolute(
        std::string{"./build/test-state/diskann/"}
        + ::testing::UnitTest::GetInstance()->current_test_info()->name()
        + "/this-path-does-not-exist/missing.fbin").string();
    builder.setDataPath(missing);
    builder.setIndexPrefix(std::filesystem::absolute(
        std::string{"./build/test-state/diskann/"}
        + ::testing::UnitTest::GetInstance()->current_test_info()->name()
        + "/index").string());

    try
    {
        builder.build();
        FAIL() << "expected DB::Exception from missing input vectors file";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::EXTERNAL_LIBRARY_ERROR);
    }
}


TEST_F(DiskANNAlgorithmTest, FactoryRegistration)
{
    auto & factory = MaterializedIndexAlgorithmFactory::instance();
    EXPECT_TRUE(factory.familySupportsImpl("ann", "diskann"));

    KwargBuild b{};
    auto kwargs = buildKwargList(b);
    MaterializedIndexContext ctx;

    auto algo = factory.get("ann", "diskann", kwargs, ctx);
    ASSERT_TRUE(algo);
    EXPECT_EQ(algo->getName(), "diskann");
    EXPECT_EQ(algo->getFamily(), "ann");
}


TEST_F(DiskANNAlgorithmTest, CancelBeforeStage3Honored)
{
    constexpr UInt32 dim = 128;
    constexpr size_t rows = 2000;

    DiskANNAlgorithm algo;
    KwargBuild b{};
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeRandomEmbeddingBlock(rows, dim, /*seed=*/42);

    std::atomic<bool> cancelled{false};
    AlgorithmBuildContext ctx;
    ctx.output_storage = output_storage;
    ctx.intermediate_storage = intermediate_storage;
    ctx.is_cancelled = &cancelled;
    ctx.total_rows = rows;

    /// Flip the flag before the build phase; depending on the row count and
    /// the cancel granule, either prepareBuild itself or buildAlgorithmPrivate
    /// catches the flag — both legal aborts.
    cancelled.store(true, std::memory_order_relaxed);

    bool aborted = false;
    try
    {
        algo.prepareBuild(ctx, block);
        algo.buildAlgorithmPrivate(ctx);
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::ABORTED);
        aborted = true;
    }
    EXPECT_TRUE(aborted);

    /// The on-disk DiskANN artefact must not exist: cancellation should fire
    /// before the FFI build runs.
    EXPECT_FALSE(output_storage->existsFile("algorithm_private/diskann_disk.index"));
    EXPECT_FALSE(output_storage->existsFile("algorithm_private/diskann.index"));
}

#endif
