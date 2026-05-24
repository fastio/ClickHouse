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
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/AuxiliaryIndex/DiskANNAlgorithm.h>
#include <Storages/AuxiliaryIndex/DiskANNFfi.h>
#include <Storages/AuxiliaryIndex/IAuxiliaryIndexAlgorithm.h>
#include <Storages/AuxiliaryIndex/ANNAlgorithmFactory.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexContext.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexPartReverseLookup.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Stringifier.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>

#include <atomic>
#include <filesystem>
#include <limits>
#include <random>
#include <string>
#include <utility>
#include <vector>


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
    UInt64 pruned_degree_value = 32;
    bool with_unknown = false;
};

ASTPtr buildKwargList(const KwargBuild & b)
{
    auto list = make_intrusive<ASTExpressionList>();
    if (b.with_metric)
        list->children.push_back(makeKwarg("metric", b.metric_value));
    if (b.with_dim)
        list->children.push_back(makeKwarg("dim", b.dim_value));
    list->children.push_back(makeKwarg("pruned_degree", b.pruned_degree_value));
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


TEST_F(DiskANNAlgorithmTest, ValidateBuildParamsRejectsUInt32Overflow)
{
    DiskANNAlgorithm algo;
    KwargBuild b{};
    b.pruned_degree_value = static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1;
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


TEST_F(DiskANNAlgorithmTest, MatchChecksQueryMetric)
{
    constexpr UInt32 dim = 8;

    QueryFeatures features;
    features.query_vector.resize(dim, 0.0f);
    features.k = 3;

    DiskANNAlgorithm l2_algo;
    KwargBuild l2_build;
    l2_build.dim_value = dim;
    l2_algo.setBuildParameters(buildKwargList(l2_build), nullptr);

    features.distance_function = "L2Distance";
    auto l2_match = l2_algo.match(features);
    ASSERT_TRUE(l2_match.has_value());
    EXPECT_EQ(l2_match->distance.exact_function_name, "__materializedIndexDiskANNDistance");
    EXPECT_EQ(l2_match->distance.metric_name, "L2");
    EXPECT_EQ(l2_match->distance.metric_id, static_cast<UInt64>(DISKANN_METRIC_L2));
    EXPECT_EQ(l2_match->distance.dim, dim);

    features.distance_function = "cosineDistance";
    EXPECT_FALSE(l2_algo.match(features).has_value());
    features.distance_function = "dotProduct";
    EXPECT_FALSE(l2_algo.match(features).has_value());

    DiskANNAlgorithm cosine_algo;
    KwargBuild cosine_build;
    cosine_build.metric_value = "cosine";
    cosine_build.dim_value = dim;
    cosine_algo.setBuildParameters(buildKwargList(cosine_build), nullptr);

    features.distance_function = "cosineDistance";
    auto cosine_match = cosine_algo.match(features);
    ASSERT_TRUE(cosine_match.has_value());
    EXPECT_EQ(cosine_match->distance.metric_name, "cosine");
    EXPECT_EQ(cosine_match->distance.metric_id, static_cast<UInt64>(DISKANN_METRIC_COSINE));

    features.distance_function = "L2Distance";
    EXPECT_FALSE(cosine_algo.match(features).has_value());
}


TEST_F(DiskANNAlgorithmTest, ComputeDistancesUsesDiskANNMetricSemantics)
{
    {
        const std::vector<float> query = {0.0f, 0.0f};
        const std::vector<float> candidates = {
            3.0f, 4.0f,
            1.0f, 0.0f,
        };
        std::vector<float> out(2);

        computeDiskANNDistances(DISKANN_METRIC_L2, 2, query.data(), candidates.data(), 2, out.data());

        EXPECT_FLOAT_EQ(out[0], 25.0f);
        EXPECT_FLOAT_EQ(out[1], 1.0f);
    }

    {
        const std::vector<float> query = {1.0f, 0.0f};
        const std::vector<float> candidates = {
            0.0f, 1.0f,
            1.0f, 0.0f,
        };
        std::vector<float> out(2);

        computeDiskANNDistances(DISKANN_METRIC_COSINE, 2, query.data(), candidates.data(), 2, out.data());

        EXPECT_NEAR(out[0], 1.0f, 1e-6f);
        EXPECT_NEAR(out[1], 0.0f, 1e-6f);
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

    /// At least one DiskANN artefact exists with the algorithm_private prefix.
    bool any_index_file = false;
    static constexpr std::string_view candidate_suffixes[] = {
        "_disk.index",
        "_disk.index_pq_compressed.bin",
        "_disk.index_pq_pivots.bin",
        ".index",
    };
    for (auto suffix : candidate_suffixes)
    {
        const String rel = "algorithm_private_diskann" + std::string{suffix};
        if (output_storage->existsFile(rel))
        {
            any_index_file = true;
            break;
        }
    }
    EXPECT_TRUE(any_index_file)
        << "no DiskANN-produced index file found with algorithm_private_ prefix";
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

    ASSERT_TRUE(output_storage->existsFile("algorithm_private_fingerprint.json"));

    auto buf = output_storage->readFile("algorithm_private_fingerprint.json", ReadSettings{}, std::nullopt);
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
    auto & factory = ANNAlgorithmFactory::instance();
    EXPECT_TRUE(factory.familySupportsImpl("ann", "diskann"));

    KwargBuild b{};
    auto kwargs = buildKwargList(b);
    AuxiliaryIndexContext ctx;

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
    EXPECT_FALSE(output_storage->existsFile("algorithm_private_diskann_disk.index"));
    EXPECT_FALSE(output_storage->existsFile("algorithm_private_diskann.index"));
}


namespace
{

void synthesiseMidLayerWithMutableOffset(
    IDataPartStorage & part_storage,
    const UUID & source_uuid,
    const std::vector<std::pair<UInt32, UInt64>> & locator_rows)
{
    const size_t rows = locator_rows.size();

    part_storage.createDirectories();
    {
        auto writer = part_storage.writeFile("source_row_id_0.bin", 4096, WriteSettings{});
        for (size_t i = 0; i < rows; ++i)
        {
            writeBinaryLittleEndian(static_cast<UInt64>(i / 8192), *writer);
            writeBinaryLittleEndian(static_cast<UInt64>(i % 8192), *writer);
        }
        writer->finalize();
    }

    {
        auto writer = part_storage.writeFile("locator_0.bin", 4096, WriteSettings{});
        for (const auto & [part_uuid_id, part_offset] : locator_rows)
        {
            const auto entry = part_uuid_id == AuxiliaryIndexPartReverseLookup::TOMBSTONE_PART_UUID_ID
                ? AuxiliaryIndexPartReverseLookup::tombstoneLocatorEntry()
                : AuxiliaryIndexPartReverseLookup::liveLocatorEntry(part_uuid_id, part_offset);
            AuxiliaryIndexPartReverseLookup::writeLocatorEntry(entry, *writer);
        }
        writer->finalize();
    }

    {
        Poco::JSON::Object header;
        header.set("version", 1);
        AuxiliaryIndexPartReverseLookup::addLocatorHeaderFields(header);
        Poco::JSON::Array uuid_table;
        uuid_table.add(toString(source_uuid));
        header.set("part_uuid_table", uuid_table);
        Poco::JSON::Array boundaries;
        boundaries.add(static_cast<UInt64>(0));
        boundaries.add(static_cast<UInt64>(rows));
        header.set("segment_boundaries", boundaries);
        std::ostringstream oss;
        Poco::JSON::Stringifier::stringify(header, oss);
        const std::string text = oss.str();

        auto writer = part_storage.writeFile("header.json", 4096, WriteSettings{});
        writer->write(text.data(), text.size());
        writer->finalize();
    }
}


/// Synthesise the mid-layer files (header.json, source_row_id_0.bin,
/// locator_0.bin) that the query path consumes,
/// so the test can drive `DiskANNAlgorithm::search` without standing up the
/// full AuxiliaryIndexBuildTask pipeline. locator stores `_part_offset = i * 10`
/// for row i — distinguishable from the build-time `internal_id` so we can
/// prove the value really came from locator.
void synthesiseMidLayer(IDataPartStorage & part_storage, const UUID & source_uuid, size_t rows)
{
    std::vector<std::pair<UInt32, UInt64>> locator_rows;
    locator_rows.reserve(rows);
    for (size_t i = 0; i < rows; ++i)
        locator_rows.emplace_back(static_cast<UInt32>(0), static_cast<UInt64>(i * 10));
    synthesiseMidLayerWithMutableOffset(part_storage, source_uuid, locator_rows);
}

}


TEST_F(DiskANNAlgorithmTest, ReverseLookupDistinguishesZeroOffsetFromTombstone)
{
    const UUID source_uuid = UUIDHelpers::generateV4();
    synthesiseMidLayerWithMutableOffset(
        *output_storage,
        source_uuid,
        {
            {0, 0},
            {AuxiliaryIndexPartReverseLookup::TOMBSTONE_PART_UUID_ID, 0},
            {0, 42},
        });

    AuxiliaryIndexPartReverseLookup lookup(*output_storage);

    auto first = lookup.lookup(0);
    EXPECT_FALSE(first.is_tombstone);
    EXPECT_EQ(first.part_uuid, source_uuid);
    EXPECT_EQ(first.part_offset, 0u);

    auto tombstone = lookup.lookup(1);
    EXPECT_TRUE(tombstone.is_tombstone);

    auto live = lookup.lookup(2);
    EXPECT_FALSE(live.is_tombstone);
    EXPECT_EQ(live.part_uuid, source_uuid);
    EXPECT_EQ(live.part_offset, 42u);
}


TEST_F(DiskANNAlgorithmTest, ReverseLookupLoadsLocatorPagesAcrossBoundary)
{
    const UUID source_uuid = UUIDHelpers::generateV4();
    const size_t rows = AuxiliaryIndexPartReverseLookup::LOCATOR_PAGE_ROWS + 3;
    synthesiseMidLayer(*output_storage, source_uuid, rows);

    AuxiliaryIndexPartReverseLookup lookup(*output_storage);

    auto last_on_first_page = lookup.lookup(AuxiliaryIndexPartReverseLookup::LOCATOR_PAGE_ROWS - 1);
    EXPECT_FALSE(last_on_first_page.is_tombstone);
    EXPECT_EQ(last_on_first_page.part_uuid, source_uuid);
    EXPECT_EQ(last_on_first_page.part_offset, (AuxiliaryIndexPartReverseLookup::LOCATOR_PAGE_ROWS - 1) * 10);

    auto first_on_second_page = lookup.lookup(AuxiliaryIndexPartReverseLookup::LOCATOR_PAGE_ROWS);
    EXPECT_FALSE(first_on_second_page.is_tombstone);
    EXPECT_EQ(first_on_second_page.part_uuid, source_uuid);
    EXPECT_EQ(first_on_second_page.part_offset, AuxiliaryIndexPartReverseLookup::LOCATOR_PAGE_ROWS * 10);

    auto first_again = lookup.lookup(0);
    EXPECT_FALSE(first_again.is_tombstone);
    EXPECT_EQ(first_again.part_uuid, source_uuid);
    EXPECT_EQ(first_again.part_offset, 0u);
}


TEST_F(DiskANNAlgorithmTest, ReverseLookupRejectsPartUuidIdOutOfRange)
{
    const UUID source_uuid = UUIDHelpers::generateV4();
    synthesiseMidLayerWithMutableOffset(*output_storage, source_uuid, {{1, 0}});

    AuxiliaryIndexPartReverseLookup lookup(*output_storage);
    EXPECT_THROW((void)lookup.lookup(0), DB::Exception);
}


TEST_F(DiskANNAlgorithmTest, ReverseLookupRejectsMissingPartUuidTable)
{
    const UUID source_uuid = UUIDHelpers::generateV4();
    synthesiseMidLayerWithMutableOffset(*output_storage, source_uuid, {{0, 0}});

    Poco::JSON::Object header;
    header.set("version", 1);
    header.set("tombstone_part_uuid_id", static_cast<UInt64>(AuxiliaryIndexPartReverseLookup::TOMBSTONE_PART_UUID_ID));
    Poco::JSON::Array boundaries;
    boundaries.add(static_cast<UInt64>(0));
    boundaries.add(static_cast<UInt64>(1));
    header.set("segment_boundaries", boundaries);

    std::ostringstream oss;
    Poco::JSON::Stringifier::stringify(header, oss);
    const std::string text = oss.str();

    auto writer = output_storage->writeFile("header.json", 4096, WriteSettings{});
    writer->write(text.data(), text.size());
    writer->finalize();

    EXPECT_THROW(
        {
            AuxiliaryIndexPartReverseLookup lookup(*output_storage);
            (void)lookup;
        },
        DB::Exception);
}


TEST_F(DiskANNAlgorithmTest, EstimateCostUsesCandidateLimit)
{
    DiskANNAlgorithm algo;
    MatchDescriptor desc;
    desc.k = 10;

    CoverageSnapshot coverage;
    coverage.candidate_limit = 40;

    auto estimated = algo.estimateCost(desc, coverage);
    EXPECT_EQ(estimated.estimated_result_rows, 40u);
    EXPECT_EQ(estimated.algorithm_search_cost, 4000u);

    CoverageSnapshot empty_coverage;
    estimated = algo.estimateCost(desc, empty_coverage);
    EXPECT_EQ(estimated.estimated_result_rows, 10u);
    EXPECT_EQ(estimated.algorithm_search_cost, 1000u);
}


TEST_F(DiskANNAlgorithmTest, MatchAndSearchEndToEnd)
{
    constexpr UInt32 dim = 64;
    constexpr size_t rows = 256;
    constexpr size_t k = 10;

    DiskANNAlgorithm algo;
    KwargBuild b{};
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeRandomEmbeddingBlock(rows, dim, /*seed=*/7);

    std::atomic<bool> cancelled{false};
    AlgorithmBuildContext ctx;
    ctx.output_storage = output_storage;
    ctx.intermediate_storage = intermediate_storage;
    ctx.is_cancelled = &cancelled;
    ctx.total_rows = rows;

    ASSERT_NO_THROW(algo.prepareBuild(ctx, block));
    ASSERT_NO_THROW(algo.buildAlgorithmPrivate(ctx));
    ASSERT_NO_THROW(algo.finishBuild(ctx));

    const UUID source_uuid = UUIDHelpers::generateV4();
    synthesiseMidLayer(*output_storage, source_uuid, rows);

    /// Pull the stored vector for row 5 out of the block and use it as the
    /// query — DiskANN should return that row first with distance zero.
    const auto & embedding_col = block.getByName("embedding").column;
    const auto & array_col = typeid_cast<const ColumnArray &>(*embedding_col);
    const auto & inner_col = typeid_cast<const ColumnVector<Float32> &>(array_col.getData());
    const size_t target_row = 5;
    std::vector<float> query(dim);
    for (UInt32 d = 0; d < dim; ++d)
        query[d] = inner_col.getData()[target_row * dim + d];

    QueryFeatures features;
    features.query_vector = query;
    features.distance_function = "L2Distance";
    features.k = k;

    auto match_descriptor = algo.match(features);
    ASSERT_TRUE(match_descriptor.has_value());

    ReadyAuxiliaryIndexPartSnapshot ready_parts;
    ready_parts.parts.push_back({output_storage, {}});

    InternalSearchResult result = algo.search(*match_descriptor, ready_parts, k, nullptr);

    ASSERT_EQ(result.per_auxiliary_index_part.size(), 1u);
    const auto & set = result.per_auxiliary_index_part.front();
    EXPECT_EQ(set.auxiliary_index_part_storage, output_storage);
    ASSERT_FALSE(set.internal_ids.empty());
    EXPECT_EQ(set.internal_ids.size(), set.distances.size());
    EXPECT_LE(set.internal_ids.size(), k);
    EXPECT_NE(std::find(set.internal_ids.begin(), set.internal_ids.end(), target_row), set.internal_ids.end());

    /// The query vector matches row 5 exactly; the closest hit should be
    /// row 5 itself with a distance below the noise floor.
    EXPECT_FLOAT_EQ(set.distances.front(), 0.0f);
}

#endif
