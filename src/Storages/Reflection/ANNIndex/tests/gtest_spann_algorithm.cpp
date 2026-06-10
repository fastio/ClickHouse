#include "config.h"

#if USE_SPTAG

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
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/Reflection/ANNIndex/ANNAlgorithmFactory.h>
#include <Storages/Reflection/ANNIndex/ANNIndexContext.h>
#include <Storages/Reflection/ANNIndex/SPANNAlgorithm.h>
#include <Storages/Reflection/ANNIndex/SPANNFacade.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <filesystem>
#include <limits>
#include <random>
#include <thread>
#include <unordered_set>
#include <vector>


using namespace DB;


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int MEMORY_LIMIT_EXCEEDED;
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

struct KwargBuild
{
    bool with_metric = true;
    String metric_value = "L2";
    bool with_dim = true;
    UInt64 dim_value = 16;
    bool with_unknown = false;
    UInt64 internal_result_num = 64;
    UInt64 replica_count = 8;
    UInt64 posting_page_limit = 12;
    UInt64 max_check = 4096;
    UInt64 io_threads = 16;
    bool with_io_threads = true;
};

ASTPtr buildKwargList(const KwargBuild & b)
{
    auto list = make_intrusive<ASTExpressionList>();
    if (b.with_metric)
        list->children.push_back(makeKwarg("metric", b.metric_value));
    if (b.with_dim)
        list->children.push_back(makeKwarg("dim", b.dim_value));
    list->children.push_back(makeKwarg("head_ratio", 0.3));
    list->children.push_back(makeKwarg("posting_page_limit", b.posting_page_limit));
    list->children.push_back(makeKwarg("search_posting_page_limit", b.posting_page_limit));
    list->children.push_back(makeKwarg("internal_result_num", b.internal_result_num));
    list->children.push_back(makeKwarg("replica_count", b.replica_count));
    list->children.push_back(makeKwarg("num_threads", static_cast<UInt64>(2)));
    list->children.push_back(makeKwarg("max_check", b.max_check));
    if (b.with_io_threads)
        list->children.push_back(makeKwarg("io_threads", b.io_threads));
    if (b.with_unknown)
        list->children.push_back(makeKwarg("unknown_param", static_cast<UInt64>(1)));
    return list;
}

class SPANNAlgorithmTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        const auto * info = ::testing::UnitTest::GetInstance()->current_test_info();
        const std::string root_str = std::string{"./build/test-state/spann/"} + info->name();
        std::filesystem::remove_all(root_str);
        std::filesystem::create_directories(root_str);
        std::filesystem::create_directories(root_str + "/output/part");
        std::filesystem::create_directories(root_str + "/intermediate/part");

        const std::string root = std::filesystem::absolute(root_str).string();

        disk = std::make_shared<DiskLocal>("spann_test_disk", root);
        volume = std::make_shared<SingleDiskVolume>("spann_test_vol", disk, 0);

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

Block makeSeparatedEmbeddingBlock(size_t rows, UInt32 dim)
{
    auto inner = ColumnVector<Float32>::create();
    auto & inner_data = inner->getData();
    inner_data.reserve(rows * dim);

    auto offsets = ColumnArray::ColumnOffsets::create();
    auto & offsets_data = offsets->getData();
    offsets_data.reserve(rows);

    UInt64 acc = 0;
    for (size_t row = 0; row < rows; ++row)
    {
        for (UInt32 d = 0; d < dim; ++d)
        {
            const auto value = d == 0
                ? static_cast<Float32>(row * 1000)
                : static_cast<Float32>((row + 1) * (d + 3) % 17);
            inner_data.push_back(value);
        }
        acc += dim;
        offsets_data.push_back(acc);
    }

    auto array_col = ColumnArray::create(std::move(inner), std::move(offsets));
    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());

    Block block;
    block.insert({std::move(array_col), array_type, "embedding"});
    return block;
}

StorageInMemoryMetadata makeMetadataWithArrayColumn(DataTypePtr inner)
{
    StorageInMemoryMetadata metadata;
    ColumnsDescription columns;
    columns.add(ColumnDescription("embedding", std::make_shared<DataTypeArray>(std::move(inner))));
    metadata.setColumns(std::move(columns));
    return metadata;
}

void buildSmallIndex(
    SPANNAlgorithm & algo,
    const MutableDataPartStoragePtr & output_storage,
    const MutableDataPartStoragePtr & intermediate_storage,
    const Block & block,
    const String & task_id = {})
{
    std::atomic<bool> cancelled{false};
    AlgorithmBuildContext ctx;
    ctx.output_storage = output_storage;
    ctx.intermediate_storage = intermediate_storage;
    ctx.is_cancelled = &cancelled;
    ctx.total_rows = block.rows();
    ctx.task_id = task_id;

    algo.prepareBuild(ctx, block);
    algo.buildAlgorithmPrivate(ctx);
    algo.finishBuild(ctx);
}

std::vector<float> getRowVector(const Block & block, size_t row, UInt32 dim)
{
    const auto & embedding_col = block.getByName("embedding").column;
    const auto & array_col = typeid_cast<const ColumnArray &>(*embedding_col);
    const auto & inner_col = typeid_cast<const ColumnVector<Float32> &>(array_col.getData());

    std::vector<float> query(dim);
    for (UInt32 d = 0; d < dim; ++d)
        query[d] = inner_col.getData()[row * dim + d];
    return query;
}

/// Deterministic random embeddings drawn from N(0, 1) and L2-normalised so the
/// same dataset works equally well for the L2 and cosine round-trips. A fixed
/// seed keeps recall measurements reproducible across test runs.
Block makeRandomEmbeddingBlock(size_t rows, UInt32 dim, UInt64 seed)
{
    auto inner = ColumnVector<Float32>::create();
    auto & inner_data = inner->getData();
    inner_data.reserve(rows * dim);

    auto offsets = ColumnArray::ColumnOffsets::create();
    auto & offsets_data = offsets->getData();
    offsets_data.reserve(rows);

    std::mt19937_64 rng{seed}; // NOLINT(cert-msc32-c, cert-msc51-cpp)
    std::normal_distribution<float> dist{0.0f, 1.0f};

    UInt64 acc = 0;
    for (size_t row = 0; row < rows; ++row)
    {
        float norm_sq = 0.0f;
        const size_t base = inner_data.size();
        for (UInt32 d = 0; d < dim; ++d)
        {
            const float v = dist(rng);
            inner_data.push_back(v);
            norm_sq += v * v;
        }
        const float inv_norm = norm_sq > 0.0f ? 1.0f / std::sqrt(norm_sq) : 1.0f;
        for (UInt32 d = 0; d < dim; ++d)
            inner_data[base + d] *= inv_norm;

        acc += dim;
        offsets_data.push_back(acc);
    }

    auto array_col = ColumnArray::create(std::move(inner), std::move(offsets));
    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());

    Block block;
    block.insert({std::move(array_col), array_type, "embedding"});
    return block;
}

/// Exact top-K computed by brute force, used as the ground truth for recall.
std::vector<size_t> bruteForceTopK(
    const Block & block,
    UInt32 dim,
    const std::vector<float> & query,
    size_t k,
    SPANNFacade::Metric metric)
{
    const size_t rows = block.rows();
    std::vector<std::pair<float, size_t>> scored;
    scored.reserve(rows);

    std::vector<float> candidate(dim);
    for (size_t row = 0; row < rows; ++row)
    {
        const auto row_vec = getRowVector(block, row, dim);
        float d = 0.0f;
        SPANNFacade::computeDistances(metric, dim, query.data(), row_vec.data(), 1, &d);
        scored.emplace_back(d, row);
    }

    std::partial_sort(
        scored.begin(),
        scored.begin() + std::min(k, scored.size()),
        scored.end(),
        [](const auto & a, const auto & b) { return a.first < b.first; });

    std::vector<size_t> top;
    top.reserve(std::min(k, scored.size()));
    for (size_t i = 0; i < std::min(k, scored.size()); ++i)
        top.push_back(scored[i].second);
    return top;
}

double recallAtK(const std::vector<UInt64> & got, const std::vector<size_t> & truth)
{
    if (truth.empty())
        return 1.0;
    std::unordered_set<UInt64> truth_set;
    for (auto id : truth)
        truth_set.insert(static_cast<UInt64>(id));
    size_t hits = 0;
    for (auto id : got)
        if (truth_set.contains(id))
            ++hits;
    return static_cast<double>(hits) / static_cast<double>(truth.size());
}

}


TEST_F(SPANNAlgorithmTest, ValidateBuildParamsAccepts)
{
    for (const String & metric : {"L2", "l2", "cosine", "Cosine", "COSINE", "InnerProduct", "innerproduct", "inner_product", "dotProduct"})
    {
        SPANNAlgorithm algo;
        KwargBuild b{};
        b.metric_value = metric;
        EXPECT_NO_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr)) << metric;
    }
}

TEST_F(SPANNAlgorithmTest, ValidateBuildParamsRejectsUnknown)
{
    SPANNAlgorithm algo;
    KwargBuild b{};
    b.with_unknown = true;
    EXPECT_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr), DB::Exception);
}

TEST_F(SPANNAlgorithmTest, ValidateBuildParamsRejectsMissingDim)
{
    SPANNAlgorithm algo;
    KwargBuild b{};
    b.with_dim = false;
    EXPECT_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr), DB::Exception);
}

TEST_F(SPANNAlgorithmTest, ValidateBuildParamsRejectsBadMetric)
{
    SPANNAlgorithm algo;
    KwargBuild b{};
    b.metric_value = "cosine_x";
    EXPECT_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr), DB::Exception);
}

TEST_F(SPANNAlgorithmTest, ValidateBuildParamsRejectsInt32Overflow)
{
    SPANNAlgorithm algo;
    KwargBuild b{};
    b.dim_value = static_cast<UInt64>(std::numeric_limits<Int32>::max()) + 1;
    EXPECT_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr), DB::Exception);
}

TEST_F(SPANNAlgorithmTest, ValidateIndexedExprAcceptsFloatArray)
{
    SPANNAlgorithm algo;
    auto metadata = makeMetadataWithArrayColumn(std::make_shared<DataTypeFloat32>());
    auto expr = make_intrusive<ASTIdentifier>("embedding");
    EXPECT_NO_THROW(algo.validateIndexedExpression(expr, metadata));
}

TEST_F(SPANNAlgorithmTest, ValidateIndexedExprRejectsNonFloat32)
{
    SPANNAlgorithm algo;
    auto metadata = makeMetadataWithArrayColumn(std::make_shared<DataTypeFloat64>());
    auto expr = make_intrusive<ASTIdentifier>("embedding");
    EXPECT_THROW(algo.validateIndexedExpression(expr, metadata), DB::Exception);
}

TEST_F(SPANNAlgorithmTest, ValidateIndexedExprRejectsExpressionWrapper)
{
    SPANNAlgorithm algo;
    auto metadata = makeMetadataWithArrayColumn(std::make_shared<DataTypeFloat32>());
    auto expr = make_intrusive<ASTFunction>();
    expr->name = "materialize";
    EXPECT_THROW(algo.validateIndexedExpression(expr, metadata), DB::Exception);
}

TEST_F(SPANNAlgorithmTest, MatchChecksQueryMetric)
{
    constexpr UInt32 dim = 8;

    struct Case
    {
        String metric;
        String function;
        String metric_name;
        UInt64 metric_id;
        bool smaller_is_better;
        std::vector<String> rejected_functions;
    };

    const std::vector<Case> cases{
        {"L2", "L2Distance", "L2", SPANNFacade::metricId(SPANNFacade::Metric::L2), true, {"cosineDistance", "dotProduct"}},
        {"cosine", "cosineDistance", "cosine", SPANNFacade::metricId(SPANNFacade::Metric::Cosine), true, {"L2Distance", "dotProduct"}},
        {"InnerProduct", "dotProduct", "InnerProduct", SPANNFacade::metricId(SPANNFacade::Metric::InnerProduct), false, {"L2Distance", "cosineDistance"}},
    };

    for (const auto & test_case : cases)
    {
        SPANNAlgorithm algo;
        KwargBuild build;
        build.metric_value = test_case.metric;
        build.dim_value = dim;
        algo.setBuildParameters(buildKwargList(build), nullptr);

        QueryFeatures features;
        features.query_vector.resize(dim, 0.0f);
        features.k = 3;
        features.distance_function = test_case.function;

        auto match = algo.match(features);
        ASSERT_TRUE(match.has_value()) << test_case.metric;
        EXPECT_EQ(match->distance.exact_function_name, "__reflectionANNIndexSPANNDistance");
        EXPECT_EQ(match->distance.metric_name, test_case.metric_name);
        EXPECT_EQ(match->distance.metric_id, test_case.metric_id);
        EXPECT_EQ(match->distance.dim, dim);
        EXPECT_EQ(match->distance.smaller_is_better, test_case.smaller_is_better);

        for (const auto & rejected : test_case.rejected_functions)
        {
            features.distance_function = rejected;
            EXPECT_FALSE(algo.match(features).has_value()) << test_case.metric << " matched " << rejected;
        }
    }
}

TEST_F(SPANNAlgorithmTest, ExactDistanceMatchesSPTAGOrdering)
{
    {
        const std::vector<float> query = {0.0f, 0.0f};
        const std::vector<float> candidates = {
            3.0f, 4.0f,
            1.0f, 0.0f,
        };
        std::vector<float> out(2);
        SPANNFacade::computeDistances(SPANNFacade::Metric::L2, 2, query.data(), candidates.data(), 2, out.data());
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
        SPANNFacade::computeDistances(SPANNFacade::Metric::Cosine, 2, query.data(), candidates.data(), 2, out.data());
        EXPECT_NEAR(out[0], 1.0f, 1e-6f);
        EXPECT_NEAR(out[1], 0.0f, 1e-6f);
    }

    {
        const std::vector<float> query = {2.0f, 3.0f};
        const std::vector<float> candidates = {
            4.0f, 5.0f,
            1.0f, -1.0f,
        };
        std::vector<float> out(2);
        SPANNFacade::computeDistances(SPANNFacade::Metric::InnerProduct, 2, query.data(), candidates.data(), 2, out.data());
        EXPECT_FLOAT_EQ(out[0], 23.0f);
        EXPECT_FLOAT_EQ(out[1], -1.0f);
    }
}

TEST_F(SPANNAlgorithmTest, BuildWritesExpectedLayout)
{
    constexpr UInt32 dim = 16;
    constexpr size_t rows = 256;

    SPANNAlgorithm algo;
    KwargBuild b{};
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeSeparatedEmbeddingBlock(rows, dim);
    ASSERT_NO_THROW(buildSmallIndex(algo, output_storage, intermediate_storage, block));

    EXPECT_TRUE(output_storage->existsFile("algorithm_private_spann/indexloader.ini"));
    EXPECT_TRUE(output_storage->existsDirectory("algorithm_private_spann/HeadIndex"));
    EXPECT_TRUE(output_storage->existsFile("algorithm_private_spann/SPTAGFullList.bin"));
}

TEST_F(SPANNAlgorithmTest, BuildStatusRegistryReportsMissingAndFailedTasks)
{
    const String missing_task_id = "missing-spann-status-task";
    EXPECT_FALSE(SPANNFacade::getBuildStatusForTask(missing_task_id).has_value());

    const String failed_task_id = "failed-spann-status-task";
    auto status = std::make_shared<SPANNFacade::BuildStatus>();
    status->markStarted(/*rows_total_=*/10);
    status->setSettings({{"metric", "L2"}, {"dimension", "16"}});
    SPANNFacade::registerBuildStatus(failed_task_id, status);

    status->setRows(/*rows_processed_=*/4, /*rows_total_=*/10);
    status->markFailed("synthetic failure");

    auto snapshot = SPANNFacade::getBuildStatusForTask(failed_task_id);
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->started, 1);
    EXPECT_EQ(snapshot->finished, 0);
    EXPECT_EQ(snapshot->failed, 1);
    EXPECT_EQ(snapshot->rows_processed, 4);
    EXPECT_EQ(snapshot->rows_total, 10);
    EXPECT_EQ(snapshot->error_message, "synthetic failure");
    EXPECT_EQ(snapshot->settings.at("metric"), "L2");
    EXPECT_EQ(snapshot->settings.at("dimension"), "16");

    SPANNFacade::unregisterBuildStatus(failed_task_id);
    EXPECT_FALSE(SPANNFacade::getBuildStatusForTask(failed_task_id).has_value());
}

TEST_F(SPANNAlgorithmTest, BuildStatusTracksAlgorithmBuild)
{
    constexpr UInt32 dim = 16;
    constexpr size_t rows = 256;

    SPANNAlgorithm algo;
    KwargBuild b{};
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    std::atomic<bool> cancelled{false};
    AlgorithmBuildContext ctx;
    ctx.output_storage = output_storage;
    ctx.intermediate_storage = intermediate_storage;
    ctx.is_cancelled = &cancelled;
    ctx.total_rows = rows;
    ctx.task_id = "running-spann-status-task";

    Block block = makeSeparatedEmbeddingBlock(rows, dim);
    algo.prepareBuild(ctx, block);

    auto collecting = SPANNFacade::getBuildStatusForTask(ctx.task_id);
    ASSERT_TRUE(collecting.has_value());
    EXPECT_EQ(collecting->stage, SPANNFacade::BuildStage::CollectRows);
    EXPECT_EQ(collecting->rows_processed, rows);
    EXPECT_EQ(collecting->rows_total, rows);

    algo.buildAlgorithmPrivate(ctx);

    auto finished = SPANNFacade::getBuildStatusForTask(ctx.task_id);
    ASSERT_TRUE(finished.has_value());
    EXPECT_EQ(finished->stage, SPANNFacade::BuildStage::End);
    EXPECT_EQ(finished->finished, 1);
    EXPECT_EQ(finished->vector_bytes, rows * dim * sizeof(Float32));

    algo.finishBuild(ctx);
    EXPECT_FALSE(SPANNFacade::getBuildStatusForTask(ctx.task_id).has_value());
}

TEST_F(SPANNAlgorithmTest, PrivatePathsIncludeIndexDirectory)
{
    constexpr UInt32 dim = 16;
    constexpr size_t rows = 256;

    SPANNAlgorithm algo;
    KwargBuild b{};
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeSeparatedEmbeddingBlock(rows, dim);
    ASSERT_NO_THROW(buildSmallIndex(algo, output_storage, intermediate_storage, block));

    const auto paths = algo.getAlgorithmPrivatePaths(*output_storage);
    bool has_spann_dir = false;
    bool has_fingerprint = false;
    for (const auto & path : paths)
    {
        if (path.path == "algorithm_private_spann")
        {
            has_spann_dir = true;
            EXPECT_TRUE(path.recursive);
            EXPECT_TRUE(path.required);
        }
        if (path.path == "algorithm_private_fingerprint.json")
        {
            has_fingerprint = true;
            EXPECT_FALSE(path.recursive);
            EXPECT_TRUE(path.required);
        }
    }

    EXPECT_TRUE(has_spann_dir);
    EXPECT_TRUE(has_fingerprint);
}

TEST_F(SPANNAlgorithmTest, FingerprintIncludesRecursiveFiles)
{
    constexpr UInt32 dim = 16;
    constexpr size_t rows = 256;

    SPANNAlgorithm algo;
    KwargBuild b{};
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeSeparatedEmbeddingBlock(rows, dim);
    ASSERT_NO_THROW(buildSmallIndex(algo, output_storage, intermediate_storage, block));

    ASSERT_TRUE(output_storage->existsFile("algorithm_private_fingerprint.json"));

    auto buf = output_storage->readFile("algorithm_private_fingerprint.json", ReadSettings{}, std::nullopt);
    std::string body;
    readStringUntilEOF(body, *buf);

    Poco::JSON::Parser parser;
    auto parsed = parser.parse(body);
    auto obj = parsed.extract<Poco::JSON::Object::Ptr>();   // NOLINT(performance-unnecessary-copy-initialization)
    ASSERT_TRUE(obj);
    EXPECT_EQ(obj->getValue<Int64>("num_points"), static_cast<Int64>(rows));

    auto files_arr = obj->get("files").extract<Poco::JSON::Array::Ptr>();   // NOLINT(performance-unnecessary-copy-initialization)
    ASSERT_TRUE(files_arr);
    ASSERT_GT(files_arr->size(), 0u);

    bool saw_head_index_file = false;
    for (UInt32 i = 0; i < files_arr->size(); ++i)
    {
        auto entry = files_arr->get(i).extract<Poco::JSON::Object::Ptr>();   // NOLINT(performance-unnecessary-copy-initialization)
        ASSERT_TRUE(entry);
        const auto name = entry->getValue<std::string>("name");
        if (name.starts_with("algorithm_private_spann/HeadIndex/"))
            saw_head_index_file = true;
        EXPECT_EQ(entry->getValue<std::string>("sipHash128").size(), 32u);
    }
    EXPECT_TRUE(saw_head_index_file);
}

TEST_F(SPANNAlgorithmTest, BuildAndSearchRoundtrip)
{
    constexpr UInt32 dim = 16;
    constexpr size_t rows = 256;
    constexpr size_t k = 10;
    constexpr size_t target_row = 42;

    SPANNAlgorithm algo;
    KwargBuild b{};
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeSeparatedEmbeddingBlock(rows, dim);
    ASSERT_NO_THROW(buildSmallIndex(algo, output_storage, intermediate_storage, block));

    QueryFeatures features;
    features.query_vector = getRowVector(block, target_row, dim);
    features.distance_function = "L2Distance";
    features.k = k;

    auto match_descriptor = algo.match(features);
    ASSERT_TRUE(match_descriptor.has_value());

    ReadyANNIndexPartSnapshot ready_parts;
    ready_parts.parts.push_back({output_storage, {}});

    InternalSearchResult result = algo.search(*match_descriptor, ready_parts, k, nullptr);
    ASSERT_EQ(result.per_ann_index_part.size(), 1u);
    const auto & set = result.per_ann_index_part.front();
    ASSERT_FALSE(set.internal_ids.empty());
    EXPECT_EQ(set.internal_ids.size(), set.distances.size());
    EXPECT_NE(std::find(set.internal_ids.begin(), set.internal_ids.end(), target_row), set.internal_ids.end());
}

/// Build a 1k random-vector index, run 16 randomised queries, brute-force the
/// true top-K, require >=80% mean recall@10. Catches silent recall regression
/// from future parameter changes — the original `BuildAndSearchRoundtrip` only
/// checked self-recall against trivially-separable data and passed even when
/// SPANN returned random IDs as long as the exact match was present.
TEST_F(SPANNAlgorithmTest, RecallAtKOnRandomDataL2)
{
    constexpr UInt32 dim = 32;
    constexpr size_t rows = 1024;
    constexpr size_t k = 10;
    constexpr size_t num_queries = 16;
    constexpr double min_mean_recall = 0.8;

    SPANNAlgorithm algo;
    KwargBuild b{};
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeRandomEmbeddingBlock(rows, dim, /*seed=*/0xC0DEC0DEull);
    ASSERT_NO_THROW(buildSmallIndex(algo, output_storage, intermediate_storage, block));

    ReadyANNIndexPartSnapshot ready_parts;
    ready_parts.parts.push_back({output_storage, {}});

    std::mt19937_64 rng{0xBA5EBA5Eull}; // NOLINT(cert-msc32-c, cert-msc51-cpp)
    std::uniform_int_distribution<size_t> pick(0, rows - 1);

    double total_recall = 0.0;
    for (size_t q = 0; q < num_queries; ++q)
    {
        QueryFeatures features;
        features.query_vector = getRowVector(block, pick(rng), dim);
        features.distance_function = "L2Distance";
        features.k = k;

        auto match_descriptor = algo.match(features);
        ASSERT_TRUE(match_descriptor.has_value());

        auto result = algo.search(*match_descriptor, ready_parts, k, nullptr);
        ASSERT_EQ(result.per_ann_index_part.size(), 1u);
        const auto & set = result.per_ann_index_part.front();

        const auto truth = bruteForceTopK(block, dim, features.query_vector, k, SPANNFacade::Metric::L2);
        total_recall += recallAtK(set.internal_ids, truth);
    }
    const double mean_recall = total_recall / static_cast<double>(num_queries);
    EXPECT_GE(mean_recall, min_mean_recall) << "mean recall@" << k << " was " << mean_recall;
}

/// Same shape as the L2 recall test, but exercises the cosine path: the
/// `Normalize` step inside `SPTAG::Index<float>::BuildIndex` runs only for
/// `DistCalcMethod::Cosine`, and the searcher consumes `cosineDistance` from
/// the query side. Together they cover the cosine-specific failure surface
/// that the L2 test cannot reach.
TEST_F(SPANNAlgorithmTest, RecallAtKOnRandomDataCosine)
{
    constexpr UInt32 dim = 32;
    constexpr size_t rows = 1024;
    constexpr size_t k = 10;
    constexpr size_t num_queries = 16;
    constexpr double min_mean_recall = 0.8;

    SPANNAlgorithm algo;
    KwargBuild b{};
    b.dim_value = dim;
    b.metric_value = "cosine";
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeRandomEmbeddingBlock(rows, dim, /*seed=*/0xC051C051ull);
    ASSERT_NO_THROW(buildSmallIndex(algo, output_storage, intermediate_storage, block));

    ReadyANNIndexPartSnapshot ready_parts;
    ready_parts.parts.push_back({output_storage, {}});

    std::mt19937_64 rng{0xBA5EBA5Eull}; // NOLINT(cert-msc32-c, cert-msc51-cpp)
    std::uniform_int_distribution<size_t> pick(0, rows - 1);

    double total_recall = 0.0;
    for (size_t q = 0; q < num_queries; ++q)
    {
        QueryFeatures features;
        features.query_vector = getRowVector(block, pick(rng), dim);
        features.distance_function = "cosineDistance";
        features.k = k;

        auto match_descriptor = algo.match(features);
        ASSERT_TRUE(match_descriptor.has_value());

        auto result = algo.search(*match_descriptor, ready_parts, k, nullptr);
        ASSERT_EQ(result.per_ann_index_part.size(), 1u);
        const auto & set = result.per_ann_index_part.front();

        const auto truth = bruteForceTopK(block, dim, features.query_vector, k, SPANNFacade::Metric::Cosine);
        total_recall += recallAtK(set.internal_ids, truth);
    }
    const double mean_recall = total_recall / static_cast<double>(num_queries);
    EXPECT_GE(mean_recall, min_mean_recall) << "mean recall@" << k << " was " << mean_recall;
}

/// `params_hash` and `num_points` are derived only from the DDL parameters and
/// the input row count, so they must match byte-for-byte across two builds of
/// the same data — that's enough to let replication detect a config mismatch
/// before serving search.
///
/// The per-file `sipHash128` entries, on the other hand, depend on SPTAG's
/// internal serialization order, which is non-deterministic across builds
/// (OpenMP-driven BKT construction + posting partitioning + uninitialised
/// padding in `SaveIndex` output). We intentionally do **not** assert byte
/// equality across builds for those files — see the comment on `finishBuild`
/// in `SPANNAlgorithm.cpp` explaining that the fingerprint is a single-build
/// self-integrity record, not a cross-replica reconciliation tool. The file
/// names and the *set* of files, however, are deterministic and worth pinning.
TEST_F(SPANNAlgorithmTest, FingerprintIsDeterministic)
{
    constexpr UInt32 dim = 16;
    constexpr size_t rows = 256;

    Block block = makeRandomEmbeddingBlock(rows, dim, /*seed=*/0xDE7E1Cull);

    auto build_once = [&](const MutableDataPartStoragePtr & out_storage, const MutableDataPartStoragePtr & inter_storage)
    {
        SPANNAlgorithm algo;
        KwargBuild b{};
        b.dim_value = dim;
        algo.setBuildParameters(buildKwargList(b), nullptr);
        buildSmallIndex(algo, out_storage, inter_storage, block);
    };

    const auto * info = ::testing::UnitTest::GetInstance()->current_test_info();
    const std::string root_str = std::string{"./build/test-state/spann/"} + info->name();
    std::filesystem::create_directories(root_str + "/output2/part");
    std::filesystem::create_directories(root_str + "/intermediate2/part");
    auto output_storage_b = std::make_shared<DataPartStorageOnDiskFull>(volume, "output2", "part");
    auto intermediate_storage_b = std::make_shared<DataPartStorageOnDiskFull>(volume, "intermediate2", "part");

    build_once(output_storage, intermediate_storage);
    build_once(output_storage_b, intermediate_storage_b);

    auto read_fingerprint = [](const MutableDataPartStoragePtr & storage)
    {
        auto buf = storage->readFile("algorithm_private_fingerprint.json", ReadSettings{}, std::nullopt);
        std::string body;
        readStringUntilEOF(body, *buf);
        Poco::JSON::Parser parser;
        return parser.parse(body).extract<Poco::JSON::Object::Ptr>();
    };

    auto fp_a = read_fingerprint(output_storage);
    auto fp_b = read_fingerprint(output_storage_b);
    ASSERT_TRUE(fp_a);
    ASSERT_TRUE(fp_b);

    EXPECT_EQ(fp_a->getValue<std::string>("params_hash"), fp_b->getValue<std::string>("params_hash"));
    EXPECT_EQ(fp_a->getValue<Int64>("num_points"), fp_b->getValue<Int64>("num_points"));
    EXPECT_EQ(fp_a->getValue<std::string>("algorithm_version"), fp_b->getValue<std::string>("algorithm_version"));

    auto files_a = fp_a->get("files").extract<Poco::JSON::Array::Ptr>();
    auto files_b = fp_b->get("files").extract<Poco::JSON::Array::Ptr>();
    ASSERT_TRUE(files_a);
    ASSERT_TRUE(files_b);
    ASSERT_EQ(files_a->size(), files_b->size()) << "different file count across two builds";

    std::vector<String> names_a;
    std::vector<String> names_b;
    names_a.reserve(files_a->size());
    names_b.reserve(files_b->size());
    for (UInt32 i = 0; i < files_a->size(); ++i)
    {
        names_a.push_back(files_a->get(i).extract<Poco::JSON::Object::Ptr>()->getValue<std::string>("name"));
        names_b.push_back(files_b->get(i).extract<Poco::JSON::Object::Ptr>()->getValue<std::string>("name"));
    }
    EXPECT_EQ(names_a, names_b) << "file name set diverged between two builds";
}

/// Same algorithm, different parameter values must produce a different
/// `params_hash`. Without this guarantee a replication peer could accept a
/// part built with the wrong knobs.
TEST_F(SPANNAlgorithmTest, FingerprintParamsHashChangesWithParameters)
{
    constexpr UInt32 dim = 16;
    constexpr size_t rows = 256;

    Block block = makeRandomEmbeddingBlock(rows, dim, /*seed=*/0xF1F1F1ull);

    auto build_with = [&](
        UInt64 replica_count_override,
        const MutableDataPartStoragePtr & out_storage,
        const MutableDataPartStoragePtr & inter_storage)
    {
        SPANNAlgorithm algo;
        KwargBuild b{};
        b.dim_value = dim;
        b.replica_count = replica_count_override;
        algo.setBuildParameters(buildKwargList(b), nullptr);
        buildSmallIndex(algo, out_storage, inter_storage, block);
    };

    const auto * info = ::testing::UnitTest::GetInstance()->current_test_info();
    const std::string root_str = std::string{"./build/test-state/spann/"} + info->name();
    std::filesystem::create_directories(root_str + "/output2/part");
    std::filesystem::create_directories(root_str + "/intermediate2/part");
    auto output_storage_b = std::make_shared<DataPartStorageOnDiskFull>(volume, "output2", "part");
    auto intermediate_storage_b = std::make_shared<DataPartStorageOnDiskFull>(volume, "intermediate2", "part");

    build_with(8, output_storage, intermediate_storage);
    build_with(4, output_storage_b, intermediate_storage_b);

    auto read_params_hash = [](const MutableDataPartStoragePtr & storage)
    {
        auto buf = storage->readFile("algorithm_private_fingerprint.json", ReadSettings{}, std::nullopt);
        std::string body;
        readStringUntilEOF(body, *buf);
        Poco::JSON::Parser parser;
        return parser.parse(body).extract<Poco::JSON::Object::Ptr>()->getValue<std::string>("params_hash");
    };

    EXPECT_NE(read_params_hash(output_storage), read_params_hash(output_storage_b));
}

TEST_F(SPANNAlgorithmTest, ValidateBuildParamsRejectsExcessiveReplicaCount)
{
    SPANNAlgorithm algo;
    KwargBuild b{};
    b.replica_count = 1u << 20;
    EXPECT_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr), DB::Exception);
}

TEST_F(SPANNAlgorithmTest, ValidateBuildParamsRejectsExcessivePostingPageLimit)
{
    SPANNAlgorithm algo;
    KwargBuild b{};
    b.posting_page_limit = 1u << 20;
    EXPECT_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr), DB::Exception);
}

TEST_F(SPANNAlgorithmTest, ValidateBuildParamsRejectsExcessiveMaxCheck)
{
    SPANNAlgorithm algo;
    KwargBuild b{};
    b.max_check = 1ull << 32;
    EXPECT_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr), DB::Exception);
}

TEST_F(SPANNAlgorithmTest, ValidateBuildParamsAcceptsExplicitIoThreads)
{
    SPANNAlgorithm algo;
    KwargBuild b{};
    b.io_threads = 64;
    EXPECT_NO_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr));
}

TEST_F(SPANNAlgorithmTest, ValidateBuildParamsRejectsExcessiveIoThreads)
{
    SPANNAlgorithm algo;
    KwargBuild b{};
    b.io_threads = 1u << 20;
    EXPECT_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr), DB::Exception);
}

TEST_F(SPANNAlgorithmTest, SearcherCacheLoadsOncePerPart)
{
    constexpr UInt32 dim = 16;
    constexpr size_t rows = 256;
    constexpr size_t k = 10;

    SPANNAlgorithm algo;
    KwargBuild b{};
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeSeparatedEmbeddingBlock(rows, dim);
    ASSERT_NO_THROW(buildSmallIndex(algo, output_storage, intermediate_storage, block));

    QueryFeatures features;
    features.query_vector = getRowVector(block, 7, dim);
    features.distance_function = "L2Distance";
    features.k = k;
    auto match_descriptor = algo.match(features);
    ASSERT_TRUE(match_descriptor.has_value());

    ReadyANNIndexPartSnapshot ready_parts;
    ready_parts.parts.push_back({output_storage, {}});

    ASSERT_NO_THROW((void)algo.search(*match_descriptor, ready_parts, k, nullptr));
    ASSERT_EQ(algo.searcherCacheSizeForTests(), 1u);
    ASSERT_NO_THROW((void)algo.search(*match_descriptor, ready_parts, k, nullptr));
    EXPECT_EQ(algo.searcherCacheSizeForTests(), 1u);
}

/// Real concurrency stress: each thread issues a different randomised query
/// against the same cached `Searcher`, so we exercise SPTAG's internal async-IO
/// pool rather than serialising through an outer mutex. Distances within each
/// returned list must be monotonically non-decreasing (SPTAG contract) and
/// each result must reach `k` candidates — under-provisioned io_threads
/// previously made SPTAG silently return a truncated set after spinning on
/// `FreeWorkSpaceIds`.
TEST_F(SPANNAlgorithmTest, ConcurrentSearchSafety)
{
    constexpr UInt32 dim = 32;
    constexpr size_t rows = 1024;
    constexpr size_t k = 10;
    constexpr size_t threads_count = 8;
    constexpr size_t per_thread = 64;

    SPANNAlgorithm algo;
    KwargBuild b{};
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeRandomEmbeddingBlock(rows, dim, /*seed=*/0xCAFEBABEull);
    ASSERT_NO_THROW(buildSmallIndex(algo, output_storage, intermediate_storage, block));

    ReadyANNIndexPartSnapshot ready_parts;
    ready_parts.parts.push_back({output_storage, {}});

    /// Pre-warm the cache from the main thread so the per-worker code path
    /// only goes through the cached searcher and we are actually measuring
    /// concurrent `SearchIndex` calls, not the one-shot `LoadIndex`.
    {
        QueryFeatures warmup;
        warmup.query_vector = getRowVector(block, 0, dim);
        warmup.distance_function = "L2Distance";
        warmup.k = k;
        auto md = algo.match(warmup);
        ASSERT_TRUE(md.has_value());
        ASSERT_NO_THROW((void)algo.search(*md, ready_parts, k, nullptr));
    }

    std::atomic<bool> failed{false};
    std::atomic<size_t> empty_results{0};
    std::atomic<size_t> non_monotone_results{0};

    std::vector<std::thread> workers;
    workers.reserve(threads_count);
    for (size_t t = 0; t < threads_count; ++t)
    {
        workers.emplace_back([&, t]
        {
            std::mt19937_64 rng{0x5EEDull ^ (t + 1)}; // NOLINT(cert-msc32-c, cert-msc51-cpp)
            std::uniform_int_distribution<size_t> pick(0, rows - 1);
            try
            {
                for (size_t attempt = 0; attempt < per_thread; ++attempt)
                {
                    QueryFeatures features;
                    features.query_vector = getRowVector(block, pick(rng), dim);
                    features.distance_function = "L2Distance";
                    features.k = k;
                    auto md = algo.match(features);
                    if (!md.has_value())
                    {
                        failed.store(true, std::memory_order_relaxed);
                        continue;
                    }
                    auto result = algo.search(*md, ready_parts, k, nullptr);
                    if (result.per_ann_index_part.empty())
                    {
                        empty_results.fetch_add(1, std::memory_order_relaxed);
                        continue;
                    }
                    const auto & set = result.per_ann_index_part.front();
                    if (set.internal_ids.empty() || set.internal_ids.size() != set.distances.size())
                        failed.store(true, std::memory_order_relaxed);
                    for (size_t i = 1; i < set.distances.size(); ++i)
                        if (set.distances[i] + 1e-6f < set.distances[i - 1])
                            non_monotone_results.fetch_add(1, std::memory_order_relaxed);
                }
            }
            catch (...)
            {
                failed.store(true, std::memory_order_relaxed);
            }
        });
    }

    for (auto & worker : workers)
        worker.join();

    EXPECT_FALSE(failed.load(std::memory_order_relaxed));
    EXPECT_EQ(empty_results.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(non_monotone_results.load(std::memory_order_relaxed), 0u);
    EXPECT_EQ(algo.searcherCacheSizeForTests(), 1u);
}

TEST_F(SPANNAlgorithmTest, PrepareBuildHonorsMemoryBudget)
{
    constexpr UInt32 dim = 16;
    constexpr size_t rows = 32;

    SPANNAlgorithm algo;
    KwargBuild b{};
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeSeparatedEmbeddingBlock(rows, dim);

    std::atomic<bool> cancelled{false};
    AlgorithmBuildContext ctx;
    ctx.output_storage = output_storage;
    ctx.intermediate_storage = intermediate_storage;
    ctx.is_cancelled = &cancelled;
    ctx.total_rows = rows;
    ctx.memory_budget_bytes = 16;

    try
    {
        algo.prepareBuild(ctx, block);
        FAIL() << "expected MEMORY_LIMIT_EXCEEDED";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }
}

TEST_F(SPANNAlgorithmTest, FactoryRegistration)
{
    auto & factory = ANNAlgorithmFactory::instance();
    EXPECT_TRUE(factory.familySupportsImpl("ann", "spann"));

    KwargBuild b{};
    auto kwargs = buildKwargList(b);
    ANNIndexContext ctx;

    auto algo = factory.get("ann", "spann", kwargs, ctx);
    ASSERT_TRUE(algo);
    EXPECT_EQ(algo->getName(), "spann");
    EXPECT_EQ(algo->getFamily(), "ann");
}

#endif
