#include "config.h"

#if USE_DISKANN_CPP

#include <gtest/gtest.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Core/Block.h>
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
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/Reflection/ANNIndex/ANNAlgorithmFactory.h>
#include <Storages/Reflection/ANNIndex/ANNIndexContext.h>
#include <Storages/Reflection/ANNIndex/DiskANNCppAlgorithm.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <limits>
#include <random>
#include <string>
#include <type_traits>
#include <vector>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
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
    UInt64 dim_value = 128;
    UInt64 pruned_degree_value = 64;
    UInt64 max_degree_value = 64;
    bool with_pq_chunks = true;
    UInt64 pq_chunks_value = 4;
    bool with_build_quantization = false;
    String build_quantization_value = "PQ_4";
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
    list->children.push_back(makeKwarg("max_degree", b.max_degree_value));
    list->children.push_back(makeKwarg("l_build", static_cast<UInt64>(128)));
    list->children.push_back(makeKwarg("alpha", 1.2));
    list->children.push_back(makeKwarg("num_threads", static_cast<UInt64>(4)));
    if (b.with_pq_chunks)
        list->children.push_back(makeKwarg("pq_chunks", b.pq_chunks_value));
    if (b.with_build_quantization)
        list->children.push_back(makeKwarg("build_quantization", b.build_quantization_value));
    list->children.push_back(makeKwarg("build_ram_limit_gb", 1.0));
    if (b.with_unknown)
        list->children.push_back(makeKwarg("unknown_param", static_cast<UInt64>(1)));
    return list;
}

class DiskANNCppAlgorithmTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        const auto * info = ::testing::UnitTest::GetInstance()->current_test_info();
        const std::string root_str = std::string{"./build/test-state/diskann_cpp/"} + info->name();
        std::filesystem::remove_all(root_str);
        std::filesystem::create_directories(root_str);
        std::filesystem::create_directories(root_str + "/output/part");
        std::filesystem::create_directories(root_str + "/intermediate/part");

        const std::string root = std::filesystem::absolute(root_str).string();
        disk = std::make_shared<DiskLocal>("diskann_cpp_test_disk", root);
        volume = std::make_shared<SingleDiskVolume>("diskann_cpp_test_vol", disk, 0);
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

template <typename T = Float32>
Block makeRandomEmbeddingBlock(size_t rows, UInt32 dim, uint32_t seed)
{
    std::mt19937 rng(seed);
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);

    auto inner = ColumnVector<T>::create();
    auto & inner_data = inner->getData();
    inner_data.reserve(rows * dim);

    auto offsets = ColumnArray::ColumnOffsets::create();
    auto & offsets_data = offsets->getData();
    offsets_data.reserve(rows);

    UInt64 acc = 0;
    for (size_t r = 0; r < rows; ++r)
    {
        for (UInt32 d = 0; d < dim; ++d)
            inner_data.push_back(static_cast<T>(dist(rng)));
        acc += dim;
        offsets_data.push_back(acc);
    }

    DataTypePtr nested_type;
    if constexpr (std::is_same_v<T, BFloat16>)
        nested_type = std::make_shared<DataTypeBFloat16>();
    else
        nested_type = std::make_shared<DataTypeFloat32>();

    Block block;
    block.insert({ColumnArray::create(std::move(inner), std::move(offsets)), std::make_shared<DataTypeArray>(nested_type), "embedding"});
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

AlgorithmBuildContext makeBuildContext(
    MutableDataPartStoragePtr output_storage,
    MutableDataPartStoragePtr intermediate_storage,
    const std::atomic<bool> & cancelled,
    UInt64 total_rows)
{
    AlgorithmBuildContext ctx;
    ctx.output_storage = output_storage;
    ctx.intermediate_storage = intermediate_storage;
    ctx.is_cancelled = &cancelled;
    ctx.total_rows = total_rows;
    return ctx;
}

}

TEST_F(DiskANNCppAlgorithmTest, ValidateBuildParamsAccepts)
{
    for (const String & metric : {"L2", "l2", "cosine", "Cosine", "COSINE", "InnerProduct", "innerproduct", "inner_product", "INNER_PRODUCT", "dotProduct", "CosineNormalized", "cosinenormalized", "cosine_normalized"})
    {
        DiskANNCppAlgorithm algo;
        KwargBuild b;
        b.metric_value = metric;
        EXPECT_NO_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr)) << metric;
    }
}

TEST_F(DiskANNCppAlgorithmTest, ValidateBuildParamsDerivesPQChunksFromDimension)
{
    struct Case
    {
        UInt64 dim;
        String pq_chunks;
        String build_quantization;
    };
    const std::vector<Case> cases{
        {1, "1", "PQ_1"},
        {8, "1", "PQ_8"},
        {16, "2", "PQ_16"},
        {32, "4", "PQ_16"},
        {64, "8", "PQ_16"},
        {128, "16", "PQ_16"},
    };

    for (const auto & test_case : cases)
    {
        DiskANNCppAlgorithm algo;
        KwargBuild b;
        b.dim_value = test_case.dim;
        b.with_pq_chunks = false;

        algo.validateBuildParameters(buildKwargList(b), nullptr);
        const auto fields = algo.getAlgorithmObservabilityFields();
        EXPECT_EQ(fields.at("pq_chunks"), test_case.pq_chunks) << test_case.dim;
        EXPECT_EQ(fields.at("build_quantization"), test_case.build_quantization) << test_case.dim;
    }
}

TEST_F(DiskANNCppAlgorithmTest, ValidateBuildParamsRejectsUnsupportedSQBuildQuantization)
{
    for (const String & build_quantization : {"SQ_1", "SQ_1_2.0"})
    {
        DiskANNCppAlgorithm algo;
        KwargBuild b;
        b.with_build_quantization = true;
        b.build_quantization_value = build_quantization;
        EXPECT_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr), DB::Exception) << build_quantization;
    }
}

TEST_F(DiskANNCppAlgorithmTest, ValidateBuildParamsRejectsInvalid)
{
    {
        DiskANNCppAlgorithm algo;
        KwargBuild b;
        b.with_unknown = true;
        EXPECT_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr), DB::Exception);
    }
    {
        DiskANNCppAlgorithm algo;
        KwargBuild b;
        b.with_dim = false;
        EXPECT_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr), DB::Exception);
    }
    {
        DiskANNCppAlgorithm algo;
        KwargBuild b;
        b.metric_value = "cosine_x";
        EXPECT_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr), DB::Exception);
    }
    {
        DiskANNCppAlgorithm algo;
        KwargBuild b;
        b.pruned_degree_value = static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1;
        EXPECT_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr), DB::Exception);
    }
    {
        DiskANNCppAlgorithm algo;
        KwargBuild b;
        b.dim_value = 8;
        b.pq_chunks_value = 9;
        EXPECT_THROW(algo.validateBuildParameters(buildKwargList(b), nullptr), DB::Exception);
    }
}

TEST_F(DiskANNCppAlgorithmTest, ValidateIndexedExprAcceptsFloatArrays)
{
    {
        DiskANNCppAlgorithm algo;
        auto metadata = makeMetadataWithArrayColumn(std::make_shared<DataTypeFloat32>());
        EXPECT_NO_THROW(algo.validateIndexedExpression(make_intrusive<ASTIdentifier>("embedding"), metadata));
    }
    {
        DiskANNCppAlgorithm algo;
        auto metadata = makeMetadataWithArrayColumn(std::make_shared<DataTypeBFloat16>());
        EXPECT_NO_THROW(algo.validateIndexedExpression(make_intrusive<ASTIdentifier>("embedding"), metadata));
    }
}

TEST_F(DiskANNCppAlgorithmTest, ValidateIndexedExprRejectsUnsupportedType)
{
    DiskANNCppAlgorithm algo;
    auto metadata = makeMetadataWithArrayColumn(std::make_shared<DataTypeFloat64>());
    EXPECT_THROW(algo.validateIndexedExpression(make_intrusive<ASTIdentifier>("embedding"), metadata), DB::Exception);
}

TEST_F(DiskANNCppAlgorithmTest, MatchChecksQueryMetric)
{
    constexpr UInt32 dim = 8;
    struct Case
    {
        String metric;
        String function;
        String metric_name;
        UInt64 metric_id;
        bool smaller_is_better;
    };
    const std::vector<Case> cases{
        {"L2", "L2Distance", "L2", static_cast<UInt64>(DiskANNCppFacade::Metric::L2), true},
        {"cosine", "cosineDistance", "cosine", static_cast<UInt64>(DiskANNCppFacade::Metric::Cosine), true},
        {"InnerProduct", "dotProduct", "InnerProduct", static_cast<UInt64>(DiskANNCppFacade::Metric::InnerProduct), false},
        {"CosineNormalized", "cosineDistance", "CosineNormalized", static_cast<UInt64>(DiskANNCppFacade::Metric::CosineNormalized), true},
    };

    for (const auto & test_case : cases)
    {
        DiskANNCppAlgorithm algo;
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
        EXPECT_EQ(match->distance.exact_function_name, "__reflectionANNIndexDiskANNCppDistance");
        EXPECT_EQ(match->distance.metric_name, test_case.metric_name);
        EXPECT_EQ(match->distance.metric_id, test_case.metric_id);
        EXPECT_EQ(match->distance.dim, dim);
        EXPECT_EQ(match->distance.smaller_is_better, test_case.smaller_is_better);
    }
}

TEST_F(DiskANNCppAlgorithmTest, ComputeDistancesUsesDiskANNCppMetricSemantics)
{
    {
        const std::vector<float> query = {0.0f, 0.0f};
        const std::vector<float> candidates = {3.0f, 4.0f, 1.0f, 0.0f};
        std::vector<float> out(2);
        DiskANNCppFacade::computeDistances(DiskANNCppFacade::Metric::L2, 2, query.data(), candidates.data(), 2, out.data());
        EXPECT_FLOAT_EQ(out[0], 25.0f);
        EXPECT_FLOAT_EQ(out[1], 1.0f);
    }
    {
        const std::vector<float> query = {1.0f, 0.0f};
        const std::vector<float> candidates = {0.0f, 1.0f, 1.0f, 0.0f};
        std::vector<float> out(2);
        DiskANNCppFacade::computeDistances(DiskANNCppFacade::Metric::Cosine, 2, query.data(), candidates.data(), 2, out.data());
        EXPECT_NEAR(out[0], 1.0f, 1e-6f);
        EXPECT_NEAR(out[1], 0.0f, 1e-6f);
    }
    {
        const std::vector<float> query = {2.0f, 3.0f};
        const std::vector<float> candidates = {4.0f, 5.0f, 1.0f, -1.0f};
        std::vector<float> out(2);
        DiskANNCppFacade::computeDistances(DiskANNCppFacade::Metric::InnerProduct, 2, query.data(), candidates.data(), 2, out.data());
        EXPECT_FLOAT_EQ(-out[0], 23.0f);
        EXPECT_FLOAT_EQ(-out[1], -1.0f);
    }
}

TEST_F(DiskANNCppAlgorithmTest, BuildThenSearchSmoke)
{
    constexpr UInt32 dim = 128;
    constexpr size_t rows = 1000;

    DiskANNCppAlgorithm algo;
    KwargBuild b;
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeRandomEmbeddingBlock(rows, dim, 42);
    std::atomic<bool> cancelled{false};
    auto ctx = makeBuildContext(output_storage, intermediate_storage, cancelled, rows);

    ASSERT_NO_THROW(algo.prepareBuild(ctx, block));
    ASSERT_NO_THROW(algo.buildAlgorithmPrivate(ctx));
    ASSERT_NO_THROW(algo.finishBuild(ctx));

    bool any_index_file = false;
    for (auto it = output_storage->iterate(); it->isValid(); it->next())
    {
        if (it->isFile() && it->name().starts_with("algorithm_private_diskann_cpp"))
            any_index_file = true;
    }
    EXPECT_TRUE(any_index_file);
}

TEST_F(DiskANNCppAlgorithmTest, FingerprintContents)
{
    constexpr UInt32 dim = 128;
    constexpr size_t rows = 1000;

    DiskANNCppAlgorithm algo;
    KwargBuild b;
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeRandomEmbeddingBlock(rows, dim, 42);
    std::atomic<bool> cancelled{false};
    auto ctx = makeBuildContext(output_storage, intermediate_storage, cancelled, rows);

    ASSERT_NO_THROW(algo.prepareBuild(ctx, block));
    ASSERT_NO_THROW(algo.buildAlgorithmPrivate(ctx));
    ASSERT_NO_THROW(algo.finishBuild(ctx));

    ASSERT_TRUE(output_storage->existsFile("algorithm_private_fingerprint.json"));
    auto buf = output_storage->readFile("algorithm_private_fingerprint.json", ReadSettings{}, std::nullopt);
    std::string body;
    readStringUntilEOF(body, *buf);

    Poco::JSON::Parser parser;
    auto obj = parser.parse(body).extract<Poco::JSON::Object::Ptr>();
    ASSERT_TRUE(obj);
    EXPECT_EQ(obj->getValue<std::string>("algorithm_version"), "diskann_cpp");
    EXPECT_EQ(obj->getValue<Int64>("num_points"), static_cast<Int64>(rows));

    auto files_arr = obj->get("files").extract<Poco::JSON::Array::Ptr>();
    ASSERT_TRUE(files_arr);
    ASSERT_GT(files_arr->size(), 0u);
    for (UInt32 i = 0; i < files_arr->size(); ++i)
    {
        auto entry = files_arr->get(i).extract<Poco::JSON::Object::Ptr>();
        ASSERT_TRUE(entry);
        EXPECT_TRUE(entry->has("name"));
        EXPECT_TRUE(entry->has("size"));
        EXPECT_TRUE(entry->has("sipHash128"));
        EXPECT_EQ(entry->getValue<std::string>("sipHash128").size(), 32u);
    }
}

TEST_F(DiskANNCppAlgorithmTest, FactoryRegistration)
{
    auto & factory = ANNAlgorithmFactory::instance();
    EXPECT_TRUE(factory.familySupportsImpl("ann", "diskann_cpp"));

    KwargBuild b;
    ANNIndexContext ctx;
    auto algo = factory.get("ann", "diskann_cpp", buildKwargList(b), ctx);
    ASSERT_TRUE(algo);
    EXPECT_EQ(algo->getName(), "diskann_cpp");
    EXPECT_EQ(algo->getFamily(), "ann");
}

TEST_F(DiskANNCppAlgorithmTest, PrepareBuildAcceptsBFloat16Embeddings)
{
    constexpr UInt32 dim = 8;
    constexpr size_t rows = 32;

    DiskANNCppAlgorithm algo;
    KwargBuild b;
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeRandomEmbeddingBlock<BFloat16>(rows, dim, 13);
    std::atomic<bool> cancelled{false};
    auto ctx = makeBuildContext(output_storage, intermediate_storage, cancelled, rows);
    EXPECT_NO_THROW(algo.prepareBuild(ctx, block));
}

TEST_F(DiskANNCppAlgorithmTest, CancelBeforeBuildInvocationHonored)
{
    constexpr UInt32 dim = 128;
    constexpr size_t rows = 1000;

    DiskANNCppAlgorithm algo;
    KwargBuild b;
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeRandomEmbeddingBlock(rows, dim, 42);
    std::atomic<bool> cancelled{true};
    auto ctx = makeBuildContext(output_storage, intermediate_storage, cancelled, rows);

    EXPECT_THROW(algo.prepareBuild(ctx, block), DB::Exception);
    EXPECT_FALSE(output_storage->existsFile("algorithm_private_diskann_cpp_disk.index"));
}

TEST_F(DiskANNCppAlgorithmTest, MatchAndSearchEndToEndUsesSearcherCache)
{
    constexpr UInt32 dim = 64;
    constexpr size_t rows = 256;
    constexpr size_t k = 10;

    DiskANNCppAlgorithm algo;
    KwargBuild b;
    b.dim_value = dim;
    algo.setBuildParameters(buildKwargList(b), nullptr);

    Block block = makeRandomEmbeddingBlock(rows, dim, 7);
    std::atomic<bool> cancelled{false};
    auto ctx = makeBuildContext(output_storage, intermediate_storage, cancelled, rows);

    ASSERT_NO_THROW(algo.prepareBuild(ctx, block));
    ASSERT_NO_THROW(algo.buildAlgorithmPrivate(ctx));
    ASSERT_NO_THROW(algo.finishBuild(ctx));

    const auto & array_col = typeid_cast<const ColumnArray &>(*block.getByName("embedding").column);
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

    ReadyANNIndexPartSnapshot ready_parts;
    ready_parts.parts.push_back({output_storage, {}});

    InternalSearchResult result = algo.search(*match_descriptor, ready_parts, k, nullptr);
    ASSERT_EQ(result.per_ann_index_part.size(), 1u);
    EXPECT_EQ(algo.searcherCacheSizeForTests(), 1u);

    result = algo.search(*match_descriptor, ready_parts, k, nullptr);
    ASSERT_EQ(result.per_ann_index_part.size(), 1u);
    EXPECT_EQ(algo.searcherCacheSizeForTests(), 1u);

    const auto & set = result.per_ann_index_part.front();
    ASSERT_FALSE(set.internal_ids.empty());
    EXPECT_EQ(set.internal_ids.size(), set.distances.size());
    EXPECT_LE(set.internal_ids.size(), k);
    EXPECT_NE(std::find(set.internal_ids.begin(), set.internal_ids.end(), target_row), set.internal_ids.end());
}

#endif
