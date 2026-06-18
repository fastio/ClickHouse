#include "config.h"

#if USE_CPPDISKANN

#include <gtest/gtest.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/Reflection/ANNIndex/ANNAlgorithmFactory.h>
#include <Storages/Reflection/ANNIndex/ANNIndexContext.h>
#include <Storages/Reflection/ANNIndex/CppDiskANNAlgorithm.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <random>
#include <string>
#include <vector>

using namespace DB;

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

ASTPtr buildKwargList(UInt64 dim = 4, const String & metric = "L2", UInt64 pq_chunks = 2, const String & build_quantization = "PQ_2")
{
    auto list = make_intrusive<ASTExpressionList>();
    list->children.push_back(makeKwarg("metric", metric));
    list->children.push_back(makeKwarg("dim", dim));
    list->children.push_back(makeKwarg("pruned_degree", static_cast<UInt64>(16)));
    list->children.push_back(makeKwarg("max_degree", static_cast<UInt64>(16)));
    list->children.push_back(makeKwarg("l_build", static_cast<UInt64>(32)));
    list->children.push_back(makeKwarg("alpha", 1.2));
    list->children.push_back(makeKwarg("num_threads", static_cast<UInt64>(2)));
    list->children.push_back(makeKwarg("pq_chunks", pq_chunks));
    list->children.push_back(makeKwarg("build_quantization", build_quantization));
    list->children.push_back(makeKwarg("build_ram_limit_gb", 1.0));
    return list;
}

ASTPtr minimalBuildKwargList(UInt64 dim = 4, const String & metric = "L2")
{
    auto list = make_intrusive<ASTExpressionList>();
    list->children.push_back(makeKwarg("metric", metric));
    list->children.push_back(makeKwarg("dim", dim));
    return list;
}

Block makeEmbeddingBlock(size_t rows, UInt32 dim)
{
    auto inner = ColumnVector<Float32>::create();
    auto & inner_data = inner->getData();
    inner_data.reserve(rows * dim);

    auto offsets = ColumnArray::ColumnOffsets::create();
    auto & offsets_data = offsets->getData();
    offsets_data.reserve(rows);

    UInt64 offset = 0;
    for (size_t row = 0; row < rows; ++row)
    {
        for (UInt32 d = 0; d < dim; ++d)
            inner_data.push_back(static_cast<Float32>((row + 1) * (d + 1)));
        offset += dim;
        offsets_data.push_back(offset);
    }

    Block block;
    block.insert({ColumnArray::create(std::move(inner), std::move(offsets)), std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>()), "embedding"});
    return block;
}

StorageInMemoryMetadata makeMetadata()
{
    StorageInMemoryMetadata metadata;
    ColumnsDescription columns;
    columns.add(ColumnDescription("embedding", std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>())));
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
    ctx.ann_index_part_uuid = UUIDHelpers::generateV4();
    return ctx;
}

class CppDiskANNAlgorithmTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        const auto * info = ::testing::UnitTest::GetInstance()->current_test_info();
        root_str = std::string{"./build/test-state/cppdiskann/"} + info->name();
        std::filesystem::remove_all(root_str);
        std::filesystem::create_directories(root_str + "/output/part");
        std::filesystem::create_directories(root_str + "/intermediate/part");

        const std::string root = std::filesystem::absolute(root_str).string();
        disk = std::make_shared<DiskLocal>("cppdiskann_test_disk", root);
        volume = std::make_shared<SingleDiskVolume>("cppdiskann_test_vol", disk, 0);
        output_storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "output", "part");
        intermediate_storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "intermediate", "part");
    }

    void TearDown() override
    {
        output_storage.reset();
        intermediate_storage.reset();
        volume.reset();
        disk.reset();
        std::filesystem::remove_all(root_str);
    }

    std::string root_str;
    std::shared_ptr<DiskLocal> disk;
    VolumePtr volume;
    MutableDataPartStoragePtr output_storage;
    MutableDataPartStoragePtr intermediate_storage;
};

}

TEST_F(CppDiskANNAlgorithmTest, ValidateBuildParamsAccepts)
{
    CppDiskANNAlgorithm algo;
    EXPECT_NO_THROW(algo.validateBuildParameters(buildKwargList(), nullptr));
    const auto fields = algo.getAlgorithmObservabilityFields();
    EXPECT_EQ(fields.at("metric"), "L2");
    EXPECT_EQ(fields.at("dimension"), "4");
    EXPECT_EQ(fields.at("pq_chunks"), "2");
    EXPECT_EQ(fields.at("build_quantization"), "PQ_2");
}

TEST_F(CppDiskANNAlgorithmTest, ValidateBuildParamsUsesMilvusStyleDefaults)
{
    CppDiskANNAlgorithm algo;
    EXPECT_NO_THROW(algo.validateBuildParameters(minimalBuildKwargList(1536, "cosine"), nullptr));
    const auto fields = algo.getAlgorithmObservabilityFields();
    EXPECT_EQ(fields.at("metric"), "cosine");
    EXPECT_EQ(fields.at("dimension"), "1536");
    EXPECT_EQ(fields.at("pruned_degree"), "56");
    EXPECT_EQ(fields.at("max_degree"), "56");
    EXPECT_EQ(fields.at("l_build"), "100");
    EXPECT_EQ(fields.at("pq_chunks"), "0");
    EXPECT_EQ(fields.at("build_quantization"), "FP");
    EXPECT_EQ(fields.at("pq_code_budget_gb"), "0");
    EXPECT_EQ(fields.at("pq_code_budget_gb_ratio"), "0.125");
    EXPECT_EQ(fields.at("search_cache_budget_gb"), "0");
    EXPECT_EQ(fields.at("search_cache_budget_gb_ratio"), "0.1");
    EXPECT_EQ(fields.at("disk_pq_dims"), "0");
}

TEST_F(CppDiskANNAlgorithmTest, BuildQuantizationFPDisablesPQ)
{
    CppDiskANNAlgorithm algo;
    EXPECT_NO_THROW(algo.validateBuildParameters(buildKwargList(4, "L2", 0, "FP"), nullptr));
    const auto fields = algo.getAlgorithmObservabilityFields();
    EXPECT_EQ(fields.at("pq_chunks"), "0");
    EXPECT_EQ(fields.at("build_quantization"), "FP");
}

TEST_F(CppDiskANNAlgorithmTest, BuildQuantizationRejectsConflictingPQChunks)
{
    CppDiskANNAlgorithm algo;
    EXPECT_THROW(algo.validateBuildParameters(buildKwargList(4, "L2", 2, "PQ_3"), nullptr), Exception);
    EXPECT_THROW(algo.validateBuildParameters(buildKwargList(4, "L2", 2, "FP"), nullptr), Exception);
}

TEST_F(CppDiskANNAlgorithmTest, ValidateBuildParamsAcceptsMilvusParityKnobs)
{
    auto params = buildKwargList(4, "L2", 0, "FP");
    params->children.push_back(makeKwarg("pq_code_budget_gb_ratio", 0.125));
    params->children.push_back(makeKwarg("search_cache_budget_gb_ratio", 0.1));
    params->children.push_back(makeKwarg("disk_pq_dims", static_cast<UInt64>(0)));
    params->children.push_back(makeKwarg("accelerate_build", false));
    params->children.push_back(makeKwarg("num_nodes_to_cache", static_cast<UInt64>(0)));

    CppDiskANNAlgorithm algo;
    EXPECT_NO_THROW(algo.validateBuildParameters(params, nullptr));
    const auto fields = algo.getAlgorithmObservabilityFields();
    EXPECT_EQ(fields.at("build_quantization"), "FP");
    EXPECT_EQ(fields.at("disk_pq_dims"), "0");
    EXPECT_EQ(fields.at("pq_code_budget_gb_ratio"), "0.125");
    EXPECT_EQ(fields.at("search_cache_budget_gb_ratio"), "0.1");
}

TEST_F(CppDiskANNAlgorithmTest, ValidateBuildParamsRejectsConflictingDiskPQDims)
{
    auto params = buildKwargList(4, "L2", 0, "PQ_2");
    params->children.push_back(makeKwarg("disk_pq_dims", static_cast<UInt64>(0)));

    CppDiskANNAlgorithm algo;
    EXPECT_THROW(algo.validateBuildParameters(params, nullptr), Exception);
}

TEST_F(CppDiskANNAlgorithmTest, ValidateIndexedExprAcceptsFloatArray)
{
    CppDiskANNAlgorithm algo;
    EXPECT_NO_THROW(algo.validateIndexedExpression(make_intrusive<ASTIdentifier>("embedding"), makeMetadata()));
}

TEST_F(CppDiskANNAlgorithmTest, MatchChecksQueryMetric)
{
    CppDiskANNAlgorithm algo;
    algo.validateBuildParameters(buildKwargList(4, "L2"), nullptr);

    QueryFeatures features;
    features.query_vector = {1.0f, 2.0f, 3.0f, 4.0f};
    features.distance_function = "L2Distance";
    features.k = 3;

    auto match = algo.match(features);
    ASSERT_TRUE(match.has_value());
    EXPECT_EQ(match->distance.exact_function_name, "__reflectionANNIndexCppDiskANNDistance");
    EXPECT_EQ(match->distance.metric_name, "L2");
    EXPECT_EQ(match->distance.dim, 4);

    features.distance_function = "cosineDistance";
    EXPECT_FALSE(algo.match(features).has_value());
}

TEST_F(CppDiskANNAlgorithmTest, ComputeDistancesUsesCppDiskANNMetricSemantics)
{
    const std::vector<float> query{1.0f, 0.0f};
    const std::vector<float> candidates{1.0f, 0.0f, 0.0f, 1.0f};
    std::vector<float> out(2);

    CppDiskANNFacade::computeDistances(CppDiskANNFacade::Metric::L2, 2, query.data(), candidates.data(), 2, out.data());
    EXPECT_FLOAT_EQ(out[0], 0.0f);
    EXPECT_FLOAT_EQ(out[1], 2.0f);

    CppDiskANNFacade::computeDistances(CppDiskANNFacade::Metric::Cosine, 2, query.data(), candidates.data(), 2, out.data());
    EXPECT_NEAR(out[0], 0.0f, 1e-6);
    EXPECT_NEAR(out[1], 1.0f, 1e-6);
}

TEST_F(CppDiskANNAlgorithmTest, FactoryRegistration)
{
    auto & factory = ANNAlgorithmFactory::instance();
    EXPECT_TRUE(factory.familySupportsImpl("ann", "cppdiskann"));

    ANNIndexContext ctx;
    auto algo = factory.get("ann", "cppdiskann", buildKwargList(), ctx);
    ASSERT_NE(algo, nullptr);
    EXPECT_EQ(algo->getName(), "cppdiskann");
}

TEST_F(CppDiskANNAlgorithmTest, BuildThenSearchSmoke)
{
    CppDiskANNAlgorithm algo;
    algo.validateBuildParameters(buildKwargList(), nullptr);
    algo.initialize(ANNIndexContext{});

    std::atomic<bool> cancelled{false};
    auto ctx = makeBuildContext(output_storage, intermediate_storage, cancelled, 64);
    algo.prepareBuild(ctx, makeEmbeddingBlock(64, 4));
    algo.buildAlgorithmPrivate(ctx);
    algo.finishBuild(ctx);

    bool has_index_file = false;
    for (auto it = output_storage->iterate(); it->isValid(); it->next())
        if (it->isFile() && it->name().starts_with("algorithm_private_cppdiskann"))
            has_index_file = true;
    EXPECT_TRUE(has_index_file);

    QueryFeatures features;
    features.query_vector = {1.0f, 2.0f, 3.0f, 4.0f};
    features.distance_function = "L2Distance";
    features.k = 5;
    auto match = algo.match(features);
    ASSERT_TRUE(match.has_value());

    ReadyANNIndexPartSnapshot ready;
    ReadyANNIndexPart part;
    part.ann_index_part_uuid = ctx.ann_index_part_uuid;
    part.storage = output_storage;
    ready.parts.push_back(std::move(part));

    const auto result = algo.search(*match, ready, 5, nullptr);
    ASSERT_EQ(result.per_ann_index_part.size(), 1);
    EXPECT_FALSE(result.per_ann_index_part.front().internal_ids.empty());
}

TEST_F(CppDiskANNAlgorithmTest, BuildCloneWarmsSharedSearcherCache)
{
    CppDiskANNAlgorithm storage_algo;
    storage_algo.validateBuildParameters(buildKwargList(), nullptr);
    storage_algo.initialize(ANNIndexContext{});
    ASSERT_EQ(storage_algo.searcherCacheSizeForTests(), 0u);

    auto build_algo = storage_algo.cloneForBuild();
    std::atomic<bool> cancelled{false};
    auto ctx = makeBuildContext(output_storage, intermediate_storage, cancelled, 64);

    build_algo->prepareBuild(ctx, makeEmbeddingBlock(64, 4));
    build_algo->buildAlgorithmPrivate(ctx);
    build_algo->finishBuild(ctx);

    EXPECT_EQ(storage_algo.searcherCacheSizeForTests(), 1u);
}

TEST_F(CppDiskANNAlgorithmTest, SearcherCacheUsesStablePartUuidAcrossPathChanges)
{
    CppDiskANNAlgorithm storage_algo;
    storage_algo.validateBuildParameters(buildKwargList(), nullptr);
    storage_algo.initialize(ANNIndexContext{});
    ASSERT_EQ(storage_algo.searcherCacheSizeForTests(), 0u);

    auto build_algo = storage_algo.cloneForBuild();
    std::atomic<bool> cancelled{false};
    auto ctx = makeBuildContext(output_storage, intermediate_storage, cancelled, 64);

    build_algo->prepareBuild(ctx, makeEmbeddingBlock(64, 4));
    build_algo->buildAlgorithmPrivate(ctx);
    build_algo->finishBuild(ctx);
    ASSERT_EQ(storage_algo.searcherCacheSizeForTests(), 1u);

    QueryFeatures features;
    features.query_vector = {1.0f, 2.0f, 3.0f, 4.0f};
    features.distance_function = "L2Distance";
    features.k = 5;
    auto match = storage_algo.match(features);
    ASSERT_TRUE(match.has_value());

    std::filesystem::create_directories(root_str + "/final/part");
    auto final_storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "final", "part");

    ReadyANNIndexPartSnapshot ready;
    ReadyANNIndexPart part;
    part.ann_index_part_uuid = ctx.ann_index_part_uuid;
    part.storage = final_storage;
    ready.parts.push_back(std::move(part));

    const auto result = storage_algo.search(*match, ready, 5, nullptr);
    ASSERT_EQ(result.per_ann_index_part.size(), 1);
    EXPECT_FALSE(result.per_ann_index_part.front().internal_ids.empty());
    EXPECT_EQ(storage_algo.searcherCacheSizeForTests(), 1u);
}

#endif
