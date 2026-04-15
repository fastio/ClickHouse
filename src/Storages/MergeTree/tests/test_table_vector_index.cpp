#include <gtest/gtest.h>
#include <Storages/MergeTree/MergeTreeTableVectorIndex.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <cmath>
#include <thread>

using namespace DB;

class MergeTreeTableVectorIndexTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        config.index_name = "test_index";
        config.column_name = "embeddings";
        config.distance_function = "L2Distance";
        config.dimension = 16;
        config.quantization = "bf16";
        config.hnsw_m = 32;
        config.hnsw_ef_construction = 128;
        config.hnsw_ef_search = 256;
        config.index_granularity = 100000;
        
        index = std::make_shared<MergeTreeTableVectorIndex>(config);
    }
    
    TableVectorIndexConfig config;
    MergeTreeTableVectorIndexPtr index;
    
    /// Helper to create test granule metadata
    GranuleVectorMetadata createTestGranule(
        const String & part_name,
        UInt32 granule_id,
        const std::vector<Float32> & centroid,
        Float32 max_dist = 1.5f,
        UInt64 num_vecs = 1000)
    {
        GranuleVectorMetadata granule;
        granule.part_name = part_name;
        granule.granule_id = granule_id;
        granule.centroid = centroid;
        
        // Create bounds from centroid
        granule.bounds_min = centroid;
        granule.bounds_max = centroid;
        for (auto & val : granule.bounds_min)
            val -= 0.5f;
        for (auto & val : granule.bounds_max)
            val += 0.5f;
        
        granule.max_distance_to_centroid = max_dist;
        granule.num_vectors = num_vecs;
        granule.hnsw_node_id = granule_id;
        
        return granule;
    }
    
    /// Helper to create test part metadata
    PartVectorIndexMetadata createTestPart(
        const String & part_name,
        UInt32 num_granules)
    {
        PartVectorIndexMetadata part;
        part.part_name = part_name;
        part.index_name = config.index_name;
        part.column_name = config.column_name;
        part.distance_function = config.distance_function;
        part.dimension = config.dimension;
        part.index_version = 1;
        part.last_built_time = time(nullptr);
        
        for (UInt32 i = 0; i < num_granules; ++i)
        {
            std::vector<Float32> centroid(config.dimension);
            for (auto & val : centroid)
                val = static_cast<Float32>(i) / 10.0f;
            
            part.granules.push_back(createTestGranule(part_name, i, centroid));
        }
        
        return part;
    }
};

/// Test 1: Basic metadata management
TEST_F(MergeTreeTableVectorIndexTest, AddPartMetadata)
{
    PartVectorIndexMetadata part = createTestPart("part_0_10_2", 5);
    
    index->addPartMetadata(part);
    
    EXPECT_EQ(index->getTotalParts(), 1);
    EXPECT_EQ(index->getTotalGranules(), 5);
    EXPECT_GT(index->getTotalVectors(), 0);
}

/// Test 2: Multiple parts
TEST_F(MergeTreeTableVectorIndexTest, AddMultipleParts)
{
    PartVectorIndexMetadata part1 = createTestPart("part_0_10_2", 3);
    PartVectorIndexMetadata part2 = createTestPart("part_10_20_2", 4);
    
    index->addPartMetadata(part1);
    index->addPartMetadata(part2);
    
    EXPECT_EQ(index->getTotalParts(), 2);
    EXPECT_EQ(index->getTotalGranules(), 7);
}

/// Test 3: Remove part metadata
TEST_F(MergeTreeTableVectorIndexTest, RemovePartMetadata)
{
    PartVectorIndexMetadata part1 = createTestPart("part_0_10_2", 3);
    PartVectorIndexMetadata part2 = createTestPart("part_10_20_2", 4);
    
    index->addPartMetadata(part1);
    index->addPartMetadata(part2);
    index->removePartMetadata("part_0_10_2");
    
    EXPECT_EQ(index->getTotalParts(), 1);
    EXPECT_EQ(index->getTotalGranules(), 4);
}

/// Test 4: Update part metadata
TEST_F(MergeTreeTableVectorIndexTest, UpdatePartMetadata)
{
    PartVectorIndexMetadata part1 = createTestPart("part_0_10_2", 3);
    PartVectorIndexMetadata part1_updated = createTestPart("part_0_10_2", 5);
    
    index->addPartMetadata(part1);
    EXPECT_EQ(index->getTotalGranules(), 3);
    
    index->updatePartMetadata("part_0_10_2", part1_updated);
    EXPECT_EQ(index->getTotalGranules(), 5);
}

/// Test 5: L2 distance-based candidate selection
TEST_F(MergeTreeTableVectorIndexTest, SelectCandidatesL2Distance)
{
    // Create 3 granules with different centroids
    PartVectorIndexMetadata part = createTestPart("part_0_100_2", 3);
    
    // Set specific centroids for testing
    for (UInt32 i = 0; i < part.granules.size(); ++i)
    {
        part.granules[i].centroid.assign(config.dimension, static_cast<Float32>(i));
    }
    
    index->addPartMetadata(part);
    
    // Query vector close to granule 0
    std::vector<Float64> query(config.dimension, 0.1);
    
    auto candidates = index->selectCandidateGranules(
        query, "L2Distance", 10, 0.0f);
    
    // Should return candidates (order depends on distance)
    EXPECT_GT(candidates.size(), 0);
    EXPECT_LE(candidates.size(), 3);
}

/// Test 6: Cosine distance-based candidate selection
TEST_F(MergeTreeTableVectorIndexTest, SelectCandidatsCosineDistance)
{
    config.distance_function = "cosineDistance";
    auto index_cos = std::make_shared<MergeTreeTableVectorIndex>(config);
    
    PartVectorIndexMetadata part = createTestPart("part_0_100_2", 3);
    for (UInt32 i = 0; i < part.granules.size(); ++i)
    {
        part.granules[i].centroid.assign(config.dimension, static_cast<Float32>(i + 1));
    }
    
    index_cos->addPartMetadata(part);
    
    std::vector<Float64> query(config.dimension, 1.0);
    auto candidates = index_cos->selectCandidateGranules(
        query, "cosineDistance", 10, 0.0f);
    
    EXPECT_GT(candidates.size(), 0);
}

/// Test 7: Score threshold filtering
TEST_F(MergeTreeTableVectorIndexTest, ScoreThresholdFiltering)
{
    PartVectorIndexMetadata part = createTestPart("part_0_100_2", 5);
    index->addPartMetadata(part);
    
    std::vector<Float64> query(config.dimension, 100.0f);  // Very different from centroids
    
    // With high threshold, should get fewer results
    auto candidates_high = index->selectCandidateGranules(
        query, "L2Distance", 10, 0.9f);
    
    // With low threshold, should get more results
    auto candidates_low = index->selectCandidateGranules(
        query, "L2Distance", 10, 0.1f);
    
    EXPECT_LE(candidates_high.size(), candidates_low.size());
}

/// Test 8: Max candidates limit
TEST_F(MergeTreeTableVectorIndexTest, MaxCandidatesLimit)
{
    PartVectorIndexMetadata part = createTestPart("part_0_100_2", 20);
    index->addPartMetadata(part);
    
    std::vector<Float64> query(config.dimension, 1.0f);
    
    auto candidates_all = index->selectCandidateGranules(
        query, "L2Distance", 100, 0.0f);
    
    auto candidates_limited = index->selectCandidateGranules(
        query, "L2Distance", 5, 0.0f);
    
    EXPECT_GE(candidates_all.size(), candidates_limited.size());
    EXPECT_LE(candidates_limited.size(), 5);
}

/// Test 9: Statistics and metadata retrieval
TEST_F(MergeTreeTableVectorIndexTest, GetMetadata)
{
    PartVectorIndexMetadata part = createTestPart("part_0_100_2", 3);
    index->addPartMetadata(part);
    
    auto metadata = index->getGranuleMetadataForPart("part_0_100_2");
    EXPECT_EQ(metadata.size(), 3);
    
    for (const auto & granule : metadata)
    {
        EXPECT_EQ(granule.part_name, "part_0_100_2");
        EXPECT_EQ(granule.centroid.size(), config.dimension);
    }
}

/// Test 10: Serialization and deserialization
TEST_F(MergeTreeTableVectorIndexTest, Serialization)
{
    PartVectorIndexMetadata part1 = createTestPart("part_0_100_2", 2);
    PartVectorIndexMetadata part2 = createTestPart("part_100_200_2", 3);
    
    index->addPartMetadata(part1);
    index->addPartMetadata(part2);
    
    // Serialize
    WriteBufferFromString write_buf;
    index->serialize(write_buf);
    
    String data = write_buf.str();
    EXPECT_GT(data.size(), 0);
    
    // Deserialize into new index
    TableVectorIndexConfig config2 = config;
    auto index2 = std::make_shared<MergeTreeTableVectorIndex>(config2);
    
    ReadBufferFromMemory read_buf(data.data(), data.size());
    index2->deserialize(read_buf);
    
    // Verify deserialized data
    EXPECT_EQ(index2->getTotalParts(), 2);
    EXPECT_EQ(index2->getTotalGranules(), 5);
}

/// Test 11: Configuration validation
TEST_F(MergeTreeTableVectorIndexTest, ConfigurationValidation)
{
    EXPECT_TRUE(index->validateConfiguration(
        config.column_name, config.distance_function, config.dimension));
    
    EXPECT_FALSE(index->validateConfiguration(
        "wrong_column", config.distance_function, config.dimension));
    
    EXPECT_FALSE(index->validateConfiguration(
        config.column_name, "wrongDistance", config.dimension));
    
    EXPECT_FALSE(index->validateConfiguration(
        config.column_name, config.distance_function, 32));
}

/// Test 12: Thread safety - concurrent reads
TEST_F(MergeTreeTableVectorIndexTest, ConcurrentReads)
{
    PartVectorIndexMetadata part = createTestPart("part_0_100_2", 10);
    index->addPartMetadata(part);
    
    std::vector<std::thread> threads;
    std::vector<Float64> query(config.dimension, 1.0f);
    
    // Launch multiple reader threads
    for (int i = 0; i < 5; ++i)
    {
        threads.emplace_back([this, &query]() {
            auto candidates = index->selectCandidateGranules(
                query, "L2Distance", 10, 0.0f);
            EXPECT_GT(candidates.size(), 0);
        });
    }
    
    // Wait for all threads
    for (auto & t : threads)
        t.join();
}

/// Test 13: Thread safety - read during write
TEST_F(MergeTreeTableVectorIndexTest, ConcurrentReadWrite)
{
    PartVectorIndexMetadata part1 = createTestPart("part_0_100_2", 5);
    index->addPartMetadata(part1);
    
    std::vector<std::thread> threads;
    std::vector<Float64> query(config.dimension, 1.0f);
    
    // Launch reader threads
    for (int i = 0; i < 3; ++i)
    {
        threads.emplace_back([this, &query]() {
            auto candidates = index->selectCandidateGranules(
                query, "L2Distance", 10, 0.0f);
            EXPECT_GE(candidates.size(), 0);
        });
    }
    
    // Writer thread adds more parts
    threads.emplace_back([this]() {
        PartVectorIndexMetadata part2 = createTestPart("part_100_200_2", 3);
        index->addPartMetadata(part2);
    });
    
    // Wait for all threads
    for (auto & t : threads)
        t.join();
    
    EXPECT_EQ(index->getTotalParts(), 2);
}

/// Test 14: Memory usage estimation
TEST_F(MergeTreeTableVectorIndexTest, MemoryUsage)
{
    PartVectorIndexMetadata part = createTestPart("part_0_100_2", 10);
    index->addPartMetadata(part);
    
    size_t memory = index->getMemoryUsage();
    EXPECT_GT(memory, 0);
    
    // Should be roughly: 10 granules * (16 * 12 + 50) bytes = ~2040 bytes
    // Plus some overhead for maps, strings, etc.
    EXPECT_LT(memory, 100000);  // Should be much less than 100KB
}

/// Test 15: Empty index behavior
TEST_F(MergeTreeTableVectorIndexTest, EmptyIndex)
{
    EXPECT_EQ(index->getTotalParts(), 0);
    EXPECT_EQ(index->getTotalGranules(), 0);
    EXPECT_EQ(index->getTotalVectors(), 0);
    EXPECT_EQ(index->getMemoryUsage(), 0);
    
    std::vector<Float64> query(config.dimension, 1.0f);
    auto candidates = index->selectCandidateGranules(
        query, "L2Distance", 10, 0.0f);
    EXPECT_EQ(candidates.size(), 0);
}

