#include "config.h"
#if USE_DISKANN

#include <gtest/gtest.h>
#include <Storages/MergeTree/DiskANNIndex.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <algorithm>
#include <cmath>
#include <numeric>
#include <random>
#include <set>
#include <vector>

using namespace DB;

namespace
{

/// Generate random float vectors of given dimensions and count using a fixed seed.
std::vector<float> generateRandomVectors(size_t count, size_t dim, unsigned seed = 42)
{
    std::mt19937 rng(seed);
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
    std::vector<float> data(count * dim);
    for (auto & v : data)
        v = dist(rng);
    return data;
}

/// Compute squared L2 distance between two vectors.
float l2DistanceSquared(const float * a, const float * b, size_t dim)
{
    float sum = 0.0f;
    for (size_t i = 0; i < dim; ++i)
    {
        float diff = a[i] - b[i];
        sum += diff * diff;
    }
    return sum;
}

/// Brute-force top-k search by L2 distance. Returns sorted vector of (distance, id).
std::vector<std::pair<float, uint64_t>> bruteForceSearchL2(
    const float * vectors, size_t count, size_t dim,
    const float * query, size_t k)
{
    std::vector<std::pair<float, uint64_t>> all;
    all.reserve(count);
    for (size_t i = 0; i < count; ++i)
        all.emplace_back(l2DistanceSquared(query, vectors + i * dim, dim), static_cast<uint64_t>(i));

    std::sort(all.begin(), all.end());
    if (all.size() > k)
        all.resize(k);
    return all;
}

/// Compute cosine distance = 1 - cosine_similarity.
float cosineDistance(const float * a, const float * b, size_t dim)
{
    float dot = 0.0f, norm_a = 0.0f, norm_b = 0.0f;
    for (size_t i = 0; i < dim; ++i)
    {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    float denom = std::sqrt(norm_a) * std::sqrt(norm_b);
    if (denom < 1e-12f)
        return 1.0f;
    return 1.0f - dot / denom;
}

/// Brute-force top-k search by cosine distance.
std::vector<std::pair<float, uint64_t>> bruteForceSearchCosine(
    const float * vectors, size_t count, size_t dim,
    const float * query, size_t k)
{
    std::vector<std::pair<float, uint64_t>> all;
    all.reserve(count);
    for (size_t i = 0; i < count; ++i)
        all.emplace_back(cosineDistance(query, vectors + i * dim, dim), static_cast<uint64_t>(i));

    std::sort(all.begin(), all.end());
    if (all.size() > k)
        all.resize(k);
    return all;
}

/// Compute recall: fraction of ground truth IDs found in the result set.
double computeRecall(const uint64_t * result_ids, size_t result_count,
                     const std::vector<std::pair<float, uint64_t>> & ground_truth)
{
    std::set<uint64_t> gt_set;
    for (const auto & [dist, id] : ground_truth)
        gt_set.insert(id);

    size_t hits = 0;
    for (size_t i = 0; i < result_count; ++i)
    {
        if (gt_set.count(result_ids[i]))
            ++hits;
    }
    return static_cast<double>(hits) / static_cast<double>(gt_set.size());
}

}


TEST(DiskANNIndex, BuildAndSearch)
{
    constexpr size_t num_vectors = 1000;
    constexpr size_t dim = 128;
    constexpr size_t k = 10;

    auto data = generateRandomVectors(num_vectors, dim);

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::L2);
    index.build(data.data(), num_vectors);

    ASSERT_EQ(index.size(), num_vectors);
    ASSERT_FALSE(index.empty());

    /// Use the first vector as query
    const float * query = data.data();
    std::vector<uint64_t> ids(k);
    std::vector<float> distances(k);
    size_t found = index.search(query, k, ids.data(), distances.data());

    ASSERT_EQ(found, k);

    auto ground_truth = bruteForceSearchL2(data.data(), num_vectors, dim, query, k);
    double recall = computeRecall(ids.data(), found, ground_truth);
    EXPECT_GT(recall, 0.9) << "Recall is too low: " << recall;
}


TEST(DiskANNIndex, SerializationRoundTrip)
{
    constexpr size_t num_vectors = 500;
    constexpr size_t dim = 64;
    constexpr size_t k = 10;

    auto data = generateRandomVectors(num_vectors, dim);

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::L2);
    index.build(data.data(), num_vectors);

    /// Serialize
    std::string serialized;
    {
        WriteBufferFromString wbuf(serialized);
        index.serialize(wbuf);
        wbuf.finalize();
    }

    ASSERT_FALSE(serialized.empty());

    /// Deserialize
    ReadBufferFromString rbuf(serialized);
    auto restored = DiskANNIndexWithSerialization::deserialize(rbuf, dim, DiskANNMetric::L2);

    ASSERT_EQ(restored.size(), num_vectors);

    /// Search both and compare results
    const float * query = data.data();
    std::vector<uint64_t> ids_orig(k), ids_restored(k);
    std::vector<float> dists_orig(k), dists_restored(k);

    size_t found_orig = index.search(query, k, ids_orig.data(), dists_orig.data());
    size_t found_restored = restored.search(query, k, ids_restored.data(), dists_restored.data());

    ASSERT_EQ(found_orig, found_restored);

    /// Results should be identical after round-trip
    for (size_t i = 0; i < found_orig; ++i)
    {
        EXPECT_EQ(ids_orig[i], ids_restored[i]) << "Mismatch at position " << i;
        EXPECT_FLOAT_EQ(dists_orig[i], dists_restored[i]) << "Distance mismatch at position " << i;
    }
}


TEST(DiskANNIndex, EmptyIndex)
{
    constexpr size_t dim = 32;

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::L2);

    ASSERT_TRUE(index.empty());
    ASSERT_EQ(index.size(), 0u);

    /// Search on empty index should return 0
    std::vector<float> query(dim, 1.0f);
    std::vector<uint64_t> ids(10);
    std::vector<float> distances(10);
    size_t found = index.search(query.data(), 10, ids.data(), distances.data());
    EXPECT_EQ(found, 0u);
}


TEST(DiskANNIndex, DimensionMismatch)
{
    constexpr size_t dim = 128;
    constexpr size_t num_vectors = 100;

    auto data = generateRandomVectors(num_vectors, dim);

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::L2);
    index.build(data.data(), num_vectors);

    /// Search with wrong dimension (64 instead of 128)
    constexpr size_t wrong_dim = 64;
    std::vector<float> wrong_query(wrong_dim, 1.0f);
    std::vector<uint64_t> ids(10);
    std::vector<float> distances(10);

    EXPECT_THROW(index.search(wrong_query.data(), 10, ids.data(), distances.data()), Exception);
}


TEST(DiskANNIndex, L2Distance)
{
    constexpr size_t dim = 16;
    constexpr size_t num_vectors = 200;
    constexpr size_t k = 5;

    auto data = generateRandomVectors(num_vectors, dim, 123);

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::L2);
    index.build(data.data(), num_vectors);

    /// Use a known query point
    std::vector<float> query(dim, 0.0f);
    std::vector<uint64_t> ids(k);
    std::vector<float> distances(k);
    size_t found = index.search(query.data(), k, ids.data(), distances.data());

    ASSERT_EQ(found, k);

    /// Verify the results are valid: all returned IDs should be in range
    for (size_t i = 0; i < found; ++i)
        EXPECT_LT(ids[i], num_vectors);

    /// Verify distances are non-negative and sorted (ascending)
    for (size_t i = 0; i < found; ++i)
        EXPECT_GE(distances[i], 0.0f);

    for (size_t i = 1; i < found; ++i)
        EXPECT_LE(distances[i - 1], distances[i] + 1e-6f);

    /// Check recall against brute-force
    auto ground_truth = bruteForceSearchL2(data.data(), num_vectors, dim, query.data(), k);
    double recall = computeRecall(ids.data(), found, ground_truth);
    EXPECT_GT(recall, 0.8) << "L2 recall too low: " << recall;
}


TEST(DiskANNIndex, CosineDistance)
{
    constexpr size_t dim = 16;
    constexpr size_t num_vectors = 200;
    constexpr size_t k = 5;

    auto data = generateRandomVectors(num_vectors, dim, 456);

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::Cosine);
    index.build(data.data(), num_vectors);

    std::vector<float> query(dim);
    std::mt19937 rng(789);
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
    for (auto & v : query)
        v = dist(rng);

    std::vector<uint64_t> ids(k);
    std::vector<float> distances(k);
    size_t found = index.search(query.data(), k, ids.data(), distances.data());

    ASSERT_EQ(found, k);

    for (size_t i = 0; i < found; ++i)
        EXPECT_LT(ids[i], num_vectors);

    /// Check recall against brute-force cosine search
    auto ground_truth = bruteForceSearchCosine(data.data(), num_vectors, dim, query.data(), k);
    double recall = computeRecall(ids.data(), found, ground_truth);
    EXPECT_GT(recall, 0.8) << "Cosine recall too low: " << recall;
}


TEST(DiskANNIndex, MoveSemantics)
{
    constexpr size_t dim = 32;
    constexpr size_t num_vectors = 50;

    auto data = generateRandomVectors(num_vectors, dim);

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::L2);
    index.build(data.data(), num_vectors);

    ASSERT_EQ(index.size(), num_vectors);

    /// Move construct
    DiskANNIndexWithSerialization moved(std::move(index));
    ASSERT_EQ(moved.size(), num_vectors);
    ASSERT_EQ(moved.getDimensions(), dim);
    ASSERT_EQ(moved.getMetric(), DiskANNMetric::L2);

    /// Original should be invalidated (handle == -1, size returns 0)
    ASSERT_EQ(index.size(), 0u); // NOLINT(bugprone-use-after-move)
    ASSERT_TRUE(index.empty()); // NOLINT(bugprone-use-after-move)

    /// Move assign
    DiskANNIndexWithSerialization another(dim, DiskANNMetric::Cosine);
    another = std::move(moved);
    ASSERT_EQ(another.size(), num_vectors);
    ASSERT_EQ(moved.size(), 0u); // NOLINT(bugprone-use-after-move)
}

#endif
