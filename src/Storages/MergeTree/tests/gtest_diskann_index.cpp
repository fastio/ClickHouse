#include "config.h"
#if USE_DISKANN

#include <gtest/gtest.h>
#include <Storages/MergeTree/DiskANNIndex.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <algorithm>
#include <cmath>
#include <random>
#include <set>
#include <thread>
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
    index.build(data.data(), num_vectors, dim);

    ASSERT_EQ(index.size(), num_vectors);
    ASSERT_FALSE(index.empty());

    /// Use the first vector as query
    const float * query = data.data();
    std::vector<uint64_t> ids(k);
    std::vector<float> distances(k);
    size_t found = index.search(query, dim, k, ids.data(), distances.data());

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
    index.build(data.data(), num_vectors, dim);

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

    size_t found_orig = index.search(query, dim, k, ids_orig.data(), dists_orig.data());
    size_t found_restored = restored.search(query, dim, k, ids_restored.data(), dists_restored.data());

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
    size_t found = index.search(query.data(), dim, 10, ids.data(), distances.data());
    EXPECT_EQ(found, 0u);
}


TEST(DiskANNIndex, DimensionMismatch)
{
    constexpr size_t dim = 128;
    constexpr size_t num_vectors = 100;

    auto data = generateRandomVectors(num_vectors, dim);

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::L2);
    index.build(data.data(), num_vectors, dim);

    /// Search with wrong dimension (64 instead of 128)
    constexpr size_t wrong_dim = 64;
    std::vector<float> wrong_query(wrong_dim, 1.0f);
    std::vector<uint64_t> ids(10);
    std::vector<float> distances(10);

    EXPECT_THROW(index.search(wrong_query.data(), wrong_dim, 10, ids.data(), distances.data()), Exception);
}


TEST(DiskANNIndex, L2Distance)
{
    constexpr size_t dim = 16;
    constexpr size_t num_vectors = 200;
    constexpr size_t k = 5;

    auto data = generateRandomVectors(num_vectors, dim, 123);

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::L2);
    index.build(data.data(), num_vectors, dim);

    /// Use a known query point
    std::vector<float> query(dim, 0.0f);
    std::vector<uint64_t> ids(k);
    std::vector<float> distances(k);
    size_t found = index.search(query.data(), dim, k, ids.data(), distances.data());

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
    index.build(data.data(), num_vectors, dim);

    std::vector<float> query(dim);
    std::mt19937 rng(789);
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
    for (auto & v : query)
        v = dist(rng);

    std::vector<uint64_t> ids(k);
    std::vector<float> distances(k);
    size_t found = index.search(query.data(), dim, k, ids.data(), distances.data());

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
    index.build(data.data(), num_vectors, dim);

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


/// k larger than the number of vectors in the index — should return all available vectors.
TEST(DiskANNIndex, KExceedsVectorCount)
{
    constexpr size_t dim = 8;
    constexpr size_t num_vectors = 5;
    constexpr size_t k = 100;

    auto data = generateRandomVectors(num_vectors, dim, 101);

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::L2);
    index.build(data.data(), num_vectors, dim);

    std::vector<float> query(dim, 0.0f);
    std::vector<uint64_t> ids(k);
    std::vector<float> distances(k);
    size_t found = index.search(query.data(), dim, k, ids.data(), distances.data());

    /// Should return exactly num_vectors, not k
    ASSERT_EQ(found, num_vectors);

    /// All returned IDs should be in range
    for (size_t i = 0; i < found; ++i)
        EXPECT_LT(ids[i], num_vectors);

    /// All IDs should be unique (every vector returned exactly once)
    std::set<uint64_t> unique_ids(ids.begin(), ids.begin() + found);
    EXPECT_EQ(unique_ids.size(), num_vectors);
}


/// All vectors are identical — search should still work and return valid results.
TEST(DiskANNIndex, DuplicateVectors)
{
    constexpr size_t dim = 4;
    constexpr size_t num_vectors = 50;
    constexpr size_t k = 10;

    /// Fill all vectors with the same value
    std::vector<float> data(num_vectors * dim, 1.0f);

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::L2);
    index.build(data.data(), num_vectors, dim);

    ASSERT_EQ(index.size(), num_vectors);

    std::vector<float> query(dim, 1.0f);
    std::vector<uint64_t> ids(k);
    std::vector<float> distances(k);
    size_t found = index.search(query.data(), dim, k, ids.data(), distances.data());

    ASSERT_EQ(found, k);

    /// All distances should be 0 since query == every vector
    for (size_t i = 0; i < found; ++i)
        EXPECT_NEAR(distances[i], 0.0f, 1e-6f);

    /// All IDs should be valid
    for (size_t i = 0; i < found; ++i)
        EXPECT_LT(ids[i], num_vectors);
}


/// Zero vectors with cosine distance — denominator is zero, should return distance = 1.0.
TEST(DiskANNIndex, ZeroVectorsCosine)
{
    constexpr size_t dim = 4;
    constexpr size_t num_vectors = 3;
    constexpr size_t k = 3;

    /// Insert: one zero vector and two non-zero vectors
    std::vector<float> data = {
        0.0f, 0.0f, 0.0f, 0.0f,   /// vector 0: zero
        1.0f, 0.0f, 0.0f, 0.0f,   /// vector 1: unit along axis 0
        0.0f, 1.0f, 0.0f, 0.0f,   /// vector 2: unit along axis 1
    };

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::Cosine);
    index.build(data.data(), num_vectors, dim);

    /// Query with a zero vector — cosine distance to everything is 1.0
    std::vector<float> zero_query(dim, 0.0f);
    std::vector<uint64_t> ids(k);
    std::vector<float> distances(k);
    size_t found = index.search(zero_query.data(), dim, k, ids.data(), distances.data());

    ASSERT_EQ(found, k);

    /// Distance from zero vector to any vector should be 1.0
    for (size_t i = 0; i < found; ++i)
        EXPECT_NEAR(distances[i], 1.0f, 1e-5f);

    /// Query with non-zero vector — distance to itself should be ~0
    std::vector<float> unit_query = {1.0f, 0.0f, 0.0f, 0.0f};
    found = index.search(unit_query.data(), dim, k, ids.data(), distances.data());

    ASSERT_EQ(found, k);

    /// The closest should be vector 1 (exact match), distance ~0
    EXPECT_EQ(ids[0], 1u);
    EXPECT_NEAR(distances[0], 0.0f, 1e-5f);
}


/// Custom DiskANNParams — verify that non-default parameters are accepted.
TEST(DiskANNIndex, CustomParams)
{
    constexpr size_t dim = 16;
    constexpr size_t num_vectors = 100;
    constexpr size_t k = 5;

    DiskANNParams params;
    params.pruned_degree = 16;
    params.max_degree = 32;
    params.l_build = 64;
    params.alpha = 1.5f;

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::L2, params);

    auto data = generateRandomVectors(num_vectors, dim, 777);
    index.build(data.data(), num_vectors, dim);

    ASSERT_EQ(index.size(), num_vectors);

    /// Search should still produce correct results
    std::vector<float> query(dim, 0.0f);
    std::vector<uint64_t> ids(k);
    std::vector<float> distances(k);
    size_t found = index.search(query.data(), dim, k, ids.data(), distances.data());

    ASSERT_EQ(found, k);

    auto ground_truth = bruteForceSearchL2(data.data(), num_vectors, dim, query.data(), k);
    double recall = computeRecall(ids.data(), found, ground_truth);
    EXPECT_GT(recall, 0.8) << "Custom params recall too low: " << recall;
}


/// Larger scale: 10K vectors to verify correctness beyond trivial sizes.
TEST(DiskANNIndex, LargerScale)
{
    constexpr size_t dim = 64;
    constexpr size_t num_vectors = 10000;
    constexpr size_t k = 20;

    auto data = generateRandomVectors(num_vectors, dim, 999);

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::L2);
    index.build(data.data(), num_vectors, dim);

    ASSERT_EQ(index.size(), num_vectors);

    /// Query with a random vector from the dataset
    const float * query = data.data() + 5000 * dim;
    std::vector<uint64_t> ids(k);
    std::vector<float> distances(k);
    size_t found = index.search(query, dim, k, ids.data(), distances.data());

    ASSERT_EQ(found, k);

    /// First result should be the query vector itself
    EXPECT_EQ(ids[0], 5000u);
    EXPECT_NEAR(distances[0], 0.0f, 1e-6f);

    /// Distances should be sorted ascending
    for (size_t i = 1; i < found; ++i)
        EXPECT_LE(distances[i - 1], distances[i] + 1e-6f);

    /// Check recall
    auto ground_truth = bruteForceSearchL2(data.data(), num_vectors, dim, query, k);
    double recall = computeRecall(ids.data(), found, ground_truth);
    EXPECT_GT(recall, 0.9) << "Large scale recall too low: " << recall;
}


/// Corrupted serialization data — deserialize should throw.
TEST(DiskANNIndex, DeserializeCorruptedData)
{
    /// Completely random garbage
    std::string garbage(256, '\x42');
    ReadBufferFromString rbuf(garbage);
    EXPECT_THROW(
        DiskANNIndexWithSerialization::deserialize(rbuf, 16, DiskANNMetric::L2),
        Exception);

    /// Empty data
    std::string empty_data;
    ReadBufferFromString rbuf_empty(empty_data);
    EXPECT_THROW(
        DiskANNIndexWithSerialization::deserialize(rbuf_empty, 16, DiskANNMetric::L2),
        Exception);

    /// Truncated valid data: serialize then corrupt by truncating
    constexpr size_t dim = 8;
    constexpr size_t num_vectors = 10;
    auto data = generateRandomVectors(num_vectors, dim, 333);

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::L2);
    index.build(data.data(), num_vectors, dim);

    std::string serialized;
    {
        WriteBufferFromString wbuf(serialized);
        index.serialize(wbuf);
        wbuf.finalize();
    }

    /// Truncate to half the original size
    std::string truncated = serialized.substr(0, serialized.size() / 2);
    ReadBufferFromString rbuf_trunc(truncated);
    EXPECT_THROW(
        DiskANNIndexWithSerialization::deserialize(rbuf_trunc, dim, DiskANNMetric::L2),
        Exception);
}


/// Concurrent reads — multiple threads searching the same index simultaneously.
TEST(DiskANNIndex, ConcurrentSearch)
{
    constexpr size_t dim = 32;
    constexpr size_t num_vectors = 500;
    constexpr size_t k = 10;
    constexpr size_t num_threads = 8;
    constexpr size_t queries_per_thread = 50;

    auto data = generateRandomVectors(num_vectors, dim, 555);

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::L2);
    index.build(data.data(), num_vectors, dim);

    std::atomic<size_t> errors{0};
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (size_t t = 0; t < num_threads; ++t)
    {
        threads.emplace_back([&, t]()
        {
            std::mt19937 rng(static_cast<unsigned>(t * 1000));
            std::uniform_real_distribution<float> dist(-1.0f, 1.0f);

            for (size_t q = 0; q < queries_per_thread; ++q)
            {
                std::vector<float> query(dim);
                for (auto & v : query)
                    v = dist(rng);

                std::vector<uint64_t> ids(k);
                std::vector<float> distances(k);

                try
                {
                    size_t found = index.search(query.data(), dim, k, ids.data(), distances.data());
                    if (found != k)
                        errors.fetch_add(1);

                    /// Verify distances are sorted
                    for (size_t i = 1; i < found; ++i)
                    {
                        if (distances[i - 1] > distances[i] + 1e-5f)
                            errors.fetch_add(1);
                    }
                }
                catch (...)
                {
                    errors.fetch_add(1);
                }
            }
        });
    }

    for (auto & thread : threads)
        thread.join();

    EXPECT_EQ(errors.load(), 0u)
        << "Concurrent search had " << errors.load() << " errors across "
        << num_threads * queries_per_thread << " queries";
}


/// Build with dimension mismatch — data dimension differs from index dimension.
TEST(DiskANNIndex, BuildDimensionMismatch)
{
    constexpr size_t dim = 16;
    constexpr size_t wrong_dim = 8;
    constexpr size_t num_vectors = 10;

    DiskANNIndexWithSerialization index(dim, DiskANNMetric::L2);

    auto data = generateRandomVectors(num_vectors, wrong_dim, 444);

    EXPECT_THROW(index.build(data.data(), num_vectors, wrong_dim), Exception);

    /// Index should remain empty after failed build
    EXPECT_TRUE(index.empty());
}

#endif
