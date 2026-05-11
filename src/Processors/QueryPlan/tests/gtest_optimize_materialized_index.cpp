#include <Core/UUID.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/VectorSearchUtils.h>

#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

namespace
{

NearestNeighbours makeNearestNeighbours(std::vector<UInt64> rows, std::vector<float> distances)
{
    NearestNeighbours nn;
    nn.rows = std::move(rows);
    nn.distances = std::move(distances);
    return nn;
}

UUID makeUUID(UInt64 lo)
{
    UUID u;
    UUIDHelpers::getLowBytes(u) = lo;
    UUIDHelpers::getHighBytes(u) = lo + 1;
    return u;
}

RangesInDataPartReadHints emptyHints()
{
    return RangesInDataPartReadHints{};
}

}

TEST(HintsExpansion, NonDistributed)
{
    UUID uuid_a = makeUUID(1);
    UUID uuid_b = makeUUID(2);
    UUID uuid_c = makeUUID(3);

    MaterializedIndexHints hints;
    hints.covered_source_parts = {uuid_a, uuid_b};
    hints.hits_per_part[uuid_a] = makeNearestNeighbours({10, 20, 30}, {0.1f, 0.2f, 0.3f});
    hints.hits_per_part[uuid_b] = makeNearestNeighbours({40, 50}, {0.4f, 0.5f});

    RangesInDataPartReadHints rh_a = emptyHints();
    RangesInDataPartReadHints rh_b = emptyHints();
    RangesInDataPartReadHints rh_c = emptyHints();

    attachMaterializedIndexHintForPart(uuid_a, rh_a, hints);
    attachMaterializedIndexHintForPart(uuid_b, rh_b, hints);
    attachMaterializedIndexHintForPart(uuid_c, rh_c, hints);

    ASSERT_TRUE(rh_a.materialized_index_search_results.has_value());
    EXPECT_EQ(rh_a.materialized_index_search_results->rows, (std::vector<UInt64>{10, 20, 30}));
    ASSERT_TRUE(rh_a.materialized_index_search_results->distances.has_value());
    EXPECT_EQ(rh_a.materialized_index_search_results->distances->size(), 3u);

    ASSERT_TRUE(rh_b.materialized_index_search_results.has_value());
    EXPECT_EQ(rh_b.materialized_index_search_results->rows, (std::vector<UInt64>{40, 50}));

    EXPECT_FALSE(rh_c.materialized_index_search_results.has_value());
    EXPECT_FALSE(rh_a.vector_search_results.has_value());
    EXPECT_FALSE(rh_b.vector_search_results.has_value());
}

TEST(HintsExpansion, DistributedBranch)
{
    /// The distributed branch in ReadFromMergeTree calls the same kernel after
    /// std::move(result_parts_ranges); behavioural equivalence is guaranteed by reusing the same
    /// helper. Re-driving it here documents that intent and fails if the contract drifts.
    UUID uuid_a = makeUUID(7);
    UUID uuid_b = makeUUID(8);

    MaterializedIndexHints hints;
    hints.covered_source_parts = {uuid_a};
    hints.hits_per_part[uuid_a] = makeNearestNeighbours({100}, {0.01f});

    RangesInDataPartReadHints rh_a = emptyHints();
    RangesInDataPartReadHints rh_b = emptyHints();

    attachMaterializedIndexHintForPart(uuid_a, rh_a, hints);
    attachMaterializedIndexHintForPart(uuid_b, rh_b, hints);

    ASSERT_TRUE(rh_a.materialized_index_search_results.has_value());
    EXPECT_EQ(rh_a.materialized_index_search_results->rows, (std::vector<UInt64>{100}));
    EXPECT_FALSE(rh_b.materialized_index_search_results.has_value());
}

TEST(HintsExpansion, DoubleWriteAssert)
{
#ifdef NDEBUG
    GTEST_SKIP() << "chassert is compiled out in release";
#else
    UUID uuid_a = makeUUID(42);
    MaterializedIndexHints hints;
    hints.covered_source_parts = {uuid_a};
    hints.hits_per_part[uuid_a] = makeNearestNeighbours({1}, {0.0f});

    RangesInDataPartReadHints rh_a = emptyHints();
    attachMaterializedIndexHintForPart(uuid_a, rh_a, hints);
    ASSERT_TRUE(rh_a.materialized_index_search_results.has_value());

    EXPECT_DEATH(attachMaterializedIndexHintForPart(uuid_a, rh_a, hints), "");
#endif
}

TEST(HintsExpansion, CoveredPartWithoutHitsAttachesEmptyResult)
{
    UUID uuid_a = makeUUID(100);
    UUID uuid_b = makeUUID(101);

    MaterializedIndexHints hints;
    hints.covered_source_parts = {uuid_a};

    RangesInDataPartReadHints rh_a = emptyHints();
    RangesInDataPartReadHints rh_b = emptyHints();

    attachMaterializedIndexHintForPart(uuid_a, rh_a, hints);
    attachMaterializedIndexHintForPart(uuid_b, rh_b, hints);

    ASSERT_TRUE(rh_a.materialized_index_search_results.has_value());
    EXPECT_TRUE(rh_a.materialized_index_search_results->rows.empty());
    ASSERT_TRUE(rh_a.materialized_index_search_results->distances.has_value());
    EXPECT_TRUE(rh_a.materialized_index_search_results->distances->empty());

    EXPECT_FALSE(rh_b.materialized_index_search_results.has_value());
}

TEST(OptimizationOrder, MaterializedIndexFirst)
{
    const auto & opts = getOptimizations();

    auto find_index = [&](const char * name) -> std::optional<size_t>
    {
        for (size_t i = 0; i < opts.size(); ++i)
            if (std::string(opts[i].name) == name)
                return i;
        return std::nullopt;
    };

    auto materialized_index_pos = find_index("useMaterializedIndex");
    auto vs_pos = find_index("useVectorSearch");

    ASSERT_TRUE(materialized_index_pos.has_value());
    ASSERT_TRUE(vs_pos.has_value());
    EXPECT_LT(*materialized_index_pos, *vs_pos);
}
