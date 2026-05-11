#include <Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.h>
#include <Storages/MaterializedIndex/IMaterializedIndexAlgorithm.h>

#include <optional>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;


TEST(FrameworkCostComputation, Basic)
{
    AlgorithmCostEstimate est;
    est.estimated_result_rows = 100;
    est.algorithm_search_cost = 50;

    constexpr size_t candidate_limit = 25;
    /// verify_cost factor is 1.0 (one PREWHERE re-evaluation per row); rerank_cost is 0.
    EXPECT_EQ(computeMaterializedIndexTotalCost(est, candidate_limit), 50u + 25u);

    AlgorithmCostEstimate zero;
    EXPECT_EQ(computeMaterializedIndexTotalCost(zero, 0u), 0u);

    AlgorithmCostEstimate only_search;
    only_search.algorithm_search_cost = 1000;
    EXPECT_EQ(computeMaterializedIndexTotalCost(only_search, 0u), 1000u);
}


TEST(AllMatchFailed, NoRewrite)
{
    std::vector<std::pair<String, size_t>> scored;
    const auto winner = pickMaterializedIndexWinner(scored, /*force_name=*/{}, /*fallback_cost=*/1'000'000, /*log=*/nullptr);
    EXPECT_FALSE(winner.has_value());
}


TEST(CostTie, StableSelection)
{
    std::vector<std::pair<String, size_t>> scored = {{"mi_a", 100}, {"mi_b", 100}};
    /// fallback_cost > tied cost so the cost path picks a winner.
    auto winner = pickMaterializedIndexWinner(scored, /*force_name=*/{}, /*fallback_cost=*/1'000, /*log=*/nullptr);
    ASSERT_TRUE(winner.has_value());
    EXPECT_EQ(*winner, 0u);
    EXPECT_EQ(scored[*winner].first, "mi_a");

    /// Re-running yields the same winner regardless of how many times.
    for (int i = 0; i < 5; ++i)
    {
        auto repeat = pickMaterializedIndexWinner(scored, /*force_name=*/{}, /*fallback_cost=*/1'000, /*log=*/nullptr);
        ASSERT_TRUE(repeat.has_value());
        EXPECT_EQ(*repeat, 0u);
    }
}


TEST(ForceOverride, BypassesFallback)
{
    /// mi_b cost (5000) is worse than fallback (1000), but force_materialized_index='mi_b' must still pick mi_b.
    std::vector<std::pair<String, size_t>> scored = {{"mi_a", 200}, {"mi_b", 5000}};
    auto winner = pickMaterializedIndexWinner(scored, "mi_b", /*fallback_cost=*/1'000, /*log=*/nullptr);
    ASSERT_TRUE(winner.has_value());
    EXPECT_EQ(scored[*winner].first, "mi_b");
}


TEST(ForceOverride, MissingFallsBackToCost)
{
    std::vector<std::pair<String, size_t>> scored = {{"mi_a", 200}, {"mi_b", 500}};
    auto winner = pickMaterializedIndexWinner(scored, "nonexistent", /*fallback_cost=*/10'000, /*log=*/nullptr);
    ASSERT_TRUE(winner.has_value());
    /// mi_a wins on cost when force is missing.
    EXPECT_EQ(scored[*winner].first, "mi_a");
}


TEST(FallbackWins, NoRewrite)
{
    /// All candidate costs exceed fallback → no winner.
    std::vector<std::pair<String, size_t>> scored = {{"mi_a", 5000}, {"mi_b", 6000}};
    auto winner = pickMaterializedIndexWinner(scored, /*force_name=*/{}, /*fallback_cost=*/1'000, /*log=*/nullptr);
    EXPECT_FALSE(winner.has_value());
}
