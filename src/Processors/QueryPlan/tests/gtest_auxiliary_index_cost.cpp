#include <Processors/QueryPlan/Optimizations/optimizeAuxiliaryIndex.h>
#include <Storages/AuxiliaryIndex/IAuxiliaryIndexAlgorithm.h>

#include <optional>
#include <limits>
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
    EXPECT_EQ(computeAuxiliaryIndexTotalCost(est, candidate_limit), 50u + 25u);

    AlgorithmCostEstimate zero;
    EXPECT_EQ(computeAuxiliaryIndexTotalCost(zero, 0u), 0u);

    AlgorithmCostEstimate only_search;
    only_search.algorithm_search_cost = 1000;
    EXPECT_EQ(computeAuxiliaryIndexTotalCost(only_search, 0u), 1000u);
}


TEST(FrameworkCostComputation, PartialCoverageIncludesUncoveredScanAndOverheads)
{
    AlgorithmCostEstimate est;
    est.algorithm_search_cost = 50;

    CoverageSnapshot coverage;
    coverage.active_source_parts = 2;
    coverage.covered_source_parts = 1;
    coverage.uncovered_source_rows = 900;
    coverage.ready_auxiliary_index_parts = 3;
    coverage.full_coverage = false;

    /// 50 algorithm + 25 verify + 900 uncovered scan + 1024 Union + 3 * 64 materialized-index-part overhead.
    EXPECT_EQ(computeAuxiliaryIndexTotalCost(est, /*candidate_limit=*/25, coverage), 2191u);
}


TEST(FrameworkCostComputation, FullCoverageAvoidsUncoveredScanAndUnionOverhead)
{
    AlgorithmCostEstimate est;
    est.algorithm_search_cost = 50;

    CoverageSnapshot coverage;
    coverage.active_source_parts = 2;
    coverage.covered_source_parts = 2;
    coverage.uncovered_source_rows = 0;
    coverage.ready_auxiliary_index_parts = 1;
    coverage.full_coverage = true;

    /// 50 algorithm + 25 verify + 1 * 64 materialized-index-part overhead.
    EXPECT_EQ(computeAuxiliaryIndexTotalCost(est, /*candidate_limit=*/25, coverage), 139u);
}


TEST(CandidateLimit, AppliesOverfetchFactor)
{
    auto limit = computeAuxiliaryIndexCandidateLimit(/*top_k=*/10, /*overfetch_factor=*/4);
    ASSERT_TRUE(limit.has_value());
    EXPECT_EQ(*limit, 40u);
}


TEST(CandidateLimit, DisablesInvalidFactorsAndOverflow)
{
    EXPECT_FALSE(computeAuxiliaryIndexCandidateLimit(/*top_k=*/10, /*overfetch_factor=*/0).has_value());
    EXPECT_FALSE(computeAuxiliaryIndexCandidateLimit(/*top_k=*/10, /*overfetch_factor=*/1025).has_value());
    EXPECT_FALSE(computeAuxiliaryIndexCandidateLimit(
        std::numeric_limits<size_t>::max(), /*overfetch_factor=*/2).has_value());
}


TEST(AllMatchFailed, NoRewrite)
{
    std::vector<std::pair<String, size_t>> scored;
    const auto winner = pickAuxiliaryIndexWinner(scored, /*force_name=*/{}, /*fallback_cost=*/1'000'000, /*log=*/nullptr);
    EXPECT_FALSE(winner.has_value());
}


TEST(CostTie, StableSelection)
{
    std::vector<std::pair<String, size_t>> scored = {{"auxiliary_index_a", 100}, {"auxiliary_index_b", 100}};
    /// fallback_cost > tied cost so the cost path picks a winner.
    auto winner = pickAuxiliaryIndexWinner(scored, /*force_name=*/{}, /*fallback_cost=*/1'000, /*log=*/nullptr);
    ASSERT_TRUE(winner.has_value());
    EXPECT_EQ(*winner, 0u);
    EXPECT_EQ(scored[*winner].first, "auxiliary_index_a");

    /// Re-running yields the same winner regardless of how many times.
    for (int i = 0; i < 5; ++i)
    {
        auto repeat = pickAuxiliaryIndexWinner(scored, /*force_name=*/{}, /*fallback_cost=*/1'000, /*log=*/nullptr);
        ASSERT_TRUE(repeat.has_value());
        EXPECT_EQ(*repeat, 0u);
    }
}


TEST(ForceOverride, BypassesFallback)
{
    /// auxiliary_index_b cost (5000) is worse than fallback (1000), but force_auxiliary_index='auxiliary_index_b' must still pick auxiliary_index_b.
    std::vector<std::pair<String, size_t>> scored = {{"auxiliary_index_a", 200}, {"auxiliary_index_b", 5000}};
    auto winner = pickAuxiliaryIndexWinner(scored, "auxiliary_index_b", /*fallback_cost=*/1'000, /*log=*/nullptr);
    ASSERT_TRUE(winner.has_value());
    EXPECT_EQ(scored[*winner].first, "auxiliary_index_b");
}


TEST(ForceOverride, MissingFallsBackToCost)
{
    std::vector<std::pair<String, size_t>> scored = {{"auxiliary_index_a", 200}, {"auxiliary_index_b", 500}};
    auto winner = pickAuxiliaryIndexWinner(scored, "nonexistent", /*fallback_cost=*/10'000, /*log=*/nullptr);
    ASSERT_TRUE(winner.has_value());
    /// auxiliary_index_a wins on cost when force is missing.
    EXPECT_EQ(scored[*winner].first, "auxiliary_index_a");
}


TEST(FallbackWins, NoRewrite)
{
    /// All candidate costs exceed fallback → no winner.
    std::vector<std::pair<String, size_t>> scored = {{"auxiliary_index_a", 5000}, {"auxiliary_index_b", 6000}};
    auto winner = pickAuxiliaryIndexWinner(scored, /*force_name=*/{}, /*fallback_cost=*/1'000, /*log=*/nullptr);
    EXPECT_FALSE(winner.has_value());
}
