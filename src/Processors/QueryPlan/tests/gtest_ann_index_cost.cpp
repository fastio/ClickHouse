#include <Processors/QueryPlan/Optimizations/ReflectionReadHint.h>

#include <optional>
#include <limits>
#include <vector>

#include <gtest/gtest.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;


TEST(FrameworkCostComputation, Basic)
{
    /// verify_cost factor is 1.0 (one PREWHERE re-evaluation per row); rerank_cost is 0.
    EXPECT_EQ(
        computeReflectionReadHintTotalCost(ReflectionReadHintCost{
            .engine_search_cost = 50,
            .candidate_limit = 25}),
        50u + 25u);

    EXPECT_EQ(computeReflectionReadHintTotalCost(ReflectionReadHintCost{}), 0u);

    EXPECT_EQ(
        computeReflectionReadHintTotalCost(ReflectionReadHintCost{.engine_search_cost = 1000}),
        1000u);
}


TEST(FrameworkCostComputation, PartialCoverageIncludesUncoveredScanAndOverheads)
{
    /// 50 algorithm + 25 verify + 900 uncovered scan + 1024 Union + 3 * 64 ready-part overhead.
    EXPECT_EQ(
        computeReflectionReadHintTotalCost(ReflectionReadHintCost{
            .engine_search_cost = 50,
            .candidate_limit = 25,
            .uncovered_source_rows = 900,
            .ready_reflection_parts = 3,
            .has_source_parts = true,
            .full_coverage = false}),
        2191u);
}


TEST(FrameworkCostComputation, FullCoverageAvoidsUncoveredScanAndUnionOverhead)
{
    /// 50 algorithm + 25 verify + 1 * 64 ready-part overhead.
    EXPECT_EQ(
        computeReflectionReadHintTotalCost(ReflectionReadHintCost{
            .engine_search_cost = 50,
            .candidate_limit = 25,
            .uncovered_source_rows = 0,
            .ready_reflection_parts = 1,
            .has_source_parts = true,
            .full_coverage = true}),
        139u);
}


TEST(CandidateLimit, AppliesOverfetchFactor)
{
    auto limit = computeReflectionReadHintCandidateLimit(/*top_k=*/10, /*overfetch_factor=*/4);
    ASSERT_TRUE(limit.has_value());
    EXPECT_EQ(*limit, 40u);
}


TEST(CandidateLimit, DisablesInvalidFactorsAndOverflow)
{
    EXPECT_FALSE(computeReflectionReadHintCandidateLimit(/*top_k=*/10, /*overfetch_factor=*/0).has_value());
    EXPECT_FALSE(computeReflectionReadHintCandidateLimit(/*top_k=*/10, /*overfetch_factor=*/1025).has_value());
    EXPECT_FALSE(computeReflectionReadHintCandidateLimit(
        std::numeric_limits<size_t>::max(), /*overfetch_factor=*/2).has_value());
}


TEST(AllMatchFailed, NoRewrite)
{
    std::vector<ReflectionReadHintCandidateScore> scored;
    const auto winner = pickReflectionReadHintWinner(
        scored, /*force_name=*/{}, /*preferred_engine=*/{}, /*fallback_cost=*/1'000'000, /*log=*/nullptr);
    EXPECT_FALSE(winner.has_value());
}


TEST(CostTie, StableSelection)
{
    std::vector<ReflectionReadHintCandidateScore> scored = {
        {.name = "ann_index_a", .engine = "diskann", .cost = 100},
        {.name = "ann_index_b", .engine = "diskann", .cost = 100}};
    /// fallback_cost > tied cost so the cost path picks a winner.
    auto winner = pickReflectionReadHintWinner(
        scored, /*force_name=*/{}, /*preferred_engine=*/{}, /*fallback_cost=*/1'000, /*log=*/nullptr);
    ASSERT_TRUE(winner.has_value());
    EXPECT_EQ(*winner, 0u);
    EXPECT_EQ(scored[*winner].name, "ann_index_a");

    /// Re-running yields the same winner regardless of how many times.
    for (int i = 0; i < 5; ++i)
    {
        auto repeat = pickReflectionReadHintWinner(
            scored, /*force_name=*/{}, /*preferred_engine=*/{}, /*fallback_cost=*/1'000, /*log=*/nullptr);
        ASSERT_TRUE(repeat.has_value());
        EXPECT_EQ(*repeat, 0u);
    }
}


TEST(ForceOverride, BypassesFallback)
{
    /// `ann_index_b` cost (5000) is worse than fallback (1000), but `force_ann_index='ann_index_b'`
    /// must still pick `ann_index_b`.
    std::vector<ReflectionReadHintCandidateScore> scored = {
        {.name = "ann_index_a", .engine = "diskann", .cost = 200},
        {.name = "ann_index_b", .engine = "spann", .cost = 5000}};
    auto winner = pickReflectionReadHintWinner(
        scored, "ann_index_b", /*preferred_engine=*/{}, /*fallback_cost=*/1'000, /*log=*/nullptr);
    ASSERT_TRUE(winner.has_value());
    EXPECT_EQ(scored[*winner].name, "ann_index_b");
}


TEST(ForceOverride, MissingFallsBackToCost)
{
    std::vector<ReflectionReadHintCandidateScore> scored = {
        {.name = "ann_index_a", .engine = "diskann", .cost = 200},
        {.name = "ann_index_b", .engine = "spann", .cost = 500}};
    auto winner = pickReflectionReadHintWinner(
        scored, "nonexistent", /*preferred_engine=*/{}, /*fallback_cost=*/10'000, /*log=*/nullptr);
    ASSERT_TRUE(winner.has_value());
    /// `ann_index_a` wins on cost when force is missing.
    EXPECT_EQ(scored[*winner].name, "ann_index_a");
}


TEST(FallbackWins, NoRewrite)
{
    /// All candidate costs exceed fallback → no winner.
    std::vector<ReflectionReadHintCandidateScore> scored = {
        {.name = "ann_index_a", .engine = "diskann", .cost = 5000},
        {.name = "ann_index_b", .engine = "spann", .cost = 6000}};
    auto winner = pickReflectionReadHintWinner(
        scored, /*force_name=*/{}, /*preferred_engine=*/{}, /*fallback_cost=*/1'000, /*log=*/nullptr);
    EXPECT_FALSE(winner.has_value());
}


TEST(PreferredEngine, BypassesCrossEngineCost)
{
    /// `spann` is more expensive than `diskann` and fallback, but source-table
    /// preference selects it without cross-engine cost comparison.
    std::vector<ReflectionReadHintCandidateScore> scored = {
        {.name = "ann_index_a", .engine = "diskann", .cost = 200},
        {.name = "ann_index_b", .engine = "spann", .cost = 5000}};
    auto winner = pickReflectionReadHintWinner(
        scored, /*force_name=*/{}, "spann", /*fallback_cost=*/1'000, /*log=*/nullptr);
    ASSERT_TRUE(winner.has_value());
    EXPECT_EQ(scored[*winner].name, "ann_index_b");
}


TEST(PreferredEngine, UsesCostWithinPreferredEngine)
{
    std::vector<ReflectionReadHintCandidateScore> scored = {
        {.name = "ann_index_a", .engine = "spann", .cost = 500},
        {.name = "ann_index_b", .engine = "spann", .cost = 200},
        {.name = "ann_index_c", .engine = "diskann", .cost = 100}};
    auto winner = pickReflectionReadHintWinner(
        scored, /*force_name=*/{}, "spann", /*fallback_cost=*/150, /*log=*/nullptr);
    ASSERT_TRUE(winner.has_value());
    EXPECT_EQ(scored[*winner].name, "ann_index_b");
}


TEST(PreferredEngine, ForceOverrideWins)
{
    std::vector<ReflectionReadHintCandidateScore> scored = {
        {.name = "ann_index_a", .engine = "diskann", .cost = 200},
        {.name = "ann_index_b", .engine = "spann", .cost = 500}};
    auto winner = pickReflectionReadHintWinner(
        scored, "ann_index_a", "spann", /*fallback_cost=*/1'000, /*log=*/nullptr);
    ASSERT_TRUE(winner.has_value());
    EXPECT_EQ(scored[*winner].name, "ann_index_a");
}


TEST(PreferredEngine, MissingFallsBackToCost)
{
    std::vector<ReflectionReadHintCandidateScore> scored = {
        {.name = "ann_index_a", .engine = "diskann", .cost = 200},
        {.name = "ann_index_b", .engine = "spann", .cost = 500}};
    auto winner = pickReflectionReadHintWinner(
        scored, /*force_name=*/{}, "unknown", /*fallback_cost=*/1'000, /*log=*/nullptr);
    ASSERT_TRUE(winner.has_value());
    EXPECT_EQ(scored[*winner].name, "ann_index_a");
}


TEST(PreferredEngine, DisableFilterCanRemovePreferredCandidate)
{
    /// `disable_ann_index` is applied before scoring. This models the
    /// post-filter candidate set after a disabled `spann` candidate was removed.
    std::vector<ReflectionReadHintCandidateScore> scored = {
        {.name = "ann_index_a", .engine = "diskann", .cost = 200}};
    auto winner = pickReflectionReadHintWinner(
        scored, /*force_name=*/{}, "spann", /*fallback_cost=*/1'000, /*log=*/nullptr);
    ASSERT_TRUE(winner.has_value());
    EXPECT_EQ(scored[*winner].name, "ann_index_a");
}
