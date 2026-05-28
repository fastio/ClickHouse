#include <Processors/QueryPlan/Optimizations/ReflectionReadHint.h>

#include <Common/logger_useful.h>

#include <limits>

namespace DB::QueryPlanOptimizations
{

size_t computeReflectionReadHintTotalCost(const ReflectionReadHintCost & cost)
{
    constexpr double VERIFY_COST_PER_ROW = 1.0;
    constexpr size_t UNION_BRANCH_FIXED_COST = 1024;
    constexpr size_t REFLECTION_PART_SEARCH_FIXED_COST = 64;

    const auto verify_cost = static_cast<size_t>(static_cast<double>(cost.candidate_limit) * VERIFY_COST_PER_ROW);
    size_t total = cost.engine_search_cost + verify_cost;
    total += cost.uncovered_source_rows;
    if (cost.has_source_parts && !cost.full_coverage)
        total += UNION_BRANCH_FIXED_COST;
    total += cost.ready_reflection_parts * REFLECTION_PART_SEARCH_FIXED_COST;
    return total;
}

std::optional<size_t> computeReflectionReadHintCandidateLimit(size_t top_k, UInt64 overfetch_factor)
{
    if (top_k == 0 || overfetch_factor == 0 || overfetch_factor > 1024)
        return std::nullopt;
    if (top_k > std::numeric_limits<size_t>::max() / overfetch_factor)
        return std::nullopt;
    return top_k * overfetch_factor;
}

std::optional<size_t> pickReflectionReadHintWinner(
    const std::vector<ReflectionReadHintCandidateScore> & scored,
    const String & force_name,
    const String & preferred_engine,
    size_t fallback_cost,
    LoggerPtr log)
{
    if (scored.empty())
        return std::nullopt;

    if (!force_name.empty())
    {
        for (size_t i = 0; i < scored.size(); ++i)
            if (scored[i].name == force_name)
                return i;
        if (log)
            LOG_WARNING(log, "force_ann_index={} did not match any Reflection ReadHint candidate; falling back to default selection", force_name);
    }

    std::optional<size_t> best_idx;
    if (!preferred_engine.empty())
    {
        for (size_t i = 0; i < scored.size(); ++i)
        {
            if (scored[i].engine != preferred_engine)
                continue;
            if (!best_idx || scored[i].cost < scored[*best_idx].cost)
                best_idx = i;
        }

        if (best_idx)
            return best_idx;

        if (log)
            LOG_WARNING(log, "preferred Reflection ReadHint engine {} did not match any usable candidate; falling back to cost-based selection", preferred_engine);
    }

    best_idx = 0;
    for (size_t i = 1; i < scored.size(); ++i)
        if (scored[i].cost < scored[*best_idx].cost)
            best_idx = i;

    if (scored[*best_idx].cost >= fallback_cost)
        return std::nullopt;
    return best_idx;
}

}
