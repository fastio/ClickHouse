#pragma once

#include <Core/Types.h>
#include <Common/Logger.h>

#include <optional>
#include <vector>

namespace DB::QueryPlanOptimizations
{

struct ReflectionReadHintCost
{
    size_t engine_search_cost = 0;
    size_t candidate_limit = 0;
    size_t uncovered_source_rows = 0;
    size_t ready_reflection_parts = 0;
    bool has_source_parts = false;
    bool full_coverage = false;
};

struct ReflectionReadHintCandidateScore
{
    String name;
    String engine;
    size_t cost = 0;
};

size_t computeReflectionReadHintTotalCost(const ReflectionReadHintCost & cost);

std::optional<size_t> computeReflectionReadHintCandidateLimit(size_t top_k, UInt64 overfetch_factor);

std::optional<size_t> pickReflectionReadHintWinner(
    const std::vector<ReflectionReadHintCandidateScore> & scored,
    const String & force_name,
    const String & preferred_engine,
    size_t fallback_cost,
    LoggerPtr log);

}
