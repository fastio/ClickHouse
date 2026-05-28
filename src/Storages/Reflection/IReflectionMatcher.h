#pragma once

#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/VectorSearchUtils.h>
#include <Storages/Reflection/IReflection.h>

#include <memory>
#include <optional>
#include <vector>


namespace DB
{

/// Input bundle handed by the framework optimizer to every `IReflectionMatcher`
/// candidate so it can decide whether to offer a rewrite.
///
/// Fields are shaped around the only currently-supported `MatchKind`
/// (vector ANN `ReadHint`). When other kinds land (`Cube`/`AsyncMV` `PlanRewrite`,
/// `Cardinality` `CostHint`), this struct will be split per-kind.
struct ReflectionPlanShape
{
    String distance_function;
    String search_column;
    std::vector<float> query_vector;
    size_t top_k = 0;
    size_t candidate_limit = 0;
    const RangesInDataParts * active_source_parts = nullptr;
};

/// A non-binding offer returned by `matchReadHint` — cheap to compute, just
/// enough information for the framework to score candidates and pick a winner.
///
/// `engine_search_cost` / `uncovered_source_rows` / `ready_reflection_parts` /
/// `has_source_parts` / `full_coverage` are raw cost inputs that the optimizer
/// folds into the final total via `computeReflectionReadHintTotalCost`. Keeping
/// cost assembly framework-side avoids a storage→optimizer reverse dependency.
struct ReflectionReadHintOffer
{
    size_t engine_search_cost = 0;
    size_t uncovered_source_rows = 0;
    size_t ready_reflection_parts = 0;
    bool has_source_parts = false;
    bool full_coverage = false;

    String reflection_name;
    String engine_name;
    std::shared_ptr<void> private_handle;
};

/// Generic distance descriptor for the uncovered branch's exact evaluator.
/// Mirrors the algorithm-side `AlgorithmDistanceDescriptor` but lives in the
/// framework header to keep engine includes out of optimizer code.
struct ReflectionDistanceInfo
{
    String exact_function_name;
    UInt64 metric_id = 0;
    UInt32 dim = 0;
};

/// The full result returned by `realizeReadHint` — produced once for the winner.
struct ReflectionReadHintRealization
{
    ANNIndexHints hits;
    ReflectionDistanceInfo distance;
    String virtual_column;
};

/// A storage that knows how to participate in optimizer plan rewriting.
///
/// Currently only the `ReadHint` form is supported; engines returning other
/// `ReflectionMatchKind` values must be added when their first concrete engine
/// (`Cube`/`AsyncMV` `PlanRewrite`, `Cardinality` `CostHint`) lands.
class IReflectionMatcher
{
public:
    virtual ~IReflectionMatcher() = default;

    virtual ReflectionMatchKind matchKind() const = 0;

    /// Cheap, per-candidate match decision. Returns `std::nullopt` to decline.
    /// The returned offer's `private_handle` is ferried back into
    /// `realizeReadHint` for the winner; runners-up never see realize.
    virtual std::optional<ReflectionReadHintOffer> matchReadHint(
        const ReflectionPlanShape & shape, ContextPtr context) = 0;

    /// Expensive realisation step, called exactly once on the winner. Runs the
    /// engine-private search, translates internal ids into source coordinates
    /// and returns the per-source-part hits the framework attaches to the read
    /// step.
    virtual ReflectionReadHintRealization realizeReadHint(
        const ReflectionPlanShape & shape,
        const ReflectionReadHintOffer & offer,
        ContextPtr context) = 0;
};

}
