#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/Reflection/IReflection.h>

#include <functional>
#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>


namespace DB
{

class ReadFromMergeTree;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Input bundle handed by the framework optimizer to every `IReflectionMatcher`
/// candidate so it can decide whether to offer a rewrite.
///
/// The optimizer fills this from a generic plan-shape analysis: a `TopK` query
/// whose single `ORDER BY` key is `f(search_column, <constant operand>)`. The
/// optimizer deliberately does NOT validate whether `f` (`distance_function`)
/// is one a given engine accelerates, nor whether `sort_direction` is
/// compatible, nor whether the source has a competing native index — those are
/// engine-specific decisions made in `matchReadHint`. `source_metadata` is the
/// source table's metadata so the engine can inspect competing secondary
/// indices; `sort_direction` is +1 for ascending and -1 for descending.
///
/// Fields are shaped around the only currently-supported `MatchKind`
/// (`ReadHint`). When other kinds land (`Cube`/`AsyncMV` `PlanRewrite`,
/// `Cardinality` `CostHint`), this struct will be split per-kind.
struct ReflectionPlanShape
{
    String distance_function;
    String search_column;
    std::vector<float> query_vector;
    size_t top_k = 0;
    size_t candidate_limit = 0;
    int sort_direction = 0;
    StorageMetadataPtr source_metadata;
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

/// Engine-neutral descriptor for the uncovered branch's exact evaluator. Carries
/// only POD identity (a function name plus two scalar parameters) so the generic
/// optimizer can build the brute-force fallback expression without naming any
/// engine type.
struct ReflectionDistanceInfo
{
    String exact_function_name;
    UInt64 metric_id = 0;
    UInt32 dim = 0;
};

/// The full result returned by `realizeReadHint` — produced once for the winner.
///
/// Engine-agnostic by construction: the framework optimizer never names an
/// engine-specific hint type. The winner engine instead hands back
///   * `covered_source_parts` — which active source parts it fully serves, so
///     the framework can choose full vs partial coverage and split the Union;
///   * `apply_to_covered_read_step` — a callback that attaches the engine's read
///     hints to the covered `ReadFromMergeTree` and declares its virtual
///     column(s). `keep_search_column` tells the engine whether a downstream
///     output still needs the physical search column alongside the virtual one;
///   * `virtual_column` — the virtual column the rewrite routes `ORDER BY`
///     through (e.g. `_distance`);
///   * `distance` — neutral descriptor used to rebuild `virtual_column` on the
///     uncovered brute-force branch.
struct ReflectionReadHintRealization
{
    std::unordered_set<UUID> covered_source_parts;
    std::function<void(ReadFromMergeTree & read_step, bool keep_search_column)> apply_to_covered_read_step;
    String virtual_column;
    ReflectionDistanceInfo distance;
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
