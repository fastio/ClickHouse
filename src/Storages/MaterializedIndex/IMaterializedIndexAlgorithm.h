#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <atomic>
#include <memory>
#include <optional>
#include <vector>


namespace DB
{

class Block;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class IDataPartStorage;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;
using DataPartStoragePtr = std::shared_ptr<const IDataPartStorage>;

struct MaterializedIndexContext;

struct QueryFeatures
{
    std::vector<float> query_vector;
    size_t k = 0;
};

/// Output of `match`: the algorithm-side handle that `search` later consumes.
/// Carries the query vector + k so that `search` can run without
/// re-extracting them from the optimizer.
struct MatchDescriptor
{
    std::vector<float> query_vector;
    size_t k = 0;
};

/// Cost estimate returned by `estimateCost`. Units are "equivalent scanned rows":
/// the framework compares this against full-scan cost (= source row count) when
/// deciding whether to rewrite the plan.
struct AlgorithmCostEstimate
{
    size_t estimated_result_rows = 0;
    size_t algorithm_search_cost = 0;
};

/// Per-source-part nearest-neighbour result returned by `search`. Each entry
/// belongs to one source MergeTree part and carries the matching rows as
/// `_part_offset` values plus their distances. The reader path joins these
/// against the source's `_part_offset` virtual column to materialise hits.
struct SourceRowSet
{
    UUID source_part_uuid;
    std::vector<UInt64> part_offsets;
    std::vector<float> distances;
};

struct SearchResult
{
    std::vector<SourceRowSet> per_part;
};

struct CoverageSnapshot {};

struct ReadyMaterializedIndexPartSnapshot
{
    std::vector<DataPartStoragePtr> parts;
};

/// Build-time context handed to the three-phase build interface below.
///
/// Fields are populated by the framework (the mid-layer Build stage) and
/// consumed by the algorithm. In stage-2 the framework only declares this
/// struct; actual population happens in the mid-layer Build wiring.
///
/// Lifetime: all pointer-typed fields are owned elsewhere and must not be
/// dereferenced after the enclosing build call returns. In particular,
/// `is_cancelled` is a bare pointer whose lifetime is bound to the
/// containing build task; the algorithm must not hold on to it past
/// `finishBuild`.
struct AlgorithmBuildContext
{
    /// Final index data is written here.
    MutableDataPartStoragePtr output_storage;

    /// Scratch area for multi-pass algorithms (e.g. sort / spill / shard).
    /// Reclaimed by the framework after `finishBuild` returns.
    MutableDataPartStoragePtr intermediate_storage;

    /// Soft memory budget for the whole build. Zero means "unbounded";
    /// algorithms are expected to self-police when non-zero.
    UInt64 memory_budget_bytes = 0;

    /// Cancellation flag polled cooperatively by the algorithm. Kept as a
    /// bare pointer to stay trivially usable from pure-C algorithm backends
    /// via `atomic_load`.
    const std::atomic<bool> * is_cancelled = nullptr;

    /// Total number of rows the framework plans to feed via `prepareBuild`.
    /// Algorithms may use it to pre-size data structures.
    UInt64 total_rows = 0;

    /// Optional row-offset boundaries the algorithm asked for via
    /// `preferredSegmentBoundaries`; empty when the algorithm did not opt in.
    std::vector<UInt64> segment_boundaries;
};


/// Pluggable algorithm surface for MaterializedIndex tables.
///
/// A registered algorithm is a pair of (family, impl). The family selects a
/// C++ class, the impl selects behaviour inside that class. Only the
/// register-time and initialize-time methods are exercised by stage-1; the
/// search / build paths are reached once background tasks and query rewrite
/// land.
class IMaterializedIndexAlgorithm
{
public:
    virtual ~IMaterializedIndexAlgorithm() = default;

    virtual String getName() const = 0;
    virtual String getFamily() const = 0;

    virtual void validateBuildParameters(const ASTPtr & build_params, ContextPtr context) = 0;
    virtual void validateIndexedExpression(const ASTPtr & indexed_expression, const StorageInMemoryMetadata & source_metadata) = 0;

    virtual void initialize(const MaterializedIndexContext & ctx) = 0;

    virtual std::optional<MatchDescriptor> match(const QueryFeatures & features) const = 0;

    virtual AlgorithmCostEstimate estimateCost(const MatchDescriptor & desc, const CoverageSnapshot & coverage) const = 0;

    virtual SearchResult search(
        const MatchDescriptor & desc,
        const ReadyMaterializedIndexPartSnapshot & ready_parts,
        size_t candidate_limit,
        ContextPtr query_context) const = 0;

    virtual ColumnsWithTypeAndName rerank(const MatchDescriptor & /*desc*/, const ColumnsWithTypeAndName & verified_rows) const
    {
        return verified_rows;
    }

    /// Build-time interface. The framework calls the three methods in a
    /// fixed order, once per build:
    ///
    ///     prepareBuild(ctx, block)    // N times, one per source block
    ///     buildAlgorithmPrivate(ctx)  // exactly once, after all blocks fed
    ///     finishBuild(ctx)            // exactly once, before cleanup
    ///
    /// The three methods are deliberately non-const: the algorithm may
    /// accumulate per-build state (buffers, shards, sketches) across
    /// phases. Implementations must internally poll `ctx.is_cancelled`
    /// during long-running work.

    /// Phase 1: data ingestion. Called once per source block in row order.
    /// Algorithm may stream-process in memory or stage data to
    /// `ctx.intermediate_storage`.
    virtual void prepareBuild(const AlgorithmBuildContext & ctx, const Block & indexed_columns_batch) = 0;

    /// Phase 2: index construction. Framework no longer feeds data.
    /// Algorithm reads what it staged (if any) from
    /// `ctx.intermediate_storage` and writes the final index to
    /// `ctx.output_storage`. Must internally poll `ctx.is_cancelled`.
    virtual void buildAlgorithmPrivate(const AlgorithmBuildContext & ctx) = 0;

    /// Phase 3: finalization before `ctx.intermediate_storage` is reclaimed.
    /// For fingerprint / version writing / resource release. Called once.
    virtual void finishBuild(const AlgorithmBuildContext & ctx) = 0;

    virtual std::vector<UInt64> preferredSegmentBoundaries() const { return {}; }
};

using MaterializedIndexAlgorithmPtr = std::unique_ptr<IMaterializedIndexAlgorithm>;

}
