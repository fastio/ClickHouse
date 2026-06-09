#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPayloadCodec.h>

#include <atomic>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>


namespace DB
{

class Block;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class IDataPartStorage;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;
using DataPartStoragePtr = std::shared_ptr<const IDataPartStorage>;

struct ANNIndexContext;

struct QueryFeatures
{
    std::vector<float> query_vector;
    String distance_function;
    size_t k = 0;
};

/// Algorithm-owned distance semantics for both indexed and brute-force paths.
/// The optimizer treats this descriptor as the winner algorithm's contract:
/// indexed parts get distances from `search`, and uncovered parts build an
/// exact evaluator from `exact_function_name` plus the algorithm parameters.
struct AlgorithmDistanceDescriptor
{
    String exact_function_name;
    String metric_name;
    UInt64 metric_id = 0;
    UInt32 dim = 0;
    bool smaller_is_better = true;
};

/// Output of `match`: the algorithm-side handle that `search` later consumes.
/// Carries the query vector + k so that `search` can run without
/// re-extracting them from the optimizer.
struct MatchDescriptor
{
    std::vector<float> query_vector;
    AlgorithmDistanceDescriptor distance;
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

/// One source part covered by a ready materialized-index-part. This is coverage metadata, not
/// query result data: a covered source part may have zero hits for a query.
struct CoveredSourcePart
{
    UUID source_part_uuid;
    UInt64 rows = 0;
    String partition_id;
    Int64 min_block = 0;
    Int64 max_block = 0;
    UInt32 level = 0;
    Int64 mutation = 0;
    bool has_part_info = false;

    /// Per-MI-part dense local id this source part holds in the inline
    /// payload. Mirrors `CoverageEntry::payload_part_id`. Default sentinel
    /// `UINT32_MAX` means the manifest predates the compact payload schema
    /// — the matcher then routes hits through the cold path.
    UInt32 payload_part_id = std::numeric_limits<UInt32>::max();
};

/// One ready materialized-index-part that can participate in algorithm-side search, together
/// with the source parts it covers according to `coverage.json`.
struct ReadyANNIndexPart
{
    DataPartStoragePtr storage;
    std::vector<CoveredSourcePart> covered_source_parts;

    /// Compact-payload version recorded in this MI-part's `coverage.json`
    /// (`payload_format_version` field). The algorithm consults it inside
    /// `search` to decode the inline payload sidecar; the matcher uses it
    /// to translate `hot_part_ids` to source UUIDs together with the
    /// `payload_part_id` carried in `covered_source_parts`. Default
    /// `V1_OFFSET32` keeps legacy MI-parts (no compact payload) decoding
    /// at the smaller record width — the algorithm separately checks
    /// `existsFile` / sidecar size and falls back to cold path on
    /// mismatch.
    ANNIndexPayloadCodec::Version payload_format_version
        = ANNIndexPayloadCodec::Version::V1_OFFSET32;
};

struct ReadyANNIndexPartSnapshot
{
    std::vector<ReadyANNIndexPart> parts;
};

/// Algorithm-private query result. `internal_ids` are row ids inside the
/// corresponding materialized-index-part; the framework translates them through
/// `locator` into source-table coordinates.
///
/// Optional inline payload: when the producing algorithm has
/// `supportsPerVectorPayload() == true` and the underlying index carries
/// payload (8 or 12 bytes per vector — see `ANNIndexPayloadCodec`),
/// `hot_part_ids` and `hot_part_offsets` are populated. They are *parallel*
/// to `internal_ids`: either both vectors are empty (no payload available
/// — legacy index, or algorithm does not support payload) or both have
/// `size() == internal_ids.size()`. Mixed states (one populated, the
/// other empty, or sizes differing) are forbidden — the matcher will
/// assert in debug builds.
///
/// `hot_part_ids[i]` is the per-MI-part dense local id (`coverage.json`'s
/// `payload_part_id`); the matcher resolves it to a source UUID via the
/// MI-part's coverage manifest.
struct InternalHitSet
{
    DataPartStoragePtr ann_index_part_storage;
    std::vector<UInt64> internal_ids;
    std::vector<float> distances;
    std::vector<UInt32> hot_part_ids;
    std::vector<UInt64> hot_part_offsets;

    bool hasPayload() const
    {
        return !hot_part_ids.empty();
    }
};

struct InternalSearchResult
{
    std::vector<InternalHitSet> per_ann_index_part;
};

/// Per-source-part nearest-neighbour result produced after framework-side
/// locator translation. Each entry belongs to one source MergeTree part and
/// carries matching `_part_offset` values plus their distances.
struct SourceRowSet
{
    UUID source_part_uuid;
    std::vector<UInt64> part_offsets;
    std::vector<float> distances;
};

struct SourceSearchResult
{
    std::vector<SourceRowSet> hits_per_part;
};

struct CoverageSnapshot
{
    size_t active_source_parts = 0;
    size_t active_source_rows = 0;
    size_t covered_source_parts = 0;
    size_t covered_source_rows = 0;
    size_t uncovered_source_rows = 0;
    size_t ready_ann_index_parts = 0;
    size_t candidate_limit = 0;
    bool full_coverage = false;
};

struct AlgorithmPartCompatibility
{
    bool compatible = true;
    String reason;
};

struct AlgorithmPrivatePath
{
    String path;
    bool recursive = false;
    bool required = true;
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

    /// Scheduler task id for build observability. Algorithms may use it to
    /// publish transient build state while the task is running.
    String task_id;

    /// Optional row-offset boundaries the algorithm asked for via
    /// `preferredSegmentBoundaries`; empty when the algorithm did not opt in.
    std::vector<UInt64> segment_boundaries;

    /// Compact payload format selected by the framework before stage 1.
    /// `chooseVersion(max(source_part.rows_count))` decides between
    /// `V1_OFFSET32` (8 B / record, the common case) and `V2_OFFSET64`
    /// (12 B / record, when at least one source part exceeds UINT32_MAX
    /// rows). Algorithms that opt into per-vector payload must consult
    /// this field — it determines the on-disk record width and is
    /// persisted in `coverage.json::payload_format_version`.
    ANNIndexPayloadCodec::Version payload_format_version = ANNIndexPayloadCodec::Version::V1_OFFSET32;

    /// Map from source-part UUID to the dense local `payload_part_id` the
    /// framework will record in `coverage.json`. Used by
    /// `prepareBuildLocator` to translate the per-block UUID into the
    /// compact id that goes into the inline payload. Owned by the
    /// containing build task; pointer must remain valid until
    /// `finishBuild` returns. Never null when
    /// `IANNIndexAlgorithm::supportsPerVectorPayload()` is true.
    const std::unordered_map<UUID, UInt32> * uuid_to_payload_part_id = nullptr;
};


/// Pluggable algorithm surface for ANNIndex tables.
///
/// A registered algorithm is a pair of (family, impl). The family selects a
/// C++ class, the impl selects behaviour inside that class. Only the
/// register-time and initialize-time methods are exercised by stage-1; the
/// search / build paths are reached once background tasks and query rewrite
/// land.
class IANNIndexAlgorithm
{
public:
    virtual ~IANNIndexAlgorithm() = default;

    virtual String getName() const = 0;
    virtual String getFamily() const = 0;
    virtual String getAlgorithmVersion() const { return {}; }
    virtual String getBuildParamsHash() const { return {}; }
    virtual AlgorithmPartCompatibility checkPartCompatibility(const IDataPartStorage & storage) const;
    virtual std::vector<AlgorithmPrivatePath> getAlgorithmPrivatePaths(const IDataPartStorage & storage) const;
    virtual std::map<String, String> getAlgorithmObservabilityFields() const { return {}; }

    virtual void validateBuildParameters(const ASTPtr & build_params, ContextPtr context) = 0;
    virtual void validateIndexedExpression(const ASTPtr & indexed_expression, const StorageInMemoryMetadata & source_metadata) = 0;

    virtual void initialize(const ANNIndexContext & ctx) = 0;

    virtual std::optional<MatchDescriptor> match(const QueryFeatures & features) const = 0;

    virtual AlgorithmCostEstimate estimateCost(const MatchDescriptor & desc, const CoverageSnapshot & coverage) const = 0;

    virtual InternalSearchResult search(
        const MatchDescriptor & desc,
        const ReadyANNIndexPartSnapshot & ready_parts,
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
    ///
    /// Because per-build state lives on the algorithm instance, the
    /// framework must not run two concurrent builds against the same
    /// instance, and must not reuse a single instance across builds
    /// (a failed build can leave half-written buffers, finalized writers
    /// etc. on the object). `cloneForBuild` returns a fresh instance
    /// carrying only storage-scope state (validated params, initialised
    /// flag) so each `ANNIndex::BuildTask` can run in isolation.
    virtual std::unique_ptr<IANNIndexAlgorithm> cloneForBuild() const = 0;

    /// Phase 1: data ingestion. Called once per source block in row order.
    /// Algorithm may stream-process in memory or stage data to
    /// `ctx.intermediate_storage`.
    virtual void prepareBuild(const AlgorithmBuildContext & ctx, const Block & indexed_columns_batch) = 0;

    /// Phase 1 companion: per-vector payload ingestion. Called by the
    /// framework immediately after `prepareBuild` for each block, but only
    /// when `supportsPerVectorPayload() == true`. The locator block carries
    /// `source_uuid` (UUID const-folded for the block) and `_part_offset`
    /// (UInt64) columns; rows are aligned 1:1 with the indexed-column block.
    /// `internal_id_offset` is the absolute internal_id of the first row in
    /// this block (so the algorithm can keep payload aligned with the
    /// internal_id space it sees in `prepareBuild`).
    ///
    /// Default no-op: algorithms that do not opt in to payload simply ignore
    /// these calls; the framework's locator columns remain authoritative.
    virtual void prepareBuildLocator(
        const AlgorithmBuildContext & /*ctx*/,
        const Block & /*locator_columns_batch*/,
        UInt64 /*internal_id_offset*/)
    {
    }

    /// Phase 2: index construction. Framework no longer feeds data.
    /// Algorithm reads what it staged (if any) from
    /// `ctx.intermediate_storage` and writes the final index to
    /// `ctx.output_storage`. Must internally poll `ctx.is_cancelled`.
    virtual void buildAlgorithmPrivate(const AlgorithmBuildContext & ctx) = 0;

    /// Phase 3: finalization before `ctx.intermediate_storage` is reclaimed.
    /// For fingerprint / version writing / resource release. Called once.
    virtual void finishBuild(const AlgorithmBuildContext & ctx) = 0;

    virtual std::vector<UInt64> preferredSegmentBoundaries() const { return {}; }

    virtual UInt64 estimateBuildBytes(UInt64 input_source_bytes, UInt64 /*input_source_rows*/) const
    {
        return input_source_bytes;
    }

    virtual UInt64 estimateMergeBytes(UInt64 input_ann_index_bytes, UInt64 /*input_source_rows*/) const
    {
        return input_ann_index_bytes;
    }

    virtual bool supportsMerge() const
    {
        return false;
    }

    /// Returns true iff this algorithm carries per-vector payload
    /// (`{source_uuid, part_offset}`, 24 bytes) inside the index, populates
    /// `InternalHitSet::hot_uuids` / `hot_part_offsets` from `search`, and
    /// consumes the locator block in `prepareBuild`.
    ///
    /// Default false: legacy algorithms that only fill `internal_ids` /
    /// `distances` and rely on the framework's locator-column read to
    /// translate hits into source coordinates.
    ///
    /// Algorithms returning true must still tolerate legacy on-disk indexes
    /// without payload — in that case `search` should leave `hot_uuids` /
    /// `hot_part_offsets` empty so the matcher falls back to locator reads.
    /// Mixed per-hit states are forbidden (see `InternalHitSet` doc).
    virtual bool supportsPerVectorPayload() const
    {
        return false;
    }
};

using ANNIndexAlgorithmPtr = std::unique_ptr<IANNIndexAlgorithm>;

}
