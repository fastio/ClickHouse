#pragma once

#include "config.h"

#if USE_DISKANN

#include <Storages/Reflection/ANNIndex/IANNIndexAlgorithm.h>
#include <Storages/Reflection/ANNIndex/DiskANNFfi.h>
#include <Storages/Reflection/ANNIndex/ANNSearcherCache.h>

#include <memory>
#include <optional>


namespace DB
{

class DiskANNFbinWriter;
class WriteBufferFromFileBase;


/// Build / search backend for the "ann" family using the upstream DiskANN
/// disk-resident PQ index.
///
/// Build pipeline (driven by IANNIndexAlgorithm):
///   prepareBuild × N      stream rows into a `vectors.fbin` file under
///                         `intermediate_storage`, polling cancellation
///                         between rows.
///   buildAlgorithmPrivate finalize the fbin, take one final cancellation
///                         poll, then enter the FFI build. The FFI call is
///                         not interruptible; cooperative cancellation only
///                         happens before it.
///   finishBuild           write `algorithm_private_fingerprint.json` next
///                         to the on-disk index files.
class DiskANNAlgorithm final : public IANNIndexAlgorithm
{
public:
    DiskANNAlgorithm();
    ~DiskANNAlgorithm() override;

    String getName() const override { return "diskann"; }
    String getFamily() const override { return "ann"; }
    String getAlgorithmVersion() const override;
    String getBuildParamsHash() const override;

    void validateBuildParameters(const ASTPtr & build_params, ContextPtr context) override;
    void validateIndexedExpression(const ASTPtr & indexed_expression, const StorageInMemoryMetadata & source_metadata) override;

    void initialize(const ANNIndexContext & ctx) override;

    std::optional<MatchDescriptor> match(const QueryFeatures & features) const override;
    AlgorithmCostEstimate estimateCost(const MatchDescriptor & desc, const CoverageSnapshot & coverage) const override;
    std::vector<AlgorithmPrivatePath> getAlgorithmPrivatePaths(const IDataPartStorage & storage) const override;
    std::map<String, String> getAlgorithmObservabilityFields() const override;
    InternalSearchResult search(
        const MatchDescriptor & desc,
        const ReadyANNIndexPartSnapshot & ready_parts,
        size_t candidate_limit,
        ContextPtr query_context) const override;

    void prepareBuild(const AlgorithmBuildContext & ctx, const Block & indexed_columns_batch) override;
    void prepareBuildLocator(
        const AlgorithmBuildContext & ctx,
        const Block & locator_columns_batch,
        UInt64 internal_id_offset) override;
    void buildAlgorithmPrivate(const AlgorithmBuildContext & ctx) override;
    void finishBuild(const AlgorithmBuildContext & ctx) override;

    bool supportsPerVectorPayload() const override { return true; }

    std::unique_ptr<IANNIndexAlgorithm> cloneForBuild() const override;

    /// Public so tests can construct an algorithm through the factory and
    /// drive it with explicit parameter strings.
    void setBuildParameters(const ASTPtr & build_params, ContextPtr context);

    /// For unit tests that exercise the searcher cache directly.
    size_t searcherCacheSizeForTests() const;

private:
    struct BuildParams
    {
        DiskANNMetric metric = DISKANN_METRIC_L2;
        UInt32 dim = 0;
        UInt32 pruned_degree = 64;
        UInt32 max_degree = 64;
        UInt32 l_build = 128;
        float alpha = 1.2f;
        UInt32 num_threads = 64;
        /// 0 means "auto": validateBuildParameters clamps to std::min(16, dim).
        /// The DiskANN backend requires pq_chunks <= dim, so a non-zero default
        /// would silently break every build that uses dim < default.
        UInt32 pq_chunks = 0;
        double build_ram_limit_gb = 128.0;
    };

    static BuildParams parseBuildParameters(const ASTPtr & build_params);
    static String calculateParamsHash(const BuildParams & build_params);

    bool initialized = false;
    BuildParams params{};
    std::optional<BuildParams> validated_params;

    /// Per-build streaming state. Reset by `prepareBuild` on first call,
    /// torn down by `finishBuild`.
    std::unique_ptr<WriteBufferFromFileBase> fbin_buf;
    std::unique_ptr<DiskANNFbinWriter> fbin_writer;
    /// Raw DiskANN associated-data payload writer (8 or 12 B / vector).
    /// Aligned row-for-row with `fbin_buf`; populated by
    /// `prepareBuildLocator`.
    std::unique_ptr<WriteBufferFromFileBase> payload_buf;
    UInt64 rows_seen_in_build = 0;
    UInt64 payload_rows_seen = 0;
    UInt64 rows_since_last_cancel_poll = 0;

    /// Reflection-instance-level cache of opened DiskANN searchers, keyed by
    /// `(part path, build params hash)`. One entry per materialized-index-part
    /// regardless of search-time parameters. Search-time tunables
    /// (`diskann_search_num_threads`, `_io_limit`, `_nodes_to_cache`) are
    /// captured into the searcher at first-open time; subsequent queries
    /// against the same part reuse that searcher even if the query setting
    /// has changed. To take effect of new tunables, `DETACH/ATTACH TABLE`.
    mutable std::unique_ptr<ANNSearcherCache<DiskANNSearcherHandle>> searcher_cache;
};

}

#endif
