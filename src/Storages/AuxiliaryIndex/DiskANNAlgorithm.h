#pragma once

#include "config.h"

#if USE_DISKANN

#include <Storages/AuxiliaryIndex/IAuxiliaryIndexAlgorithm.h>
#include <Storages/AuxiliaryIndex/DiskANNFfi.h>

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>


namespace DB
{

class DiskANNFbinWriter;
class WriteBufferFromFileBase;


/// Build / search backend for the "ann" family using the upstream DiskANN
/// disk-resident PQ index.
///
/// Build pipeline (driven by IAuxiliaryIndexAlgorithm):
///   prepareBuild × N      stream rows into a `vectors.fbin` file under
///                         `intermediate_storage`, polling cancellation
///                         between rows.
///   buildAlgorithmPrivate finalize the fbin, take one final cancellation
///                         poll, then enter the FFI build. The FFI call is
///                         not interruptible; cooperative cancellation only
///                         happens before it.
///   finishBuild           write `algorithm_private_fingerprint.json` next
///                         to the on-disk index files.
class DiskANNAlgorithm final : public IAuxiliaryIndexAlgorithm
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

    void initialize(const AuxiliaryIndexContext & ctx) override;

    std::optional<MatchDescriptor> match(const QueryFeatures & features) const override;
    AlgorithmCostEstimate estimateCost(const MatchDescriptor & desc, const CoverageSnapshot & coverage) const override;
    InternalSearchResult search(
        const MatchDescriptor & desc,
        const ReadyAuxiliaryIndexPartSnapshot & ready_parts,
        size_t candidate_limit,
        ContextPtr query_context) const override;

    void prepareBuild(const AlgorithmBuildContext & ctx, const Block & indexed_columns_batch) override;
    void buildAlgorithmPrivate(const AlgorithmBuildContext & ctx) override;
    void finishBuild(const AlgorithmBuildContext & ctx) override;

    std::unique_ptr<IAuxiliaryIndexAlgorithm> cloneForBuild() const override;

    /// Public so tests can construct an algorithm through the factory and
    /// drive it with explicit parameter strings.
    void setBuildParameters(const ASTPtr & build_params, ContextPtr context);

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
    UInt64 rows_seen_in_build = 0;
    UInt64 rows_since_last_cancel_poll = 0;

    /// Searcher cache: keyed by on-disk `index_prefix`, one open searcher per
    /// AuxiliaryIndex part. Opening a `DiskANNSearcherHandle` involves
    /// reading the disk-index header and constructing a `DiskIndexSearcher`,
    /// which is both expensive and not safe to do concurrently on the same
    /// file path (the upstream factory has shared state during header init).
    /// `search` itself is `&self` on `DiskIndexSearcher` and the underlying
    /// scratch pool is concurrent-safe, so once an entry is in the cache,
    /// any number of threads can drive it in parallel with no locking.
    ///
    /// Mutex covers cache insertion (and the open call on a cache miss).
    /// `shared_ptr` keeps the searcher alive for the duration of any in-
    /// flight search even if a future restructuring evicts the entry.
    mutable std::mutex searcher_cache_mutex;
    mutable std::unordered_map<std::string, std::shared_ptr<DiskANNSearcherHandle>> searcher_cache;
};

}

#endif
