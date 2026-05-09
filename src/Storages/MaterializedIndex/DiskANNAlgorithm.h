#pragma once

#include "config.h"

#if USE_DISKANN

#include <Storages/MaterializedIndex/IMaterializedIndexAlgorithm.h>
#include <Storages/MaterializedIndex/DiskANNFfi.h>

#include <memory>
#include <optional>


namespace DB
{

class DiskANNFbinWriter;
class WriteBufferFromFileBase;


/// Build / search backend for the "ann" family using the upstream DiskANN
/// disk-resident PQ index.
///
/// Build pipeline (driven by IMaterializedIndexAlgorithm):
///   prepareBuild × N      stream rows into a `vectors.fbin` file under
///                         `intermediate_storage`, polling cancellation
///                         between rows.
///   buildAlgorithmPrivate finalize the fbin, take one final cancellation
///                         poll, then enter the FFI build. The FFI call is
///                         not interruptible; cooperative cancellation only
///                         happens before it.
///   finishBuild           write `algorithm_private/fingerprint.json` next
///                         to the on-disk index files.
class DiskANNAlgorithm final : public IMaterializedIndexAlgorithm
{
public:
    DiskANNAlgorithm();
    ~DiskANNAlgorithm() override;

    String getName() const override { return "diskann"; }
    String getFamily() const override { return "ann"; }

    void validateBuildParameters(const ASTPtr & build_params, ContextPtr context) override;
    void validateIndexedExpression(const ASTPtr & indexed_expression, const StorageInMemoryMetadata & source_metadata) override;

    void initialize(const MaterializedIndexContext & ctx) override;

    std::optional<MatchDescriptor> match(const QueryFeatures & features) const override;
    AlgorithmCostEstimate estimateCost(const MatchDescriptor & desc, const CoverageSnapshot & coverage) const override;
    SearchResult search(
        const MatchDescriptor & desc,
        const ReadyMaterializedIndexPartSnapshot & ready_parts,
        size_t candidate_limit,
        ContextPtr query_context) const override;

    void prepareBuild(const AlgorithmBuildContext & ctx, const Block & indexed_columns_batch) override;
    void buildAlgorithmPrivate(const AlgorithmBuildContext & ctx) override;
    void finishBuild(const AlgorithmBuildContext & ctx) override;

    /// Public so tests can construct an algorithm through the factory and
    /// drive it with explicit parameter strings.
    void setBuildParameters(const ASTPtr & build_params, ContextPtr context);

private:
    struct BuildParams
    {
        DiskANNMetric metric = DISKANN_METRIC_L2;
        UInt32 dim = 0;
        UInt32 pruned_degree = 32;
        UInt32 max_degree = 64;
        UInt32 l_build = 128;
        float alpha = 1.2f;
        UInt32 num_threads = 4;
        UInt32 pq_chunks = 4;
        double build_ram_limit_gb = 1.0;
    };

    static BuildParams parseBuildParameters(const ASTPtr & build_params);

    bool initialized = false;
    BuildParams params{};
    std::optional<BuildParams> validated_params;

    /// Per-build streaming state. Reset by `prepareBuild` on first call,
    /// torn down by `finishBuild`.
    std::unique_ptr<WriteBufferFromFileBase> fbin_buf;
    std::unique_ptr<DiskANNFbinWriter> fbin_writer;
    UInt64 rows_seen_in_build = 0;
    UInt64 rows_since_last_cancel_poll = 0;
};

}

#endif
