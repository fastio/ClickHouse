#pragma once

#include "config.h"

#if USE_DISKANN_CPP

#include <Storages/Reflection/ANNIndex/ANNSearcherCache.h>
#include <Storages/Reflection/ANNIndex/DiskANNCppFacade.h>
#include <Storages/Reflection/ANNIndex/IANNIndexAlgorithm.h>

#include <memory>
#include <optional>

namespace DB
{

class DiskANNFbinWriter;
class WriteBufferFromFileBase;

class DiskANNCppAlgorithm final : public IANNIndexAlgorithm
{
public:
    DiskANNCppAlgorithm();
    ~DiskANNCppAlgorithm() override;

    String getName() const override { return "diskann_cpp"; }
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
    void buildAlgorithmPrivate(const AlgorithmBuildContext & ctx) override;
    void finishBuild(const AlgorithmBuildContext & ctx) override;

    std::unique_ptr<IANNIndexAlgorithm> cloneForBuild() const override;

    void setBuildParameters(const ASTPtr & build_params, ContextPtr context);
    size_t searcherCacheSizeForTests() const;

private:
    struct BuildParams
    {
        DiskANNCppFacade::Metric metric = DiskANNCppFacade::Metric::L2;
        UInt32 dim = 0;
        UInt32 pruned_degree = 64;
        UInt32 max_degree = 64;
        UInt32 l_build = 128;
        float alpha = 1.2f;
        UInt32 num_threads = 64;
        UInt32 pq_chunks = 0;
        String build_quantization;
        double build_ram_limit_gb = 128.0;
    };

    static BuildParams parseBuildParameters(const ASTPtr & build_params);
    static String calculateParamsHash(const BuildParams & build_params);
    static std::map<String, String> buildSettings(const BuildParams & build_params);

    bool initialized = false;
    BuildParams params{};
    std::optional<BuildParams> validated_params;

    std::unique_ptr<WriteBufferFromFileBase> fbin_buf;
    std::unique_ptr<DiskANNFbinWriter> fbin_writer;
    UInt64 rows_seen_in_build = 0;
    UInt64 rows_since_last_cancel_poll = 0;

    mutable std::unique_ptr<ANNSearcherCache<DiskANNCppFacade::Searcher>> searcher_cache;
};

}

#endif
