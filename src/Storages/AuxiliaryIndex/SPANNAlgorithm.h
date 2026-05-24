#pragma once

#include "config.h"

#if USE_SPTAG

#include <Storages/AuxiliaryIndex/IAuxiliaryIndexAlgorithm.h>
#include <Storages/AuxiliaryIndex/SPANNFacade.h>

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>


namespace DB
{

class WriteBufferFromFileBase;


class SPANNAlgorithm final : public IAuxiliaryIndexAlgorithm
{
public:
    SPANNAlgorithm();
    ~SPANNAlgorithm() override;

    String getName() const override { return "spann"; }
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

    void setBuildParameters(const ASTPtr & build_params, ContextPtr context);

    UInt64 estimateBuildBytes(UInt64 input_source_bytes, UInt64 input_source_rows) const override;

    size_t searcherCacheSizeForTests() const;

private:
    using BuildParams = SPANNFacade::BuildParams;

    static BuildParams parseBuildParameters(const ASTPtr & build_params);
    static String calculateParamsHash(const BuildParams & build_params);

    bool initialized = false;
    BuildParams params{};
    std::optional<BuildParams> validated_params;

    std::vector<Float32> build_vectors;
    UInt64 rows_seen_in_build = 0;
    UInt64 rows_since_last_cancel_poll = 0;
    bool build_started = false;

    mutable std::mutex searcher_cache_mutex;
    mutable std::unordered_map<std::string, std::shared_ptr<SPANNFacade::Searcher>> searcher_cache;
};

}

#endif
