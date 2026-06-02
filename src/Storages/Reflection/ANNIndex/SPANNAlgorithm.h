#pragma once

#include "config.h"

#if USE_SPTAG

#include <Storages/Reflection/ANNIndex/IANNIndexAlgorithm.h>
#include <Storages/Reflection/ANNIndex/SPANNFacade.h>
#include <Storages/Reflection/ANNIndex/ANNSearcherCache.h>

#include <memory>
#include <optional>
#include <vector>


namespace DB
{

class WriteBufferFromFileBase;


class SPANNAlgorithm final : public IANNIndexAlgorithm
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
    /// Per-vector inline payload, packed by `ANNIndexPayloadCodec` (8 or 12
    /// B / record depending on `payload_format_version` baked in at build
    /// start). Mirrors `build_vectors` row-for-row when
    /// `prepareBuildLocator` is being called, otherwise stays empty (legacy
    /// build path). Consumed by `buildAlgorithmPrivate` via SPTAG's
    /// `MemMetadataSet` and persisted as `metadata.bin` + `metadataIndex.bin`.
    std::vector<uint8_t> build_payloads;
    /// Captured from `AlgorithmBuildContext::payload_format_version` on the
    /// first `prepareBuild` call. Drives `recordSize` for both the in-memory
    /// `build_payloads` buffer and the SPTAG metadata write.
    ANNIndexPayloadCodec::Version build_payload_version
        = ANNIndexPayloadCodec::Version::V1_OFFSET32;
    UInt64 rows_seen_in_build = 0;
    UInt64 rows_since_last_cancel_poll = 0;
    bool build_started = false;

    /// Reflection-instance-level cache of opened SPANN searchers, keyed by
    /// `(part path, build params hash)`. SPTAG `SearchIndex(QueryResult&)` is
    /// `const` and uses a per-thread workspace pool, so any number of threads
    /// can drive the cached `Searcher` in parallel without external locking.
    /// Search-time tunables (`spann_search_*`) are baked into the searcher
    /// at first-open time and stay fixed for that handle's lifetime; changing
    /// them requires `DETACH/ATTACH TABLE`.
    mutable std::unique_ptr<ANNSearcherCache<SPANNFacade::Searcher>> searcher_cache;
};

}

#endif
