#pragma once

#include <Storages/MaterializedIndex/IMaterializedIndexAlgorithm.h>


namespace DB
{

/// Stage-1 placeholder for the "ann" family: lets the CREATE path exercise
/// the factory / validate chain end-to-end without any real index build.
/// Replaced by the DiskANN backend once the background task plumbing lands.
class MockAnnAlgorithm final : public IMaterializedIndexAlgorithm
{
public:
    MockAnnAlgorithm() = default;

    String getName() const override { return "MockAnn"; }
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

    void buildAlgorithmPrivate(const AlgorithmBuildContext & ctx, const Block & indexed_columns_batch) override;

private:
    bool initialized = false;
};

}
