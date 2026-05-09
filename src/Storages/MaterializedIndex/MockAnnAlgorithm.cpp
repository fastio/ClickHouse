#include <Storages/MaterializedIndex/MockAnnAlgorithm.h>
#include <Storages/MaterializedIndex/MaterializedIndexContext.h>

#include <Common/Exception.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
}


void MockAnnAlgorithm::validateBuildParameters(const ASTPtr & build_params, ContextPtr /*context*/)
{
    // Minimal existence check for stage-1: the TYPE(...) clause may be
    // absent (empty argument list is legal for the mock). Real validation
    // is delegated to the DiskANN backend once it registers.
    if (build_params && !typeid_cast<const ASTFunction *>(build_params.get()) && !typeid_cast<const ASTExpressionList *>(build_params.get()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "TYPE parameters for family 'ann' must be a function-call or expression list");
}

void MockAnnAlgorithm::validateIndexedExpression(const ASTPtr & indexed_expression, const StorageInMemoryMetadata & /*source_metadata*/)
{
    if (!indexed_expression)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MaterializedIndex requires at least one indexed column");
}

void MockAnnAlgorithm::initialize(const MaterializedIndexContext & /*ctx*/)
{
    initialized = true;
}

std::optional<MatchDescriptor> MockAnnAlgorithm::match(const QueryFeatures & /*features*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MaterializedIndex query path is not implemented yet");
}

AlgorithmCostEstimate MockAnnAlgorithm::estimateCost(const MatchDescriptor & /*desc*/, const CoverageSnapshot & /*coverage*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MaterializedIndex cost estimation is not implemented yet");
}

SearchResult MockAnnAlgorithm::search(
    const MatchDescriptor & /*desc*/,
    const ReadyMaterializedIndexPartSnapshot & /*ready_parts*/,
    size_t /*candidate_limit*/,
    ContextPtr /*query_context*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MaterializedIndex search is not implemented yet");
}

void MockAnnAlgorithm::prepareBuild(const AlgorithmBuildContext & /*ctx*/, const Block & /*indexed_columns_batch*/)
{
    /// No-op for the mock backend: the framework needs the build phase to
    /// advance through every stage so the rest of the cycle (Transaction
    /// commit, log writes) can be exercised end-to-end. A real ANN backend
    /// will replace this with the actual graph build.
}

void MockAnnAlgorithm::buildAlgorithmPrivate(const AlgorithmBuildContext & /*ctx*/)
{
    /// See `prepareBuild` for the no-op rationale.
}

void MockAnnAlgorithm::finishBuild(const AlgorithmBuildContext & /*ctx*/)
{
    /// See `prepareBuild` for the no-op rationale.
}

}
