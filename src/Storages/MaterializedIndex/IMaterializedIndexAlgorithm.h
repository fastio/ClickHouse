#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>

#include <memory>
#include <optional>
#include <vector>


namespace DB
{

class Block;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

struct MaterializedIndexContext;

// Placeholder types filled in when build / search paths wire up. Stage-1
// needs complete definitions so that std::optional<T>, pass-by-value and
// by-reference all compile even though none of the fields are read yet.
struct QueryFeatures {};
struct MatchDescriptor {};
struct AlgorithmCostEstimate {};
struct SearchResult {};
struct CoverageSnapshot {};
struct AlgorithmBuildContext {};
struct ReadyMaterializedIndexPartSnapshot {};


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

    virtual void buildAlgorithmPrivate(const AlgorithmBuildContext & ctx, const Block & indexed_columns_batch) = 0;

    virtual std::vector<UInt64> preferredSegmentBoundaries() const { return {}; }
};

using MaterializedIndexAlgorithmPtr = std::unique_ptr<IMaterializedIndexAlgorithm>;

}
