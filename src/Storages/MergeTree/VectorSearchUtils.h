#pragma once
#include <Core/Types.h>
#include <Core/UUID.h>

#include <optional>
#include <unordered_map>
#include <vector>

namespace DB
{

/// A vehicle to transport elements of the SELECT query into the vector similarity index.
struct VectorSearchParameters
{
    /// Elements of the SELECT query
    String column;
    String distance_function;
    size_t limit;
    std::vector<Float64> reference_vector;

    /// Other metadata
    bool additional_filters_present; /// SELECT contains a WHERE or PREWHERE clause
    bool return_distances;
};

using OptionalVectorSearchParameters = std::optional<VectorSearchParameters>;

struct NearestNeighbours
{
    std::vector<UInt64> rows;
    std::optional<std::vector<float>> distances;
};

/// Per-source-part nearest-neighbour results produced by a MaterializedIndex search.
/// Transported from the optimizer into ReadFromMergeTree and then into
/// RangesInDataPartReadHints::mi_search_results by matching on the data part UUID.
struct MaterializedIndexHints
{
    std::unordered_map<UUID, NearestNeighbours> per_part;
};

}
