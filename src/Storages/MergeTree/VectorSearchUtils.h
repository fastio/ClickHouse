#pragma once
#include <Core/Types.h>
#include <Core/UUID.h>

#include <optional>
#include <unordered_map>
#include <unordered_set>
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

/// ANNIndex coverage plus per-source-part nearest-neighbour hits.
/// `covered_source_parts` decides which source parts are fully handled by MI.
/// `hits_per_part` contains only rows returned by the current search; a covered
/// part may legitimately have no hit entry.
struct ANNIndexHints
{
    std::unordered_set<UUID> covered_source_parts;
    std::unordered_map<UUID, NearestNeighbours> hits_per_part;
};

}
