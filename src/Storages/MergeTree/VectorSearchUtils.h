#pragma once
#include <Core/Types.h>
#include <Core/UUID.h>

#include <optional>
#include <vector>
#include <Common/VectorWithMemoryTracking.h>
#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>

namespace DB
{

/// A vehicle to transport elements of the SELECT query into the vector similarity index.
struct VectorSearchParameters
{
    /// Elements of the SELECT query
    String column;
    String distance_function;
    size_t limit;
    VectorWithMemoryTracking<Float64> reference_vector;

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
/// `covered_source_parts` lists every source part the Reflection fully serves
/// for this query — including covered parts with zero hits this turn (which
/// `pruneANNIndexRangesForPart` clears so that vanilla MergeTree does not
/// re-read them and emit fake `_distance = 0` rows).
/// `hits_per_part` carries only parts that actually returned at least one hit.
/// Consumer rule:
///   * uuid in `hits_per_part`     -> attach hits, intersect ranges
///   * uuid in `covered_source_parts` only -> clear ranges (covered, no hits)
///   * uuid in neither             -> leave ranges alone (uncovered)
/// `flat_set` / `flat_map` are used because covered counts are bounded
/// (typically a few hundred), iteration is sequential, and we want
/// cache-friendly contiguous storage rather than hash buckets.
struct ANNIndexHints
{
    boost::container::flat_set<UUID> covered_source_parts;
    boost::container::flat_map<UUID, NearestNeighbours> hits_per_part;
};

}
