#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/DiskANNIndexSearcherAdapter.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <limits>

namespace ProfileEvents
{
    extern const Event DiskANNSearchCount;
    extern const Event DiskANNSearchMicroseconds;
    extern const Event DiskANNSearchResultsReturned;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

DiskANNIndexSearcherAdapter::DiskANNIndexSearcherAdapter(DiskANNDiskIndexSearcherPtr searcher_)
    : searcher(std::move(searcher_))
{
}

std::vector<ANNSearcherHit> DiskANNIndexSearcherAdapter::search(
    const float * query,
    size_t query_dim,
    size_t k) const
{
    if (k == 0)
        return {};

    std::vector<uint64_t> ids(k);
    std::vector<float> distances(k);

    /// Zero means "use the defaults baked into the FFI searcher at construction".
    /// Per-query overrides are intentionally not part of `IANNIndexSearcher` — the knobs
    /// (`search_list_size`, `beam_width`) are DiskANN-specific and no caller currently
    /// supplies per-query values at the group-search level.
    /// TODO: graph-hops / distance-comparisons / disk-IO counters require extending
    /// `diskann_search_disk_index` in `rust/workspace/diskann-clickhouse` to return
    /// per-call statistics. Until then, only call count, wall-time and result count
    /// are observable here.
    Stopwatch watch;
    const size_t found = searcher->search(
        query, query_dim, k, ids.data(), distances.data(),
        /*search_list_size=*/0, /*beam_width=*/0);
    ProfileEvents::increment(ProfileEvents::DiskANNSearchCount);
    ProfileEvents::increment(ProfileEvents::DiskANNSearchMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::DiskANNSearchResultsReturned, found);

    std::vector<ANNSearcherHit> hits;
    hits.reserve(found);
    for (size_t i = 0; i < found; ++i)
    {
        const uint64_t raw = ids[i];
        if (raw > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "DiskANN FFI returned internal_id {} which exceeds UInt32", raw);
        hits.push_back(ANNSearcherHit{static_cast<UInt32>(raw), distances[i]});
    }
    return hits;
}

}

#endif
