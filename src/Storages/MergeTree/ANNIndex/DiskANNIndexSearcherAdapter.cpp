#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/DiskANNIndexSearcherAdapter.h>

#include <Common/Exception.h>

#include <limits>

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
    const size_t found = searcher->search(
        query, query_dim, k, ids.data(), distances.data(),
        /*search_list_size=*/0, /*beam_width=*/0);

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
