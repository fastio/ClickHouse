#pragma once

#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/IANNIndexSearcher.h>
#include <Storages/MergeTree/DiskANNIndex.h>

namespace DB
{

/// DiskANN tuning knobs wrapped as an `IANNSearchDefaults` so they can flow through the
/// algorithm-neutral layers (`ANNIndexManager::Config`, `ANNIndexDefinition`, `ANNIndexGroup`)
/// without exposing `DiskANNSearchOptions` outside of `DiskANN*` translation units.
struct DiskANNSearchDefaults : public IANNSearchDefaults
{
    DiskANNSearchOptions options;

    DiskANNSearchDefaults() = default;
    explicit DiskANNSearchDefaults(DiskANNSearchOptions options_) : options(options_) {}
};

/// Adapts a `DiskANNDiskIndexSearcher` to `IANNIndexSearcher`. The adapter translates the
/// FFI-style `search(query, k, ids, distances, ...)` into the interface's return-by-value
/// form and fixes the per-query tuning to zero (meaning: use the defaults baked into the
/// on-disk index and the options handed to the FFI searcher at construction).
class DiskANNIndexSearcherAdapter : public IANNIndexSearcher
{
public:
    explicit DiskANNIndexSearcherAdapter(DiskANNDiskIndexSearcherPtr searcher_);

    std::vector<ANNSearcherHit> search(
        const float * query,
        size_t query_dim,
        size_t k) const override;

private:
    DiskANNDiskIndexSearcherPtr searcher;
};

}

#endif
