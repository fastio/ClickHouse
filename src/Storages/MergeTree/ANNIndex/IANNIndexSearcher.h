#pragma once

#include <Core/Types.h>

#include <memory>
#include <string>
#include <vector>

namespace DB
{

struct ANNIndexShapeFingerprint;

/// Hit returned by `IANNIndexSearcher::search`. `internal_id` is the per-group vertex id
/// assigned by the concrete algorithm and is later mapped back to the source `PartRowId`
/// by the group-level `PartRowIdMapReader`. Distinct from the table-level `ANNSearchHit`
/// in `ANNIndexManager.h`, which carries an already-resolved `PartRowId`.
struct ANNSearcherHit
{
    UInt32 internal_id;
    float distance;
};

/// Algorithm-agnostic per-index search defaults, produced by the DDL parser and consumed by
/// the searcher factory. Concrete algorithms attach their tuning knobs in subclasses
/// (e.g. `DiskANNSearchDefaults`). Owners hold a `ANNSearchDefaultsPtr`; defaults are
/// immutable once constructed so sharing by `shared_ptr` is safe and keeps the enclosing
/// structs copyable.
class IANNSearchDefaults
{
public:
    virtual ~IANNSearchDefaults() = default;
};
using ANNSearchDefaultsPtr = std::shared_ptr<IANNSearchDefaults>;

/// Per-group vector searcher. Concrete implementations bundle the FFI state and the tuning
/// defaults supplied at construction time — per-query tuning is not part of the contract
/// because the relevant knobs (`search_list_size`, `beam_width`, posting-list fanout, ...)
/// are algorithm-specific.
class IANNIndexSearcher
{
public:
    virtual ~IANNIndexSearcher() = default;

    virtual std::vector<ANNSearcherHit> search(
        const float * query,
        size_t query_dim,
        size_t k) const = 0;
};
using IANNIndexSearcherPtr = std::shared_ptr<IANNIndexSearcher>;

/// Open a searcher for an already-built group. Dispatches on `shape.algorithm`; currently
/// only `"diskann"` is supported. Throws `NOT_IMPLEMENTED` for unknown algorithms and
/// `LOGICAL_ERROR` if `defaults` has a concrete type that does not match `shape.algorithm`.
IANNIndexSearcherPtr createANNIndexSearcher(
    const ANNIndexShapeFingerprint & shape,
    const std::string & index_directory,
    const IANNSearchDefaults & defaults);

}
