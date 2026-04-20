#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{

/// Validator for `diskann` group-based vector index.
///
/// Registered as a **validator-only** index type (no creator, no per-part granule path):
/// the `diskann` index is materialised outside the regular `IMergeTreeIndex`
/// pipeline by a dedicated group-based manager.
///
/// Accepts both positional and named arguments, for example:
///   INDEX idx vec TYPE diskann(metric='l2', dim=128)
///   INDEX idx vec TYPE diskann('l2', 128)
///
/// Known keys (all but `metric` and `dim` are optional; unknown keys are rejected):
///   metric              : 'l2' | 'cosine' (required)
///   dim                 : positive UInt64 (required)
///   pq_bytes            : UInt64, optional
///   ann_index_*         : any key prefixed with "ann_index_" is accepted
///                         and forwarded to the group-based builder/searcher.
void diskannIndexValidator(const IndexDescription & index, bool attach);

}
