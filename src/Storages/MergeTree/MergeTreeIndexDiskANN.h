#pragma once

#include <Storages/IndicesDescription.h>
#include <base/types.h>

namespace DB
{

/// Parameter bag for the `diskann` skip index DDL layer.
///
/// Populated by `ANNIndexParams::fromDescription` from an `IndexDescription` that
/// has already been validated by `diskANNIndexValidator`. Default values match
/// DiskANN CLI conventions. The DDL argument names remain `R` / `L` to stay
/// consistent with the DiskANN paper; the C++ fields use descriptive snake_case.
///   - graph_degree     (DDL: `R`)     = 64
///   - search_list_size (DDL: `L`)     = 100
///   - alpha                            = 1.2 (pruning factor)
///   - pq_bytes                         = 0   (disabled)
///
/// `metric` is intentionally a free-form `String` ('l2' | 'mips' | 'cosine'):
/// the DDL layer accepts all three, but the underlying `DiskANNMetric` enum
/// currently only implements L2 and Cosine. Downstream consumers (task-5 /
/// task-8 / task-10) map `metric` to the enum and surface `NOT_IMPLEMENTED`
/// for unsupported values at build time.
struct ANNIndexParams
{
    String metric;                 /// 'l2' | 'mips' | 'cosine'
    UInt64 dim = 0;                /// > 0
    UInt64 graph_degree = 64;      /// DDL `R`: in [16, 128]
    UInt64 search_list_size = 100; /// DDL `L`: in [graph_degree, 512]
    Float64 alpha = 1.2;           /// in [1.0, 2.0]
    UInt64 pq_bytes = 0;           /// in {0, 8, 16, 32, 64}

    /// Build an `ANNIndexParams` from an `IndexDescription`.
    /// Precondition: `index` has already been through `diskANNIndexValidator`.
    /// Calling on an unvalidated description re-runs the full argument parsing
    /// (including range checks) for defence in depth.
    static ANNIndexParams fromDescription(const IndexDescription & index);
};

/// DDL-time validator for the `diskann` skip index.
/// Registered from `MergeTreeIndices.cpp`; no creator is registered, so this
/// index type is metadata-only in Phase 1 and does not participate in the
/// skip-index query or write pipelines.
///
/// Table-level cross-cutting checks (`enable_block_number_column`,
/// `enable_block_offset_column`, at-most-one `diskann` index per table) live in
/// `MergeTreeData::checkProperties` because the validator signature does not
/// expose `MergeTreeSettings`.
void diskANNIndexValidator(const IndexDescription & index, bool attach);

}
