#pragma once

#include "config.h"

#if USE_SPTAG

#include <Core/Types.h>

#include <cstdint>
#include <memory>
#include <vector>


namespace DB::SPANNFacade
{

enum class Metric : UInt8
{
    L2 = 0,
    Cosine = 1,
    InnerProduct = 2,
};

struct BuildParams
{
    /// Mandatory.
    Metric metric = Metric::L2;
    UInt32 dim = 0;

    /// SelectHead.* — head sampling for the BKT head index.
    /// `select_type` picks the head-sampling strategy: SPTAG's default `"BKT"`
    /// builds a BKT and runs the serial `SelectHeadDynamically` binary search
    /// (`SPANNIndex.cpp:827`) — high recall, but linear in head_ratio × N and
    /// single-threaded. `"Random"` skips that step entirely and shuffles the
    /// base set, picking the first `ratio × N` indices in milliseconds.
    String select_type             = "BKT";
    float  head_ratio              = 0.1f;
    UInt32 select_samples_number   = 1000;
    UInt32 select_threshold        = 6;
    UInt32 split_factor            = 5;
    UInt32 split_threshold         = 25;

    /// BuildHead.* — internal BKT graph construction. Forwarded to the inner
    /// BKT `VectorIndex` via `SPANN::Index::SetParameter("BuildHead.*", ...)`.
    UInt32 bkt_number                  = 1;
    UInt32 bkt_kmeans_k                = 32;
    UInt32 bkt_leaf_size               = 8;
    UInt32 neighborhood_size           = 32;
    UInt32 cef                         = 1000;
    UInt32 max_check_for_refine_graph  = 8192;
    UInt32 refine_iterations           = 2;
    UInt32 tpt_number                  = 32;
    float  rng_factor                  = 1.0f;

    /// BuildSSDIndex.* — posting layout (build-time, structural).
    UInt32 posting_page_limit          = 15;
    UInt32 posting_vector_limit        = 118;
    UInt32 replica_count               = 8;
    /// `num_threads` historically wired SelectHead, BuildHead and BuildSSDIndex
    /// to the same value. SPTAG's K-means inside `BKTree::BuildTrees` reaches
    /// `KmeansAssign` with batches of 1000 sample vectors per iteration; at
    /// `_TH=32` each thread gets ~32 distance evaluations, which is dwarfed by
    /// `std::thread` construction (~10–50 µs). The end result is a 1.5-core
    /// workload that scales backwards with thread count. The other two phases
    /// (RNG graph refinement in BuildHead, candidate searching in BuildSSDIndex)
    /// are true parallel and want all available CPU. Splitting the knob lets
    /// the defaults capture both shapes (4 / 32 / 32) without forcing users
    /// to know SPTAG internals.
    UInt32 num_threads                 = 4;
    /// SPTAG `SelectHead.NumberOfThreads`. K-means-on-1k-sample is bound by
    /// `std::thread` overhead; default kept tiny (matches old `num_threads=4`
    /// SPTAG default).
    UInt32 select_head_threads         = 4;
    /// SPTAG `BuildHead.NumberOfThreads`. Drives both the head-set BKTree
    /// build (same K-means overhead pattern as SelectHead) and the RNG graph
    /// TpTree partition (true parallel). Default favours the parallel phase.
    UInt32 build_head_threads          = 32;
    /// `IOThreadsPerHandler` in SPTAG terminology — the size of SPTAG's
    /// internal async-IO workspace pool for `ExtraFileController`. The
    /// SPTAG default of 4 underflows under concurrent search; we raise the
    /// default to 16 and let users override it.
    UInt32 io_threads                  = 16;

    /// BuildSSDIndex.* — disk layout / compression switches (build-time only;
    /// they shape the on-disk posting format and have no search-time twin).
    bool   enable_data_compression       = false;
    bool   enable_delta_encoding         = false;
    bool   enable_posting_list_rearrange = false;

    /// BuildSSDIndex.* — search-time tunables (also overridable via session
    /// settings; the values here are the DDL-baked defaults).
    UInt32 search_posting_page_limit   = 15;
    UInt32 internal_result_num         = 64;
    UInt32 max_check                   = 4096;
    float  max_dist_ratio              = 10000.0f;
    UInt32 hash_table_exponent         = 4;
    UInt32 io_timeout_us               = 30;
};

struct SearchResult
{
    std::vector<UInt64> vids;
    std::vector<float> distances;
};

String metricName(Metric metric);
UInt64 metricId(Metric metric);

class Searcher
{
public:
    Searcher(const String & folder_path, const BuildParams & params_);
    ~Searcher();

    Searcher(const Searcher &) = delete;
    Searcher & operator=(const Searcher &) = delete;

    Searcher(Searcher &&) noexcept;
    Searcher & operator=(Searcher &&) noexcept;

    SearchResult search(const float * query, UInt32 dim, size_t candidate_limit) const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
    BuildParams params;
};

void buildIndex(const BuildParams & params, float * vectors, UInt64 rows, const String & folder_path);

void computeDistances(
    Metric metric,
    UInt32 dim,
    const float * query,
    const float * candidates,
    UInt64 rows,
    float * out);

}

#endif
