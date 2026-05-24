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
};

struct BuildParams
{
    /// Mandatory.
    Metric metric = Metric::L2;
    UInt32 dim = 0;

    /// SelectHead.* — head sampling for the BKT head index.
    float  head_ratio              = 0.2f;
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
    UInt32 posting_page_limit          = 12;
    UInt32 posting_vector_limit        = 118;
    UInt32 replica_count               = 8;
    UInt32 num_threads                 = 4;
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
    UInt32 search_posting_page_limit   = 12;
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
