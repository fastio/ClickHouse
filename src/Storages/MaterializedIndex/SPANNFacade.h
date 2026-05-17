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
    Metric metric = Metric::L2;
    UInt32 dim = 0;
    float head_ratio = 0.2f;
    UInt32 posting_page_limit = 12;
    UInt32 search_posting_page_limit = 12;
    UInt32 internal_result_num = 64;
    UInt32 replica_count = 8;
    UInt32 num_threads = 4;
    UInt32 max_check = 4096;
    /// `IOThreadsPerHandler` in SPTAG terminology — the size of SPTAG's
    /// internal async-IO workspace pool for `ExtraFileController`. The
    /// SPTAG default of 4 underflows under concurrent search; we raise the
    /// default to 16 and let users override it.
    UInt32 io_threads = 16;
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
