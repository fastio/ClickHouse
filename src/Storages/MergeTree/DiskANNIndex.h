#pragma once
#include "config.h"
#if USE_DISKANN

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace DB
{

enum class DiskANNMetric : uint8_t
{
    L2 = 0,
    Cosine = 1,
};

struct DiskANNBuildOptions
{
    uint32_t pruned_degree = 32;
    uint32_t max_degree = 64;
    uint32_t l_build = 128;
    float alpha = 1.2f;
    uint32_t num_threads = 1;
    uint32_t pq_chunks = 4;
    double build_ram_limit_gb = 0.25;
};

struct DiskANNSearchOptions
{
    uint32_t num_threads = 1;
    uint32_t search_io_limit = 4;
    uint32_t num_nodes_to_cache = 0;
    uint32_t default_search_list_size = 64;
    uint32_t default_beam_width = 4;
};

class DiskANNDiskIndexBuilder
{
public:
    DiskANNDiskIndexBuilder(
        size_t dimensions,
        DiskANNMetric metric,
        DiskANNBuildOptions options = {});

    ~DiskANNDiskIndexBuilder();

    DiskANNDiskIndexBuilder(const DiskANNDiskIndexBuilder &) = delete;
    DiskANNDiskIndexBuilder & operator=(const DiskANNDiskIndexBuilder &) = delete;
    DiskANNDiskIndexBuilder(DiskANNDiskIndexBuilder && other) noexcept;
    DiskANNDiskIndexBuilder & operator=(DiskANNDiskIndexBuilder && other) noexcept;

    void setDataPath(const std::string & path);
    void setIndexPrefix(const std::string & prefix);
    void build() const;

    static bool indexFilesExist(const std::string & index_prefix);

private:
    int64_t handle = -1;
    size_t dim;

    [[noreturn]] static void throwFromFFIError(const std::string & context);
};

class DiskANNDiskIndexSearcher
{
public:
    DiskANNDiskIndexSearcher(
        size_t dimensions,
        DiskANNMetric metric,
        const std::string & index_prefix,
        DiskANNSearchOptions options = {});

    ~DiskANNDiskIndexSearcher();

    DiskANNDiskIndexSearcher(const DiskANNDiskIndexSearcher &) = delete;
    DiskANNDiskIndexSearcher & operator=(const DiskANNDiskIndexSearcher &) = delete;
    DiskANNDiskIndexSearcher(DiskANNDiskIndexSearcher && other) noexcept;
    DiskANNDiskIndexSearcher & operator=(DiskANNDiskIndexSearcher && other) noexcept;

    size_t search(
        const float * query,
        size_t query_dim,
        size_t k,
        uint64_t * ids,
        float * distances,
        size_t search_list_size = 0,
        size_t beam_width = 0) const;

private:
    int64_t handle = -1;
    size_t dim;
    DiskANNSearchOptions options;

    [[noreturn]] static void throwFromFFIError(const std::string & context);
};

using DiskANNDiskIndexBuilderPtr = std::shared_ptr<DiskANNDiskIndexBuilder>;
using DiskANNDiskIndexSearcherPtr = std::shared_ptr<DiskANNDiskIndexSearcher>;

}

#endif
