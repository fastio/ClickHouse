#pragma once

#include "config.h"

#if USE_CPPDISKANN

#include <base/types.h>

#include <memory>
#include <string>
#include <vector>

namespace DB
{

class CppDiskANNFacade
{
public:
    enum class Metric : UInt8
    {
        L2,
        Cosine,
        InnerProduct,
        CosineNormalized,
    };

    /// Size of the fixed per-node payload `diskann-internal` carries inline in
    /// each graph node (== `sizeof(diskann::NodePayload)`, asserted in the .cpp).
    /// The locator bakes its `(part_id, part_offset)` record into it.
    static constexpr UInt32 PAYLOAD_RECORD_SIZE = 16;

    struct BuildParams
    {
        Metric metric = Metric::L2;
        UInt32 pruned_degree = 56;
        UInt32 max_degree = 56;
        UInt32 l_build = 100;
        float alpha = 1.2f;
        UInt32 num_threads = 0;
        UInt32 pq_chunks = 0;
        std::string build_quantization;
        double pq_code_budget_gb = 0.0;
        double build_ram_limit_gb = 0.0;
        UInt32 disk_pq_dims = 0;
        bool accelerate_build = false;
        UInt32 num_nodes_to_cache = 0;

        /// Optional path to a per-node payload file in the diskann payload-file
        /// format (`[u32 npts][u32 bytes_per_node][npts × bytes_per_node]`, rows
        /// in internal-id order). Empty means "do not bake payload".
        std::string payload_file;
    };

    struct SearchParams
    {
        UInt32 num_threads = 1;
        UInt32 search_list_size = 100;
        UInt32 beam_width = 4;
        UInt32 nodes_to_cache = 0;
    };

    class Searcher;

    static void build(const std::string & data_path, const std::string & index_prefix, const BuildParams & params);

    static std::unique_ptr<Searcher> openSearcher(
        const std::string & index_prefix,
        Metric metric,
        const SearchParams & params);

    static void computeDistances(
        Metric metric,
        UInt32 dim,
        const float * query,
        const float * candidates,
        UInt64 n,
        float * out);
};

class CppDiskANNFacade::Searcher
{
public:
    ~Searcher();

    Searcher(Searcher &&) noexcept;
    Searcher & operator=(Searcher &&) noexcept;

    Searcher(const Searcher &) = delete;
    Searcher & operator=(const Searcher &) = delete;

    UInt64 numPoints() const;
    UInt64 dimensions() const;

    /// Bytes of inline per-node payload this index physically carries (0 if it
    /// was built without payload). Used to decide whether `searchWithPayload` is
    /// available for this index.
    UInt64 payloadBytesPerNode() const;

    UInt32 search(
        const float * query,
        UInt32 dim,
        UInt32 k,
        UInt32 search_list_size,
        UInt32 beam_width,
        UInt64 * results,
        float * distances) const;

    /// Like `search`, but also copies each hit's inline payload (parallel to
    /// `results`) into `payload`, `payload_record_size` bytes per hit. Requires
    /// `payload_record_size == payloadBytesPerNode()`.
    UInt32 searchWithPayload(
        const float * query,
        UInt32 dim,
        UInt32 k,
        UInt32 search_list_size,
        UInt32 beam_width,
        UInt64 * results,
        float * distances,
        UInt8 * payload,
        UInt32 payload_record_size) const;

private:
    friend class CppDiskANNFacade;

    struct Impl;

    explicit Searcher(std::unique_ptr<Impl> impl_);

    std::unique_ptr<Impl> impl;
};

}

#endif
