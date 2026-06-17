#include <Storages/Reflection/ANNIndex/DiskANNCppFacade.h>

#if USE_DISKANN_CPP

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <immintrin.h>

#include <ann_exception.h>
#include <disk_utils.h>
#include <distance.h>
#include <linux_aligned_file_reader.h>
#include <omp.h>
#include <pq_flash_index.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <sstream>

namespace ProfileEvents
{
    extern const Event ANNIndexDiskANNCppSearchInterfaceMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int EXTERNAL_LIBRARY_ERROR;
}

namespace
{

diskann::Metric toDiskANNMetric(DiskANNCppFacade::Metric metric)
{
    switch (metric)
    {
        case DiskANNCppFacade::Metric::L2:
            return diskann::Metric::L2;
        case DiskANNCppFacade::Metric::Cosine:
        case DiskANNCppFacade::Metric::CosineNormalized:
            return diskann::Metric::COSINE;
        case DiskANNCppFacade::Metric::InnerProduct:
            return diskann::Metric::INNER_PRODUCT;
    }
}

UInt32 buildPQBytesFromQuantization(const std::string & build_quantization)
{
    if (build_quantization == "FP")
        return 0;

    constexpr std::string_view pq_prefix = "PQ_";
    if (build_quantization.starts_with(pq_prefix))
        return static_cast<UInt32>(std::stoul(build_quantization.substr(pq_prefix.size())));

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "DiskANN C++ build_quantization '{}' is not supported by the C++ backend; use FP or PQ_N",
        build_quantization);
}

[[noreturn]] void throwDiskANNCppException(std::string_view context, const std::exception & e)
{
    throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "DiskANN C++ {} failed: {}", context, e.what());
}

void checkBuildResult(int code, std::string_view context)
{
    if (code != 0)
        throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "DiskANN C++ {} failed with status {}", context, code);
}

class OpenMPThreadSettingsGuard
{
public:
    explicit OpenMPThreadSettingsGuard(UInt32 num_threads)
        : previous_dynamic(omp_get_dynamic())
        , previous_max_threads(omp_get_max_threads())
    {
        omp_set_dynamic(0);
        omp_set_num_threads(static_cast<int>(num_threads));
    }

    ~OpenMPThreadSettingsGuard()
    {
        omp_set_num_threads(previous_max_threads);
        omp_set_dynamic(previous_dynamic);
    }

private:
    int previous_dynamic;
    int previous_max_threads;
};

}

struct DiskANNCppFacade::Searcher::Impl
{
    std::shared_ptr<AlignedFileReader> reader;
    std::unique_ptr<diskann::PQFlashIndex<float>> index;
    UInt64 dim = 0;
    UInt64 points = 0;
};

DiskANNCppFacade::Searcher::Searcher(std::unique_ptr<Impl> impl_)
    : impl(std::move(impl_))
{
}

DiskANNCppFacade::Searcher::~Searcher() = default;
DiskANNCppFacade::Searcher::Searcher(Searcher &&) noexcept = default;
DiskANNCppFacade::Searcher & DiskANNCppFacade::Searcher::operator=(Searcher &&) noexcept = default;

UInt64 DiskANNCppFacade::Searcher::numPoints() const
{
    return impl->points;
}

UInt64 DiskANNCppFacade::Searcher::dimensions() const
{
    return impl->dim;
}

UInt32 DiskANNCppFacade::Searcher::search(
    const float * query,
    UInt32 dim,
    UInt32 k,
    UInt32 search_list_size,
    UInt32 beam_width,
    UInt32 search_io_limit,
    UInt64 * results,
    float * distances) const
{
    if (dim != impl->dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ query dimension {} does not match index dimension {}", dim, impl->dim);

    try
    {
        const UInt32 io_limit = search_io_limit == 0 ? std::numeric_limits<UInt32>::max() : search_io_limit;
        const UInt32 effective_k = static_cast<UInt32>(std::min<UInt64>(k, impl->points));
        if (effective_k == 0)
            return 0;
        Stopwatch watch;
        impl->index->cached_beam_search(query, effective_k, search_list_size, results, distances, beam_width, io_limit);
        ProfileEvents::increment(ProfileEvents::ANNIndexDiskANNCppSearchInterfaceMicroseconds, watch.elapsedMicroseconds());
        return effective_k;
    }
    catch (const std::exception & e)
    {
        throwDiskANNCppException("search", e);
    }
}

void DiskANNCppFacade::build(const std::string & data_path, const std::string & index_prefix, const BuildParams & params)
{
    if (params.pruned_degree != params.max_degree)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN C++ backend exposes a single graph degree; pruned_degree ({}) must equal max_degree ({})",
            params.pruned_degree,
            params.max_degree);
    if (params.alpha != 1.2f)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN C++ backend does not expose alpha; only the default 1.2 is supported, got {}",
            params.alpha);

    const auto metric = toDiskANNMetric(params.metric);
    const UInt32 disk_pq_bytes = params.pq_chunks;
    const UInt32 build_pq_bytes = buildPQBytesFromQuantization(params.build_quantization);
    const UInt32 quantized_dimension = 0;
    const bool append_reorder_data = false;

    std::ostringstream build_params;
    build_params
        << params.max_degree << ' '
        << params.l_build << ' '
        << 0.25 << ' '
        << params.build_ram_limit_gb << ' '
        << params.num_threads << ' '
        << disk_pq_bytes << ' '
        << append_reorder_data << ' '
        << build_pq_bytes << ' '
        << quantized_dimension;

    try
    {
        OpenMPThreadSettingsGuard openmp_guard(params.num_threads);
        checkBuildResult(
            diskann::build_disk_index<float>(
                data_path.c_str(),
                index_prefix.c_str(),
                build_params.str().c_str(),
                metric,
                false),
            "build");
    }
    catch (const std::exception & e)
    {
        throwDiskANNCppException("build", e);
    }
}

std::unique_ptr<DiskANNCppFacade::Searcher> DiskANNCppFacade::openSearcher(
    const std::string & index_prefix,
    Metric metric,
    const SearchParams & params)
{
    auto impl = std::make_unique<Searcher::Impl>();
    impl->reader = std::make_shared<LinuxAlignedFileReader>();
    impl->index = std::make_unique<diskann::PQFlashIndex<float>>(impl->reader, toDiskANNMetric(metric));

    try
    {
        OpenMPThreadSettingsGuard openmp_guard(params.num_threads);
        const int status = impl->index->load(params.num_threads, index_prefix.c_str());
        if (status != 0)
            throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "DiskANN C++ open failed with status {}", status);

        if (params.nodes_to_cache > 0)
        {
            std::vector<UInt32> node_list;
            impl->index->cache_bfs_levels(params.nodes_to_cache, node_list);
            impl->index->load_cache_list(node_list);
        }

        impl->dim = impl->index->get_data_dim();
        impl->points = impl->index->get_num_points();
    }
    catch (const std::exception & e)
    {
        throwDiskANNCppException("open", e);
    }

    return std::unique_ptr<Searcher>(new Searcher(std::move(impl)));
}

void DiskANNCppFacade::computeDistances(
    Metric metric,
    UInt32 dim,
    const float * query,
    const float * candidates,
    UInt64 n,
    float * out)
{
    switch (metric)
    {
        case Metric::L2:
        {
            for (UInt64 row = 0; row < n; ++row)
            {
                float result = 0;
                const auto * candidate = candidates + row * dim;
                for (UInt32 i = 0; i < dim; ++i)
                {
                    const float diff = query[i] - candidate[i];
                    result += diff * diff;
                }
                out[row] = result;
            }
            return;
        }
        case Metric::Cosine:
        case Metric::CosineNormalized:
        {
            float query_norm = 0;
            for (UInt32 i = 0; i < dim; ++i)
                query_norm += query[i] * query[i];
            query_norm = std::sqrt(query_norm);

            for (UInt64 row = 0; row < n; ++row)
            {
                float dot = 0;
                float candidate_norm = 0;
                const auto * candidate = candidates + row * dim;
                for (UInt32 i = 0; i < dim; ++i)
                {
                    dot += query[i] * candidate[i];
                    candidate_norm += candidate[i] * candidate[i];
                }
                candidate_norm = std::sqrt(candidate_norm);
                out[row] = (query_norm == 0 || candidate_norm == 0)
                    ? 1.0f
                    : 1.0f - dot / (query_norm * candidate_norm);
            }
            return;
        }
        case Metric::InnerProduct:
        {
            for (UInt64 row = 0; row < n; ++row)
            {
                float dot = 0;
                const auto * candidate = candidates + row * dim;
                for (UInt32 i = 0; i < dim; ++i)
                    dot += query[i] * candidate[i];
                out[row] = -dot;
            }
            return;
        }
    }
}

}

#endif
