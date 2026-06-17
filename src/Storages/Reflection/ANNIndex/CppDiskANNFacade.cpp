#include <Storages/Reflection/ANNIndex/CppDiskANNFacade.h>

#if USE_CPPDISKANN

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <cppdiskann_blas_shim.h>
#include <diskann/ann_exception.h>
#include <diskann/aux_utils.h>
#include <diskann/distance.h>
#include <diskann/linux_aligned_file_reader.h>
#include <diskann/pq_flash_index.h>

#include <omp.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <mutex>
#include <sstream>

namespace ProfileEvents
{
    extern const Event ANNIndexCppDiskANNSearchInterfaceMicroseconds;
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

diskann::Metric toDiskANNMetric(CppDiskANNFacade::Metric metric)
{
    switch (metric)
    {
        case CppDiskANNFacade::Metric::L2:
            return diskann::Metric::L2;
        case CppDiskANNFacade::Metric::Cosine:
        case CppDiskANNFacade::Metric::CosineNormalized:
            return diskann::Metric::COSINE;
        case CppDiskANNFacade::Metric::InnerProduct:
            return diskann::Metric::INNER_PRODUCT;
    }
}

std::string_view diskANNMetricName(diskann::Metric metric)
{
    switch (metric)
    {
        case diskann::Metric::L2:
            return "L2";
        case diskann::Metric::COSINE:
            return "COSINE";
        case diskann::Metric::INNER_PRODUCT:
            return "INNER_PRODUCT";
    }
    return "UNKNOWN";
}

void validateBuildQuantization(const std::string & build_quantization)
{
    if (build_quantization == "FP")
        return;

    constexpr std::string_view pq_prefix = "PQ_";
    if (build_quantization.starts_with(pq_prefix))
        return;

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "cppdiskann build_quantization '{}' is not supported by the diskann-internal backend; use FP or PQ_N",
        build_quantization);
}

[[noreturn]] void throwCppDiskANNException(std::string_view context, const std::exception & e)
{
    throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "cppdiskann {} failed: {}", context, e.what());
}

void initializeCppDiskANNLogging()
{
    static std::once_flag init_flag;
    std::call_once(init_flag, []
    {
        cppdiskann::setLogCallback([](cppdiskann::LogLevel level, std::string_view message)
        {
            while (!message.empty() && (message.back() == '\n' || message.back() == '\r'))
                message.remove_suffix(1);
            if (message.empty())
                return;

            auto logger = getLogger("CppDiskANN");
            switch (level)
            {
                case cppdiskann::LogLevel::Debug:
                    LOG_DEBUG(logger, "{}", message);
                    break;
                case cppdiskann::LogLevel::Info:
                    LOG_INFO(logger, "{}", message);
                    break;
                case cppdiskann::LogLevel::Warning:
                    LOG_WARNING(logger, "{}", message);
                    break;
                case cppdiskann::LogLevel::Error:
                    LOG_ERROR(logger, "{}", message);
                    break;
            }
        });
    });
}

void checkBuildResult(int code, std::string_view context)
{
    if (code != 0)
        throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "cppdiskann {} failed with status {}", context, code);
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

struct CppDiskANNFacade::Searcher::Impl
{
    std::shared_ptr<AlignedFileReader> reader;
    std::unique_ptr<diskann::PQFlashIndex<float>> index;
    UInt64 dim = 0;
    UInt64 points = 0;
};

CppDiskANNFacade::Searcher::Searcher(std::unique_ptr<Impl> impl_)
    : impl(std::move(impl_))
{
}

CppDiskANNFacade::Searcher::~Searcher() = default;
CppDiskANNFacade::Searcher::Searcher(Searcher &&) noexcept = default;
CppDiskANNFacade::Searcher & CppDiskANNFacade::Searcher::operator=(Searcher &&) noexcept = default;

UInt64 CppDiskANNFacade::Searcher::numPoints() const
{
    return impl->points;
}

UInt64 CppDiskANNFacade::Searcher::dimensions() const
{
    return impl->dim;
}

UInt32 CppDiskANNFacade::Searcher::search(
    const float * query,
    UInt32 dim,
    UInt32 k,
    UInt32 search_list_size,
    UInt32 beam_width,
    UInt64 * results,
    float * distances) const
{
    initializeCppDiskANNLogging();

    if (dim != impl->dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann query dimension {} does not match index dimension {}", dim, impl->dim);

    try
    {
        const UInt32 effective_k = static_cast<UInt32>(std::min<UInt64>(k, impl->points));
        if (effective_k == 0)
            return 0;

        std::vector<int64_t> internal_results(effective_k, -1);
        Stopwatch watch;
        impl->index->cached_beam_search(
            query,
            effective_k,
            search_list_size,
            internal_results.data(),
            distances,
            beam_width);
        ProfileEvents::increment(ProfileEvents::ANNIndexCppDiskANNSearchInterfaceMicroseconds, watch.elapsedMicroseconds());

        UInt32 written = 0;
        for (UInt32 i = 0; i < effective_k; ++i)
        {
            if (internal_results[i] < 0)
                continue;
            results[written] = static_cast<UInt64>(internal_results[i]);
            distances[written] = distances[i];
            ++written;
        }
        return written;
    }
    catch (const std::exception & e)
    {
        throwCppDiskANNException("search", e);
    }
}

void CppDiskANNFacade::build(const std::string & data_path, const std::string & index_prefix, const BuildParams & params)
{
    initializeCppDiskANNLogging();

    if (params.pruned_degree != params.max_degree)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "cppdiskann exposes a single graph degree; pruned_degree ({}) must equal max_degree ({})",
            params.pruned_degree,
            params.max_degree);
    if (params.alpha != 1.2f)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "cppdiskann does not expose alpha; only the default 1.2 is supported, got {}",
            params.alpha);
    validateBuildQuantization(params.build_quantization);

    diskann::BuildConfig config;
    config.data_file_path = data_path;
    config.index_file_path = index_prefix;
    config.compare_metric = toDiskANNMetric(params.metric);
    config.max_degree = params.max_degree;
    config.search_list_size = params.l_build;
    config.pq_code_size_gb = params.pq_code_budget_gb;
    config.index_mem_gb = params.build_ram_limit_gb;
    config.disk_pq_dims = params.disk_pq_dims;
    config.reorder = false;
    config.accelerate_build = params.accelerate_build;
    config.num_nodes_to_cache = params.num_nodes_to_cache;

    LOG_INFO(
        getLogger("CppDiskANNFacade"),
        "cppdiskann final diskann BuildConfig: data_file_path='{}', index_file_path='{}', compare_metric={}, max_degree={}, "
        "search_list_size={}, pq_code_size_gb={}, index_mem_gb={}, disk_pq_dims={}, reorder={}, accelerate_build={}, "
        "num_nodes_to_cache={}, num_threads={}",
        config.data_file_path,
        config.index_file_path,
        diskANNMetricName(config.compare_metric),
        config.max_degree,
        config.search_list_size,
        config.pq_code_size_gb,
        config.index_mem_gb,
        config.disk_pq_dims,
        config.reorder,
        config.accelerate_build,
        config.num_nodes_to_cache,
        params.num_threads);

    try
    {
        OpenMPThreadSettingsGuard openmp_guard(params.num_threads);
        checkBuildResult(diskann::build_disk_index<float>(config), "build");
    }
    catch (const std::exception & e)
    {
        throwCppDiskANNException("build", e);
    }
}

std::unique_ptr<CppDiskANNFacade::Searcher> CppDiskANNFacade::openSearcher(
    const std::string & index_prefix,
    Metric metric,
    const SearchParams & params)
{
    initializeCppDiskANNLogging();

    auto impl = std::make_unique<Searcher::Impl>();
    impl->reader = std::make_shared<LinuxAlignedFileReader>();
    impl->index = std::make_unique<diskann::PQFlashIndex<float>>(impl->reader, toDiskANNMetric(metric));

    try
    {
        OpenMPThreadSettingsGuard openmp_guard(params.num_threads);
        const int status = impl->index->load(params.num_threads, index_prefix.c_str());
        if (status != 0)
            throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "cppdiskann open failed with status {}", status);

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
        throwCppDiskANNException("open", e);
    }

    return std::unique_ptr<Searcher>(new Searcher(std::move(impl)));
}

void CppDiskANNFacade::computeDistances(
    Metric metric,
    UInt32 dim,
    const float * query,
    const float * candidates,
    UInt64 n,
    float * out)
{
    initializeCppDiskANNLogging();

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
