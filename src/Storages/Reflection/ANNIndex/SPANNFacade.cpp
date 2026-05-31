#include <Storages/Reflection/ANNIndex/SPANNFacade.h>

#if USE_SPTAG

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <inc/Core/Common.h>
#include <inc/Core/SearchQuery.h>
#include <inc/Core/VectorIndex.h>
#include <inc/Helper/Logging.h>

#include <fmt/format.h>

#include <algorithm>
#include <cmath>
#include <cstdarg>
#include <cstdio>
#include <limits>
#include <memory>
#include <mutex>
#include <string_view>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int EXTERNAL_LIBRARY_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace SPANNFacade
{
namespace
{

/// SPTAG's default `SimpleLogger` spams `printf("[1] Using AVX512 ...")` and
/// every build/search status line to **stdout**, which corrupts
/// `clickhouse-client` query output (the AVX banner alone makes 04195's
/// `.reference` diff fail). Route every line through a Poco logger named
/// `SPANN` instead: it goes to `system.text_log` / `server.log` (so phase
/// timings stay visible for diagnostics) but never to stdout/stderr (so
/// query output stays clean). The process-wide `SetLogger` call lives in a
/// static initializer so it runs before any SPTAG translation unit can emit
/// its first banner.
class PocoLogger final : public SPTAG::Helper::Logger
{
public:
    void Logging(const char *, SPTAG::Helper::LogLevel level, const char *, int, const char *, const char * format, ...) override
    {
        auto * logger = &Poco::Logger::get("SPANN");
        const Poco::Message::Priority prio = toPocoPriority(level);
        if (!logger->is(prio))
            return;

        char buf[1024];
        va_list args;
        va_start(args, format);
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wformat-nonliteral"
        int n = std::vsnprintf(buf, sizeof(buf), format, args);
#pragma clang diagnostic pop
        va_end(args);
        if (n <= 0)
            return;
        std::string_view view(buf, std::min<size_t>(static_cast<size_t>(n), sizeof(buf) - 1));
        while (!view.empty() && (view.back() == '\n' || view.back() == '\r'))
            view.remove_suffix(1);
        if (view.empty())
            return;
        logger->log(Poco::Message("SPANN", std::string(view), prio));
    }

private:
    static Poco::Message::Priority toPocoPriority(SPTAG::Helper::LogLevel level)
    {
        switch (level)
        {
            case SPTAG::Helper::LogLevel::LL_Debug:   return Poco::Message::PRIO_DEBUG;
            case SPTAG::Helper::LogLevel::LL_Info:    return Poco::Message::PRIO_INFORMATION;
            case SPTAG::Helper::LogLevel::LL_Status:  return Poco::Message::PRIO_INFORMATION;
            case SPTAG::Helper::LogLevel::LL_Warning: return Poco::Message::PRIO_WARNING;
            case SPTAG::Helper::LogLevel::LL_Error:   return Poco::Message::PRIO_ERROR;
            case SPTAG::Helper::LogLevel::LL_Assert:  return Poco::Message::PRIO_FATAL;
            default:                                  return Poco::Message::PRIO_INFORMATION;
        }
    }
};

/// First line of defense: installed at static init time. Its only job is
/// to stop SPTAG's `[1] Using AVX512 InstructionSet!` banner (emitted by
/// the library's first `SPTAGLIB_LOG` call) from hitting `stdout` and
/// corrupting `clickhouse-client` output. It never touches Poco, which
/// would deadlock against `Application::initialize`.
class SilentSinkLogger final : public SPTAG::Helper::Logger
{
public:
    void Logging(const char *, SPTAG::Helper::LogLevel, const char *, int, const char *, const char *, ...) override
    {
    }
};

[[maybe_unused]] const bool sptag_silent_sink_installed = []
{
    SPTAG::SetLogger(std::make_shared<SilentSinkLogger>());
    return true;
}();

void installSPTAGLoggerOnce()
{
    /// Upgrade to the Poco-backed logger the first time a SPANN entry point
    /// is reached. By now `Application::initialize` has wired the logger
    /// backends, so `Poco::Logger::get` is safe (a static init would
    /// deadlock against the daemon startup path).
    static std::once_flag flag;
    std::call_once(flag, []
    {
        SPTAG::SetLogger(std::make_shared<PocoLogger>());
    });
}

constexpr UInt64 SPTAG_SIZE_LIMIT = static_cast<UInt64>(std::numeric_limits<SPTAG::SizeType>::max());

const char * toSPTAGMetricString(Metric metric)
{
    switch (metric)
    {
        case Metric::L2:
            return "L2";
        case Metric::Cosine:
            return "Cosine";
        case Metric::InnerProduct:
            return "InnerProduct";
    }
}

void throwFromSPTAGError(SPTAG::ErrorCode code, std::string_view context)
{
    throw Exception(
        ErrorCodes::EXTERNAL_LIBRARY_ERROR,
        "SPTAG {} failed with code {}",
        context,
        static_cast<UInt16>(code));
}

void checkSPTAG(SPTAG::ErrorCode code, std::string_view context)
{
    if (code != SPTAG::ErrorCode::Success)
        throwFromSPTAGError(code, context);
}

void setParameter(
    const std::shared_ptr<SPTAG::VectorIndex> & index,
    const char * section,
    const char * name,
    const String & value)
{
    checkSPTAG(index->SetParameter(name, value.c_str(), section), fmt::format("SetParameter({}.{})", section, name));
}

void setParameter(
    const std::shared_ptr<SPTAG::VectorIndex> & index,
    const char * section,
    const char * name,
    const char * value)
{
    checkSPTAG(index->SetParameter(name, value, section), fmt::format("SetParameter({}.{})", section, name));
}

void applyBuildParameters(
    const std::shared_ptr<SPTAG::VectorIndex> & index,
    const BuildParams & params,
    const String & folder_path)
{
    /// `ValueType=Float`, `IndexAlgoType=BKT`, the three `isExecute=true`
    /// flags below, and `Storage=STATIC` are facade-level invariants —
    /// not knobs. Indexed columns are validated as `Array(Float32)` (see
    /// `SPANNAlgorithm::validateIndexedExpression`); BKT is the only head
    /// algorithm whose params we plumb through (`BuildHead.BKT*`); STATIC
    /// is the only storage path `ExtraStaticSearcher` is wired against.
    /// Changing any of these would require a separate facade.
    setParameter(index, "Base", "ValueType", "Float");
    setParameter(index, "Base", "DistCalcMethod", toSPTAGMetricString(params.metric));
    setParameter(index, "Base", "IndexAlgoType", "BKT");
    setParameter(index, "Base", "Dim", std::to_string(params.dim));
    setParameter(index, "Base", "IndexDirectory", folder_path);

    /// SelectHead — head sampling on the BKT clustering of input vectors.
    setParameter(index, "SelectHead", "isExecute", "true");
    setParameter(index, "SelectHead", "SelectType", params.select_type);
    setParameter(index, "SelectHead", "Ratio", std::to_string(params.head_ratio));
    setParameter(index, "SelectHead", "NumberOfThreads", std::to_string(params.select_head_threads));
    setParameter(index, "SelectHead", "SamplesNumber", std::to_string(params.select_samples_number));
    setParameter(index, "SelectHead", "SelectThreshold", std::to_string(params.select_threshold));
    setParameter(index, "SelectHead", "SplitFactor", std::to_string(params.split_factor));
    setParameter(index, "SelectHead", "SplitThreshold", std::to_string(params.split_threshold));

    /// BuildHead — forwarded by SPANN::Index::SetParameter to the inner BKT
    /// VectorIndex (see SPTAG/AnnService/src/Core/SPANN/SPANNIndex.cpp:1412).
    setParameter(index, "BuildHead", "isExecute", "true");
    setParameter(index, "BuildHead", "BKTNumber", std::to_string(params.bkt_number));
    setParameter(index, "BuildHead", "BKTKmeansK", std::to_string(params.bkt_kmeans_k));
    setParameter(index, "BuildHead", "BKTLeafSize", std::to_string(params.bkt_leaf_size));
    setParameter(index, "BuildHead", "NeighborhoodSize", std::to_string(params.neighborhood_size));
    setParameter(index, "BuildHead", "CEF", std::to_string(params.cef));
    setParameter(index, "BuildHead", "MaxCheckForRefineGraph", std::to_string(params.max_check_for_refine_graph));
    setParameter(index, "BuildHead", "RefineIterations", std::to_string(params.refine_iterations));
    setParameter(index, "BuildHead", "TPTNumber", std::to_string(params.tpt_number));
    setParameter(index, "BuildHead", "RNGFactor", std::to_string(params.rng_factor));
    setParameter(index, "BuildHead", "NumberOfThreads", std::to_string(params.build_head_threads));

    setParameter(index, "BuildSSDIndex", "isExecute", "true");
    setParameter(index, "BuildSSDIndex", "BuildSsdIndex", "true");
    setParameter(index, "BuildSSDIndex", "Storage", "STATIC");
    setParameter(index, "BuildSSDIndex", "PostingPageLimit", std::to_string(params.posting_page_limit));
    setParameter(index, "BuildSSDIndex", "SearchPostingPageLimit", std::to_string(params.search_posting_page_limit));
    setParameter(index, "BuildSSDIndex", "PostingVectorLimit", std::to_string(params.posting_vector_limit));
    setParameter(index, "BuildSSDIndex", "InternalResultNum", std::to_string(params.internal_result_num));
    setParameter(index, "BuildSSDIndex", "SearchInternalResultNum", std::to_string(params.internal_result_num));
    setParameter(index, "BuildSSDIndex", "ReplicaCount", std::to_string(params.replica_count));
    /// `BuildSSDIndex.NumberOfThreads` doubles as the async-IO thread-pool
    /// size at search time when `BATCH_READ` is enabled in SPTAG (see
    /// `inc/Helper/AsyncFileReader.h:34` and `ExtraStaticSearcher.h:203`).
    /// Take the max with `io_threads` so the persisted ini opens the
    /// posting-list reader with enough workspaces — otherwise SPTAG
    /// spins on "FreeWorkSpaceIds is not initialized" under concurrent
    /// search load.
    const UInt32 ssd_threads = std::max(params.num_threads, params.io_threads);
    setParameter(index, "BuildSSDIndex", "NumberOfThreads", std::to_string(ssd_threads));
    setParameter(index, "BuildSSDIndex", "MaxCheck", std::to_string(params.max_check));
    setParameter(index, "BuildSSDIndex", "MaxDistRatio", std::to_string(params.max_dist_ratio));
    setParameter(index, "BuildSSDIndex", "HashTableExponent", std::to_string(params.hash_table_exponent));
    /// `IOTimeout` becomes a **process-global** value at LoadIndex time
    /// (`ExtraStaticSearcher.h:247` writes into `Helper::AIOTimeout.tv_nsec`).
    /// We still persist the per-index value so freshly loaded indexes use
    /// the DDL choice; mixed-DDL processes see the last LoadIndex win.
    setParameter(index, "BuildSSDIndex", "IOTimeout", std::to_string(params.io_timeout_us));
    setParameter(index, "BuildSSDIndex", "RNGFactor", std::to_string(params.rng_factor));
    /// Kept for the dynamic-mode (`Storage != STATIC`) path which uses
    /// `BlockController::Initialize` and reads `m_ioThreads` directly.
    setParameter(index, "BuildSSDIndex", "IOThreadsPerHandler", std::to_string(params.io_threads));
    /// Disk-layout / compression switches. They reshape the persisted
    /// posting format, so they have no search-time twin — flipping any of
    /// them requires a fresh build.
    setParameter(index, "BuildSSDIndex", "EnableDataCompression", params.enable_data_compression ? "true" : "false");
    setParameter(index, "BuildSSDIndex", "EnableDeltaEncoding", params.enable_delta_encoding ? "true" : "false");
    setParameter(index, "BuildSSDIndex", "EnablePostingListRearrange", params.enable_posting_list_rearrange ? "true" : "false");
}

float l2Distance(const float * lhs, const float * rhs, UInt32 dim)
{
    float result = 0.0f;
    for (UInt32 i = 0; i < dim; ++i)
    {
        const float diff = lhs[i] - rhs[i];
        result += diff * diff;
    }
    return result;
}

float dotProduct(const float * lhs, const float * rhs, UInt32 dim)
{
    float result = 0.0f;
    for (UInt32 i = 0; i < dim; ++i)
        result += lhs[i] * rhs[i];
    return result;
}

float cosineDistance(const float * lhs, const float * rhs, UInt32 dim)
{
    float lhs_norm = 0.0f;
    float rhs_norm = 0.0f;
    for (UInt32 i = 0; i < dim; ++i)
    {
        lhs_norm += lhs[i] * lhs[i];
        rhs_norm += rhs[i] * rhs[i];
    }

    if (lhs_norm == 0.0f || rhs_norm == 0.0f)
        return 1.0f;
    return 1.0f - dotProduct(lhs, rhs, dim) / (std::sqrt(lhs_norm) * std::sqrt(rhs_norm));
}

}

String metricName(Metric metric)
{
    switch (metric)
    {
        case Metric::L2:
            return "L2";
        case Metric::Cosine:
            return "cosine";
        case Metric::InnerProduct:
            return "InnerProduct";
    }
}

UInt64 metricId(Metric metric)
{
    return static_cast<UInt64>(metric);
}

struct Searcher::Impl
{
    /// SPTAG `SearchIndex` is `const` and uses per-call workspaces from an
    /// internal pool sized by `IOThreadsPerHandler`. Multiple ClickHouse
    /// threads can drive the same loaded `VectorIndex` concurrently without
    /// an outer mutex; we tune the pool through `BuildParams::io_threads`.
    std::shared_ptr<SPTAG::VectorIndex> index;
};

Searcher::Searcher(const String & folder_path, const BuildParams & params_)
    : impl(std::make_unique<Impl>())
    , params(params_)
{
    installSPTAGLoggerOnce();
    checkSPTAG(SPTAG::VectorIndex::LoadIndex(folder_path, impl->index), "LoadIndex");
    if (!impl->index)
        throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "SPTAG LoadIndex returned a null index for '{}'", folder_path);

    if (impl->index->GetFeatureDim() != static_cast<SPTAG::DimensionType>(params.dim))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "SPTAG index dimension {} does not match expected dimension {}",
            impl->index->GetFeatureDim(),
            params.dim);

    /// Post-LoadIndex hot-patch. SPTAG's top-level SetParameter routes by
    /// `(section, param)` differently:
    ///
    ///   * `BuildSSDIndex.*` writes only to `m_options.*`. Two of these are
    ///     re-read on every search from `m_options`: `MaxDistRatio` (head
    ///     result early-stop in `SPANNIndex.cpp:325`) and the workspace
    ///     pool inputs `SearchPostingPageLimit`/`SearchInternalResultNum`
    ///     (re-read when the pool spawns a new `ExtraWorkSpace`).
    ///
    ///   * `BuildHead.<param>` (with `param != "isExecute"`) forwards to
    ///     `m_index->SetParameter(param, value)` — the only way to touch
    ///     the head BKT's actual fields. `MaxCheck` and `HashTableExponent`
    ///     are BKT fields (`BKT::ParameterDefinitionList.h:45,49`); writing
    ///     them through `BuildSSDIndex` only updates the unread copy in
    ///     `m_options` and the head BKT keeps its load-time values, so the
    ///     query path appears to ignore the override entirely. Route them
    ///     through `BuildHead` so the BKT actually sees the new values.
    ///
    /// `IOTimeout` is intentionally NOT hot-patched here: it writes the
    /// process-global `Helper::AIOTimeout` once during
    /// `ExtraStaticSearcher::LoadIndex` (`ExtraStaticSearcher.h:247`); a
    /// second LoadIndex of another part on the same server would race the
    /// last writer's value. Treat it as build-only.
    setParameter(impl->index, "BuildSSDIndex", "SearchPostingPageLimit",  std::to_string(params.search_posting_page_limit));
    setParameter(impl->index, "BuildSSDIndex", "SearchInternalResultNum", std::to_string(params.internal_result_num));
    setParameter(impl->index, "BuildSSDIndex", "MaxDistRatio",            std::to_string(params.max_dist_ratio));
    setParameter(impl->index, "BuildHead",     "MaxCheck",                std::to_string(params.max_check));
    setParameter(impl->index, "BuildHead",     "HashTableExponent",       std::to_string(params.hash_table_exponent));
}

Searcher::~Searcher() = default;
Searcher::Searcher(Searcher &&) noexcept = default;
Searcher & Searcher::operator=(Searcher &&) noexcept = default;

SearchResult Searcher::search(const float * query, UInt32 dim, size_t candidate_limit) const
{
    if (!impl || !impl->index)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SPTAG search invoked on an empty searcher");
    if (dim != params.dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPTAG query dimension {} does not match index dimension {}", dim, params.dim);
    if (candidate_limit == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPTAG candidate_limit must be greater than zero");
    if (candidate_limit > static_cast<size_t>(std::numeric_limits<int>::max()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPTAG candidate_limit is out of int range");

    const int result_num = static_cast<int>(std::max<UInt64>(candidate_limit, params.internal_result_num));

    /// SPTAG SPANN normalises **stored** vectors at build time when
    /// `DistCalcMethod=Cosine`, but does not touch the **query** vector at
    /// search time. Feeding a raw query into a cosine index yields nonsense
    /// distances (e.g. negative self-similarity scaled by |q|) and breaks
    /// the ranking contract `cosineDistance(a, a) = 0`. Normalise on our
    /// side before handing the pointer to `SearchIndex`.
    std::vector<float> normalised_query;
    const float * effective_query = query;
    if (params.metric == Metric::Cosine)
    {
        float norm_sq = 0.0f;
        for (UInt32 i = 0; i < dim; ++i)
            norm_sq += query[i] * query[i];
        if (norm_sq > 0.0f)
        {
            const float inv_norm = 1.0f / std::sqrt(norm_sq);
            normalised_query.assign(query, query + dim);
            for (UInt32 i = 0; i < dim; ++i)
                normalised_query[i] *= inv_norm;
            effective_query = normalised_query.data();
        }
    }

    SPTAG::QueryResult query_result(effective_query, result_num, false);
    checkSPTAG(impl->index->SearchIndex(query_result), "SearchIndex");

    SearchResult out;
    out.vids.reserve(candidate_limit);
    out.distances.reserve(candidate_limit);

    for (int i = 0; i < query_result.GetResultNum() && out.vids.size() < candidate_limit; ++i)
    {
        const auto * result = query_result.GetResult(i);
        if (!result || result->VID < 0)
            break;

        out.vids.push_back(static_cast<UInt64>(result->VID));
        out.distances.push_back(params.metric == Metric::InnerProduct ? 1.0f - result->Dist : result->Dist);
    }

    return out;
}

void buildIndex(const BuildParams & params, float * vectors, UInt64 rows, const String & folder_path)
{
    installSPTAGLoggerOnce();
    if (!vectors)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPTAG build requires a non-null vector buffer");
    if (rows == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPTAG build requires at least one vector");
    if (rows > SPTAG_SIZE_LIMIT)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPTAG build row count {} exceeds SPTAG SizeType limit {}", rows, SPTAG_SIZE_LIMIT);
    if (params.dim == 0 || params.dim > SPTAG_SIZE_LIMIT)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPTAG dimension {} is out of range", params.dim);

    auto index = SPTAG::VectorIndex::CreateInstance(SPTAG::IndexAlgoType::SPANN, SPTAG::VectorValueType::Float);
    if (!index)
        throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "SPTAG CreateInstance(SPANN, Float) returned null");

    applyBuildParameters(index, params, folder_path);

    checkSPTAG(
        index->BuildIndex(
            vectors,
            static_cast<SPTAG::SizeType>(rows),
            static_cast<SPTAG::DimensionType>(params.dim),
            /*p_normalized=*/false,
            /*p_shareOwnership=*/true),
        "BuildIndex");
    checkSPTAG(index->SaveIndex(folder_path), "SaveIndex");
}

void computeDistances(
    Metric metric,
    UInt32 dim,
    const float * query,
    const float * candidates,
    UInt64 rows,
    float * out)
{
    for (UInt64 row = 0; row < rows; ++row)
    {
        const float * candidate = candidates + row * dim;
        switch (metric)
        {
            case Metric::L2:
                out[row] = l2Distance(query, candidate, dim);
                break;
            case Metric::Cosine:
                out[row] = cosineDistance(query, candidate, dim);
                break;
            case Metric::InnerProduct:
                out[row] = dotProduct(query, candidate, dim);
                break;
        }
    }
}

}
}

#endif
