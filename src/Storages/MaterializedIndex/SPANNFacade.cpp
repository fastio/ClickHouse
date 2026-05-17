#include <Storages/MaterializedIndex/SPANNFacade.h>

#if USE_SPTAG

#include <Common/Exception.h>

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
/// `.reference` diff fail). Install a silent sink that swallows everything
/// below `LL_Error` and routes errors to stderr; the process-wide
/// `SetLogger` call lives in a static initializer so it runs before any
/// SPTAG translation unit can emit its first banner.
class StderrErrorOnlyLogger final : public SPTAG::Helper::Logger
{
public:
    void Logging(const char *, SPTAG::Helper::LogLevel level, const char *, int, const char *, const char * format, ...) override
    {
        if (level < SPTAG::Helper::LogLevel::LL_Error)
            return;
        va_list args;
        va_start(args, format);
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wformat-nonliteral"
        (void)std::vfprintf(stderr, format, args);
#pragma clang diagnostic pop
        va_end(args);
    }
};

[[maybe_unused]] const bool sptag_logger_installed = []
{
    SPTAG::SetLogger(std::make_shared<StderrErrorOnlyLogger>());
    return true;
}();

constexpr UInt64 SPTAG_SIZE_LIMIT = static_cast<UInt64>(std::numeric_limits<SPTAG::SizeType>::max());

const char * toSPTAGMetricString(Metric metric)
{
    switch (metric)
    {
        case Metric::L2:
            return "L2";
        case Metric::Cosine:
            return "Cosine";
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
    setParameter(index, "Base", "ValueType", "Float");
    setParameter(index, "Base", "DistCalcMethod", toSPTAGMetricString(params.metric));
    setParameter(index, "Base", "IndexAlgoType", "BKT");
    setParameter(index, "Base", "Dim", std::to_string(params.dim));
    setParameter(index, "Base", "IndexDirectory", folder_path);

    setParameter(index, "SelectHead", "isExecute", "true");
    setParameter(index, "SelectHead", "Ratio", std::to_string(params.head_ratio));
    setParameter(index, "SelectHead", "NumberOfThreads", std::to_string(params.num_threads));

    setParameter(index, "BuildHead", "isExecute", "true");

    setParameter(index, "BuildSSDIndex", "isExecute", "true");
    setParameter(index, "BuildSSDIndex", "BuildSsdIndex", "true");
    setParameter(index, "BuildSSDIndex", "Storage", "STATIC");
    setParameter(index, "BuildSSDIndex", "PostingPageLimit", std::to_string(params.posting_page_limit));
    setParameter(index, "BuildSSDIndex", "SearchPostingPageLimit", std::to_string(params.search_posting_page_limit));
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
    /// Kept for the dynamic-mode (`Storage != STATIC`) path which uses
    /// `BlockController::Initialize` and reads `m_ioThreads` directly.
    setParameter(index, "BuildSSDIndex", "IOThreadsPerHandler", std::to_string(params.io_threads));
    setParameter(index, "BuildSSDIndex", "EnableDataCompression", "false");
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

float cosineDistance(const float * lhs, const float * rhs, UInt32 dim)
{
    float dot = 0.0f;
    float lhs_norm = 0.0f;
    float rhs_norm = 0.0f;
    for (UInt32 i = 0; i < dim; ++i)
    {
        dot += lhs[i] * rhs[i];
        lhs_norm += lhs[i] * lhs[i];
        rhs_norm += rhs[i] * rhs[i];
    }

    if (lhs_norm == 0.0f || rhs_norm == 0.0f)
        return 1.0f;
    return 1.0f - dot / (std::sqrt(lhs_norm) * std::sqrt(rhs_norm));
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
    checkSPTAG(SPTAG::VectorIndex::LoadIndex(folder_path, impl->index), "LoadIndex");
    if (!impl->index)
        throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "SPTAG LoadIndex returned a null index for '{}'", folder_path);

    if (impl->index->GetFeatureDim() != static_cast<SPTAG::DimensionType>(params.dim))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "SPTAG index dimension {} does not match expected dimension {}",
            impl->index->GetFeatureDim(),
            params.dim);

    setParameter(impl->index, "BuildSSDIndex", "SearchPostingPageLimit", std::to_string(params.search_posting_page_limit));
    setParameter(impl->index, "BuildSSDIndex", "SearchInternalResultNum", std::to_string(params.internal_result_num));
    setParameter(impl->index, "BuildSSDIndex", "MaxCheck", std::to_string(params.max_check));
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
        out.distances.push_back(result->Dist);
    }

    return out;
}

void buildIndex(const BuildParams & params, float * vectors, UInt64 rows, const String & folder_path)
{
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
        }
    }
}

}
}

#endif
