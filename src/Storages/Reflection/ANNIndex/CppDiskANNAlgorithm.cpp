#include <Storages/Reflection/ANNIndex/CppDiskANNAlgorithm.h>

#if USE_CPPDISKANN

#include <Storages/Reflection/ANNIndex/ANNIndexContext.h>
#include <Storages/Reflection/ANNIndex/ANNIndexLocator.h>
#include <Storages/Reflection/ANNIndex/DiskANNFbinWriter.h>

#include <Common/getNumberOfCPUCoresToUse.h>
#include <base/scope_guard.h>
#include <base/getMemoryAmount.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <fmt/format.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <sstream>
#include <string_view>
#include <vector>

namespace ProfileEvents
{
    extern const Event ANNIndexCppDiskANNBuildStarted;
    extern const Event ANNIndexCppDiskANNBuildFinished;
    extern const Event ANNIndexCppDiskANNBuildFailed;
    extern const Event ANNIndexCppDiskANNSearchStarted;
    extern const Event ANNIndexCppDiskANNSearchFinished;
    extern const Event ANNIndexCppDiskANNSearchFailed;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_diskann_index_search_using_inline_locator;
    extern const SettingsUInt64 diskann_search_list_size;
    extern const SettingsUInt64 diskann_search_beam_width;
    extern const SettingsUInt64 diskann_search_num_threads;
    extern const SettingsUInt64 diskann_search_nodes_to_cache;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsString ann_searcher_cache_policy;
    extern const MergeTreeSettingsUInt64 ann_searcher_cache_size;
    extern const MergeTreeSettingsUInt64 ann_searcher_cache_max_entries;
    extern const MergeTreeSettingsFloat ann_searcher_cache_size_ratio;
}

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{
constexpr UInt64 CANCEL_POLL_ROW_GRANULE = 100;
constexpr const char * INDEX_PREFIX = "algorithm_private_cppdiskann";
constexpr UInt32 SEARCHER_NUM_THREADS_DEFAULT = 8;
constexpr UInt32 SEARCHER_NODES_TO_CACHE_DEFAULT = 1024;
constexpr UInt32 SEARCHER_NUM_THREADS_MAX = 64;
constexpr UInt32 SEARCHER_NODES_TO_CACHE_MAX = 65536;
constexpr double CACHE_EXPANSION_RATE = 1.2;
constexpr UInt64 DEFAULT_BUILD_RAM_LIMIT_BYTES = 128ULL << 30;

std::optional<CppDiskANNFacade::Metric> parseMetric(std::string_view text)
{
    if (text == "L2" || text == "l2")
        return CppDiskANNFacade::Metric::L2;
    if (text == "cosine" || text == "Cosine" || text == "COSINE")
        return CppDiskANNFacade::Metric::Cosine;
    if (text == "InnerProduct" || text == "innerproduct" || text == "inner_product" || text == "INNER_PRODUCT" || text == "dotProduct")
        return CppDiskANNFacade::Metric::InnerProduct;
    if (text == "CosineNormalized" || text == "cosinenormalized" || text == "cosine_normalized" || text == "COSINE_NORMALIZED")
        return CppDiskANNFacade::Metric::CosineNormalized;
    return {};
}

String metricName(CppDiskANNFacade::Metric metric)
{
    switch (metric)
    {
        case CppDiskANNFacade::Metric::L2:
            return "L2";
        case CppDiskANNFacade::Metric::Cosine:
            return "cosine";
        case CppDiskANNFacade::Metric::InnerProduct:
            return "InnerProduct";
        case CppDiskANNFacade::Metric::CosineNormalized:
            return "CosineNormalized";
    }
}

bool queryFunctionMatchesMetric(const String & distance_function, CppDiskANNFacade::Metric metric)
{
    switch (metric)
    {
        case CppDiskANNFacade::Metric::L2:
            return distance_function == "L2Distance";
        case CppDiskANNFacade::Metric::Cosine:
        case CppDiskANNFacade::Metric::CosineNormalized:
            return distance_function == "cosineDistance";
        case CppDiskANNFacade::Metric::InnerProduct:
            return distance_function == "dotProduct";
    }
}

void checkSearchCancelled(ContextPtr query_context)
{
    if (!query_context)
        return;
    if (auto process_list_element = query_context->getProcessListElementSafe())
        process_list_element->checkTimeLimit();
}

UInt32 settingOrDefault(UInt64 value, UInt32 fallback, UInt32 upper_inclusive, std::string_view name)
{
    if (value == 0)
        return fallback;
    if (value > upper_inclusive)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann search setting '{}' must be in range [0, {}], got {}", name, upper_inclusive, value);
    return static_cast<UInt32>(value);
}

std::shared_ptr<ANNSearcherCache<CppDiskANNFacade::Searcher>> defaultCppDiskANNSearcherCache()
{
    return std::make_shared<ANNSearcherCache<CppDiskANNFacade::Searcher>>(
        /*cache_policy=*/ "SLRU",
        /*max_size_in_bytes=*/ 16ULL << 30,
        /*max_count=*/ 1024,
        /*size_ratio=*/ 0.5);
}

std::vector<String> collectPrivateIndexFiles(const IDataPartStorage & storage)
{
    std::vector<String> files;
    for (auto it = storage.iterate(); it->isValid(); it->next())
    {
        if (!it->isFile())
            continue;

        const String file_name = it->name();
        if (file_name.starts_with(INDEX_PREFIX))
            files.push_back(file_name);
    }

    std::sort(files.begin(), files.end());
    return files;
}

std::string fieldAsString(const Field & field)
{
    if (field.getType() == Field::Types::String)
        return field.safeGet<String>();
    return DB::fieldToString(field);
}

UInt64 fieldToUInt64(const Field & field, std::string_view name)
{
    switch (field.getType())
    {
        case Field::Types::UInt64:
            return field.safeGet<UInt64>();
        case Field::Types::Int64:
        {
            const Int64 v = field.safeGet<Int64>();
            if (v < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann parameter '{}' must be non-negative, got {}", name, v);
            return static_cast<UInt64>(v);
        }
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann parameter '{}' must be an integer literal", name);
    }
}

UInt32 fieldToUInt32(const Field & field, std::string_view name)
{
    const UInt64 value = fieldToUInt64(field, name);
    if (value > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann parameter '{}' is out of UInt32 range", name);
    return static_cast<UInt32>(value);
}

double fieldToDouble(const Field & field, std::string_view name)
{
    switch (field.getType())
    {
        case Field::Types::UInt64:
            return static_cast<double>(field.safeGet<UInt64>());
        case Field::Types::Int64:
            return static_cast<double>(field.safeGet<Int64>());
        case Field::Types::Float64:
            return field.safeGet<Float64>();
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann parameter '{}' must be a numeric literal", name);
    }
}

bool fieldToBool(const Field & field, std::string_view name)
{
    switch (field.getType())
    {
        case Field::Types::UInt64:
            return field.safeGet<UInt64>() != 0;
        case Field::Types::Int64:
            return field.safeGet<Int64>() != 0;
        case Field::Types::Bool:
            return field.safeGet<bool>();
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann parameter '{}' must be a boolean literal", name);
    }
}

bool isKnownParam(std::string_view name)
{
    return name == "metric" || name == "dim"
        || name == "pruned_degree" || name == "max_degree"
        || name == "l_build" || name == "alpha"
        || name == "num_threads" || name == "pq_chunks"
        || name == "build_quantization"
        || name == "pq_code_budget_gb"
        || name == "pq_code_budget_gb_ratio"
        || name == "build_ram_limit_gb"
        || name == "search_cache_budget_gb"
        || name == "search_cache_budget_gb_ratio"
        || name == "disk_pq_dims"
        || name == "accelerate_build"
        || name == "num_nodes_to_cache";
}

bool parseUInt32Text(std::string_view text, UInt32 & value)
{
    if (text.empty())
        return false;

    UInt64 parsed = 0;
    for (char c : text)
    {
        if (c < '0' || c > '9')
            return false;
        parsed = parsed * 10 + static_cast<UInt64>(c - '0');
        if (parsed > std::numeric_limits<UInt32>::max())
            return false;
    }

    value = static_cast<UInt32>(parsed);
    return true;
}

String normalizeBuildQuantization(const String & text, UInt32 dim, UInt32 effective_pq_chunks)
{
    if (text.empty())
        return effective_pq_chunks == 0 ? "FP" : "PQ_" + toString(effective_pq_chunks);

    std::vector<std::string_view> parts;
    std::string_view view{text};
    while (true)
    {
        const size_t pos = view.find('_');
        parts.push_back(view.substr(0, pos));
        if (pos == std::string_view::npos)
            break;
        view.remove_prefix(pos + 1);
    }

    if (parts.size() == 1 && (parts[0] == "FP" || parts[0] == "fp"))
        return "FP";

    if (parts.size() == 2 && (parts[0] == "PQ" || parts[0] == "pq"))
    {
        UInt32 chunks = 0;
        if (!parseUInt32Text(parts[1], chunks) || chunks == 0 || chunks > dim)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann: 'build_quantization' PQ chunks must be in range [1, dim], got '{}'", text);
        return "PQ_" + toString(chunks);
    }

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann: 'build_quantization' must be 'FP' or 'PQ_N', got '{}'", text);
}

UInt32 pqChunksFromBuildQuantization(const String & build_quantization)
{
    if (build_quantization == "FP")
        return 0;

    constexpr std::string_view pq_prefix = "PQ_";
    if (!build_quantization.starts_with(pq_prefix))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann: normalized 'build_quantization' must be 'FP' or 'PQ_N', got '{}'", build_quantization);

    UInt32 chunks = 0;
    if (!parseUInt32Text(std::string_view{build_quantization}.substr(pq_prefix.size()), chunks))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann: normalized 'build_quantization' has invalid PQ chunk count '{}'", build_quantization);
    return chunks;
}

double rawVectorDataSizeGiB(UInt64 rows, UInt32 dim)
{
    return static_cast<double>(rows) * static_cast<double>(dim) * sizeof(Float32) / static_cast<double>(1ULL << 30);
}

UInt64 getMemAvailableBytes()
{
#if defined(OS_LINUX)
    std::ifstream meminfo("/proc/meminfo");
    std::string name;
    UInt64 kb = 0;
    std::string unit;
    while (meminfo >> name >> kb >> unit)
    {
        if (name == "MemAvailable:")
            return kb * 1024;
    }
#endif
    return 0;
}

UInt64 getBuildMemoryBudgetBytes()
{
    const UInt64 memory_available = getMemAvailableBytes();
    const UInt64 memory_limit = getMemoryAmountOrZero();
    if (memory_available != 0 && memory_limit != 0)
        return std::min(memory_available, memory_limit);
    if (memory_available != 0)
        return memory_available;
    if (memory_limit != 0)
        return memory_limit;
    return DEFAULT_BUILD_RAM_LIMIT_BYTES;
}

double getBuildMemoryBudgetGiB()
{
    return static_cast<double>(getBuildMemoryBudgetBytes()) / static_cast<double>(1ULL << 30);
}

UInt32 cachedNodeCountFromBudget(double cache_budget_gb, UInt32 data_dim, size_t chunk_size, UInt32 max_degree)
{
    if (cache_budget_gb <= 0.0)
        return 0;

    const double one_cached_node_budget = static_cast<double>((static_cast<UInt64>(max_degree) + 1) * sizeof(unsigned))
        + static_cast<double>(chunk_size) * static_cast<double>(data_dim);
    const double nodes = cache_budget_gb * static_cast<double>(1ULL << 30) / (one_cached_node_budget * CACHE_EXPANSION_RATE);
    if (nodes <= 0.0)
        return 0;
    if (nodes > static_cast<double>(std::numeric_limits<UInt32>::max()))
        return std::numeric_limits<UInt32>::max();
    return static_cast<UInt32>(nodes);
}

String fileSipHashHex(IDataPartStorage & storage, const String & rel_path)
{
    SipHash hash;
    auto reader = storage.readFile(rel_path, ReadSettings{}, std::nullopt);
    std::vector<char> buf(64 * 1024);
    while (!reader->eof())
    {
        const size_t n = reader->read(buf.data(), buf.size());
        if (n == 0)
            break;
        hash.update(buf.data(), n);
    }
    UInt64 lo = 0;
    UInt64 hi = 0;
    hash.get128(lo, hi);
    return fmt::format("{:016x}{:016x}", lo, hi);
}

}

CppDiskANNAlgorithm::CppDiskANNAlgorithm() = default;
CppDiskANNAlgorithm::~CppDiskANNAlgorithm() = default;

String CppDiskANNAlgorithm::getAlgorithmVersion() const
{
    return "cppdiskann";
}

String CppDiskANNAlgorithm::getBuildParamsHash() const
{
    if (!validated_params)
        return {};
    return calculateParamsHash(*validated_params);
}

std::map<String, String> CppDiskANNAlgorithm::getAlgorithmObservabilityFields() const
{
    if (!validated_params)
        return {};
    return buildSettings(*validated_params);
}

std::map<String, String> CppDiskANNAlgorithm::buildSettings(const BuildParams & p)
{
    return {
        {"metric", metricName(p.metric)},
        {"dimension", toString(p.dim)},
        {"pruned_degree", toString(p.pruned_degree)},
        {"max_degree", toString(p.max_degree)},
        {"l_build", toString(p.l_build)},
        {"alpha", toString(p.alpha)},
        {"num_threads", toString(p.num_threads)},
        {"pq_chunks", toString(p.pq_chunks)},
        {"build_quantization", p.build_quantization},
        {"pq_code_budget_gb", toString(p.pq_code_budget_gb)},
        {"pq_code_budget_gb_ratio", toString(p.pq_code_budget_gb_ratio)},
        {"build_ram_limit_gb", toString(p.build_ram_limit_gb)},
        {"search_cache_budget_gb", toString(p.search_cache_budget_gb)},
        {"search_cache_budget_gb_ratio", toString(p.search_cache_budget_gb_ratio)},
        {"disk_pq_dims", toString(p.disk_pq_dims)},
        {"accelerate_build", p.accelerate_build ? "1" : "0"},
        {"num_nodes_to_cache", toString(p.num_nodes_to_cache)},
    };
}

std::unique_ptr<IANNIndexAlgorithm> CppDiskANNAlgorithm::cloneForBuild() const
{
    auto fresh = std::make_unique<CppDiskANNAlgorithm>();
    fresh->initialized = initialized;
    fresh->params = params;
    fresh->validated_params = validated_params;
    fresh->searcher_cache = searcher_cache;
    return fresh;
}

CppDiskANNAlgorithm::BuildParams CppDiskANNAlgorithm::parseBuildParameters(const ASTPtr & build_params)
{
    if (!build_params)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann requires keyword parameters: metric, dim are mandatory");

    const ASTExpressionList * list = nullptr;
    if (const auto * fn = typeid_cast<const ASTFunction *>(build_params.get()))
        list = fn->arguments ? typeid_cast<const ASTExpressionList *>(fn->arguments.get()) : nullptr;
    else
        list = typeid_cast<const ASTExpressionList *>(build_params.get());

    if (!list)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann parameters must be a keyword argument list");

    BuildParams out{};
    bool seen_metric = false;
    bool seen_dim = false;
    bool seen_pq_code_budget_gb = false;
    bool seen_pq_code_budget_gb_ratio = false;
    bool seen_search_cache_budget_gb = false;
    bool seen_search_cache_budget_gb_ratio = false;

    for (const auto & child : list->children)
    {
        const auto * eq = typeid_cast<const ASTFunction *>(child.get());
        if (!eq || eq->name != "equals" || !eq->arguments || eq->arguments->children.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann parameters must be keyword arguments");

        const auto * name_ast = typeid_cast<const ASTIdentifier *>(eq->arguments->children[0].get());
        const auto * value_ast = typeid_cast<const ASTLiteral *>(eq->arguments->children[1].get());
        if (!name_ast || !value_ast)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann parameters must be literal keyword arguments");

        const String name = name_ast->name();
        if (!isKnownParam(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown cppdiskann parameter '{}'", name);

        const auto & value = value_ast->value;
        if (name == "metric")
        {
            auto metric = parseMetric(fieldAsString(value));
            if (!metric)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported cppdiskann metric '{}'", fieldAsString(value));
            out.metric = *metric;
            seen_metric = true;
        }
        else if (name == "dim")
        {
            out.dim = fieldToUInt32(value, name);
            seen_dim = true;
        }
        else if (name == "pruned_degree")
            out.pruned_degree = fieldToUInt32(value, name);
        else if (name == "max_degree")
            out.max_degree = fieldToUInt32(value, name);
        else if (name == "l_build")
            out.l_build = fieldToUInt32(value, name);
        else if (name == "alpha")
            out.alpha = static_cast<float>(fieldToDouble(value, name));
        else if (name == "num_threads")
            out.num_threads = fieldToUInt32(value, name);
        else if (name == "pq_chunks")
            out.pq_chunks = fieldToUInt32(value, name);
        else if (name == "build_quantization")
            out.build_quantization = fieldAsString(value);
        else if (name == "pq_code_budget_gb")
        {
            out.pq_code_budget_gb = fieldToDouble(value, name);
            seen_pq_code_budget_gb = true;
        }
        else if (name == "pq_code_budget_gb_ratio")
        {
            out.pq_code_budget_gb_ratio = fieldToDouble(value, name);
            seen_pq_code_budget_gb_ratio = true;
        }
        else if (name == "build_ram_limit_gb")
            out.build_ram_limit_gb = fieldToDouble(value, name);
        else if (name == "search_cache_budget_gb")
        {
            out.search_cache_budget_gb = fieldToDouble(value, name);
            seen_search_cache_budget_gb = true;
        }
        else if (name == "search_cache_budget_gb_ratio")
        {
            out.search_cache_budget_gb_ratio = fieldToDouble(value, name);
            seen_search_cache_budget_gb_ratio = true;
        }
        else if (name == "disk_pq_dims")
        {
            out.disk_pq_dims = fieldToUInt32(value, name);
            out.disk_pq_dims_explicit = true;
        }
        else if (name == "accelerate_build")
            out.accelerate_build = fieldToBool(value, name);
        else if (name == "num_nodes_to_cache")
            out.num_nodes_to_cache = fieldToUInt32(value, name);
    }

    if (!seen_metric)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann: 'metric' is mandatory");
    if (!seen_dim || out.dim == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann: 'dim' is mandatory and must be greater than zero");
    if (out.pq_chunks > out.dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann: 'pq_chunks' must be in range [1, dim], got {} for dim {}", out.pq_chunks, out.dim);
    if (out.disk_pq_dims > out.dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann: 'disk_pq_dims' must be in range [0, dim], got {} for dim {}", out.disk_pq_dims, out.dim);
    if (out.pq_code_budget_gb < 0.0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann: 'pq_code_budget_gb' must be non-negative, got {}", out.pq_code_budget_gb);
    if (out.pq_code_budget_gb_ratio < 0.0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann: 'pq_code_budget_gb_ratio' must be non-negative, got {}", out.pq_code_budget_gb_ratio);
    if (seen_pq_code_budget_gb && !seen_pq_code_budget_gb_ratio)
        out.pq_code_budget_gb_ratio = 0.0;
    if (out.pq_code_budget_gb <= 0.0 && out.pq_code_budget_gb_ratio <= 0.0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann: either 'pq_code_budget_gb' must be greater than zero or 'pq_code_budget_gb_ratio' must be greater than zero");
    if (out.build_ram_limit_gb < 0.0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann: 'build_ram_limit_gb' must be non-negative, got {}", out.build_ram_limit_gb);
    if (out.search_cache_budget_gb < 0.0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann: 'search_cache_budget_gb' must be non-negative, got {}", out.search_cache_budget_gb);
    if (out.search_cache_budget_gb_ratio < 0.0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann: 'search_cache_budget_gb_ratio' must be non-negative, got {}", out.search_cache_budget_gb_ratio);
    if (seen_search_cache_budget_gb && !seen_search_cache_budget_gb_ratio)
        out.search_cache_budget_gb_ratio = 0.0;
    if (out.num_threads == 0)
        out.num_threads = getNumberOfCPUCoresToUse();

    if (out.disk_pq_dims_explicit)
        out.pq_chunks = out.disk_pq_dims;

    out.build_quantization = normalizeBuildQuantization(out.build_quantization, out.dim, out.pq_chunks);
    const UInt32 quantization_pq_chunks = pqChunksFromBuildQuantization(out.build_quantization);
    if (quantization_pq_chunks != 0 && out.pq_chunks != 0 && quantization_pq_chunks != out.pq_chunks)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "cppdiskann: 'build_quantization' ({}) conflicts with 'pq_chunks' ({})",
            out.build_quantization,
            out.pq_chunks);
    if (quantization_pq_chunks == 0 && out.pq_chunks != 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "cppdiskann: 'build_quantization' FP conflicts with non-zero 'pq_chunks' ({})",
            out.pq_chunks);
    if (out.disk_pq_dims_explicit && quantization_pq_chunks != out.disk_pq_dims)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "cppdiskann: 'build_quantization' ({}) conflicts with 'disk_pq_dims' ({})",
            out.build_quantization,
            out.disk_pq_dims);
    out.pq_chunks = quantization_pq_chunks;
    out.disk_pq_dims = out.pq_chunks;

    return out;
}

String CppDiskANNAlgorithm::calculateParamsHash(const BuildParams & build_params)
{
    SipHash params_hasher;
    params_hasher.update(static_cast<UInt8>(build_params.metric));
    params_hasher.update(build_params.dim);
    params_hasher.update(build_params.pruned_degree);
    params_hasher.update(build_params.max_degree);
    params_hasher.update(build_params.l_build);
    params_hasher.update(build_params.alpha);
    params_hasher.update(build_params.num_threads);
    params_hasher.update(build_params.pq_chunks);
    params_hasher.update(build_params.build_quantization);
    params_hasher.update(build_params.pq_code_budget_gb);
    params_hasher.update(build_params.pq_code_budget_gb_ratio);
    params_hasher.update(build_params.build_ram_limit_gb);
    params_hasher.update(build_params.search_cache_budget_gb);
    params_hasher.update(build_params.search_cache_budget_gb_ratio);
    params_hasher.update(build_params.disk_pq_dims);
    params_hasher.update(build_params.accelerate_build);
    params_hasher.update(build_params.num_nodes_to_cache);
    const UInt128 ph = params_hasher.get128();
    return fmt::format("{:016x}{:016x}", ph.items[UInt128::_impl::little(0)], ph.items[UInt128::_impl::little(1)]);
}

void CppDiskANNAlgorithm::validateBuildParameters(const ASTPtr & build_params, ContextPtr)
{
    validated_params = parseBuildParameters(build_params);
}

void CppDiskANNAlgorithm::validateIndexedExpression(const ASTPtr & indexed_expression, const StorageInMemoryMetadata & source_metadata)
{
    if (!indexed_expression)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann requires an indexed expression referring to one Array(Float32) or Array(BFloat16) column");

    const IAST * target = indexed_expression.get();
    if (const auto * list = typeid_cast<const ASTExpressionList *>(target))
    {
        if (list->children.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann supports indexing exactly one Array(Float32) or Array(BFloat16) column, got {}", list->children.size());
        target = list->children.front().get();
    }

    const auto * ident = typeid_cast<const ASTIdentifier *>(target);
    if (!ident)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann indexed expression must be a bare column reference");

    const auto & columns = source_metadata.columns;
    if (!columns.has(ident->name()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann column '{}' does not exist in the source table", ident->name());

    const auto column_type = columns.get(ident->name()).type;
    const auto * array_type = typeid_cast<const DataTypeArray *>(column_type.get());
    if (!array_type)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann indexed column '{}' must be Array(Float32) or Array(BFloat16), got {}", ident->name(), column_type->getName());

    const auto * nested_type = array_type->getNestedType().get();
    if (!typeid_cast<const DataTypeFloat32 *>(nested_type) && !typeid_cast<const DataTypeBFloat16 *>(nested_type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann indexed column '{}' must be Array(Float32) or Array(BFloat16), got {}", ident->name(), column_type->getName());
}

void CppDiskANNAlgorithm::initialize(const ANNIndexContext & ctx)
{
    initialized = true;
    if (ctx.reflection_settings)
    {
        const auto & settings = *ctx.reflection_settings;
        searcher_cache = std::make_shared<ANNSearcherCache<CppDiskANNFacade::Searcher>>(
            settings[MergeTreeSetting::ann_searcher_cache_policy],
            settings[MergeTreeSetting::ann_searcher_cache_size],
            settings[MergeTreeSetting::ann_searcher_cache_max_entries],
            settings[MergeTreeSetting::ann_searcher_cache_size_ratio]);
    }
    else
    {
        searcher_cache = defaultCppDiskANNSearcherCache();
    }
}

void CppDiskANNAlgorithm::setBuildParameters(const ASTPtr & build_params, ContextPtr)
{
    params = parseBuildParameters(build_params);
    validated_params = params;
}

size_t CppDiskANNAlgorithm::searcherCacheSizeForTests() const
{
    return searcher_cache ? searcher_cache->count() : 0;
}

CppDiskANNFacade::SearchParams CppDiskANNAlgorithm::defaultSearchParams() const
{
    CppDiskANNFacade::SearchParams search_params;
    search_params.search_list_size = 10;
    search_params.beam_width = 4;
    search_params.num_threads = SEARCHER_NUM_THREADS_DEFAULT;
    search_params.nodes_to_cache = SEARCHER_NODES_TO_CACHE_DEFAULT;
    return search_params;
}

std::shared_ptr<CppDiskANNFacade::Searcher> CppDiskANNAlgorithm::cacheSearcherForIndexPrefix(
    const UUID & ann_index_part_uuid,
    const std::string & index_prefix,
    const CppDiskANNFacade::SearchParams & search_params) const
{
    if (!validated_params || validated_params->dim == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "cppdiskann searcher cache warmup invoked before parameters were validated");
    if (ann_index_part_uuid == UUIDHelpers::Nil)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "cppdiskann searcher cache requires a stable ANN index part UUID");

    if (!searcher_cache)
        searcher_cache = defaultCppDiskANNSearcherCache();

    const auto active_params = *validated_params;
    ANNSearcherCacheKey cache_key{ann_index_part_uuid, getBuildParamsHash()};
    return searcher_cache->getOrSet(
        cache_key,
        [&]() -> std::pair<std::shared_ptr<CppDiskANNFacade::Searcher>, size_t>
        {
            auto opened = CppDiskANNFacade::openSearcher(index_prefix, active_params.metric, search_params);
            std::shared_ptr<CppDiskANNFacade::Searcher> fresh(std::move(opened));
            return {std::move(fresh), 0};
        });
}

std::optional<MatchDescriptor> CppDiskANNAlgorithm::match(const QueryFeatures & features) const
{
    if (!validated_params || validated_params->dim == 0 || features.k == 0)
        return std::nullopt;
    if (features.query_vector.size() != validated_params->dim)
        return std::nullopt;
    if (!queryFunctionMatchesMetric(features.distance_function, validated_params->metric))
        return std::nullopt;

    MatchDescriptor desc;
    desc.query_vector = features.query_vector;
    desc.distance.exact_function_name = "__reflectionANNIndexCppDiskANNDistance";
    desc.distance.metric_name = metricName(validated_params->metric);
    desc.distance.metric_id = static_cast<UInt64>(validated_params->metric);
    desc.distance.dim = validated_params->dim;
    desc.distance.smaller_is_better = validated_params->metric != CppDiskANNFacade::Metric::InnerProduct;
    desc.k = features.k;
    return desc;
}

AlgorithmCostEstimate CppDiskANNAlgorithm::estimateCost(const MatchDescriptor & desc, const CoverageSnapshot & coverage) const
{
    AlgorithmCostEstimate est;
    est.estimated_result_rows = coverage.candidate_limit != 0 ? coverage.candidate_limit : desc.k;
    est.algorithm_search_cost = coverage.ready_ann_index_parts * (desc.k + 1);
    return est;
}

std::vector<AlgorithmPrivatePath> CppDiskANNAlgorithm::getAlgorithmPrivatePaths(const IDataPartStorage & storage) const
{
    std::vector<AlgorithmPrivatePath> paths;
    for (const auto & rel : collectPrivateIndexFiles(storage))
        paths.push_back({.path = rel, .recursive = false, .required = true});
    paths.push_back({.path = "algorithm_private_fingerprint.json", .recursive = false, .required = true});
    return paths;
}

InternalSearchResult CppDiskANNAlgorithm::search(
    const MatchDescriptor & desc,
    const ReadyANNIndexPartSnapshot & ready_parts,
    size_t candidate_limit,
    ContextPtr query_context) const
{
    ProfileEvents::increment(ProfileEvents::ANNIndexCppDiskANNSearchStarted);
    bool search_finished = false;
    scope_guard failed_search_guard = [&search_finished]
    {
        if (!search_finished)
            ProfileEvents::increment(ProfileEvents::ANNIndexCppDiskANNSearchFailed);
    };

    if (ready_parts.parts.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "cppdiskann search invoked without any ready parts");
    if (!validated_params || validated_params->dim == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "cppdiskann search invoked before parameters were validated");

    const auto active_params = *validated_params;
    if (desc.query_vector.size() != active_params.dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann search query vector size {} does not match index dim {}", desc.query_vector.size(), active_params.dim);
    if (candidate_limit == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann search candidate_limit must be > 0");
    if (candidate_limit > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann search candidate_limit is out of UInt32 range");

    const UInt32 k = static_cast<UInt32>(candidate_limit);

    CppDiskANNFacade::SearchParams search_params = defaultSearchParams();
    bool enable_inline_locator = true;
    if (query_context)
    {
        const auto & settings = query_context->getSettingsRef();
        enable_inline_locator = settings[Setting::enable_diskann_index_search_using_inline_locator];
        search_params.search_list_size = settingOrDefault(settings[Setting::diskann_search_list_size], search_params.search_list_size, std::numeric_limits<UInt32>::max(), "diskann_search_list_size");
        search_params.beam_width = settingOrDefault(settings[Setting::diskann_search_beam_width], search_params.beam_width, std::numeric_limits<UInt32>::max(), "diskann_search_beam_width");
        search_params.num_threads = settingOrDefault(settings[Setting::diskann_search_num_threads], search_params.num_threads, SEARCHER_NUM_THREADS_MAX, "diskann_search_num_threads");
        search_params.nodes_to_cache = settingOrDefault(settings[Setting::diskann_search_nodes_to_cache], search_params.nodes_to_cache, SEARCHER_NODES_TO_CACHE_MAX, "diskann_search_nodes_to_cache");
    }

    if (!searcher_cache)
        searcher_cache = defaultCppDiskANNSearcherCache();

    InternalSearchResult result;
    result.per_ann_index_part.reserve(ready_parts.parts.size());

    for (const auto & ready_part : ready_parts.parts)
    {
        checkSearchCancelled(query_context);

        const auto & part_storage = ready_part.storage;
        if (!part_storage)
            continue;

        const std::string index_prefix = part_storage->getFullPath() + INDEX_PREFIX;
        auto searcher = cacheSearcherForIndexPrefix(ready_part.ann_index_part_uuid, index_prefix, search_params);

        /// Fast path: if this part was never remapped and its graph carries the
        /// 16-byte locator payload, fetch (part_id, part_offset) inline from the
        /// search so the framework skips the offset.bin read. A remapped part's
        /// baked payload is stale, so it falls back to the locator sidecar.
        constexpr UInt32 payload_record_size = CppDiskANNFacade::PAYLOAD_RECORD_SIZE;
        const bool want_payload = enable_inline_locator
            && !ready_part.is_remaped
            && searcher->payloadBytesPerNode() == static_cast<UInt64>(payload_record_size);

        std::vector<UInt64> hits(k, 0);
        std::vector<float> distances(k, 0.0f);
        std::vector<UInt8> payload_bytes;
        UInt32 hit_count;
        if (want_payload)
        {
            payload_bytes.assign(static_cast<size_t>(k) * payload_record_size, 0);
            hit_count = searcher->searchWithPayload(
                desc.query_vector.data(),
                active_params.dim,
                k,
                search_params.search_list_size,
                search_params.beam_width,
                hits.data(),
                distances.data(),
                payload_bytes.data(),
                payload_record_size);
        }
        else
        {
            hit_count = searcher->search(
                desc.query_vector.data(),
                active_params.dim,
                k,
                search_params.search_list_size,
                search_params.beam_width,
                hits.data(),
                distances.data());
        }

        hits.resize(hit_count);
        distances.resize(hit_count);
        if (active_params.metric == CppDiskANNFacade::Metric::InnerProduct)
        {
            for (auto & distance : distances)
                distance = -distance;
        }

        if (hit_count == 0)
            continue;

        InternalHitSet hit_set;
        hit_set.ann_index_part_storage = part_storage;
        hit_set.internal_ids = std::move(hits);
        hit_set.distances = std::move(distances);
        if (want_payload)
        {
            /// Decode the inline payload (parallel to results): UInt32 part_id at
            /// [0, 4), 4 reserved bytes, UInt64 part_offset at [8, 16) — the exact
            /// layout baked at build time.
            hit_set.payload_offsets.resize(hit_count);
            for (UInt32 i = 0; i < hit_count; ++i)
            {
                const UInt8 * record = payload_bytes.data() + static_cast<size_t>(i) * payload_record_size;
                memcpy(&hit_set.payload_offsets[i].part_id, record, sizeof(UInt32));
                memcpy(&hit_set.payload_offsets[i].part_offset, record + sizeof(UInt64), sizeof(UInt64));
            }
        }
        result.per_ann_index_part.push_back(std::move(hit_set));
    }

    ProfileEvents::increment(ProfileEvents::ANNIndexCppDiskANNSearchFinished);
    search_finished = true;
    return result;
}

void CppDiskANNAlgorithm::prepareBuild(const AlgorithmBuildContext & ctx, const Block & indexed_columns_batch)
{
    if (!validated_params)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "cppdiskann build invoked before parameters were validated");
    params = *validated_params;

    if (!ctx.intermediate_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "cppdiskann build requires intermediate_storage in AlgorithmBuildContext");

    auto throwIfCancelled = [&]
    {
        if (!ctx.is_cancelled || !ctx.is_cancelled->load(std::memory_order_relaxed))
            return;

        fbin_writer.reset();
        if (fbin_buf)
        {
            fbin_buf->cancel();
            fbin_buf.reset();
        }
        throw Exception(ErrorCodes::ABORTED, "cppdiskann build cancelled during prepareBuild");
    };

    throwIfCancelled();

    if (!fbin_writer)
    {
        ProfileEvents::increment(ProfileEvents::ANNIndexCppDiskANNBuildStarted);
        ctx.intermediate_storage->createDirectories();
        fbin_buf = ctx.intermediate_storage->writeFile("vectors.fbin", 64 * 1024, WriteSettings{});
        fbin_writer = std::make_unique<DiskANNFbinWriter>(*fbin_buf, params.dim);
        rows_seen_in_build = 0;
        rows_since_last_cancel_poll = 0;
    }

    if (indexed_columns_batch.columns() == 0)
        return;

    const auto & first_col = indexed_columns_batch.getByPosition(0).column;
    const auto * arr_col = typeid_cast<const ColumnArray *>(first_col.get());
    if (!arr_col)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "cppdiskann indexed column was not Array; got {}", first_col->getName());

    const auto & offsets = arr_col->getOffsets();
    const auto * float_col = typeid_cast<const ColumnVector<Float32> *>(&arr_col->getData());
    const auto * bfloat16_col = typeid_cast<const ColumnVector<BFloat16> *>(&arr_col->getData());
    if (!float_col && !bfloat16_col)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "cppdiskann indexed column was Array of {} (expected Array(Float32) or Array(BFloat16))", arr_col->getData().getName());

    std::vector<Float32> converted_bfloat16_row;
    if (bfloat16_col)
        converted_bfloat16_row.resize(params.dim);

    UInt64 prev_offset = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        if (rows_since_last_cancel_poll >= CANCEL_POLL_ROW_GRANULE)
        {
            throwIfCancelled();
            rows_since_last_cancel_poll = 0;
        }

        const UInt64 cur_offset = offsets[i];
        const UInt64 row_size = cur_offset - prev_offset;
        if (row_size != params.dim)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "cppdiskann row {} has dim {} but index expects {}", rows_seen_in_build, row_size, params.dim);

        if (float_col)
        {
            const auto & flat = float_col->getData();
            fbin_writer->appendRow(&flat[prev_offset], static_cast<size_t>(row_size));
        }
        else
        {
            const auto & flat = bfloat16_col->getData();
            for (UInt32 d = 0; d < params.dim; ++d)
                converted_bfloat16_row[d] = static_cast<Float32>(flat[prev_offset + d]);
            fbin_writer->appendRow(converted_bfloat16_row.data(), static_cast<size_t>(row_size));
        }

        prev_offset = cur_offset;
        ++rows_seen_in_build;
        ++rows_since_last_cancel_poll;
    }
}

void CppDiskANNAlgorithm::buildAlgorithmPrivate(const AlgorithmBuildContext & ctx)
{
    if (!fbin_writer || !fbin_buf)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "cppdiskann buildAlgorithmPrivate called before prepareBuild");

    if (ctx.is_cancelled && ctx.is_cancelled->load(std::memory_order_relaxed))
    {
        fbin_writer.reset();
        fbin_buf->cancel();
        fbin_buf.reset();
        throw Exception(ErrorCodes::ABORTED, "cppdiskann build cancelled before build invocation");
    }

    fbin_writer->finalize();
    fbin_buf->finalize();

    if (!ctx.intermediate_storage || !ctx.output_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "cppdiskann build requires both intermediate_storage and output_storage");

    ctx.output_storage->createDirectories();
    const double raw_data_size_gb = rawVectorDataSizeGiB(rows_seen_in_build, params.dim);
    const double pq_code_budget_gb = std::max(params.pq_code_budget_gb, raw_data_size_gb * params.pq_code_budget_gb_ratio);
    const double search_cache_budget_gb = std::max(params.search_cache_budget_gb, raw_data_size_gb * params.search_cache_budget_gb_ratio);
    const double build_ram_limit_gb = params.build_ram_limit_gb > 0.0 ? params.build_ram_limit_gb : getBuildMemoryBudgetGiB();
    UInt32 num_nodes_to_cache = params.num_nodes_to_cache;
    if (num_nodes_to_cache == 0 && search_cache_budget_gb > 0.0)
    {
        num_nodes_to_cache = cachedNodeCountFromBudget(
            search_cache_budget_gb,
            params.dim,
            sizeof(Float32),
            params.max_degree);
    }

    CppDiskANNFacade::BuildParams facade_params;
    facade_params.metric = params.metric;
    facade_params.pruned_degree = params.pruned_degree;
    facade_params.max_degree = params.max_degree;
    facade_params.l_build = params.l_build;
    facade_params.alpha = params.alpha;
    facade_params.num_threads = params.num_threads;
    facade_params.pq_chunks = params.pq_chunks;
    facade_params.build_quantization = params.build_quantization;
    facade_params.pq_code_budget_gb = pq_code_budget_gb;
    facade_params.build_ram_limit_gb = build_ram_limit_gb;
    facade_params.disk_pq_dims = params.disk_pq_dims;
    facade_params.accelerate_build = params.accelerate_build;
    facade_params.num_nodes_to_cache = num_nodes_to_cache;

    /// Bake the framework's locator offsets into the graph as inline per-node
    /// payload so the search path can return (part_id, part_offset) without a
    /// separate offset.bin read for parts that were never remapped. The framework
    /// hands packed 12-byte records (UInt32 part_id + UInt64 part_offset);
    /// diskann-internal carries a fixed 16-byte NodePayload, so re-pack each
    /// record into part_id at [0,4), 4 reserved bytes, part_offset at [8,16).
    /// The search path reverses this. The payload file gets the diskann
    /// payload-file header ([u32 npts][u32 bytes_per_node]); empty content (a
    /// zero-row build) leaves payload_file unset = "do not bake payload".
    if (ctx.associated_data_record_size != 0)
    {
        if (ctx.associated_data_record_size != ANNIndexLocator::OFFSET_RECORD_SIZE)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "cppdiskann expects {}-byte locator payload records, got {}",
                ANNIndexLocator::OFFSET_RECORD_SIZE,
                ctx.associated_data_record_size);

        const size_t npts = ctx.associated_data_content.size() / ANNIndexLocator::OFFSET_RECORD_SIZE;
        if (npts != rows_seen_in_build)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "cppdiskann locator payload carries {} records but the build saw {} rows",
                npts,
                rows_seen_in_build);

        auto payload_out = ctx.intermediate_storage->writeFile("locator_payload.bin", 64 * 1024, WriteSettings{});
        writeBinary(static_cast<UInt32>(npts), *payload_out);
        writeBinary(static_cast<UInt32>(CppDiskANNFacade::PAYLOAD_RECORD_SIZE), *payload_out);
        const std::array<char, 4> reserved{};
        for (size_t i = 0; i < npts; ++i)
        {
            const char * record
                = reinterpret_cast<const char *>(ctx.associated_data_content.data()) + i * ANNIndexLocator::OFFSET_RECORD_SIZE;
            payload_out->write(record, sizeof(UInt32));                  /// part_id  -> [0, 4)
            payload_out->write(reserved.data(), reserved.size());        /// reserved -> [4, 8)
            payload_out->write(record + sizeof(UInt32), sizeof(UInt64)); /// part_offset -> [8, 16)
        }
        payload_out->finalize();

        facade_params.payload_file = ctx.intermediate_storage->getFullPath() + "locator_payload.bin";
    }

    LOG_INFO(
        getLogger("CppDiskANNAlgorithm"),
        "cppdiskann effective build params: rows={}, dim={}, metric={}, pruned_degree={}, max_degree={}, l_build={}, alpha={}, "
        "num_threads={}, pq_chunks={}, build_quantization='{}', raw_data_size_gb={}, pq_code_budget_gb={}, "
        "pq_code_budget_gb_ratio={}, build_ram_limit_gb={}, search_cache_budget_gb={}, search_cache_budget_gb_ratio={}, "
        "disk_pq_dims={}, disk_pq_dims_explicit={}, accelerate_build={}, num_nodes_to_cache={}, data_path='{}', index_prefix='{}'",
        rows_seen_in_build,
        params.dim,
        metricName(params.metric),
        params.pruned_degree,
        params.max_degree,
        params.l_build,
        params.alpha,
        params.num_threads,
        params.pq_chunks,
        params.build_quantization,
        raw_data_size_gb,
        pq_code_budget_gb,
        params.pq_code_budget_gb_ratio,
        build_ram_limit_gb,
        search_cache_budget_gb,
        params.search_cache_budget_gb_ratio,
        params.disk_pq_dims,
        params.disk_pq_dims_explicit,
        params.accelerate_build,
        num_nodes_to_cache,
        ctx.intermediate_storage->getFullPath() + "vectors.fbin",
        ctx.output_storage->getFullPath() + INDEX_PREFIX);

    try
    {
        const std::string index_prefix = ctx.output_storage->getFullPath() + INDEX_PREFIX;
        CppDiskANNFacade::build(ctx.intermediate_storage->getFullPath() + "vectors.fbin", index_prefix, facade_params);
        /// Build tasks have no query settings, so prewarm with the same defaults
        /// used by a search without an explicit query context.
        cacheSearcherForIndexPrefix(ctx.ann_index_part_uuid, index_prefix, defaultSearchParams());
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::ANNIndexCppDiskANNBuildFailed);
        throw;
    }

    ProfileEvents::increment(ProfileEvents::ANNIndexCppDiskANNBuildFinished);
}

void CppDiskANNAlgorithm::finishBuild(const AlgorithmBuildContext & ctx)
{
    fbin_writer.reset();
    fbin_buf.reset();

    if (!ctx.output_storage)
        return;

    const String params_hash = calculateParamsHash(params);

    Poco::JSON::Array files_arr;
    for (const auto & rel : collectPrivateIndexFiles(*ctx.output_storage))
    {
        Poco::JSON::Object entry;
        entry.set("name", rel);
        entry.set("size", static_cast<Int64>(ctx.output_storage->getFileSize(rel)));
        entry.set("sipHash128", fileSipHashHex(*ctx.output_storage, rel));
        files_arr.add(entry);
    }

    Poco::JSON::Object fingerprint;
    fingerprint.set("algorithm_version", getAlgorithmVersion());
    fingerprint.set("params_hash", params_hash);
    fingerprint.set("num_points", static_cast<Int64>(rows_seen_in_build));
    fingerprint.set("files", files_arr);

    auto writer = ctx.output_storage->writeFile("algorithm_private_fingerprint.json", 4096, WriteSettings{});
    std::ostringstream oss;
    Poco::JSON::Stringifier::stringify(fingerprint, oss);
    const std::string body = oss.str();
    writer->write(body.data(), body.size());
    writer->finalize();

    rows_seen_in_build = 0;
    rows_since_last_cancel_poll = 0;
}

}

#endif
