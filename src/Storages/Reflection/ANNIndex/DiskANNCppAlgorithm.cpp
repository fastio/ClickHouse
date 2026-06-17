#include <Storages/Reflection/ANNIndex/DiskANNCppAlgorithm.h>

#if USE_DISKANN_CPP

#include <Storages/Reflection/ANNIndex/DiskANNFbinWriter.h>
#include <Storages/Reflection/ANNIndex/ANNIndexContext.h>

#include <base/scope_guard.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
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
#include <filesystem>
#include <limits>
#include <sstream>
#include <string_view>
#include <vector>

namespace ProfileEvents
{
    extern const Event ANNIndexDiskANNCppBuildStarted;
    extern const Event ANNIndexDiskANNCppBuildFinished;
    extern const Event ANNIndexDiskANNCppBuildFailed;
    extern const Event ANNIndexDiskANNCppSearchStarted;
    extern const Event ANNIndexDiskANNCppSearchFinished;
    extern const Event ANNIndexDiskANNCppSearchFailed;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 diskann_search_list_size;
    extern const SettingsUInt64 diskann_search_beam_width;
    extern const SettingsUInt64 diskann_search_num_threads;
    extern const SettingsUInt64 diskann_search_io_limit;
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
constexpr const char * INDEX_PREFIX = "algorithm_private_diskann_cpp";
constexpr UInt32 SEARCHER_NUM_THREADS_DEFAULT = 8;
constexpr UInt32 SEARCHER_IO_LIMIT_DEFAULT = 256;
constexpr UInt32 SEARCHER_NODES_TO_CACHE_DEFAULT = 1024;
constexpr UInt32 SEARCHER_NUM_THREADS_MAX = 64;
constexpr UInt32 SEARCHER_IO_LIMIT_MAX = 4096;
constexpr UInt32 SEARCHER_NODES_TO_CACHE_MAX = 65536;

std::optional<DiskANNCppFacade::Metric> parseMetric(std::string_view text)
{
    if (text == "L2" || text == "l2")
        return DiskANNCppFacade::Metric::L2;
    if (text == "cosine" || text == "Cosine" || text == "COSINE")
        return DiskANNCppFacade::Metric::Cosine;
    if (text == "InnerProduct" || text == "innerproduct" || text == "inner_product" || text == "INNER_PRODUCT" || text == "dotProduct")
        return DiskANNCppFacade::Metric::InnerProduct;
    if (text == "CosineNormalized" || text == "cosinenormalized" || text == "cosine_normalized" || text == "COSINE_NORMALIZED")
        return DiskANNCppFacade::Metric::CosineNormalized;
    return {};
}

String metricName(DiskANNCppFacade::Metric metric)
{
    switch (metric)
    {
        case DiskANNCppFacade::Metric::L2:
            return "L2";
        case DiskANNCppFacade::Metric::Cosine:
            return "cosine";
        case DiskANNCppFacade::Metric::InnerProduct:
            return "InnerProduct";
        case DiskANNCppFacade::Metric::CosineNormalized:
            return "CosineNormalized";
    }
}

bool queryFunctionMatchesMetric(const String & distance_function, DiskANNCppFacade::Metric metric)
{
    switch (metric)
    {
        case DiskANNCppFacade::Metric::L2:
            return distance_function == "L2Distance";
        case DiskANNCppFacade::Metric::Cosine:
        case DiskANNCppFacade::Metric::CosineNormalized:
            return distance_function == "cosineDistance";
        case DiskANNCppFacade::Metric::InnerProduct:
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
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN C++ search setting '{}' must be in range [0, {}], got {}",
            name,
            upper_inclusive,
            value);
    return static_cast<UInt32>(value);
}

std::unique_ptr<ANNSearcherCache<DiskANNCppFacade::Searcher>> defaultDiskANNCppSearcherCache()
{
    return std::make_unique<ANNSearcherCache<DiskANNCppFacade::Searcher>>(
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
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ parameter '{}' must be non-negative, got {}", name, v);
            return static_cast<UInt64>(v);
        }
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ parameter '{}' must be an integer literal", name);
    }
}

UInt32 fieldToUInt32(const Field & field, std::string_view name)
{
    const UInt64 value = fieldToUInt64(field, name);
    if (value > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ parameter '{}' is out of UInt32 range", name);
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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ parameter '{}' must be a numeric literal", name);
    }
}

bool isKnownParam(std::string_view name)
{
    return name == "metric" || name == "dim"
        || name == "pruned_degree" || name == "max_degree"
        || name == "l_build" || name == "alpha"
        || name == "num_threads" || name == "pq_chunks"
        || name == "build_quantization"
        || name == "build_ram_limit_gb";
}

UInt32 estimateDefaultPQChunks(UInt32 dim)
{
    constexpr UInt32 target_dimensions_per_chunk = 8;
    constexpr UInt32 max_default_pq_chunks = 16;

    UInt32 chunks = (dim + target_dimensions_per_chunk - 1) / target_dimensions_per_chunk;
    chunks = std::max<UInt32>(1, chunks);

    UInt32 rounded_chunks = 1;
    while (rounded_chunks < chunks && rounded_chunks < max_default_pq_chunks)
        rounded_chunks *= 2;

    return std::min({rounded_chunks, max_default_pq_chunks, dim});
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

bool parsePositiveFloatText(std::string_view text)
{
    if (text.empty())
        return false;

    String value{text};
    char * end = nullptr;
    const double parsed = std::strtod(value.c_str(), &end);
    return end == value.c_str() + value.size() && std::isfinite(parsed) && parsed > 0.0;
}

String normalizeBuildQuantization(const String & text, UInt32 dim)
{
    constexpr UInt32 default_build_quantization_pq_chunks = 16;

    if (text.empty())
        return "PQ_" + toString(std::min(default_build_quantization_pq_chunks, dim));

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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++: 'build_quantization' PQ chunks must be in range [1, dim], got '{}'", text);
        return "PQ_" + toString(chunks);
    }

    if ((parts.size() == 2 || parts.size() == 3) && (parts[0] == "SQ" || parts[0] == "sq"))
    {
        if (parts.size() == 3)
            (void)parsePositiveFloatText(parts[2]);
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN C++ backend does not support '{}' build_quantization; use FP or PQ_N",
            text);
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "DiskANN C++: 'build_quantization' must be 'FP', 'PQ_N', 'SQ_1', or 'SQ_1_STDDEV', got '{}'",
        text);
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

DiskANNCppAlgorithm::DiskANNCppAlgorithm() = default;
DiskANNCppAlgorithm::~DiskANNCppAlgorithm() = default;

String DiskANNCppAlgorithm::getAlgorithmVersion() const
{
    return "diskann_cpp";
}

String DiskANNCppAlgorithm::getBuildParamsHash() const
{
    if (!validated_params)
        return {};
    return calculateParamsHash(*validated_params);
}

std::map<String, String> DiskANNCppAlgorithm::getAlgorithmObservabilityFields() const
{
    if (!validated_params)
        return {};

    return buildSettings(*validated_params);
}

std::map<String, String> DiskANNCppAlgorithm::buildSettings(const BuildParams & p)
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
        {"build_ram_limit_gb", toString(p.build_ram_limit_gb)},
    };
}

std::unique_ptr<IANNIndexAlgorithm> DiskANNCppAlgorithm::cloneForBuild() const
{
    auto fresh = std::make_unique<DiskANNCppAlgorithm>();
    fresh->initialized = initialized;
    fresh->params = params;
    fresh->validated_params = validated_params;
    return fresh;
}

DiskANNCppAlgorithm::BuildParams DiskANNCppAlgorithm::parseBuildParameters(const ASTPtr & build_params)
{
    if (!build_params)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ requires keyword parameters: metric, dim are mandatory");

    const ASTExpressionList * list = nullptr;
    if (const auto * fn = typeid_cast<const ASTFunction *>(build_params.get()))
        list = fn->arguments ? typeid_cast<const ASTExpressionList *>(fn->arguments.get()) : nullptr;
    else
        list = typeid_cast<const ASTExpressionList *>(build_params.get());

    if (!list)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ parameters must be a keyword argument list");

    BuildParams out{};
    bool seen_metric = false;
    bool seen_dim = false;

    for (const auto & child : list->children)
    {
        const auto * eq = typeid_cast<const ASTFunction *>(child.get());
        if (!eq || eq->name != "equals" || !eq->arguments || eq->arguments->children.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ parameters must be keyword arguments");

        const auto * name_ast = typeid_cast<const ASTIdentifier *>(eq->arguments->children[0].get());
        const auto * value_ast = typeid_cast<const ASTLiteral *>(eq->arguments->children[1].get());
        if (!name_ast || !value_ast)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ parameters must be literal keyword arguments");

        const String name = name_ast->name();
        if (!isKnownParam(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown DiskANN C++ parameter '{}'", name);

        const auto & value = value_ast->value;
        if (name == "metric")
        {
            auto metric = parseMetric(fieldAsString(value));
            if (!metric)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported DiskANN C++ metric '{}'", fieldAsString(value));
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
        else if (name == "build_ram_limit_gb")
            out.build_ram_limit_gb = fieldToDouble(value, name);
    }

    if (!seen_metric)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++: 'metric' is mandatory");
    if (!seen_dim || out.dim == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++: 'dim' is mandatory and must be greater than zero");
    if (out.pq_chunks == 0)
        out.pq_chunks = estimateDefaultPQChunks(out.dim);
    else if (out.pq_chunks > out.dim)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN C++: 'pq_chunks' must be in range [1, dim], got {} for dim {}",
            out.pq_chunks,
            out.dim);
    out.build_quantization = normalizeBuildQuantization(out.build_quantization, out.dim);

    return out;
}

String DiskANNCppAlgorithm::calculateParamsHash(const BuildParams & build_params)
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
    params_hasher.update(build_params.build_ram_limit_gb);
    const UInt128 ph = params_hasher.get128();
    return fmt::format(
        "{:016x}{:016x}",
        ph.items[UInt128::_impl::little(0)],
        ph.items[UInt128::_impl::little(1)]);
}

void DiskANNCppAlgorithm::validateBuildParameters(const ASTPtr & build_params, ContextPtr)
{
    validated_params = parseBuildParameters(build_params);
}

void DiskANNCppAlgorithm::validateIndexedExpression(const ASTPtr & indexed_expression, const StorageInMemoryMetadata & source_metadata)
{
    if (!indexed_expression)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ requires an indexed expression referring to one Array(Float32) or Array(BFloat16) column");

    const IAST * target = indexed_expression.get();
    if (const auto * list = typeid_cast<const ASTExpressionList *>(target))
    {
        if (list->children.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ supports indexing exactly one Array(Float32) or Array(BFloat16) column, got {}", list->children.size());
        target = list->children.front().get();
    }

    const auto * ident = typeid_cast<const ASTIdentifier *>(target);
    if (!ident)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ indexed expression must be a bare column reference");

    const auto & columns = source_metadata.columns;
    if (!columns.has(ident->name()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ column '{}' does not exist in the source table", ident->name());

    const auto column_type = columns.get(ident->name()).type;
    const auto * array_type = typeid_cast<const DataTypeArray *>(column_type.get());
    if (!array_type)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ indexed column '{}' must be Array(Float32) or Array(BFloat16), got {}", ident->name(), column_type->getName());

    const auto * nested_type = array_type->getNestedType().get();
    if (!typeid_cast<const DataTypeFloat32 *>(nested_type) && !typeid_cast<const DataTypeBFloat16 *>(nested_type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ indexed column '{}' must be Array(Float32) or Array(BFloat16), got {}", ident->name(), column_type->getName());
}

void DiskANNCppAlgorithm::initialize(const ANNIndexContext & ctx)
{
    initialized = true;
    if (ctx.reflection_settings)
    {
        const auto & settings = *ctx.reflection_settings;
        searcher_cache = std::make_unique<ANNSearcherCache<DiskANNCppFacade::Searcher>>(
            settings[MergeTreeSetting::ann_searcher_cache_policy],
            settings[MergeTreeSetting::ann_searcher_cache_size],
            settings[MergeTreeSetting::ann_searcher_cache_max_entries],
            settings[MergeTreeSetting::ann_searcher_cache_size_ratio]);
    }
    else
    {
        searcher_cache = defaultDiskANNCppSearcherCache();
    }
}

void DiskANNCppAlgorithm::setBuildParameters(const ASTPtr & build_params, ContextPtr)
{
    params = parseBuildParameters(build_params);
    validated_params = params;
}

size_t DiskANNCppAlgorithm::searcherCacheSizeForTests() const
{
    return searcher_cache ? searcher_cache->count() : 0;
}

std::optional<MatchDescriptor> DiskANNCppAlgorithm::match(const QueryFeatures & features) const
{
    if (!validated_params || validated_params->dim == 0 || features.k == 0)
        return std::nullopt;
    if (features.query_vector.size() != validated_params->dim)
        return std::nullopt;
    if (!queryFunctionMatchesMetric(features.distance_function, validated_params->metric))
        return std::nullopt;

    MatchDescriptor desc;
    desc.query_vector = features.query_vector;
    desc.distance.exact_function_name = "__reflectionANNIndexDiskANNCppDistance";
    desc.distance.metric_name = metricName(validated_params->metric);
    desc.distance.metric_id = static_cast<UInt64>(validated_params->metric);
    desc.distance.dim = validated_params->dim;
    desc.distance.smaller_is_better = validated_params->metric != DiskANNCppFacade::Metric::InnerProduct;
    desc.k = features.k;
    return desc;
}

AlgorithmCostEstimate DiskANNCppAlgorithm::estimateCost(const MatchDescriptor & desc, const CoverageSnapshot & coverage) const
{
    AlgorithmCostEstimate est;
    est.estimated_result_rows = coverage.candidate_limit != 0 ? coverage.candidate_limit : desc.k;
    est.algorithm_search_cost = coverage.ready_ann_index_parts * (desc.k + 1);
    return est;
}

std::vector<AlgorithmPrivatePath> DiskANNCppAlgorithm::getAlgorithmPrivatePaths(const IDataPartStorage & storage) const
{
    std::vector<AlgorithmPrivatePath> paths;
    for (const auto & rel : collectPrivateIndexFiles(storage))
        paths.push_back({.path = rel, .recursive = false, .required = true});
    paths.push_back({.path = "algorithm_private_fingerprint.json", .recursive = false, .required = true});
    return paths;
}

InternalSearchResult DiskANNCppAlgorithm::search(
    const MatchDescriptor & desc,
    const ReadyANNIndexPartSnapshot & ready_parts,
    size_t candidate_limit,
    ContextPtr query_context) const
{
    ProfileEvents::increment(ProfileEvents::ANNIndexDiskANNCppSearchStarted);
    bool search_finished = false;
    scope_guard failed_search_guard = [&search_finished]
    {
        if (!search_finished)
            ProfileEvents::increment(ProfileEvents::ANNIndexDiskANNCppSearchFailed);
    };

    if (ready_parts.parts.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskANN C++ search invoked without any ready parts");

    if (!validated_params || validated_params->dim == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskANN C++ search invoked before parameters were validated");

    const auto active_params = *validated_params;
    if (desc.query_vector.size() != active_params.dim)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN C++ search query vector size {} does not match index dim {}",
            desc.query_vector.size(),
            active_params.dim);
    if (candidate_limit == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ search candidate_limit must be > 0");
    if (candidate_limit > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ search candidate_limit is out of UInt32 range");

    const UInt32 k = static_cast<UInt32>(candidate_limit);

    DiskANNCppFacade::SearchParams search_params;
    search_params.search_list_size = 10;
    search_params.beam_width = 4;
    search_params.num_threads = SEARCHER_NUM_THREADS_DEFAULT;
    search_params.search_io_limit = SEARCHER_IO_LIMIT_DEFAULT;
    search_params.nodes_to_cache = SEARCHER_NODES_TO_CACHE_DEFAULT;
    if (query_context)
    {
        const auto & settings = query_context->getSettingsRef();
        search_params.search_list_size = settingOrDefault(
            settings[Setting::diskann_search_list_size],
            search_params.search_list_size,
            std::numeric_limits<UInt32>::max(),
            "diskann_search_list_size");
        search_params.beam_width = settingOrDefault(
            settings[Setting::diskann_search_beam_width],
            search_params.beam_width,
            std::numeric_limits<UInt32>::max(),
            "diskann_search_beam_width");
        search_params.num_threads = settingOrDefault(
            settings[Setting::diskann_search_num_threads],
            search_params.num_threads,
            SEARCHER_NUM_THREADS_MAX,
            "diskann_search_num_threads");
        search_params.search_io_limit = settingOrDefault(
            settings[Setting::diskann_search_io_limit],
            search_params.search_io_limit,
            SEARCHER_IO_LIMIT_MAX,
            "diskann_search_io_limit");
        search_params.nodes_to_cache = settingOrDefault(
            settings[Setting::diskann_search_nodes_to_cache],
            search_params.nodes_to_cache,
            SEARCHER_NODES_TO_CACHE_MAX,
            "diskann_search_nodes_to_cache");
    }

    if (!searcher_cache)
        searcher_cache = defaultDiskANNCppSearcherCache();

    InternalSearchResult result;
    result.per_ann_index_part.reserve(ready_parts.parts.size());

    for (const auto & ready_part : ready_parts.parts)
    {
        checkSearchCancelled(query_context);

        const auto & part_storage = ready_part.storage;
        if (!part_storage)
            continue;

        const std::string index_prefix = part_storage->getFullPath() + INDEX_PREFIX;
        ANNSearcherCacheKey cache_key{index_prefix, getBuildParamsHash()};
        std::shared_ptr<DiskANNCppFacade::Searcher> searcher = searcher_cache->getOrSet(
            cache_key,
            [&]() -> std::pair<std::shared_ptr<DiskANNCppFacade::Searcher>, size_t>
            {
                auto opened = DiskANNCppFacade::openSearcher(index_prefix, active_params.metric, search_params);
                std::shared_ptr<DiskANNCppFacade::Searcher> fresh(std::move(opened));
                return {
                    std::move(fresh),
                    0,
                };
            });

        std::vector<UInt64> hits(k, 0);
        std::vector<float> distances(k, 0.0f);
        const UInt32 hit_count = searcher->search(
            desc.query_vector.data(),
            active_params.dim,
            k,
            search_params.search_list_size,
            search_params.beam_width,
            search_params.search_io_limit,
            hits.data(),
            distances.data());

        hits.resize(hit_count);
        distances.resize(hit_count);
        if (active_params.metric == DiskANNCppFacade::Metric::InnerProduct)
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
        result.per_ann_index_part.push_back(std::move(hit_set));
    }

    ProfileEvents::increment(ProfileEvents::ANNIndexDiskANNCppSearchFinished);
    search_finished = true;
    return result;
}

void DiskANNCppAlgorithm::prepareBuild(const AlgorithmBuildContext & ctx, const Block & indexed_columns_batch)
{
    if (!validated_params)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskANN C++ build invoked before parameters were validated");
    params = *validated_params;

    if (!ctx.intermediate_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskANN C++ build requires intermediate_storage in AlgorithmBuildContext");

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
        throw Exception(ErrorCodes::ABORTED, "DiskANN C++ build cancelled during prepareBuild");
    };

    throwIfCancelled();

    if (!fbin_writer)
    {
        ProfileEvents::increment(ProfileEvents::ANNIndexDiskANNCppBuildStarted);
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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskANN C++ indexed column was not Array; got {}", first_col->getName());

    const auto & offsets = arr_col->getOffsets();
    const auto * float_col = typeid_cast<const ColumnVector<Float32> *>(&arr_col->getData());
    const auto * bfloat16_col = typeid_cast<const ColumnVector<BFloat16> *>(&arr_col->getData());
    if (!float_col && !bfloat16_col)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskANN C++ indexed column was Array of {} (expected Array(Float32) or Array(BFloat16))", arr_col->getData().getName());

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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN C++ row {} has dim {} but index expects {}", rows_seen_in_build, row_size, params.dim);

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

void DiskANNCppAlgorithm::buildAlgorithmPrivate(const AlgorithmBuildContext & ctx)
{
    if (!fbin_writer || !fbin_buf)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskANN C++ buildAlgorithmPrivate called before prepareBuild");

    if (ctx.is_cancelled && ctx.is_cancelled->load(std::memory_order_relaxed))
    {
        fbin_writer.reset();
        fbin_buf->cancel();
        fbin_buf.reset();
        throw Exception(ErrorCodes::ABORTED, "DiskANN C++ build cancelled before C++ build invocation");
    }

    fbin_writer->finalize();
    fbin_buf->finalize();

    if (!ctx.intermediate_storage || !ctx.output_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DiskANN C++ build requires both intermediate_storage and output_storage");

    ctx.output_storage->createDirectories();
    DiskANNCppFacade::BuildParams facade_params;
    facade_params.metric = params.metric;
    facade_params.pruned_degree = params.pruned_degree;
    facade_params.max_degree = params.max_degree;
    facade_params.l_build = params.l_build;
    facade_params.alpha = params.alpha;
    facade_params.num_threads = params.num_threads;
    facade_params.pq_chunks = params.pq_chunks;
    facade_params.build_quantization = params.build_quantization;
    facade_params.build_ram_limit_gb = params.build_ram_limit_gb;

    try
    {
        DiskANNCppFacade::build(
            ctx.intermediate_storage->getFullPath() + "vectors.fbin",
            ctx.output_storage->getFullPath() + INDEX_PREFIX,
            facade_params);
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::ANNIndexDiskANNCppBuildFailed);
        throw;
    }

    ProfileEvents::increment(ProfileEvents::ANNIndexDiskANNCppBuildFinished);
}

void DiskANNCppAlgorithm::finishBuild(const AlgorithmBuildContext & ctx)
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
