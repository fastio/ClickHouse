#include <Storages/Reflection/ANNIndex/SPANNAlgorithm.h>

#if USE_SPTAG

#include <Storages/Reflection/ANNIndex/ANNIndexContext.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartName.h>

#include <base/scope_guard.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <filesystem>
#include <limits>
#include <sstream>


namespace ProfileEvents
{
    extern const Event ANNIndexSPANNBuildStarted;
    extern const Event ANNIndexSPANNBuildFinished;
    extern const Event ANNIndexSPANNBuildFailed;
    extern const Event ANNIndexSPANNSearchStarted;
    extern const Event ANNIndexSPANNSearchFinished;
    extern const Event ANNIndexSPANNSearchFailed;
}


namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 spann_search_posting_page_limit;
    extern const SettingsUInt64 spann_search_internal_result_num;
    extern const SettingsUInt64 spann_search_max_check;
    extern const SettingsFloat  spann_search_max_dist_ratio;
    extern const SettingsUInt64 spann_search_hash_table_exponent;
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
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace
{

constexpr UInt64 CANCEL_POLL_ROW_GRANULE = 256;
constexpr UInt64 SPTAG_INT32_LIMIT = static_cast<UInt64>(std::numeric_limits<Int32>::max());

/// Pack `(part_id, part_offset)` per the compact codec layout. Layout
/// width is governed by `ANNIndexPayloadCodec::Version` and matches
/// DiskANN's sidecar so the matcher decoder is engine-agnostic.
inline void packLocatorRecord(
    uint8_t * dest,
    ANNIndexPayloadCodec::Version version,
    UInt32 part_id,
    UInt64 part_offset)
{
    ANNIndexPayloadCodec::pack(dest, version, part_id, part_offset);
}

inline void unpackLocatorRecord(
    const uint8_t * src,
    ANNIndexPayloadCodec::Version version,
    UInt32 & part_id,
    UInt64 & part_offset)
{
    ANNIndexPayloadCodec::unpack(src, version, part_id, part_offset);
}

std::map<String, String> makeSPANNBuildSettings(const SPANNFacade::BuildParams & p)
{
    return {
        {"metric", SPANNFacade::metricName(p.metric)},
        {"dimension", toString(p.dim)},
        {"select_type", p.select_type},
        {"head_ratio", toString(p.head_ratio)},
        {"posting_page_limit", toString(p.posting_page_limit)},
        {"search_posting_page_limit", toString(p.search_posting_page_limit)},
        {"internal_result_num", toString(p.internal_result_num)},
        {"replica_count", toString(p.replica_count)},
        {"num_threads", toString(p.num_threads)},
        {"select_head_threads", toString(p.select_head_threads)},
        {"build_head_threads", toString(p.build_head_threads)},
        {"io_threads", toString(p.io_threads)},
        {"posting_vector_limit", toString(p.posting_vector_limit)},
        {"max_check", toString(p.max_check)},
        {"max_dist_ratio", toString(p.max_dist_ratio)},
        {"hash_table_exponent", toString(p.hash_table_exponent)},
        {"io_timeout_us", toString(p.io_timeout_us)},
        {"bkt_number", toString(p.bkt_number)},
        {"bkt_kmeans_k", toString(p.bkt_kmeans_k)},
        {"bkt_leaf_size", toString(p.bkt_leaf_size)},
        {"neighborhood_size", toString(p.neighborhood_size)},
        {"cef", toString(p.cef)},
        {"max_check_for_refine_graph", toString(p.max_check_for_refine_graph)},
        {"refine_iterations", toString(p.refine_iterations)},
        {"tpt_number", toString(p.tpt_number)},
        {"rng_factor", toString(p.rng_factor)},
        {"select_samples_number", toString(p.select_samples_number)},
        {"select_threshold", toString(p.select_threshold)},
        {"split_factor", toString(p.split_factor)},
        {"split_threshold", toString(p.split_threshold)},
        {"enable_data_compression", toString(p.enable_data_compression)},
        {"enable_delta_encoding", toString(p.enable_delta_encoding)},
        {"enable_posting_list_rearrange", toString(p.enable_posting_list_rearrange)},
    };
}

/// Sanity caps for user-tunable SPANN parameters. SPTAG itself accepts up
/// to `INT32_MAX` for most of these, but anything past these bounds is
/// almost certainly a typo or copy-paste accident and blows up build-time
/// memory before reaching a useful index. Keep them in sync with the
/// documented per-field defaults so the cap explains "why" in the error.
/// Caps below have a physical or resource meaning. Pure-algorithm tuning
/// knobs (BKT/SelectHead graph parameters) are not capped here — they fall
/// back to `SPTAG_INT32_LIMIT` (SPTAG's own SizeType=int32 hard limit) via
/// `fieldToSPTAGPositiveInt32`. The few that need a sub-INT32 ceiling for
/// real reasons (memory budget, I/O page count, ratio domain) keep one.
constexpr UInt32 SPANN_MAX_DIM = 65536;                  // Array(Float32) practical ceiling
constexpr UInt32 SPANN_MAX_REPLICA_COUNT = 1024;         // bounds on-disk index inflation
constexpr UInt32 SPANN_MAX_POSTING_PAGE_LIMIT = 1024;    // 1024 pages = 4 MiB per posting
constexpr UInt32 SPANN_MAX_POSTING_VECTOR_LIMIT = 4096;  // ditto, vector-count flavor

/// SPTAG `ParameterDefinitionList.h` ships `PostingVectorLimit=118` (and we
/// match it in `SPANNFacade::BuildParams`). Used as the upper clamp of the
/// dim-derived default so dim ≤ 256 (sift, glove, deep, bigann) reproduces
/// pre-derive behaviour bit-for-bit; high-dim falls back to the `target /
/// entry_bytes` lane and stays well below.
constexpr UInt32 SPTAG_LEGACY_POSTING_VECTOR_LIMIT = 118;
static_assert(SPANNFacade::BuildParams{}.posting_vector_limit == SPTAG_LEGACY_POSTING_VECTOR_LIMIT);
constexpr UInt32 SPANN_MAX_INTERNAL_RESULT_NUM = 4096;   // bounds workspace memory
constexpr UInt32 SPANN_MAX_NUM_THREADS = 1024;
constexpr UInt32 SPANN_MAX_IO_THREADS = 1024;
constexpr UInt32 SPANN_MAX_CHECK = 1u << 24;             // bounds dedup hash size
constexpr UInt32 SPANN_MAX_HASH_TABLE_EXPONENT = 20;     // 1<<20 dedup slots
constexpr UInt32 SPANN_MAX_IO_TIMEOUT_US = 60'000;       // 60 ms — AIO usefulness ceiling
constexpr float  SPANN_MIN_DIST_RATIO = 1.0f;            // ratio < 1 drops top-1 itself
constexpr float  SPANN_MAX_DIST_RATIO = 1.0e6f;          // effectively "off"

std::optional<SPANNFacade::Metric> parseMetric(std::string_view text)
{
    if (text == "L2" || text == "l2")
        return SPANNFacade::Metric::L2;
    if (text == "cosine" || text == "Cosine" || text == "COSINE")
        return SPANNFacade::Metric::Cosine;
    if (text == "InnerProduct" || text == "innerproduct" || text == "inner_product" || text == "INNER_PRODUCT" || text == "dotProduct")
        return SPANNFacade::Metric::InnerProduct;
    return {};
}

bool queryFunctionMatchesMetric(const String & distance_function, SPANNFacade::Metric metric)
{
    switch (metric)
    {
        case SPANNFacade::Metric::L2:
            return distance_function == "L2Distance";
        case SPANNFacade::Metric::Cosine:
            return distance_function == "cosineDistance";
        case SPANNFacade::Metric::InnerProduct:
            return distance_function == "dotProduct";
    }
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
            const Int64 value = field.safeGet<Int64>();
            if (value < 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameter '{}' must be non-negative, got {}", name, value);
            return static_cast<UInt64>(value);
        }
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameter '{}' must be an integer literal", name);
    }
}

UInt32 fieldToUInt32(const Field & field, std::string_view name)
{
    const UInt64 value = fieldToUInt64(field, name);
    if (value > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameter '{}' is out of UInt32 range", name);
    return static_cast<UInt32>(value);
}

UInt32 fieldToSPTAGPositiveInt32(const Field & field, std::string_view name)
{
    const UInt64 value = fieldToUInt64(field, name);
    if (value == 0 || value > SPTAG_INT32_LIMIT)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameter '{}' must be in range [1, {}]", name, SPTAG_INT32_LIMIT);
    return static_cast<UInt32>(value);
}

UInt32 fieldToBoundedPositiveInt32(const Field & field, std::string_view name, UInt32 upper_inclusive)
{
    const UInt32 value = fieldToSPTAGPositiveInt32(field, name);
    if (value > upper_inclusive)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameter '{}' must be in range [1, {}]", name, upper_inclusive);
    return value;
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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameter '{}' must be a numeric literal", name);
    }
}

bool fieldToBool(const Field & field, std::string_view name)
{
    switch (field.getType())
    {
        case Field::Types::Bool:
            return field.safeGet<bool>();
        case Field::Types::UInt64:
        {
            const UInt64 v = field.safeGet<UInt64>();
            if (v > 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameter '{}' must be 0/1 or true/false", name);
            return v != 0;
        }
        case Field::Types::Int64:
        {
            const Int64 v = field.safeGet<Int64>();
            if (v != 0 && v != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameter '{}' must be 0/1 or true/false", name);
            return v != 0;
        }
        case Field::Types::String:
        {
            const String & s = field.safeGet<String>();
            if (s == "true" || s == "TRUE" || s == "True" || s == "1")
                return true;
            if (s == "false" || s == "FALSE" || s == "False" || s == "0")
                return false;
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameter '{}' must be 'true' or 'false', got '{}'", name, s);
        }
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameter '{}' must be a boolean literal", name);
    }
}

float fieldToBoundedFloat(const Field & field, std::string_view name, float lo_inclusive, float hi_inclusive)
{
    const double value = fieldToDouble(field, name);
    if (!std::isfinite(value) || value < static_cast<double>(lo_inclusive) || value > static_cast<double>(hi_inclusive))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "SPANN parameter '{}' must be in range [{}, {}]",
            name, lo_inclusive, hi_inclusive);
    return static_cast<float>(value);
}

bool isKnownParam(std::string_view name)
{
    static constexpr std::array known = {
        std::string_view{"metric"},
        std::string_view{"dim"},
        std::string_view{"head_ratio"},
        std::string_view{"posting_page_limit"},
        std::string_view{"search_posting_page_limit"},
        std::string_view{"internal_result_num"},
        std::string_view{"replica_count"},
        std::string_view{"num_threads"},
        std::string_view{"select_head_threads"},
        std::string_view{"build_head_threads"},
        std::string_view{"max_check"},
        std::string_view{"io_threads"},
        std::string_view{"posting_vector_limit"},
        std::string_view{"max_dist_ratio"},
        std::string_view{"hash_table_exponent"},
        std::string_view{"io_timeout_us"},
        std::string_view{"bkt_number"},
        std::string_view{"bkt_kmeans_k"},
        std::string_view{"bkt_leaf_size"},
        std::string_view{"neighborhood_size"},
        std::string_view{"cef"},
        std::string_view{"max_check_for_refine_graph"},
        std::string_view{"refine_iterations"},
        std::string_view{"tpt_number"},
        std::string_view{"rng_factor"},
        std::string_view{"select_type"},
        std::string_view{"select_samples_number"},
        std::string_view{"select_threshold"},
        std::string_view{"split_factor"},
        std::string_view{"split_threshold"},
        std::string_view{"enable_data_compression"},
        std::string_view{"enable_delta_encoding"},
        std::string_view{"enable_posting_list_rearrange"},
    };
    for (const auto & n : known)
        if (n == name)
            return true;
    return false;
}

UInt64 checkedVectorBytes(UInt64 rows, UInt32 dim)
{
    if (dim != 0 && rows > std::numeric_limits<UInt64>::max() / dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN vector count overflows UInt64");
    const UInt64 elements = rows * dim;
    if (elements > std::numeric_limits<UInt64>::max() / sizeof(Float32))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN vector byte size overflows UInt64");
    return elements * sizeof(Float32);
}

String hashFileSipHash128(const IDataPartStorage & storage, const String & rel_path)
{
    SipHash hasher;
    auto reader = storage.readFile(rel_path, ReadSettings{}, std::nullopt);
    std::array<char, 4096> buf{};
    while (!reader->eof())
    {
        const size_t n = reader->readBig(buf.data(), buf.size());
        if (n == 0)
            break;
        hasher.update(buf.data(), n);
    }
    const UInt128 digest = hasher.get128();
    const UInt64 lo = digest.items[UInt128::_impl::little(0)];
    const UInt64 hi = digest.items[UInt128::_impl::little(1)];
    return fmt::format("{:016x}{:016x}", lo, hi);
}

void checkSearchCancelled(ContextPtr query_context)
{
    if (!query_context)
        return;
    if (auto process_list_element = query_context->getProcessListElementSafe())
        process_list_element->checkTimeLimit();
}

std::vector<String> collectRelativeFilesRecursively(const IDataPartStorage & storage, const String & rel_root)
{
    std::vector<String> files;
    const std::filesystem::path part_root(storage.getFullPath());
    const std::filesystem::path absolute_root = part_root / rel_root;

    if (!std::filesystem::exists(absolute_root))
        return files;

    for (const auto & entry : std::filesystem::recursive_directory_iterator(absolute_root))
    {
        if (!entry.is_regular_file())
            continue;
        files.push_back(std::filesystem::relative(entry.path(), part_root).generic_string());
    }

    std::sort(files.begin(), files.end());
    return files;
}

}


SPANNAlgorithm::SPANNAlgorithm() = default;

SPANNAlgorithm::~SPANNAlgorithm()
{
    unregisterBuildStatusIfAny();
}

void SPANNAlgorithm::unregisterBuildStatusIfAny()
{
    if (!build_status_task_id.empty())
    {
        SPANNFacade::unregisterBuildStatus(build_status_task_id);
        build_status_task_id.clear();
    }
    build_status.reset();
}

String SPANNAlgorithm::getAlgorithmVersion() const
{
    return "spann";
}

String SPANNAlgorithm::getBuildParamsHash() const
{
    if (!validated_params)
        return {};
    return calculateParamsHash(*validated_params);
}

std::map<String, String> SPANNAlgorithm::getAlgorithmObservabilityFields() const
{
    if (!validated_params)
        return {};

    return makeSPANNBuildSettings(*validated_params);
}

std::unique_ptr<IANNIndexAlgorithm> SPANNAlgorithm::cloneForBuild() const
{
    auto fresh = std::make_unique<SPANNAlgorithm>();
    fresh->initialized = initialized;
    fresh->validated_params = validated_params;
    return fresh;
}

SPANNAlgorithm::BuildParams SPANNAlgorithm::parseBuildParameters(const ASTPtr & build_params)
{
    BuildParams out{};

    if (!build_params)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN requires keyword parameters: metric, dim are mandatory");

    const ASTExpressionList * list = nullptr;
    if (const auto * fn = typeid_cast<const ASTFunction *>(build_params.get()))
    {
        if (!fn->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN requires keyword parameters: metric, dim are mandatory");
        list = typeid_cast<const ASTExpressionList *>(fn->arguments.get());
    }
    else
    {
        list = typeid_cast<const ASTExpressionList *>(build_params.get());
    }
    if (!list)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameters must be a keyword argument list");

    bool seen_metric = false;
    bool seen_dim = false;
    bool seen_posting_vector_limit = false;

    for (const auto & child : list->children)
    {
        const auto * eq = typeid_cast<const ASTFunction *>(child.get());
        if (!eq || eq->name != "equals" || !eq->arguments || eq->arguments->children.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameters must be of the form name=value");

        const auto & name_node = eq->arguments->children[0];
        const auto & value_node = eq->arguments->children[1];

        const auto * name_ident = typeid_cast<const ASTIdentifier *>(name_node.get());
        if (!name_ident)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameter name must be a bare identifier");
        const String & name = name_ident->name();

        const auto * lit = typeid_cast<const ASTLiteral *>(value_node.get());
        if (!lit)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameter '{}' must be a literal", name);

        if (!isKnownParam(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN does not recognise parameter '{}'", name);

        if (name == "metric")
        {
            const String text = fieldAsString(lit->value);
            const auto parsed = parseMetric(text);
            if (!parsed)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN: 'metric' must be 'L2', 'cosine', or 'InnerProduct', got '{}'", text);
            out.metric = *parsed;
            seen_metric = true;
        }
        else if (name == "dim")
        {
            out.dim = fieldToBoundedPositiveInt32(lit->value, name, SPANN_MAX_DIM);
            seen_dim = true;
        }
        else if (name == "head_ratio")
        {
            const double value = fieldToDouble(lit->value, name);
            if (value <= 0.0 || value > 1.0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN: 'head_ratio' must be in range (0, 1]");
            out.head_ratio = static_cast<float>(value);
        }
        else if (name == "posting_page_limit")
            out.posting_page_limit = fieldToBoundedPositiveInt32(lit->value, name, SPANN_MAX_POSTING_PAGE_LIMIT);
        else if (name == "search_posting_page_limit")
            out.search_posting_page_limit = fieldToBoundedPositiveInt32(lit->value, name, SPANN_MAX_POSTING_PAGE_LIMIT);
        else if (name == "internal_result_num")
            out.internal_result_num = fieldToBoundedPositiveInt32(lit->value, name, SPANN_MAX_INTERNAL_RESULT_NUM);
        else if (name == "replica_count")
            out.replica_count = fieldToBoundedPositiveInt32(lit->value, name, SPANN_MAX_REPLICA_COUNT);
        else if (name == "num_threads")
            out.num_threads = fieldToBoundedPositiveInt32(lit->value, name, SPANN_MAX_NUM_THREADS);
        else if (name == "select_head_threads")
            out.select_head_threads = fieldToBoundedPositiveInt32(lit->value, name, SPANN_MAX_NUM_THREADS);
        else if (name == "build_head_threads")
            out.build_head_threads = fieldToBoundedPositiveInt32(lit->value, name, SPANN_MAX_NUM_THREADS);
        else if (name == "io_threads")
            out.io_threads = fieldToBoundedPositiveInt32(lit->value, name, SPANN_MAX_IO_THREADS);
        else if (name == "max_check")
        {
            const UInt32 value = fieldToUInt32(lit->value, name);
            if (value == 0 || value > SPANN_MAX_CHECK)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameter 'max_check' must be in range [1, {}]", SPANN_MAX_CHECK);
            out.max_check = value;
        }
        else if (name == "posting_vector_limit")
        {
            out.posting_vector_limit = fieldToBoundedPositiveInt32(lit->value, name, SPANN_MAX_POSTING_VECTOR_LIMIT);
            seen_posting_vector_limit = true;
        }
        else if (name == "max_dist_ratio")
            out.max_dist_ratio = fieldToBoundedFloat(lit->value, name, SPANN_MIN_DIST_RATIO, SPANN_MAX_DIST_RATIO);
        else if (name == "hash_table_exponent")
        {
            /// SPTAG allows 0 here (table sized purely by max_check); accept [0, cap].
            const UInt32 value = fieldToUInt32(lit->value, name);
            if (value > SPANN_MAX_HASH_TABLE_EXPONENT)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "SPANN parameter 'hash_table_exponent' must be in range [0, {}]", SPANN_MAX_HASH_TABLE_EXPONENT);
            out.hash_table_exponent = value;
        }
        else if (name == "io_timeout_us")
            out.io_timeout_us = fieldToBoundedPositiveInt32(lit->value, name, SPANN_MAX_IO_TIMEOUT_US);
        /// Pure SPTAG-algorithm tuning knobs below. SPTAG stores them in
        /// `int` (SizeType=int32) and applies no additional cap of its own;
        /// the only real ceiling is INT32_MAX. Don't pick fake limits here.
        else if (name == "bkt_number")
            out.bkt_number = fieldToSPTAGPositiveInt32(lit->value, name);
        else if (name == "bkt_kmeans_k")
            out.bkt_kmeans_k = fieldToSPTAGPositiveInt32(lit->value, name);
        else if (name == "bkt_leaf_size")
            out.bkt_leaf_size = fieldToSPTAGPositiveInt32(lit->value, name);
        else if (name == "neighborhood_size")
            out.neighborhood_size = fieldToSPTAGPositiveInt32(lit->value, name);
        else if (name == "cef")
            out.cef = fieldToSPTAGPositiveInt32(lit->value, name);
        else if (name == "max_check_for_refine_graph")
            out.max_check_for_refine_graph = fieldToSPTAGPositiveInt32(lit->value, name);
        else if (name == "refine_iterations")
            out.refine_iterations = fieldToSPTAGPositiveInt32(lit->value, name);
        else if (name == "tpt_number")
            out.tpt_number = fieldToSPTAGPositiveInt32(lit->value, name);
        else if (name == "rng_factor")
        {
            /// SPTAG stores RNGFactor as float. Only the domain edges matter:
            /// values ≤ 0 disable RNG pruning entirely (degenerate); the
            /// upper bound just rejects NaN/inf via fieldToDouble's finite check.
            const double value = fieldToDouble(lit->value, name);
            if (!std::isfinite(value) || value <= 0.0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "SPANN parameter '{}' must be a finite positive number", name);
            out.rng_factor = static_cast<float>(value);
        }
        else if (name == "select_type")
        {
            const String text = fieldAsString(lit->value);
            if (text != "BKT" && text != "Random")
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "SPANN: 'select_type' must be 'BKT' or 'Random', got '{}'", text);
            out.select_type = text;
        }
        else if (name == "select_samples_number")
            out.select_samples_number = fieldToSPTAGPositiveInt32(lit->value, name);
        else if (name == "select_threshold")
            out.select_threshold = fieldToSPTAGPositiveInt32(lit->value, name);
        else if (name == "split_factor")
            out.split_factor = fieldToSPTAGPositiveInt32(lit->value, name);
        else if (name == "split_threshold")
            out.split_threshold = fieldToSPTAGPositiveInt32(lit->value, name);
        else if (name == "enable_data_compression")
            out.enable_data_compression = fieldToBool(lit->value, name);
        else if (name == "enable_delta_encoding")
            out.enable_delta_encoding = fieldToBool(lit->value, name);
        else if (name == "enable_posting_list_rearrange")
            out.enable_posting_list_rearrange = fieldToBool(lit->value, name);
    }

    if (!seen_metric)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN: 'metric' is mandatory");
    if (!seen_dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN: 'dim' is mandatory");

    /// Each posting list on SSD is `entry_bytes`-per-vector × N vectors, padded
    /// up to a 4 KB page. SPTAG ships `posting_vector_limit=118` — that ~12 KB
    /// posting is the NVMe read sweet spot at 100–128d but blows the per-query
    /// IO budget at 1536d (~720 KB) or wastes pages with sparse postings. When
    /// the user doesn't override `posting_vector_limit`, derive it from `dim`
    /// to keep one posting close to ~64 KB regardless of dimensionality.
    /// Existing presets at dim ≤ 256 still get clamped to the SPTAG legacy
    /// default — their behaviour is bit-for-bit unchanged.
    constexpr UInt64 page_bytes = 4096;
    constexpr UInt64 target_posting_bytes = 64 * 1024;  // NVMe read sweet spot
    constexpr UInt32 min_vectors_per_posting = 8;       // recall floor
    const UInt64 entry_bytes = static_cast<UInt64>(out.dim) * sizeof(Float32) + sizeof(Int32);
    if (!seen_posting_vector_limit)
    {
        const UInt64 derived = std::clamp<UInt64>(
            target_posting_bytes / entry_bytes,
            min_vectors_per_posting,
            SPTAG_LEGACY_POSTING_VECTOR_LIMIT);
        out.posting_vector_limit = static_cast<UInt32>(derived);
    }

    /// SPTAG `ExtraStaticSearcher::LoadIndex` silently raises SearchPostingPageLimit
    /// to `ceil(posting_vector_limit * (dim*4 + 4) / 4096)` — the number of pages a
    /// fully populated posting list occupies on disk. Filling a smaller value here
    /// is dead config (gets overridden) **or** truncates reads (loses recall). Make
    /// the bound explicit at DDL time so users see the conflict.
    const UInt64 floor_sppl = (static_cast<UInt64>(out.posting_vector_limit) * entry_bytes + page_bytes - 1) / page_bytes;
    if (out.search_posting_page_limit < floor_sppl)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "SPANN: 'search_posting_page_limit' ({}) is below the physical floor {} "
            "(derived from dim={} and posting_vector_limit={}). Raise "
            "'search_posting_page_limit' to at least {}, or lower 'posting_vector_limit'.",
            out.search_posting_page_limit, floor_sppl, out.dim, out.posting_vector_limit, floor_sppl);
    if (out.posting_page_limit < floor_sppl)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "SPANN: 'posting_page_limit' ({}) is below the physical floor {} "
            "(derived from dim={} and posting_vector_limit={}). Raise "
            "'posting_page_limit' to at least {}, or lower 'posting_vector_limit'.",
            out.posting_page_limit, floor_sppl, out.dim, out.posting_vector_limit, floor_sppl);

    return out;
}

String SPANNAlgorithm::calculateParamsHash(const BuildParams & build_params)
{
    SipHash params_hasher;
    /// Mandatory.
    params_hasher.update(SPANNFacade::metricId(build_params.metric));
    params_hasher.update(build_params.dim);
    /// SelectHead.
    params_hasher.update(build_params.select_type);
    params_hasher.update(build_params.head_ratio);
    params_hasher.update(build_params.select_samples_number);
    params_hasher.update(build_params.select_threshold);
    params_hasher.update(build_params.split_factor);
    params_hasher.update(build_params.split_threshold);
    /// BuildHead (BKT).
    params_hasher.update(build_params.bkt_number);
    params_hasher.update(build_params.bkt_kmeans_k);
    params_hasher.update(build_params.bkt_leaf_size);
    params_hasher.update(build_params.neighborhood_size);
    params_hasher.update(build_params.cef);
    params_hasher.update(build_params.max_check_for_refine_graph);
    params_hasher.update(build_params.refine_iterations);
    params_hasher.update(build_params.tpt_number);
    params_hasher.update(build_params.rng_factor);
    /// BuildSSDIndex (structural).
    params_hasher.update(build_params.posting_page_limit);
    params_hasher.update(build_params.posting_vector_limit);
    params_hasher.update(build_params.replica_count);
    params_hasher.update(build_params.num_threads);
    params_hasher.update(build_params.select_head_threads);
    params_hasher.update(build_params.build_head_threads);
    params_hasher.update(build_params.io_threads);
    params_hasher.update(build_params.enable_data_compression);
    params_hasher.update(build_params.enable_delta_encoding);
    params_hasher.update(build_params.enable_posting_list_rearrange);
    /// BuildSSDIndex (search-tunable defaults baked into the index ini).
    params_hasher.update(build_params.search_posting_page_limit);
    params_hasher.update(build_params.internal_result_num);
    params_hasher.update(build_params.max_check);
    params_hasher.update(build_params.max_dist_ratio);
    params_hasher.update(build_params.hash_table_exponent);
    params_hasher.update(build_params.io_timeout_us);
    const UInt128 ph = params_hasher.get128();
    return fmt::format("{:016x}{:016x}", ph.items[UInt128::_impl::little(0)], ph.items[UInt128::_impl::little(1)]);
}

void SPANNAlgorithm::validateBuildParameters(const ASTPtr & build_params, ContextPtr /*context*/)
{
    auto parsed = parseBuildParameters(build_params);
    validated_params = parsed;
}

void SPANNAlgorithm::validateIndexedExpression(const ASTPtr & indexed_expression, const StorageInMemoryMetadata & source_metadata)
{
    if (!indexed_expression)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN requires an indexed expression referring to one Array(Float32) column");

    const IAST * target = indexed_expression.get();
    if (const auto * list = typeid_cast<const ASTExpressionList *>(target))
    {
        if (list->children.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN supports indexing exactly one Array(Float32) column, got {}", list->children.size());
        target = list->children.front().get();
    }

    const auto * ident = typeid_cast<const ASTIdentifier *>(target);
    if (!ident)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN: indexed expression must be a bare column reference");

    const auto & columns = source_metadata.columns;
    if (!columns.has(ident->name()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN: column '{}' does not exist in the source table", ident->name());

    const auto column_type = columns.get(ident->name()).type;
    const auto * array_type = typeid_cast<const DataTypeArray *>(column_type.get());
    if (!array_type || !typeid_cast<const DataTypeFloat32 *>(array_type->getNestedType().get()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN: indexed column '{}' must be Array(Float32), got {}", ident->name(), column_type->getName());
}

namespace
{
    /// Build a default searcher cache when no reflection-level settings are
    /// available (algorithm tests, factory smoke tests). Mirrors the values
    /// set by `MergeTreeSettings::ann_searcher_cache_*` defaults.
    std::unique_ptr<ANNSearcherCache<SPANNFacade::Searcher>> defaultSPANNSearcherCache()
    {
        return std::make_unique<ANNSearcherCache<SPANNFacade::Searcher>>(
            /*cache_policy=*/ "SLRU",
            /*max_size_in_bytes=*/ 16ULL << 30,
            /*max_count=*/ 1024,
            /*size_ratio=*/ 0.5);
    }
}

void SPANNAlgorithm::initialize(const ANNIndexContext & ctx)
{
    initialized = true;

    /// Build the per-instance searcher cache from the reflection's settings.
    /// Tests that drive the algorithm without a Reflection (and thus without
    /// settings) get the same defaults via the lazy path in `search`.
    if (ctx.reflection_settings)
    {
        const auto & settings = *ctx.reflection_settings;
        searcher_cache = std::make_unique<ANNSearcherCache<SPANNFacade::Searcher>>(
            settings[MergeTreeSetting::ann_searcher_cache_policy],
            settings[MergeTreeSetting::ann_searcher_cache_size],
            settings[MergeTreeSetting::ann_searcher_cache_max_entries],
            settings[MergeTreeSetting::ann_searcher_cache_size_ratio]);
    }
    else
    {
        searcher_cache = defaultSPANNSearcherCache();
    }
}

void SPANNAlgorithm::setBuildParameters(const ASTPtr & build_params, ContextPtr /*context*/)
{
    params = parseBuildParameters(build_params);
    validated_params = params;
}

std::optional<MatchDescriptor> SPANNAlgorithm::match(const QueryFeatures & features) const
{
    if (!validated_params || validated_params->dim == 0)
        return std::nullopt;
    if (features.k == 0)
        return std::nullopt;
    if (features.query_vector.size() != validated_params->dim)
        return std::nullopt;
    if (!queryFunctionMatchesMetric(features.distance_function, validated_params->metric))
        return std::nullopt;

    MatchDescriptor desc;
    desc.query_vector = features.query_vector;
    desc.distance.exact_function_name = "__reflectionANNIndexSPANNDistance";
    desc.distance.metric_name = SPANNFacade::metricName(validated_params->metric);
    desc.distance.metric_id = SPANNFacade::metricId(validated_params->metric);
    desc.distance.dim = validated_params->dim;
    desc.distance.smaller_is_better = validated_params->metric != SPANNFacade::Metric::InnerProduct;
    desc.k = features.k;
    return desc;
}

std::vector<AlgorithmPrivatePath> SPANNAlgorithm::getAlgorithmPrivatePaths(const IDataPartStorage &) const
{
    return {
        {.path = "algorithm_private_spann", .recursive = true, .required = true},
        {.path = "algorithm_private_fingerprint.json", .recursive = false, .required = true},
    };
}

AlgorithmCostEstimate SPANNAlgorithm::estimateCost(const MatchDescriptor & desc, const CoverageSnapshot & coverage) const
{
    AlgorithmCostEstimate est;
    const size_t search_rows = coverage.candidate_limit != 0 ? coverage.candidate_limit : desc.k;
    est.estimated_result_rows = search_rows;
    if (validated_params)
        est.algorithm_search_cost = validated_params->replica_count * validated_params->search_posting_page_limit * search_rows;
    else
        est.algorithm_search_cost = 100UL * search_rows;
    return est;
}

InternalSearchResult SPANNAlgorithm::search(
    const MatchDescriptor & desc,
    const ReadyANNIndexPartSnapshot & ready_parts,
    size_t candidate_limit,
    ContextPtr query_context) const
{
    ProfileEvents::increment(ProfileEvents::ANNIndexSPANNSearchStarted);
    bool search_finished = false;
    scope_guard failed_search_guard = [&search_finished]
    {
        if (!search_finished)
            ProfileEvents::increment(ProfileEvents::ANNIndexSPANNSearchFailed);
    };

    if (ready_parts.parts.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SPANN search invoked without any ready parts");
    if (!validated_params || validated_params->dim == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SPANN search invoked before build parameters were validated");
    if (desc.query_vector.size() != validated_params->dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN query vector size {} does not match index dim {}", desc.query_vector.size(), validated_params->dim);
    if (candidate_limit == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN search candidate_limit must be > 0");

    /// Per-query overrides for SPTAG's search-time runtime parameters.
    /// SPTAG re-reads these from `m_options` on every `SearchIndex` call
    /// (see `SPANNFacade.cpp` Searcher ctor, and the SPTAG read sites
    /// referenced there). `0` means "fall back to the value baked into the
    /// index at CREATE time". Bounds mirror the same caps applied to the
    /// build-time fields.
    auto clamp_setting = [](UInt64 value, UInt32 fallback, UInt32 max_value) -> UInt32
    {
        if (value == 0)
            return fallback;
        if (value > max_value)
            return max_value;
        return static_cast<UInt32>(value);
    };
    auto clamp_setting_f = [](Float32 value, float fallback, float min_value, float max_value) -> float
    {
        if (value == 0.0f)
            return fallback;
        if (value < min_value)
            return min_value;
        if (value > max_value)
            return max_value;
        return value;
    };

    SPANNFacade::BuildParams effective_params = *validated_params;
    if (query_context)
    {
        const auto & settings = query_context->getSettingsRef();
        effective_params.search_posting_page_limit = clamp_setting(
            settings[Setting::spann_search_posting_page_limit],
            validated_params->search_posting_page_limit, SPANN_MAX_POSTING_PAGE_LIMIT);
        effective_params.internal_result_num = clamp_setting(
            settings[Setting::spann_search_internal_result_num],
            validated_params->internal_result_num, SPANN_MAX_INTERNAL_RESULT_NUM);
        effective_params.max_check = clamp_setting(
            settings[Setting::spann_search_max_check],
            validated_params->max_check, SPANN_MAX_CHECK);
        effective_params.max_dist_ratio = clamp_setting_f(
            settings[Setting::spann_search_max_dist_ratio],
            validated_params->max_dist_ratio, SPANN_MIN_DIST_RATIO, SPANN_MAX_DIST_RATIO);
        effective_params.hash_table_exponent = clamp_setting(
            settings[Setting::spann_search_hash_table_exponent],
            validated_params->hash_table_exponent, SPANN_MAX_HASH_TABLE_EXPONENT);
        /// io_timeout_us is intentionally NOT a session setting: SPTAG
        /// stores it in the process-global Helper::AIOTimeout during
        /// ExtraStaticSearcher::LoadIndex, so a per-query override would
        /// race other concurrent searches. It stays build-only.
    }

    /// `initialize` is the canonical place where `searcher_cache` is built.
    /// Algorithm tests that bypass `initialize` end up here with a null
    /// cache; lazy-init from defaults so search still works without a
    /// reflection-level settings handle.
    if (!searcher_cache)
        searcher_cache = defaultSPANNSearcherCache();

    InternalSearchResult result;
    result.per_ann_index_part.reserve(ready_parts.parts.size());

    for (const auto & ready_part : ready_parts.parts)
    {
        checkSearchCancelled(query_context);
        const auto & part_storage = ready_part.storage;
        if (!part_storage)
            continue;

        const std::string folder = part_storage->getFullPath() + "algorithm_private_spann";

        /// Cache key is `(part path, build_params_hash)`. Search-time tunables
        /// are intentionally NOT part of the key — same part with same DDL
        /// build params yields one searcher process-wide regardless of
        /// what session settings the current query carries. The values of
        /// `effective_params` that did get derived above are baked into the
        /// searcher at first-open; subsequent queries against the same part
        /// reuse that searcher even if their session settings differ.
        ANNSearcherCacheKey cache_key{folder, getBuildParamsHash()};

        std::shared_ptr<SPANNFacade::Searcher> searcher = searcher_cache->getOrSet(
            cache_key,
            [&]() -> std::pair<std::shared_ptr<SPANNFacade::Searcher>, size_t>
            {
                auto fresh = std::make_shared<SPANNFacade::Searcher>(folder, effective_params);

                /// SPTAG does not expose memory usage on `VectorIndex`. Approximate
                /// via on-disk folder size × 1.5 to account for in-RAM head BKT,
                /// posting metadata, and workspace pool overhead. The estimate is
                /// rough but bounded; the safety margin keeps the cache from
                /// undershooting its budget.
                size_t mem_bytes = 0;
                try
                {
                    for (const auto & rel : collectRelativeFilesRecursively(*part_storage, "algorithm_private_spann"))
                        mem_bytes += part_storage->getFileSize(rel);
                    mem_bytes = mem_bytes + (mem_bytes / 2);
                }
                catch (...)
                {
                    /// Fall back to zero — cache will still admit the entry but
                    /// not charge its weight. Better than failing the search.
                    mem_bytes = 0;
                }
                return {std::move(fresh), mem_bytes};
            });

        auto search_result = searcher->search(desc.query_vector.data(), validated_params->dim, candidate_limit);
        checkSearchCancelled(query_context);
        if (search_result.vids.empty())
            continue;

        InternalHitSet hit_set;
        hit_set.ann_index_part_storage = part_storage;
        const size_t hit_count = search_result.vids.size();
        hit_set.internal_ids = std::move(search_result.vids);
        hit_set.distances = std::move(search_result.distances);

        /// Decode the per-vector payload, when present, into the matcher's
        /// hot-path fields. Record width is governed by this MI-part's
        /// `payload_format_version` (captured from `coverage.json` into
        /// `ready_part`). Layout matches DiskANN: 4 B little-endian
        /// `part_id` + 4/8 B little-endian `part_offset`.
        const auto version = ready_part.payload_format_version;
        const size_t record_size = ANNIndexPayloadCodec::recordSize(version);
        if (search_result.payload_bytes.size() == hit_count * record_size)
        {
            hit_set.hot_part_ids.resize(hit_count);
            hit_set.hot_part_offsets.resize(hit_count);
            for (size_t i = 0; i < hit_count; ++i)
            {
                unpackLocatorRecord(
                    search_result.payload_bytes.data() + i * record_size,
                    version,
                    hit_set.hot_part_ids[i],
                    hit_set.hot_part_offsets[i]);
            }
        }

        result.per_ann_index_part.push_back(std::move(hit_set));
    }

    ProfileEvents::increment(ProfileEvents::ANNIndexSPANNSearchFinished);
    search_finished = true;
    return result;
}

void SPANNAlgorithm::prepareBuild(const AlgorithmBuildContext & ctx, const Block & indexed_columns_batch)
{
    if (!validated_params)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SPANN build invoked before parameters were validated");
    params = *validated_params;

    auto throwIfCancelled = [&]
    {
        if (ctx.is_cancelled && ctx.is_cancelled->load(std::memory_order_relaxed))
            throw Exception(ErrorCodes::ABORTED, "SPANN build cancelled during prepareBuild");
    };

    throwIfCancelled();

    if (!build_started)
    {
        ProfileEvents::increment(ProfileEvents::ANNIndexSPANNBuildStarted);
        build_started = true;
        rows_seen_in_build = 0;
        rows_since_last_cancel_poll = 0;

        if (!ctx.task_id.empty())
        {
            build_status = std::make_shared<SPANNFacade::BuildStatus>();
            build_status_task_id = ctx.task_id;
            build_status->markStarted(ctx.total_rows);
            build_status->setSettings(makeSPANNBuildSettings(params));
            SPANNFacade::registerBuildStatus(build_status_task_id, build_status);
        }

        if (ctx.total_rows != 0)
        {
            const UInt64 bytes = checkedVectorBytes(ctx.total_rows, params.dim);
            if (ctx.memory_budget_bytes != 0 && bytes > ctx.memory_budget_bytes)
                throw Exception(
                    ErrorCodes::MEMORY_LIMIT_EXCEEDED,
                    "SPANN build requires at least {} bytes for vectors, memory budget is {} bytes",
                    bytes,
                    ctx.memory_budget_bytes);
            build_vectors.reserve(ctx.total_rows * params.dim);
        }
    }

    if (indexed_columns_batch.columns() == 0)
        return;

    const UInt64 expected_rows = rows_seen_in_build + indexed_columns_batch.rows();
    const UInt64 expected_bytes = checkedVectorBytes(expected_rows, params.dim);
    if (ctx.memory_budget_bytes != 0 && expected_bytes > ctx.memory_budget_bytes)
        throw Exception(
            ErrorCodes::MEMORY_LIMIT_EXCEEDED,
            "SPANN build requires at least {} bytes for vectors, memory budget is {} bytes",
            expected_bytes,
            ctx.memory_budget_bytes);

    const auto & first_col = indexed_columns_batch.getByPosition(0).column;
    const auto * arr_col = typeid_cast<const ColumnArray *>(first_col.get());
    if (!arr_col)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SPANN: indexed column was not Array; got {}", first_col->getName());

    const auto * float_col = typeid_cast<const ColumnVector<Float32> *>(&arr_col->getData());
    if (!float_col)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SPANN: indexed column was Array of {} (expected Array(Float32))", arr_col->getData().getName());

    const auto & offsets = arr_col->getOffsets();
    const auto & flat = float_col->getData();

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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN: row {} has dim {} but index expects {}", rows_seen_in_build, row_size, params.dim);

        build_vectors.insert(build_vectors.end(), &flat[prev_offset], &flat[cur_offset]);

        prev_offset = cur_offset;
        ++rows_seen_in_build;
        ++rows_since_last_cancel_poll;
    }

    if (build_status)
    {
        build_status->setRows(rows_seen_in_build, ctx.total_rows);
        build_status->setProgress(rows_seen_in_build, ctx.total_rows);
    }
}

void SPANNAlgorithm::buildAlgorithmPrivate(const AlgorithmBuildContext & ctx)
{
    if (!build_started || build_vectors.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SPANN: buildAlgorithmPrivate invoked without prior prepareBuild rows");
    if (!ctx.output_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SPANN build requires output_storage in AlgorithmBuildContext");

    if (ctx.is_cancelled && ctx.is_cancelled->load(std::memory_order_relaxed))
        throw Exception(ErrorCodes::ABORTED, "SPANN build cancelled before SPTAG invocation");

    if (rows_seen_in_build > SPTAG_INT32_LIMIT)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN row count {} exceeds SPTAG SizeType limit {}", rows_seen_in_build, SPTAG_INT32_LIMIT);

    /// If `prepareBuildLocator` was called, payload buffer must be exactly
    /// `rows_seen * record_size`. A mismatch here is a framework bug
    /// (`prepareBuild` and `prepareBuildLocator` got out of sync).
    if (build_status)
        build_status->setStage(SPANNFacade::BuildStage::ValidatePayload, SPANNFacade::BuildStage::BuildSPTAGIndex);
    const bool has_payload = !build_payloads.empty();
    const size_t payload_record_size = ANNIndexPayloadCodec::recordSize(build_payload_version);
    if (has_payload && build_payloads.size() != rows_seen_in_build * payload_record_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "SPANN build payload size {} does not match {} rows * {} bytes",
            build_payloads.size(),
            rows_seen_in_build,
            payload_record_size);

    ctx.output_storage->createDirectories();
    const std::string folder = ctx.output_storage->getFullPath() + "algorithm_private_spann";
    if (build_status)
    {
        build_status->setPayload(static_cast<UInt8>(has_payload), payload_record_size);
        build_status->setVectorBytes(checkedVectorBytes(rows_seen_in_build, params.dim));
    }

    try
    {
        if (has_payload)
        {
            SPANNFacade::buildIndexWithPayload(
                params,
                build_vectors.data(),
                rows_seen_in_build,
                build_payloads.data(),
                payload_record_size,
                folder,
                build_status.get());
        }
        else
        {
            SPANNFacade::buildIndex(params, build_vectors.data(), rows_seen_in_build, folder, build_status.get());
        }
    }
    catch (...)
    {
        if (build_status)
            build_status->markFailed(getCurrentExceptionMessage(/*with_stacktrace=*/false));
        ProfileEvents::increment(ProfileEvents::ANNIndexSPANNBuildFailed);
        throw;
    }

    if (build_status)
        build_status->markFinished();
    ProfileEvents::increment(ProfileEvents::ANNIndexSPANNBuildFinished);
}

void SPANNAlgorithm::prepareBuildLocator(
    const AlgorithmBuildContext & ctx,
    const Block & locator_columns_batch,
    UInt64 internal_id_offset)
{
    if (locator_columns_batch.columns() == 0)
        return;

    if (ctx.is_cancelled && ctx.is_cancelled->load(std::memory_order_relaxed))
        throw Exception(ErrorCodes::ABORTED, "SPANN build cancelled during prepareBuildLocator");

    /// Framework guarantees `prepareBuildLocator` is called immediately
    /// after the matching `prepareBuild`, so the absolute internal_id of
    /// the first row in this batch must equal what the algorithm has
    /// already accumulated. Re-check defensively to catch wiring bugs.
    if (internal_id_offset != rows_seen_in_build - locator_columns_batch.rows())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "SPANN payload alignment drift: expected internal_id_offset {} but got {}",
            rows_seen_in_build - locator_columns_batch.rows(),
            internal_id_offset);

    if (!ctx.uuid_to_payload_part_id)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "SPANN: prepareBuildLocator missing uuid_to_payload_part_id mapping in build context");

    /// Capture the per-build version on the first locator block so the
    /// rest of the build path (`buildAlgorithmPrivate`) and the on-disk
    /// SPTAG metadata layout agree. The framework keeps this constant
    /// across all `prepareBuildLocator` calls of a single build.
    if (build_payloads.empty())
        build_payload_version = ctx.payload_format_version;

    const auto & uuid_col = typeid_cast<const ColumnVector<UUID> &>(*locator_columns_batch.getByName(ANN_INDEX_LOCATOR_SOURCE_UUID_COLUMN).column);
    const auto & offset_col = typeid_cast<const ColumnVector<UInt64> &>(*locator_columns_batch.getByName(ANN_INDEX_LOCATOR_PART_OFFSET_COLUMN).column);
    const auto & uuid_data = uuid_col.getData();
    const auto & offset_data = offset_col.getData();
    const size_t num_rows = locator_columns_batch.rows();

    const size_t record_size = ANNIndexPayloadCodec::recordSize(build_payload_version);
    const size_t prev_size = build_payloads.size();
    build_payloads.resize(prev_size + num_rows * record_size);
    for (size_t i = 0; i < num_rows; ++i)
    {
        auto it = ctx.uuid_to_payload_part_id->find(uuid_data[i]);
        if (it == ctx.uuid_to_payload_part_id->end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "SPANN: source UUID {} missing from payload_part_id map", toString(uuid_data[i]));
        packLocatorRecord(
            build_payloads.data() + prev_size + i * record_size,
            build_payload_version,
            it->second,
            offset_data[i]);
    }
}

/// Write `algorithm_private_fingerprint.json` covering the SPTAG output. The
/// fingerprint records (1) a hash of the build parameters and (2) per-file
/// SipHash128 digests. (1) is deterministic and used by replication peers to
/// reject parts built with mismatched DDL; (2) is a single-build self-integrity
/// record only, because SPTAG's `SaveIndex` output depends on OpenMP-driven
/// internal ordering and is not byte-stable across rebuilds of the same data.
void SPANNAlgorithm::finishBuild(const AlgorithmBuildContext & ctx)
{
    if (!ctx.output_storage)
    {
        unregisterBuildStatusIfAny();
        return;
    }

    const String params_hash = calculateParamsHash(params);

    Poco::JSON::Array files_arr;
    for (const auto & rel : collectRelativeFilesRecursively(*ctx.output_storage, "algorithm_private_spann"))
    {
        Poco::JSON::Object entry;
        entry.set("name", rel);
        entry.set("size", static_cast<Int64>(ctx.output_storage->getFileSize(rel)));
        entry.set("sipHash128", hashFileSipHash128(*ctx.output_storage, rel));
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

    build_vectors.clear();
    build_vectors.shrink_to_fit();
    rows_seen_in_build = 0;
    rows_since_last_cancel_poll = 0;
    build_started = false;
    unregisterBuildStatusIfAny();
}

UInt64 SPANNAlgorithm::estimateBuildBytes(UInt64 input_source_bytes, UInt64 input_source_rows) const
{
    if (!validated_params || validated_params->dim == 0)
        return input_source_bytes;

    const UInt64 vector_bytes = checkedVectorBytes(input_source_rows, validated_params->dim);
    return std::max(input_source_bytes, vector_bytes * 2);
}

size_t SPANNAlgorithm::searcherCacheSizeForTests() const
{
    return searcher_cache ? searcher_cache->count() : 0;
}

}

#endif
