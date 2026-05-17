#include <Storages/MaterializedIndex/SPANNAlgorithm.h>

#if USE_SPTAG

#include <Storages/MaterializedIndex/MaterializedIndexContext.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteSettings.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <filesystem>
#include <limits>
#include <sstream>


namespace ProfileEvents
{
    extern const Event MaterializedIndexSPANNBuildStarted;
    extern const Event MaterializedIndexSPANNBuildFinished;
    extern const Event MaterializedIndexSPANNBuildFailed;
    extern const Event MaterializedIndexSPANNSearchStarted;
    extern const Event MaterializedIndexSPANNSearchFinished;
}


namespace DB
{

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

/// Sanity caps for user-tunable SPANN parameters. SPTAG itself accepts up
/// to `INT32_MAX` for most of these, but anything past these bounds is
/// almost certainly a typo or copy-paste accident and blows up build-time
/// memory before reaching a useful index. Keep them in sync with the
/// documented per-field defaults so the cap explains "why" in the error.
constexpr UInt32 SPANN_MAX_DIM = 65536;
constexpr UInt32 SPANN_MAX_REPLICA_COUNT = 1024;
constexpr UInt32 SPANN_MAX_POSTING_PAGE_LIMIT = 1024;
constexpr UInt32 SPANN_MAX_INTERNAL_RESULT_NUM = 4096;
constexpr UInt32 SPANN_MAX_NUM_THREADS = 1024;
constexpr UInt32 SPANN_MAX_IO_THREADS = 1024;
constexpr UInt32 SPANN_MAX_CHECK = 1u << 24;

std::optional<SPANNFacade::Metric> parseMetric(std::string_view text)
{
    if (text == "L2" || text == "l2")
        return SPANNFacade::Metric::L2;
    if (text == "cosine" || text == "Cosine" || text == "COSINE")
        return SPANNFacade::Metric::Cosine;
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

bool isKnownParam(std::string_view name)
{
    return name == "metric" || name == "dim"
        || name == "head_ratio" || name == "posting_page_limit"
        || name == "search_posting_page_limit" || name == "internal_result_num"
        || name == "replica_count" || name == "num_threads"
        || name == "max_check" || name == "io_threads";
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
SPANNAlgorithm::~SPANNAlgorithm() = default;

std::unique_ptr<IMaterializedIndexAlgorithm> SPANNAlgorithm::cloneForBuild() const
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
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN: 'metric' must be 'L2' or 'cosine', got '{}'", text);
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
        else if (name == "io_threads")
            out.io_threads = fieldToBoundedPositiveInt32(lit->value, name, SPANN_MAX_IO_THREADS);
        else if (name == "max_check")
        {
            const UInt32 value = fieldToUInt32(lit->value, name);
            if (value == 0 || value > SPANN_MAX_CHECK)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN parameter 'max_check' must be in range [1, {}]", SPANN_MAX_CHECK);
            out.max_check = value;
        }
    }

    if (!seen_metric)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN: 'metric' is mandatory");
    if (!seen_dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN: 'dim' is mandatory");

    return out;
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

void SPANNAlgorithm::initialize(const MaterializedIndexContext & /*ctx*/)
{
    initialized = true;
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
    desc.distance.exact_function_name = "__materializedIndexSPANNDistance";
    desc.distance.metric_name = SPANNFacade::metricName(validated_params->metric);
    desc.distance.metric_id = SPANNFacade::metricId(validated_params->metric);
    desc.distance.dim = validated_params->dim;
    desc.distance.smaller_is_better = true;
    desc.k = features.k;
    return desc;
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
    const ReadyMaterializedIndexPartSnapshot & ready_parts,
    size_t candidate_limit,
    ContextPtr /*query_context*/) const
{
    ProfileEvents::increment(ProfileEvents::MaterializedIndexSPANNSearchStarted);

    if (ready_parts.parts.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SPANN search invoked without any ready parts");
    if (!validated_params || validated_params->dim == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SPANN search invoked before build parameters were validated");
    if (desc.query_vector.size() != validated_params->dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN query vector size {} does not match index dim {}", desc.query_vector.size(), validated_params->dim);
    if (candidate_limit == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "SPANN search candidate_limit must be > 0");

    InternalSearchResult result;
    result.per_materialized_index_part.reserve(ready_parts.parts.size());

    for (const auto & ready_part : ready_parts.parts)
    {
        const auto & part_storage = ready_part.storage;
        if (!part_storage)
            continue;

        const std::string folder = part_storage->getFullPath() + "algorithm_private_spann";
        std::shared_ptr<SPANNFacade::Searcher> searcher;
        {
            std::lock_guard<std::mutex> guard(searcher_cache_mutex);
            auto it = searcher_cache.find(folder);
            if (it == searcher_cache.end())
            {
                auto fresh = std::make_shared<SPANNFacade::Searcher>(folder, *validated_params);
                std::tie(it, std::ignore) = searcher_cache.emplace(folder, std::move(fresh));
            }
            searcher = it->second;
        }

        auto search_result = searcher->search(desc.query_vector.data(), validated_params->dim, candidate_limit);
        if (search_result.vids.empty())
            continue;

        InternalHitSet hit_set;
        hit_set.materialized_index_part_storage = part_storage;
        hit_set.internal_ids = std::move(search_result.vids);
        hit_set.distances = std::move(search_result.distances);
        result.per_materialized_index_part.push_back(std::move(hit_set));
    }

    ProfileEvents::increment(ProfileEvents::MaterializedIndexSPANNSearchFinished);
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
        ProfileEvents::increment(ProfileEvents::MaterializedIndexSPANNBuildStarted);
        build_started = true;
        rows_seen_in_build = 0;
        rows_since_last_cancel_poll = 0;

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

    ctx.output_storage->createDirectories();
    const std::string folder = ctx.output_storage->getFullPath() + "algorithm_private_spann";

    try
    {
        SPANNFacade::buildIndex(params, build_vectors.data(), rows_seen_in_build, folder);
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::MaterializedIndexSPANNBuildFailed);
        throw;
    }

    ProfileEvents::increment(ProfileEvents::MaterializedIndexSPANNBuildFinished);
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
        return;

    SipHash params_hasher;
    params_hasher.update(SPANNFacade::metricId(params.metric));
    params_hasher.update(params.dim);
    params_hasher.update(params.head_ratio);
    params_hasher.update(params.posting_page_limit);
    params_hasher.update(params.search_posting_page_limit);
    params_hasher.update(params.internal_result_num);
    params_hasher.update(params.replica_count);
    params_hasher.update(params.num_threads);
    params_hasher.update(params.max_check);
    params_hasher.update(params.io_threads);
    const UInt128 ph = params_hasher.get128();
    const String params_hash = fmt::format("{:016x}{:016x}", ph.items[UInt128::_impl::little(0)], ph.items[UInt128::_impl::little(1)]);

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
    fingerprint.set("algorithm_version", String{"spann/sptag"});
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
    std::lock_guard<std::mutex> guard(searcher_cache_mutex);
    return searcher_cache.size();
}

}

#endif
