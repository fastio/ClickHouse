#include <Storages/Reflection/ANNIndex/DiskANNAlgorithm.h>

#include <algorithm>

#if USE_DISKANN

#include <Storages/Reflection/ANNIndex/DiskANNFbinWriter.h>
#include <Storages/Reflection/ANNIndex/DiskANNFfi.h>
#include <Storages/Reflection/ANNIndex/ANNIndexContext.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartName.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPayloadCodec.h>

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
#include <IO/ReadSettings.h>
#include <IO/ReadBufferFromFileBase.h>
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

#include <array>
#include <atomic>
#include <filesystem>
#include <limits>
#include <sstream>


namespace ProfileEvents
{
    extern const Event ANNIndexDiskANNBuildStarted;
    extern const Event ANNIndexDiskANNBuildFinished;
    extern const Event ANNIndexDiskANNBuildFailed;
    extern const Event ANNIndexDiskANNSearchStarted;
    extern const Event ANNIndexDiskANNSearchFinished;
    extern const Event ANNIndexDiskANNSearchFailed;
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
    extern const int NOT_IMPLEMENTED;
}


namespace
{
    constexpr UInt64 CANCEL_POLL_ROW_GRANULE = 100;

    std::optional<DiskANNMetric> parseMetric(std::string_view text)
    {
        if (text == "L2" || text == "l2")
            return DISKANN_METRIC_L2;
        if (text == "cosine" || text == "Cosine" || text == "COSINE")
            return DISKANN_METRIC_COSINE;
        if (text == "InnerProduct" || text == "innerproduct" || text == "inner_product" || text == "INNER_PRODUCT" || text == "dotProduct")
            return DISKANN_METRIC_INNER_PRODUCT;
        if (text == "CosineNormalized" || text == "cosinenormalized" || text == "cosine_normalized" || text == "COSINE_NORMALIZED")
            return DISKANN_METRIC_COSINE_NORMALIZED;
        return {};
    }

    String diskANNMetricName(DiskANNMetric metric)
    {
        switch (metric)
        {
            case DISKANN_METRIC_L2:
                return "L2";
            case DISKANN_METRIC_COSINE:
                return "cosine";
            case DISKANN_METRIC_INNER_PRODUCT:
                return "InnerProduct";
            case DISKANN_METRIC_COSINE_NORMALIZED:
                return "CosineNormalized";
        }
    }

    bool queryFunctionMatchesMetric(const String & distance_function, DiskANNMetric metric)
    {
        switch (metric)
        {
            case DISKANN_METRIC_L2:
                return distance_function == "L2Distance";
            case DISKANN_METRIC_COSINE:
            case DISKANN_METRIC_COSINE_NORMALIZED:
                return distance_function == "cosineDistance";
            case DISKANN_METRIC_INNER_PRODUCT:
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

    std::vector<String> collectDiskANNPrivateIndexFiles(const IDataPartStorage & storage)
    {
        std::vector<String> files;
        for (auto it = storage.iterate(); it->isValid(); it->next())
        {
            if (!it->isFile())
                continue;

            const String file_name = it->name();
            if (file_name.starts_with("algorithm_private_diskann"))
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
                Int64 v = field.safeGet<Int64>();
                if (v < 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "DiskANN parameter '{}' must be non-negative, got {}", name, v);
                return static_cast<UInt64>(v);
            }
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "DiskANN parameter '{}' must be an integer literal", name);
        }
    }

    UInt32 fieldToUInt32(const Field & field, std::string_view name)
    {
        const UInt64 value = fieldToUInt64(field, name);
        if (value > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "DiskANN parameter '{}' is out of UInt32 range", name);
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
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "DiskANN parameter '{}' must be a numeric literal", name);
        }
    }

    /// Recognised parameter names. Any other key is rejected.
    bool isKnownParam(std::string_view name)
    {
        return name == "metric" || name == "dim"
            || name == "pruned_degree" || name == "max_degree"
            || name == "l_build" || name == "alpha"
            || name == "num_threads" || name == "pq_chunks"
            || name == "build_ram_limit_gb";
    }

    /// Compute SipHash-128 over the bytes of a single file in `storage` and
    /// return its lower-case hex representation (32 chars).
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

    /// Sidecar that holds the per-vector inline payload. Layout is governed
    /// by `ANNIndexPayloadCodec`: 8 B / record under V1_OFFSET32 or
    /// 12 B / record under V2_OFFSET64. The version is per-MI-part and
    /// persisted in `coverage.json::payload_format_version`; the matcher
    /// reads it from there before decoding hits.
    constexpr const char * kPayloadFileName = "vectors.payload";

    inline void packLocatorRecord(
        uint8_t * dest,
        ANNIndexPayloadCodec::Version version,
        UInt32 part_id,
        UInt64 part_offset)
    {
        ANNIndexPayloadCodec::pack(dest, version, part_id, part_offset);
    }

    /// Read the `vectors.payload` sidecar inside the DiskANN private folder
    /// for the given hits. Missing or short sidecar (legacy index) leaves
    /// the output vectors empty so the matcher uses the cold path uniformly.
    /// `version` selects the record width.
    ///
    /// We currently issue one `readFile` per hit (`internal_id * record_size`
    /// offset). For the typical k = O(100) this is ~100 random small reads
    /// against the page cache; in cold-cache / large-k setups the matcher's
    /// overscan already absorbs the cost. If profiling later shows a hotspot
    /// here, switching to a single batched mmap-style read is a localized
    /// change.
    void readPayloadSidecarForHits(
        const IDataPartStorage & part_storage,
        ANNIndexPayloadCodec::Version version,
        const std::vector<UInt64> & internal_ids,
        std::vector<UInt32> & out_part_ids,
        std::vector<UInt64> & out_offsets)
    {
        out_part_ids.clear();
        out_offsets.clear();
        if (internal_ids.empty())
            return;

        const String rel = String("algorithm_private_diskann/") + kPayloadFileName;
        if (!part_storage.existsFile(rel))
            return;

        const size_t record_size = ANNIndexPayloadCodec::recordSize(version);
        const size_t file_size = part_storage.getFileSize(rel);
        if (file_size % record_size != 0)
            return;
        const UInt64 record_count = file_size / record_size;

        try
        {
            auto reader = part_storage.readFile(rel, ReadSettings{}, std::nullopt);
            std::array<uint8_t, 16> buf{};
            chassert(record_size <= buf.size());
            out_part_ids.resize(internal_ids.size());
            out_offsets.resize(internal_ids.size());
            for (size_t i = 0; i < internal_ids.size(); ++i)
            {
                const UInt64 id = internal_ids[i];
                if (id >= record_count)
                {
                    out_part_ids.clear();
                    out_offsets.clear();
                    return;
                }
                reader->seek(id * record_size, SEEK_SET);
                reader->readStrict(reinterpret_cast<char *>(buf.data()), record_size);
                ANNIndexPayloadCodec::unpack(buf.data(), version, out_part_ids[i], out_offsets[i]);
            }
        }
        catch (...)
        {
            /// Any IO error → treat as legacy index. The matcher cold path
            /// will read the locator columns instead.
            out_part_ids.clear();
            out_offsets.clear();
        }
    }
}


DiskANNAlgorithm::DiskANNAlgorithm() = default;
DiskANNAlgorithm::~DiskANNAlgorithm() = default;

String DiskANNAlgorithm::getAlgorithmVersion() const
{
    return "diskann";
}

String DiskANNAlgorithm::getBuildParamsHash() const
{
    if (!validated_params)
        return {};
    return calculateParamsHash(*validated_params);
}

std::map<String, String> DiskANNAlgorithm::getAlgorithmObservabilityFields() const
{
    if (!validated_params)
        return {};

    const auto & p = *validated_params;
    return {
        {"metric", diskANNMetricName(p.metric)},
        {"dimension", toString(p.dim)},
        {"pruned_degree", toString(p.pruned_degree)},
        {"max_degree", toString(p.max_degree)},
        {"l_build", toString(p.l_build)},
        {"alpha", toString(p.alpha)},
        {"num_threads", toString(p.num_threads)},
        {"pq_chunks", toString(p.pq_chunks)},
        {"build_ram_limit_gb", toString(p.build_ram_limit_gb)},
    };
}

std::unique_ptr<IANNIndexAlgorithm> DiskANNAlgorithm::cloneForBuild() const
{
    /// Carry only storage-scope state. Per-build streaming members
    /// (`fbin_buf`, `fbin_writer`, row counters) and the searcher cache stay
    /// default-initialised — a stale, finalized `fbin_writer` here is the
    /// origin of `appendRow called after finalize` on a reused instance.
    auto fresh = std::make_unique<DiskANNAlgorithm>();
    fresh->initialized = initialized;
    fresh->params = params;
    fresh->validated_params = validated_params;
    return fresh;
}

DiskANNAlgorithm::BuildParams DiskANNAlgorithm::parseBuildParameters(const ASTPtr & build_params)
{
    BuildParams out{};

    if (!build_params)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DiskANN requires keyword parameters: metric, dim are mandatory");

    /// The parser may hand us either an `ASTFunction` (the bare TYPE call,
    /// e.g. `ann('diskann', metric='L2', dim=128)`) or an `ASTExpressionList`
    /// containing the kwargs. Strip the function wrapper if present.
    const ASTExpressionList * list = nullptr;
    if (const auto * fn = typeid_cast<const ASTFunction *>(build_params.get()))
    {
        if (!fn->arguments)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "DiskANN requires keyword parameters: metric, dim are mandatory");
        list = typeid_cast<const ASTExpressionList *>(fn->arguments.get());
    }
    else
    {
        list = typeid_cast<const ASTExpressionList *>(build_params.get());
    }
    if (!list)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DiskANN parameters must be a keyword argument list");

    bool seen_metric = false;
    bool seen_dim = false;

    for (const auto & child : list->children)
    {
        const auto * eq = typeid_cast<const ASTFunction *>(child.get());
        if (!eq || eq->name != "equals" || !eq->arguments || eq->arguments->children.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "DiskANN parameters must be of the form name=value");

        const auto & name_node = eq->arguments->children[0];
        const auto & value_node = eq->arguments->children[1];

        const auto * name_ident = typeid_cast<const ASTIdentifier *>(name_node.get());
        if (!name_ident)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "DiskANN parameter name must be a bare identifier");
        const String & name = name_ident->name();

        const auto * lit = typeid_cast<const ASTLiteral *>(value_node.get());
        if (!lit)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "DiskANN parameter '{}' must be a literal", name);

        if (!isKnownParam(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "DiskANN does not recognise parameter '{}'", name);

        if (name == "metric")
        {
            const String text = fieldAsString(lit->value);
            const auto parsed = parseMetric(text);
            if (!parsed)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "DiskANN: 'metric' must be 'L2', 'cosine', 'InnerProduct', or 'CosineNormalized', got '{}'", text);
            out.metric = *parsed;
            seen_metric = true;
        }
        else if (name == "dim")
        {
            UInt64 v = fieldToUInt64(lit->value, name);
            if (v == 0 || v > std::numeric_limits<UInt32>::max())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "DiskANN: 'dim' out of range");
            out.dim = static_cast<UInt32>(v);
            seen_dim = true;
        }
        else if (name == "pruned_degree")
            out.pruned_degree = fieldToUInt32(lit->value, name);
        else if (name == "max_degree")
            out.max_degree = fieldToUInt32(lit->value, name);
        else if (name == "l_build")
            out.l_build = fieldToUInt32(lit->value, name);
        else if (name == "alpha")
            out.alpha = static_cast<float>(fieldToDouble(lit->value, name));
        else if (name == "num_threads")
            out.num_threads = fieldToUInt32(lit->value, name);
        else if (name == "pq_chunks")
            out.pq_chunks = fieldToUInt32(lit->value, name);
        else if (name == "build_ram_limit_gb")
            out.build_ram_limit_gb = fieldToDouble(lit->value, name);
    }

    if (!seen_metric)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DiskANN: 'metric' is mandatory");
    if (!seen_dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DiskANN: 'dim' is mandatory");

    /// The DiskANN backend requires pq_chunks <= dim. Clamp here so all callers
    /// (validate, setBuildParameters, hash computation) see the same effective
    /// value, and any explicit overshoot is silently bounded to `dim` rather
    /// than failing inside the FFI as `EXTERNAL_LIBRARY_ERROR`. `pq_chunks == 0`
    /// is the documented "auto" sentinel from BuildParams.
    constexpr UInt32 default_pq_chunks_target = 16;
    if (out.pq_chunks == 0 || out.pq_chunks > out.dim)
        out.pq_chunks = std::min(default_pq_chunks_target, out.dim);

    return out;
}

String DiskANNAlgorithm::calculateParamsHash(const BuildParams & build_params)
{
    SipHash params_hasher;
    params_hasher.update(build_params.metric);
    params_hasher.update(build_params.dim);
    params_hasher.update(build_params.pruned_degree);
    params_hasher.update(build_params.max_degree);
    params_hasher.update(build_params.l_build);
    params_hasher.update(build_params.alpha);
    params_hasher.update(build_params.num_threads);
    params_hasher.update(build_params.pq_chunks);
    params_hasher.update(build_params.build_ram_limit_gb);
    const UInt128 ph = params_hasher.get128();
    return fmt::format(
        "{:016x}{:016x}",
        ph.items[UInt128::_impl::little(0)],
        ph.items[UInt128::_impl::little(1)]);
}


void DiskANNAlgorithm::validateBuildParameters(const ASTPtr & build_params, ContextPtr /*context*/)
{
    /// Parse-only: keeps a copy of the validated parameters so that a
    /// subsequent `setBuildParameters` (or future direct build path) can
    /// reuse them without re-parsing. The framework currently calls validate
    /// and build with the same algorithm instance, so caching here avoids
    /// double-parsing and gives the algorithm authoritative copies of the
    /// numeric values used for fingerprint hashing. `pq_chunks` clamping
    /// happens inside `parseBuildParameters` so every caller sees the same
    /// effective value.
    auto parsed = parseBuildParameters(build_params);
    validated_params = parsed;
}

void DiskANNAlgorithm::validateIndexedExpression(const ASTPtr & indexed_expression, const StorageInMemoryMetadata & source_metadata)
{
    if (!indexed_expression)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DiskANN requires an indexed expression referring to one Array(Float32) column");

    /// Resolve the expression to an existing column in the source schema.
    /// We accept only a bare identifier so the column type can be checked
    /// statically — wrapping the column in any function would defeat the
    /// fbin streaming pipeline. The DDL parser hands us an ASTExpressionList
    /// that wraps the single column (`indexed_columns->ptr()` in
    /// validateANNIndexPrerequisites); unwrap it transparently.
    const IAST * target = indexed_expression.get();
    if (const auto * list = typeid_cast<const ASTExpressionList *>(target))
    {
        if (list->children.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "DiskANN supports indexing exactly one Array(Float32) column, got {}",
                list->children.size());
        target = list->children.front().get();
    }

    const auto * ident = typeid_cast<const ASTIdentifier *>(target);
    if (!ident)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DiskANN: indexed expression must be a bare column reference");

    const auto & columns = source_metadata.columns;
    if (!columns.has(ident->name()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DiskANN: column '{}' does not exist in the source table", ident->name());

    const auto column_type = columns.get(ident->name()).type;
    const auto * array_type = typeid_cast<const DataTypeArray *>(column_type.get());
    if (!array_type)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DiskANN: indexed column '{}' must be Array(Float32), got {}",
            ident->name(), column_type->getName());

    if (!typeid_cast<const DataTypeFloat32 *>(array_type->getNestedType().get()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DiskANN: indexed column '{}' must be Array(Float32), got {}",
            ident->name(), column_type->getName());
}

namespace
{
    /// Build a default searcher cache when no reflection-level settings are
    /// available (algorithm tests, factory smoke tests). Mirrors the values
    /// set by `MergeTreeSettings::ann_searcher_cache_*` defaults.
    std::unique_ptr<ANNSearcherCache<DiskANNSearcherHandle>> defaultDiskANNSearcherCache()
    {
        return std::make_unique<ANNSearcherCache<DiskANNSearcherHandle>>(
            /*cache_policy=*/ "SLRU",
            /*max_size_in_bytes=*/ 1ULL << 30,
            /*max_count=*/ 1024,
            /*size_ratio=*/ 0.5);
    }
}

void DiskANNAlgorithm::initialize(const ANNIndexContext & ctx)
{
    initialized = true;

    /// Build the per-instance searcher cache from the reflection's settings.
    /// Tests that drive the algorithm without a Reflection (and thus without
    /// settings) get the same defaults via the lazy path in `search`.
    if (ctx.reflection_settings)
    {
        const auto & settings = *ctx.reflection_settings;
        searcher_cache = std::make_unique<ANNSearcherCache<DiskANNSearcherHandle>>(
            settings[MergeTreeSetting::ann_searcher_cache_policy],
            settings[MergeTreeSetting::ann_searcher_cache_size],
            settings[MergeTreeSetting::ann_searcher_cache_max_entries],
            settings[MergeTreeSetting::ann_searcher_cache_size_ratio]);
    }
    else
    {
        searcher_cache = defaultDiskANNSearcherCache();
    }
}

void DiskANNAlgorithm::setBuildParameters(const ASTPtr & build_params, ContextPtr /*context*/)
{
    params = parseBuildParameters(build_params);
    validated_params = params;
}

std::optional<MatchDescriptor> DiskANNAlgorithm::match(const QueryFeatures & features) const
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
    desc.distance.exact_function_name = "__reflectionANNIndexDiskANNDistance";
    desc.distance.metric_name = diskANNMetricName(validated_params->metric);
    desc.distance.metric_id = static_cast<UInt64>(validated_params->metric);
    desc.distance.dim = validated_params->dim;
    desc.distance.smaller_is_better = validated_params->metric != DISKANN_METRIC_INNER_PRODUCT;
    desc.k = features.k;
    return desc;
}

AlgorithmCostEstimate DiskANNAlgorithm::estimateCost(const MatchDescriptor & desc, const CoverageSnapshot & coverage) const
{
    AlgorithmCostEstimate est;
    const size_t search_rows = coverage.candidate_limit != 0 ? coverage.candidate_limit : desc.k;
    est.estimated_result_rows = search_rows;
    /// Until the graph reports real candidate-visit counts, assume each query
    /// visits ~100 nodes per requested candidate (matches the SEARCH_LIST_SIZE
    /// tunable). Conservative on the high side so DiskANN only wins over
    /// fallback when the overfetched search is still cheap compared to source rows.
    est.algorithm_search_cost = 100UL * search_rows;
    return est;
}

std::vector<AlgorithmPrivatePath> DiskANNAlgorithm::getAlgorithmPrivatePaths(const IDataPartStorage & storage) const
{
    std::vector<AlgorithmPrivatePath> paths;
    for (const auto & rel : collectDiskANNPrivateIndexFiles(storage))
        paths.push_back({.path = rel, .recursive = false, .required = true});

    paths.push_back({.path = "algorithm_private_fingerprint.json", .recursive = false, .required = true});
    return paths;
}

namespace
{
    /// Built-in defaults for searcher-handle open-time tunables. Overridable
    /// per query via `diskann_search_{num_threads,io_limit,nodes_to_cache}`,
    /// but only at first-open of a (part, build_params_hash) cache entry —
    /// see `DiskANNAlgorithm::search` for the rationale.
    constexpr UInt32 SEARCHER_NUM_THREADS_DEFAULT = 8;
    constexpr UInt32 SEARCHER_IO_LIMIT_DEFAULT = 256;
    constexpr UInt32 SEARCHER_NODES_TO_CACHE_DEFAULT = 1024;
    constexpr UInt32 SEARCHER_NUM_THREADS_MAX = 64;
    constexpr UInt32 SEARCHER_IO_LIMIT_MAX = 4096;
    constexpr UInt32 SEARCHER_NODES_TO_CACHE_MAX = 65536;

    UInt32 settingOrDefault(UInt64 value, UInt32 fallback, UInt32 upper_inclusive, std::string_view name)
    {
        if (value == 0)
            return fallback;
        if (value > upper_inclusive)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "DiskANN search setting '{}' must be in range [0, {}], got {}",
                name, upper_inclusive, value);
        return static_cast<UInt32>(value);
    }
}

InternalSearchResult DiskANNAlgorithm::search(
    const MatchDescriptor & desc,
    const ReadyANNIndexPartSnapshot & ready_parts,
    size_t candidate_limit,
    ContextPtr query_context) const
{
    ProfileEvents::increment(ProfileEvents::ANNIndexDiskANNSearchStarted);
    bool search_finished = false;
    scope_guard failed_search_guard = [&search_finished]
    {
        if (!search_finished)
            ProfileEvents::increment(ProfileEvents::ANNIndexDiskANNSearchFailed);
    };

    if (ready_parts.parts.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN search invoked without any ready parts");

    if (!validated_params || validated_params->dim == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN search invoked before build parameters were validated");

    const auto & active_params = *validated_params;

    if (desc.query_vector.size() != active_params.dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DiskANN search query vector size {} does not match index dim {}",
            desc.query_vector.size(), active_params.dim);
    if (candidate_limit == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN search candidate_limit must be > 0");
    if (candidate_limit > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN search candidate_limit is out of UInt32 range");

    const UInt32 k = static_cast<UInt32>(candidate_limit);

    /// Per-query overrides. `search_list_size` and `search_beam_width` are
    /// runtime parameters of `DiskIndexSearcher::search` itself and apply
    /// per call without re-opening anything. `searcher_num_threads`,
    /// `searcher_io_limit` and `searcher_nodes_to_cache` are open-time
    /// tunables: they shape resources owned by the searcher handle (worker
    /// pool, IO ring, hot-node cache). They take effect only at first-open
    /// of a (part, build_params_hash) entry and stick for that handle's
    /// lifetime — see `searcher_cache` doc for the rationale.
    UInt32 search_list_size = 10;
    UInt32 search_beam_width = 4;
    UInt32 open_num_threads = SEARCHER_NUM_THREADS_DEFAULT;
    UInt32 open_io_limit = SEARCHER_IO_LIMIT_DEFAULT;
    UInt32 open_nodes_to_cache = SEARCHER_NODES_TO_CACHE_DEFAULT;
    if (query_context)
    {
        const auto & settings = query_context->getSettingsRef();
        search_list_size = settingOrDefault(
            settings[Setting::diskann_search_list_size],
            search_list_size,
            std::numeric_limits<UInt32>::max(),
            "diskann_search_list_size");
        search_beam_width = settingOrDefault(
            settings[Setting::diskann_search_beam_width],
            search_beam_width,
            std::numeric_limits<UInt32>::max(),
            "diskann_search_beam_width");
        open_num_threads = settingOrDefault(
            settings[Setting::diskann_search_num_threads],
            open_num_threads,
            SEARCHER_NUM_THREADS_MAX,
            "diskann_search_num_threads");
        open_io_limit = settingOrDefault(
            settings[Setting::diskann_search_io_limit],
            open_io_limit,
            SEARCHER_IO_LIMIT_MAX,
            "diskann_search_io_limit");
        open_nodes_to_cache = settingOrDefault(
            settings[Setting::diskann_search_nodes_to_cache],
            open_nodes_to_cache,
            SEARCHER_NODES_TO_CACHE_MAX,
            "diskann_search_nodes_to_cache");
    }

    /// `initialize` is the canonical place where `searcher_cache` is built.
    /// Algorithm tests that bypass `initialize` end up here with a null
    /// cache; lazy-init from defaults so search still works without a
    /// reflection-level settings handle.
    if (!searcher_cache)
        searcher_cache = defaultDiskANNSearcherCache();

    InternalSearchResult result;
    result.per_ann_index_part.reserve(ready_parts.parts.size());

    for (const auto & ready_part : ready_parts.parts)
    {
        checkSearchCancelled(query_context);
        const auto & part_storage = ready_part.storage;
        if (!part_storage)
            continue;

        const std::string index_prefix = part_storage->getFullPath() + "algorithm_private_diskann";

        /// Cache key is `(part path, build_params_hash)`. Open-time tunables
        /// are intentionally NOT part of the key — same part with same DDL
        /// build params yields one searcher process-wide, regardless of
        /// what session settings the current query carries.
        ANNSearcherCacheKey cache_key{index_prefix, getBuildParamsHash()};

        std::shared_ptr<DiskANNSearcherHandle> searcher = searcher_cache->getOrSet(
            cache_key,
            [&]() -> std::pair<std::shared_ptr<DiskANNSearcherHandle>, size_t>
            {
                auto fresh = std::make_shared<DiskANNSearcherHandle>(
                    index_prefix,
                    active_params.dim,
                    active_params.metric,
                    open_num_threads,
                    open_io_limit,
                    open_nodes_to_cache);
                const int64_t mem = fresh->memoryUsage();
                const size_t mem_bytes = mem > 0 ? static_cast<size_t>(mem) : 0;
                return {std::move(fresh), mem_bytes};
            });

        std::vector<UInt64> hits(k, 0);
        std::vector<float> distances(k, 0.0f);

        const uint32_t hit_count = searcher->search(
            desc.query_vector.data(),
            active_params.dim,
            k,
            search_list_size,
            search_beam_width,
            hits.data(),
            distances.data());
        checkSearchCancelled(query_context);

        if (hit_count == 0)
            continue;

        hits.resize(hit_count);
        distances.resize(hit_count);
        if (active_params.metric == DISKANN_METRIC_INNER_PRODUCT)
        {
            for (auto & distance : distances)
                distance = -distance;
        }

        InternalHitSet hit_set;
        hit_set.ann_index_part_storage = part_storage;
        hit_set.internal_ids = std::move(hits);
        hit_set.distances = std::move(distances);

        /// Try to enrich with inline payload from the `vectors.payload`
        /// sidecar. Record width comes from this MI-part's
        /// `coverage.json::payload_format_version`, captured into
        /// `ready_part.payload_format_version` by the matcher snapshot.
        /// Missing or short sidecar (legacy index) leaves the payload
        /// vectors empty so the matcher transparently uses the cold path.
        readPayloadSidecarForHits(
            *part_storage,
            ready_part.payload_format_version,
            hit_set.internal_ids,
            hit_set.hot_part_ids,
            hit_set.hot_part_offsets);

        result.per_ann_index_part.push_back(std::move(hit_set));
    }

    ProfileEvents::increment(ProfileEvents::ANNIndexDiskANNSearchFinished);
    search_finished = true;
    return result;
}

void DiskANNAlgorithm::prepareBuild(const AlgorithmBuildContext & ctx, const Block & indexed_columns_batch)
{
    if (!validated_params)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN build invoked before parameters were validated");
    params = *validated_params;

    if (!ctx.intermediate_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN build requires intermediate_storage in AlgorithmBuildContext");

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
        throw Exception(ErrorCodes::ABORTED, "DiskANN build cancelled during prepareBuild");
    };

    throwIfCancelled();

    if (!fbin_writer)
    {
        ProfileEvents::increment(ProfileEvents::ANNIndexDiskANNBuildStarted);
        ctx.intermediate_storage->createDirectories();
        fbin_buf = ctx.intermediate_storage->writeFile("vectors.fbin", 64 * 1024, WriteSettings{});
        fbin_writer = std::make_unique<DiskANNFbinWriter>(*fbin_buf, params.dim);
        /// Streaming sidecar for inline payload (24 B / vector). Stays open
        /// in lockstep with `fbin_buf` and is finalized in
        /// `buildAlgorithmPrivate`. If `prepareBuildLocator` is never called
        /// (legacy path / tests that skip the locator block), this file
        /// remains zero-length and the search side falls back to cold path.
        payload_buf = ctx.intermediate_storage->writeFile(kPayloadFileName, 64 * 1024, WriteSettings{});
        rows_seen_in_build = 0;
        payload_rows_seen = 0;
        rows_since_last_cancel_poll = 0;
    }

    if (indexed_columns_batch.columns() == 0)
        return;

    const auto & first_col = indexed_columns_batch.getByPosition(0).column;
    const auto * arr_col = typeid_cast<const ColumnArray *>(first_col.get());
    if (!arr_col)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN: indexed column was not Array; got {}", first_col->getName());

    const auto * float_col = typeid_cast<const ColumnVector<Float32> *>(&arr_col->getData());
    if (!float_col)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN: indexed column was Array of {} (expected Array(Float32))",
            arr_col->getData().getName());

    const auto & offsets = arr_col->getOffsets();
    const auto & flat = float_col->getData();

    /// Poll cancellation between rows on a bounded granule. The DiskANN FFI
    /// build itself is not cancellable, so this loop is the last point at
    /// which we honour cooperative cancel.
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
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "DiskANN: row {} has dim {} but index expects {}",
                rows_seen_in_build, row_size, params.dim);

        fbin_writer->appendRow(&flat[prev_offset], static_cast<size_t>(row_size));

        prev_offset = cur_offset;
        ++rows_seen_in_build;
        ++rows_since_last_cancel_poll;
    }
}

void DiskANNAlgorithm::prepareBuildLocator(
    const AlgorithmBuildContext & ctx,
    const Block & locator_columns_batch,
    UInt64 internal_id_offset)
{
    if (locator_columns_batch.columns() == 0)
        return;
    if (!payload_buf)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN: prepareBuildLocator invoked before prepareBuild opened the payload sidecar");
    if (ctx.is_cancelled && ctx.is_cancelled->load(std::memory_order_relaxed))
        throw Exception(ErrorCodes::ABORTED,
            "DiskANN build cancelled during prepareBuildLocator");

    const size_t num_rows = locator_columns_batch.rows();
    if (internal_id_offset != payload_rows_seen)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN payload alignment drift: expected internal_id_offset {} but got {}",
            payload_rows_seen,
            internal_id_offset);

    if (!ctx.uuid_to_payload_part_id)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN: prepareBuildLocator missing uuid_to_payload_part_id mapping in build context");

    const auto & uuid_col = typeid_cast<const ColumnVector<UUID> &>(*locator_columns_batch.getByName(ANN_INDEX_LOCATOR_SOURCE_UUID_COLUMN).column);
    const auto & offset_col = typeid_cast<const ColumnVector<UInt64> &>(*locator_columns_batch.getByName(ANN_INDEX_LOCATOR_PART_OFFSET_COLUMN).column);
    const auto & uuid_data = uuid_col.getData();
    const auto & offset_data = offset_col.getData();

    const auto version = ctx.payload_format_version;
    const size_t record_size = ANNIndexPayloadCodec::recordSize(version);
    std::array<uint8_t, 16> rec{};
    chassert(record_size <= rec.size());
    for (size_t i = 0; i < num_rows; ++i)
    {
        auto it = ctx.uuid_to_payload_part_id->find(uuid_data[i]);
        if (it == ctx.uuid_to_payload_part_id->end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "DiskANN: source UUID {} missing from payload_part_id map", toString(uuid_data[i]));
        packLocatorRecord(rec.data(), version, it->second, offset_data[i]);
        payload_buf->write(reinterpret_cast<const char *>(rec.data()), record_size);
    }
    payload_rows_seen += num_rows;
}

void DiskANNAlgorithm::buildAlgorithmPrivate(const AlgorithmBuildContext & ctx)
{
    if (!fbin_writer)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN: buildAlgorithmPrivate invoked without prior prepareBuild rows");

    /// Final pre-FFI cancellation poll. Past this point the FFI build runs
    /// to completion regardless of cancellation: the upstream DiskANN
    /// routine is a single synchronous call with no cancel callback.
    if (ctx.is_cancelled && ctx.is_cancelled->load(std::memory_order_relaxed))
    {
        fbin_writer.reset();
        if (fbin_buf)
        {
            fbin_buf->cancel();
            fbin_buf.reset();
        }
        if (payload_buf)
        {
            payload_buf->cancel();
            payload_buf.reset();
        }
        throw Exception(ErrorCodes::ABORTED, "DiskANN build cancelled before FFI invocation");
    }

    fbin_writer->finalize();
    fbin_buf->finalize();

    /// Finalize the payload sidecar. If `prepareBuildLocator` was called we
    /// have one record per row; otherwise the file is empty (legacy path)
    /// and we discard it so the search side stays on cold path.
    bool have_payload_sidecar = false;
    if (payload_buf)
    {
        if (payload_rows_seen > 0 && payload_rows_seen == rows_seen_in_build)
        {
            payload_buf->finalize();
            have_payload_sidecar = true;
        }
        else
        {
            payload_buf->cancel();
        }
        payload_buf.reset();
    }

    if (!ctx.intermediate_storage || !ctx.output_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN build: storage handles missing");

    const std::string fbin_path = ctx.intermediate_storage->getFullPath() + "vectors.fbin";

    /// All DiskANN files share the `algorithm_private_` filename prefix so
    /// they stay outside the framework's checksum file (which only covers the
    /// mid-layer's own files). The part directory is flat — no subdirectory
    /// is created.
    ctx.output_storage->createDirectories();
    const std::string index_prefix = ctx.output_storage->getFullPath() + "algorithm_private_diskann";

    try
    {
        DiskANNBuilderHandle builder(
            params.dim,
            params.metric,
            params.pruned_degree,
            params.max_degree,
            params.l_build,
            params.alpha,
            params.num_threads,
            params.pq_chunks,
            params.build_ram_limit_gb);
        builder.setDataPath(fbin_path);
        builder.setIndexPrefix(index_prefix);
        builder.build();
    }
    catch (...)
    {
        ProfileEvents::increment(ProfileEvents::ANNIndexDiskANNBuildFailed);
        throw;
    }

    /// Copy the streaming payload sidecar into the algorithm's private
    /// output folder so search opens it from `output_storage`. We use the
    /// generic `readFile`/`writeFile` path rather than `std::filesystem`
    /// rename so it works across IDataPartStorage backends (S3, ClickHouse
    /// own disk, etc.).
    if (have_payload_sidecar)
    {
        try
        {
            auto reader = ctx.intermediate_storage->readFile(kPayloadFileName, ReadSettings{}, std::nullopt);
            const String dst_rel = String("algorithm_private_diskann/") + kPayloadFileName;
            auto writer = ctx.output_storage->writeFile(dst_rel, 64 * 1024, WriteSettings{});
            std::vector<char> buf(64 * 1024);
            while (!reader->eof())
            {
                const size_t n = reader->readBig(buf.data(), buf.size());
                if (n == 0)
                    break;
                writer->write(buf.data(), n);
            }
            writer->finalize();
        }
        catch (...)
        {
            /// Failure here is non-fatal for query correctness: the matcher
            /// transparently falls back to the locator-column read if the
            /// sidecar is missing or unreadable.
            ProfileEvents::increment(ProfileEvents::ANNIndexDiskANNBuildFailed);
            throw;
        }
    }

    ProfileEvents::increment(ProfileEvents::ANNIndexDiskANNBuildFinished);
}

void DiskANNAlgorithm::finishBuild(const AlgorithmBuildContext & ctx)
{
    /// Even on the success path the buffers must be released before
    /// `intermediate_storage` is reclaimed by the framework.
    fbin_writer.reset();
    fbin_buf.reset();
    payload_buf.reset();

    if (!ctx.output_storage)
        return;

    const String params_hash = calculateParamsHash(params);

    /// Enumerate every file with the `algorithm_private_diskann` prefix and
    /// record name + size + SipHash-128. This intentionally follows the same
    /// discovery path as remap private-file copying so the fingerprint cannot
    /// drift from the actual files needed by the searcher.
    Poco::JSON::Array files_arr;
    for (const auto & rel : collectDiskANNPrivateIndexFiles(*ctx.output_storage))
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

    rows_seen_in_build = 0;
}

size_t DiskANNAlgorithm::searcherCacheSizeForTests() const
{
    return searcher_cache ? searcher_cache->count() : 0;
}

}

#endif
