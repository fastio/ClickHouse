#include <Storages/MaterializedIndex/DiskANNAlgorithm.h>

#if USE_DISKANN

#include <Storages/MaterializedIndex/DiskANNFbinWriter.h>
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
#include <IO/ReadSettings.h>
#include <IO/ReadBufferFromFileBase.h>
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

#include <array>
#include <atomic>
#include <filesystem>
#include <sstream>


namespace ProfileEvents
{
    extern const Event MaterializedIndexDiskANNBuildStarted;
    extern const Event MaterializedIndexDiskANNBuildFinished;
    extern const Event MaterializedIndexDiskANNBuildFailed;
    extern const Event MaterializedIndexDiskANNSearchStarted;
    extern const Event MaterializedIndexDiskANNSearchFinished;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


namespace
{
    constexpr UInt64 CANCEL_POLL_ROW_GRANULE = 1000;

    std::optional<DiskANNMetric> parseMetric(std::string_view text)
    {
        if (text == "L2" || text == "l2")
            return DISKANN_METRIC_L2;
        if (text == "cosine" || text == "Cosine" || text == "COSINE")
            return DISKANN_METRIC_COSINE;
        return {};
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
}


DiskANNAlgorithm::DiskANNAlgorithm() = default;
DiskANNAlgorithm::~DiskANNAlgorithm() = default;

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
                    "DiskANN: 'metric' must be 'L2' or 'cosine', got '{}'", text);
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
            out.pruned_degree = static_cast<UInt32>(fieldToUInt64(lit->value, name));
        else if (name == "max_degree")
            out.max_degree = static_cast<UInt32>(fieldToUInt64(lit->value, name));
        else if (name == "l_build")
            out.l_build = static_cast<UInt32>(fieldToUInt64(lit->value, name));
        else if (name == "alpha")
            out.alpha = static_cast<float>(fieldToDouble(lit->value, name));
        else if (name == "num_threads")
            out.num_threads = static_cast<UInt32>(fieldToUInt64(lit->value, name));
        else if (name == "pq_chunks")
            out.pq_chunks = static_cast<UInt32>(fieldToUInt64(lit->value, name));
        else if (name == "build_ram_limit_gb")
            out.build_ram_limit_gb = fieldToDouble(lit->value, name);
    }

    if (!seen_metric)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DiskANN: 'metric' is mandatory");
    if (!seen_dim)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "DiskANN: 'dim' is mandatory");

    return out;
}


void DiskANNAlgorithm::validateBuildParameters(const ASTPtr & build_params, ContextPtr /*context*/)
{
    /// Parse-only: keeps a copy of the validated parameters so that a
    /// subsequent `setBuildParameters` (or future direct build path) can
    /// reuse them without re-parsing. The framework currently calls validate
    /// and build with the same algorithm instance, so caching here avoids
    /// double-parsing and gives the algorithm authoritative copies of the
    /// numeric values used for fingerprint hashing.
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
    /// fbin streaming pipeline.
    const auto * ident = typeid_cast<const ASTIdentifier *>(indexed_expression.get());
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

void DiskANNAlgorithm::initialize(const MaterializedIndexContext & /*ctx*/)
{
    initialized = true;
}

void DiskANNAlgorithm::setBuildParameters(const ASTPtr & build_params, ContextPtr /*context*/)
{
    params = parseBuildParameters(build_params);
    validated_params = params;
}

std::optional<MatchDescriptor> DiskANNAlgorithm::match(const QueryFeatures & /*features*/) const
{
    /// Query-side planning is wired up later; expose the placeholder
    /// behaviour ("no match available") to the planner.
    return std::nullopt;
}

AlgorithmCostEstimate DiskANNAlgorithm::estimateCost(const MatchDescriptor & /*desc*/, const CoverageSnapshot & /*coverage*/) const
{
    return AlgorithmCostEstimate{};
}

SearchResult DiskANNAlgorithm::search(
    const MatchDescriptor & /*desc*/,
    const ReadyMaterializedIndexPartSnapshot & ready_parts,
    size_t candidate_limit,
    ContextPtr /*query_context*/) const
{
    ProfileEvents::increment(ProfileEvents::MaterializedIndexDiskANNSearchStarted);

    if (ready_parts.parts.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN search invoked without any ready parts");

    if (!validated_params || validated_params->dim == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN search invoked before build parameters were validated");

    const auto & active_params = *validated_params;

    /// We do not have a real query vector in this stage — the planner is
    /// not wired to feed `QueryFeatures` to the search method yet. Surface
    /// that explicitly so callers know they hit a placeholder.
    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
        "DiskANN search requires a query vector; received candidate_limit={} for {}-dim index",
        candidate_limit, active_params.dim);
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

    if (!fbin_writer)
    {
        ProfileEvents::increment(ProfileEvents::MaterializedIndexDiskANNBuildStarted);
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
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN: indexed column was not Array; got {}", first_col->getName());

    const auto * float_col = typeid_cast<const ColumnVector<Float32> *>(&arr_col->getData());
    if (!float_col)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN: indexed column was Array of {} (expected Array(Float32))",
            arr_col->getData().getName());

    const auto & offsets = arr_col->getOffsets();
    const auto & flat = float_col->getData();

    /// Poll cancellation between rows on a coarse granule. The DiskANN FFI
    /// build itself is not cancellable, so this loop is the last point at
    /// which we honour cooperative cancel.
    UInt64 prev_offset = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        if (rows_since_last_cancel_poll >= CANCEL_POLL_ROW_GRANULE)
        {
            if (ctx.is_cancelled && ctx.is_cancelled->load(std::memory_order_relaxed))
            {
                fbin_writer.reset();
                if (fbin_buf)
                {
                    fbin_buf->cancel();
                    fbin_buf.reset();
                }
                throw Exception(ErrorCodes::ABORTED, "DiskANN build cancelled during prepareBuild");
            }
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
        throw Exception(ErrorCodes::ABORTED, "DiskANN build cancelled before FFI invocation");
    }

    fbin_writer->finalize();
    fbin_buf->finalize();

    if (!ctx.intermediate_storage || !ctx.output_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "DiskANN build: storage handles missing");

    const std::string fbin_path = ctx.intermediate_storage->getFullPath() + "vectors.fbin";

    /// All on-disk DiskANN files live under `algorithm_private/` so they
    /// stay outside of the framework's checksum file (which only covers the
    /// mid-layer's own files). `IDataPartStorage::createDirectories` only
    /// creates the part directory itself, so we materialise the
    /// `algorithm_private/` subdirectory directly via the filesystem before
    /// handing the path to the FFI build.
    ctx.output_storage->createDirectories();
    const std::string algorithm_private_path = ctx.output_storage->getFullPath() + "algorithm_private";
    std::filesystem::create_directories(algorithm_private_path);

    const std::string index_prefix = algorithm_private_path + "/diskann";

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
        ProfileEvents::increment(ProfileEvents::MaterializedIndexDiskANNBuildFailed);
        throw;
    }

    ProfileEvents::increment(ProfileEvents::MaterializedIndexDiskANNBuildFinished);
}

void DiskANNAlgorithm::finishBuild(const AlgorithmBuildContext & ctx)
{
    /// Even on the success path the buffers must be released before
    /// `intermediate_storage` is reclaimed by the framework.
    fbin_writer.reset();
    fbin_buf.reset();

    if (!ctx.output_storage)
        return;

    /// Compute a parameter hash so a future load can detect "the binary
    /// index was built with different DDL parameters than the metadata
    /// claims".
    SipHash params_hasher;
    params_hasher.update(params.metric);
    params_hasher.update(params.dim);
    params_hasher.update(params.pruned_degree);
    params_hasher.update(params.max_degree);
    params_hasher.update(params.l_build);
    params_hasher.update(params.alpha);
    params_hasher.update(params.num_threads);
    params_hasher.update(params.pq_chunks);
    params_hasher.update(params.build_ram_limit_gb);
    const UInt128 ph = params_hasher.get128();
    const String params_hash = fmt::format(
        "{:016x}{:016x}",
        ph.items[UInt128::_impl::little(0)],
        ph.items[UInt128::_impl::little(1)]);

    /// Enumerate every file under `algorithm_private/` and record name +
    /// size + SipHash-128. `IDataPartStorage` does not expose a recursive
    /// iterator, but DiskANN's artefact filenames are fixed (a small known
    /// set of `diskann*` files), so we enumerate by presence test rather
    /// than directory listing.
    Poco::JSON::Array files_arr;
    static constexpr std::string_view candidate_suffixes[] = {
        "_disk.index",
        "_disk.index_pq_compressed.bin",
        "_disk.index_pq_pivots.bin",
        "_disk.index_centroids.bin",
        "_disk.index_max_base_norm.bin",
        "_disk.index_medoids.bin",
        "_disk.index_sample_data.bin",
        "_disk.index_sample_ids.bin",
        ".index",
    };
    for (auto suffix : candidate_suffixes)
    {
        const String rel = "algorithm_private/diskann" + std::string{suffix};
        if (!ctx.output_storage->existsFile(rel))
            continue;
        Poco::JSON::Object entry;
        entry.set("name", rel);
        entry.set("size", static_cast<Int64>(ctx.output_storage->getFileSize(rel)));
        entry.set("sipHash128", hashFileSipHash128(*ctx.output_storage, rel));
        files_arr.add(entry);
    }

    Poco::JSON::Object fingerprint;
    fingerprint.set("algorithm_version", String{"diskann/0.50"});
    fingerprint.set("params_hash", params_hash);
    fingerprint.set("num_points", static_cast<Int64>(rows_seen_in_build));
    fingerprint.set("files", files_arr);

    auto writer = ctx.output_storage->writeFile("algorithm_private/fingerprint.json", 4096, WriteSettings{});
    std::ostringstream oss;
    Poco::JSON::Stringifier::stringify(fingerprint, oss);
    const std::string body = oss.str();
    writer->write(body.data(), body.size());
    writer->finalize();

    rows_seen_in_build = 0;
}

}

#endif
