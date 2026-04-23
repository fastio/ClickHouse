#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/ANNIndexBuilder.h>
#include <Storages/MergeTree/ANNIndex/VectorStreamWriter.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromFileBase.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>

#include <base/hex.h>

#include <filesystem>
#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    constexpr size_t FBIN_WRITE_BUFFER_SIZE = 1 << 20;     /// 1 MiB
    constexpr size_t META_WRITE_BUFFER_SIZE = 4096;

    std::string formatHexU64(UInt64 v)
    {
        return "0x" + getHexUIntLowercase(v);
    }

    void writeMetaJson(
        IANNGroupStorage & storage,
        const ANNBuildInput & input,
        UInt64 num_points)
    {
        Poco::JSON::Object root;
        root.set("version", static_cast<UInt32>(1));

        Poco::JSON::Object shape_obj;
        shape_obj.set("dim", input.shape.dim);
        shape_obj.set("metric", static_cast<UInt32>(input.shape.metric));
        shape_obj.set("algorithm", input.shape.algorithm);
        shape_obj.set("params_hash", formatHexU64(input.shape.params_hash));
        root.set("shape", shape_obj);

        root.set("hash_algo", input.hash_algo);
        root.set("hash_seed", formatHexU64(input.hash_seed));
        root.set("num_points", num_points);

        Poco::JSON::Object build_opts;
        build_opts.set("pruned_degree", input.build_options.pruned_degree);
        build_opts.set("max_degree", input.build_options.max_degree);
        build_opts.set("l_build", input.build_options.l_build);
        build_opts.set("alpha", input.build_options.alpha);
        build_opts.set("num_threads", input.build_options.num_threads);
        build_opts.set("pq_chunks", input.build_options.pq_chunks);
        build_opts.set("build_ram_limit_gb", input.build_options.build_ram_limit_gb);
        root.set("build_options", build_opts);

        Poco::JSON::Object search_defaults;
        search_defaults.set("num_threads", input.search_defaults.num_threads);
        search_defaults.set("search_io_limit", input.search_defaults.search_io_limit);
        search_defaults.set("num_nodes_to_cache", input.search_defaults.num_nodes_to_cache);
        search_defaults.set("default_search_list_size", input.search_defaults.default_search_list_size);
        search_defaults.set("default_beam_width", input.search_defaults.default_beam_width);
        root.set("search_defaults", search_defaults);

        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(root, oss, 2, -1,
            Poco::JSON_WRAP_STRINGS | Poco::JSON_ESCAPE_UNICODE);
        const std::string payload = oss.str();

        auto out = storage.writeFile(std::string(ANNIndexGroup::META_FILE_NAME),
            META_WRITE_BUFFER_SIZE, WriteMode::Rewrite, WriteSettings{});
        out->write(payload.data(), payload.size());
        out->finalize();
    }
}

ANNIndexGroupPtr ANNIndexBuilder::build(
    const ANNBuildInput & input,
    ANNGroupStoragePtr tmp_storage,
    LoggerPtr log)
{
    if (!tmp_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndexBuilder::build: tmp_storage must not be null");
    if (!input.storage || !input.storage_snapshot)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndexBuilder::build: input.storage and storage_snapshot must be set");

    LOG_DEBUG(log, "ANN build: scanning {} parts into {}",
        input.selected_parts.size(), tmp_storage->getFullPath());

    /// Phase 1-2: scan parts → fbin + id_map + coverage.
    PartRowIdMapWriter id_map_writer;
    ANNGroupCoverage coverage;
    UInt64 num_points = 0;
    {
        auto fbin_buf = tmp_storage->writeFile(std::string(FBIN_FILE_NAME),
            FBIN_WRITE_BUFFER_SIZE, WriteMode::Rewrite, WriteSettings{});
        DiskANNFbinWriter fbin_writer(*fbin_buf, input.shape.dim);

        VectorStreamWriter::Params vsw_params;
        vsw_params.vector_column_name = input.vector_column_name;
        vsw_params.expected_dim = input.shape.dim;
        vsw_params.hash_seed = input.hash_seed;
        vsw_params.storage = input.storage;
        vsw_params.storage_snapshot = input.storage_snapshot;

        VectorStreamWriter writer(std::move(vsw_params), fbin_writer, log);
        writer.dumpFromParts(input.selected_parts);

        num_points = writer.getWrittenRows();
        id_map_writer = std::move(writer.idMapWriter());
        coverage = std::move(writer.coverage());

        fbin_buf->finalize();
    }

    /// Phase 3: FFI build.
    namespace fs = std::filesystem;
    const auto group_full_path = tmp_storage->getFullPath();
    const auto fbin_path = (fs::path(group_full_path) / FBIN_FILE_NAME).string();
    const auto index_prefix = (fs::path(group_full_path) / DiskANNArtifactNames::INDEX_PREFIX_BASENAME).string();

    DiskANNDiskIndexBuilder ffi_builder(
        input.shape.dim,
        static_cast<DiskANNMetric>(input.shape.metric),
        input.build_options);
    ffi_builder.setDataPath(fbin_path);
    ffi_builder.setIndexPrefix(index_prefix);
    ffi_builder.build();

    /// Phase 4: persist the runtime-visible artefacts.
    id_map_writer.writeTo(*tmp_storage, WriteSettings{});
    coverage.writeTo(*tmp_storage);
    writeMetaJson(*tmp_storage, input, num_points);

    /// Phase 5: remove the transient fbin — not needed by the searcher at query time.
    tmp_storage->removeFileIfExists(std::string(FBIN_FILE_NAME));

    /// Phase 6: reload as a fresh group. This guarantees that the on-disk state (meta.json +
    /// id_map + coverage + idx_*) is self-consistent before the group becomes observable.
    return ANNIndexGroup::load(std::move(tmp_storage), input.search_defaults);
}

}
#endif
