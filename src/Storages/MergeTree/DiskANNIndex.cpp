#include <Storages/MergeTree/DiskANNIndex.h>

#if USE_DISKANN

#include <diskann_ffi.h>
#include <Common/Exception.h>

#include <cstring>
#include <filesystem>
#include <system_error>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

void writeANNIndexGroupMappingHeader(const ANNIndexGroupMappingHeader & header, char * out) noexcept
{
    /// Explicit little-endian byte-wise write. We do NOT rely on host endianness:
    /// the on-disk format must be stable across architectures.
    auto write_u64 = [](char * dst, uint64_t v)
    {
        for (size_t i = 0; i < 8; ++i)
            dst[i] = static_cast<char>((v >> (i * 8)) & 0xFF);
    };
    auto write_u32 = [](char * dst, uint32_t v)
    {
        for (size_t i = 0; i < 4; ++i)
            dst[i] = static_cast<char>((v >> (i * 8)) & 0xFF);
    };

    write_u64(out + 0, header.magic);
    write_u32(out + 8, header.version);
    write_u32(out + 12, header.reserved);
}

bool readAndValidateANNIndexGroupMappingHeader(const char * in, ANNIndexGroupMappingHeader & header) noexcept
{
    auto read_u64 = [](const char * src) -> uint64_t
    {
        uint64_t v = 0;
        for (size_t i = 0; i < 8; ++i)
            v |= static_cast<uint64_t>(static_cast<uint8_t>(src[i])) << (i * 8);
        return v;
    };
    auto read_u32 = [](const char * src) -> uint32_t
    {
        uint32_t v = 0;
        for (size_t i = 0; i < 4; ++i)
            v |= static_cast<uint32_t>(static_cast<uint8_t>(src[i])) << (i * 8);
        return v;
    };

    header.magic = read_u64(in + 0);
    header.version = read_u32(in + 8);
    header.reserved = read_u32(in + 12);

    if (header.magic != ANNIndexGroupMappingHeader::MAGIC)
        return false;
    if (header.version != ANNIndexGroupMappingHeader::CURRENT_VERSION)
        return false;

    return true;
}

std::string retireANNIndexGroupDir(const std::string & group_dir, std::string * error_out) noexcept
{
    namespace fs = std::filesystem;

    std::error_code ec;

    /// The helper is best-effort: never throw, report via `error_out` and empty return on failure.
    try
    {
        fs::path src(group_dir);
        if (!fs::exists(src, ec) || ec)
        {
            if (error_out)
                *error_out = ec ? ec.message() : "group directory does not exist";
            return {};
        }

        fs::path parent = src.parent_path();
        fs::path broken_root = parent / "broken";

        fs::create_directories(broken_root, ec);
        if (ec)
        {
            if (error_out)
                *error_out = ec.message();
            return {};
        }

        /// Use <group_name>.<timestamp> inside `broken/` to avoid collisions when
        /// a group is retired multiple times (e.g. repeated restart on persistent corruption).
        auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          std::chrono::steady_clock::now().time_since_epoch()).count();
        std::string stem = src.filename().string();
        if (stem.empty())
            stem = "group";
        fs::path dst = broken_root / (stem + "." + std::to_string(now_ns));

        fs::rename(src, dst, ec);
        if (ec)
        {
            /// `rename` fails across filesystems — fall back to copy+remove.
            std::error_code copy_ec;
            fs::copy(src, dst, fs::copy_options::recursive, copy_ec);
            if (copy_ec)
            {
                if (error_out)
                    *error_out = copy_ec.message();
                return {};
            }
            fs::remove_all(src, copy_ec);
            if (copy_ec)
            {
                /// Copied but failed to remove: caller can still proceed; log but do not abort.
                if (error_out)
                    *error_out = "copied but failed to remove original: " + copy_ec.message();
            }
        }

        return dst.string();
    }
    catch (...)
    {
        if (error_out)
            *error_out = "unexpected filesystem exception";
        return {};
    }
}

namespace
{

::DiskANNMetric toFFIMetric(DiskANNMetric metric)
{
    switch (metric)
    {
        case DiskANNMetric::L2:
            return DISKANN_METRIC_L2;
        case DiskANNMetric::Cosine:
            return DISKANN_METRIC_COSINE;
    }
    UNREACHABLE();
}

std::string getLastFFIError()
{
    char buf[1024];
    int64_t len = diskann_last_error(buf, sizeof(buf));
    if (len <= 0)
        return "unknown error";
    return std::string(buf, static_cast<size_t>(len));
}

}

DiskANNDiskIndexBuilder::DiskANNDiskIndexBuilder(
    size_t dimensions,
    DiskANNMetric metric,
    DiskANNBuildOptions options)
    : dim(dimensions)
{
    handle = diskann_create_disk_builder(
        static_cast<uint32_t>(dim),
        toFFIMetric(metric),
        options.pruned_degree,
        options.max_degree,
        options.l_build,
        options.alpha,
        options.num_threads,
        options.pq_chunks,
        options.build_ram_limit_gb);

    if (handle < 0)
        throwFromFFIError("DiskANN create_disk_builder failed");
}

DiskANNDiskIndexBuilder::~DiskANNDiskIndexBuilder()
{
    if (handle >= 0)
        diskann_drop_builder(handle);
}

DiskANNDiskIndexBuilder::DiskANNDiskIndexBuilder(DiskANNDiskIndexBuilder && other) noexcept
    : handle(other.handle)
    , dim(other.dim)
{
    other.handle = -1;
}

DiskANNDiskIndexBuilder & DiskANNDiskIndexBuilder::operator=(DiskANNDiskIndexBuilder && other) noexcept
{
    if (this != &other)
    {
        if (handle >= 0)
            diskann_drop_builder(handle);

        handle = other.handle;
        dim = other.dim;
        other.handle = -1;
    }
    return *this;
}

void DiskANNDiskIndexBuilder::setDataPath(const std::string & path)
{
    auto rc = diskann_builder_set_data_path(handle, path.c_str());
    if (rc < 0)
        throwFromFFIError("DiskANN builder_set_data_path failed");
}

void DiskANNDiskIndexBuilder::setIndexPrefix(const std::string & prefix)
{
    auto rc = diskann_builder_set_index_prefix(handle, prefix.c_str());
    if (rc < 0)
        throwFromFFIError("DiskANN builder_set_index_prefix failed");
}

void DiskANNDiskIndexBuilder::build() const
{
    auto rc = diskann_builder_build(handle);
    if (rc < 0)
        throwFromFFIError("DiskANN builder_build failed");
}

bool DiskANNDiskIndexBuilder::indexFilesExist(const std::string & index_prefix)
{
    auto rc = diskann_index_file_exists(index_prefix.c_str());
    if (rc < 0)
        throwFromFFIError("DiskANN index_file_exists failed");
    return rc == 1;
}

[[noreturn]] void DiskANNDiskIndexBuilder::throwFromFFIError(const std::string & context)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "{}: {}", context, getLastFFIError());
}

DiskANNDiskIndexSearcher::DiskANNDiskIndexSearcher(
    size_t dimensions,
    DiskANNMetric metric,
    const std::string & index_prefix,
    DiskANNSearchOptions options_)
    : dim(dimensions)
    , options(options_)
{
    handle = diskann_open_searcher(
        index_prefix.c_str(),
        static_cast<uint32_t>(dim),
        toFFIMetric(metric),
        options.num_threads,
        options.search_io_limit,
        options.num_nodes_to_cache);

    if (handle < 0)
        throwFromFFIError("DiskANN open_searcher failed");
}

DiskANNDiskIndexSearcher::~DiskANNDiskIndexSearcher()
{
    if (handle >= 0)
        diskann_close_searcher(handle);
}

DiskANNDiskIndexSearcher::DiskANNDiskIndexSearcher(DiskANNDiskIndexSearcher && other) noexcept
    : handle(other.handle)
    , dim(other.dim)
    , options(other.options)
{
    other.handle = -1;
}

DiskANNDiskIndexSearcher & DiskANNDiskIndexSearcher::operator=(DiskANNDiskIndexSearcher && other) noexcept
{
    if (this != &other)
    {
        if (handle >= 0)
            diskann_close_searcher(handle);

        handle = other.handle;
        dim = other.dim;
        options = other.options;
        other.handle = -1;
    }
    return *this;
}

size_t DiskANNDiskIndexSearcher::search(
    const float * query,
    size_t query_dim,
    size_t k,
    uint64_t * ids,
    float * distances,
    size_t search_list_size,
    size_t beam_width) const
{
    if (query_dim != dim)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "DiskANN search: query dimension {} does not match index dimension {}",
            query_dim,
            dim);

    auto effective_search_list_size = search_list_size > 0
        ? search_list_size
        : static_cast<size_t>(options.default_search_list_size);
    auto effective_beam_width = beam_width > 0
        ? beam_width
        : static_cast<size_t>(options.default_beam_width);

    auto rc = diskann_search_disk_index(
        handle,
        query,
        static_cast<uint32_t>(query_dim),
        static_cast<uint32_t>(k),
        static_cast<uint32_t>(effective_search_list_size),
        static_cast<uint32_t>(effective_beam_width),
        ids,
        distances);

    if (rc < 0)
        throwFromFFIError("DiskANN search_disk_index failed");

    return static_cast<size_t>(rc);
}

[[noreturn]] void DiskANNDiskIndexSearcher::throwFromFFIError(const std::string & context)
{
    throw Exception(ErrorCodes::INCORRECT_DATA, "{}: {}", context, getLastFFIError());
}

}

#endif
