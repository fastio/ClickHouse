#pragma once
#include "config.h"
#if USE_DISKANN

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace DB
{

/// On-disk header for an ANN index group's `mapping.bin`.
///
/// `mapping.bin` stores the (block_number, block_offset) -> internal_id mapping
/// for rows indexed by the group-based DiskANN manager. The header is
/// 16 bytes, 8-byte aligned, little-endian on disk.
///
/// Versioning policy: when `version` does not match
/// `ANNIndexGroupMappingHeader::CURRENT_VERSION`, or `magic` mismatches,
/// the group is considered incompatible. The caller must retire the
/// group directory (move to `broken/`) and schedule a rebuild, **without**
/// blocking server startup. There is no online migration — the group is
/// rebuilt from scratch on version skew.
struct ANNIndexGroupMappingHeader
{
    /// ASCII "CHDANN\0\0" in little-endian (the "CH" tag identifies ClickHouse data).
    static constexpr uint64_t MAGIC = 0x0000'4E4E'4144'4843ULL;

    /// Current layout version. Bump whenever the on-disk mapping format changes.
    static constexpr uint32_t CURRENT_VERSION = 1;

    /// Fixed on-disk size. Must stay in sync with the binary layout below.
    static constexpr size_t SIZE_ON_DISK = 16;

    uint64_t magic = MAGIC;
    uint32_t version = CURRENT_VERSION;
    uint32_t reserved = 0;  /// For future use, must remain zero until a new version is introduced.
};

static_assert(sizeof(ANNIndexGroupMappingHeader) == ANNIndexGroupMappingHeader::SIZE_ON_DISK,
              "ANNIndexGroupMappingHeader layout must match SIZE_ON_DISK");

/// Serialise the header to the first `SIZE_ON_DISK` bytes of `out`.
/// `out` must point to at least `SIZE_ON_DISK` writable bytes.
void writeANNIndexGroupMappingHeader(const ANNIndexGroupMappingHeader & header, char * out) noexcept;

/// Parse a header from the first `SIZE_ON_DISK` bytes of `in`.
/// Returns true iff both `magic` and `version` match the current layout.
/// On mismatch, `header` holds the parsed (potentially stale) values for diagnostics
/// and the caller should retire the group directory.
bool readAndValidateANNIndexGroupMappingHeader(const char * in, ANNIndexGroupMappingHeader & header) noexcept;

/// Move a group directory into a sibling `broken/` directory for asynchronous cleanup.
/// The caller is expected to log but not throw: a broken group must never block server startup.
///
/// Returns the destination path on success (empty string if the directory does not exist
/// or the move already happened). On any filesystem error, returns an empty string and
/// sets `error_out` (if non-null) to a human-readable message; this function never throws.
std::string retireANNIndexGroupDir(const std::string & group_dir, std::string * error_out = nullptr) noexcept;

enum class DiskANNMetric : uint8_t
{
    L2 = 0,
    Cosine = 1,
};

struct DiskANNBuildOptions
{
    uint32_t pruned_degree = 32;
    uint32_t max_degree = 64;
    uint32_t l_build = 128;
    float alpha = 1.2f;
    uint32_t num_threads = 1;
    uint32_t pq_chunks = 4;
    double build_ram_limit_gb = 0.25;
};

struct DiskANNSearchOptions
{
    uint32_t num_threads = 1;
    uint32_t search_io_limit = 4;
    uint32_t num_nodes_to_cache = 0;
    uint32_t default_search_list_size = 64;
    uint32_t default_beam_width = 4;
};

class DiskANNDiskIndexBuilder
{
public:
    DiskANNDiskIndexBuilder(
        size_t dimensions,
        DiskANNMetric metric,
        DiskANNBuildOptions options = {});

    ~DiskANNDiskIndexBuilder();

    DiskANNDiskIndexBuilder(const DiskANNDiskIndexBuilder &) = delete;
    DiskANNDiskIndexBuilder & operator=(const DiskANNDiskIndexBuilder &) = delete;
    DiskANNDiskIndexBuilder(DiskANNDiskIndexBuilder && other) noexcept;
    DiskANNDiskIndexBuilder & operator=(DiskANNDiskIndexBuilder && other) noexcept;

    void setDataPath(const std::string & path);
    void setIndexPrefix(const std::string & prefix);
    void build() const;

    static bool indexFilesExist(const std::string & index_prefix);

private:
    int64_t handle = -1;
    size_t dim;

    [[noreturn]] static void throwFromFFIError(const std::string & context);
};

class DiskANNDiskIndexSearcher
{
public:
    DiskANNDiskIndexSearcher(
        size_t dimensions,
        DiskANNMetric metric,
        const std::string & index_prefix,
        DiskANNSearchOptions options = {});

    ~DiskANNDiskIndexSearcher();

    DiskANNDiskIndexSearcher(const DiskANNDiskIndexSearcher &) = delete;
    DiskANNDiskIndexSearcher & operator=(const DiskANNDiskIndexSearcher &) = delete;
    DiskANNDiskIndexSearcher(DiskANNDiskIndexSearcher && other) noexcept;
    DiskANNDiskIndexSearcher & operator=(DiskANNDiskIndexSearcher && other) noexcept;

    size_t search(
        const float * query,
        size_t query_dim,
        size_t k,
        uint64_t * ids,
        float * distances,
        size_t search_list_size = 0,
        size_t beam_width = 0) const;

private:
    int64_t handle = -1;
    size_t dim;
    DiskANNSearchOptions options;

    [[noreturn]] static void throwFromFFIError(const std::string & context);
};

using DiskANNDiskIndexBuilderPtr = std::shared_ptr<DiskANNDiskIndexBuilder>;
using DiskANNDiskIndexSearcherPtr = std::shared_ptr<DiskANNDiskIndexSearcher>;

}

#endif
