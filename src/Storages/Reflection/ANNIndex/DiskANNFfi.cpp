#include <Storages/Reflection/ANNIndex/DiskANNFfi.h>

#if USE_DISKANN

#include <Common/Exception.h>
#include <fmt/format.h>

#include <array>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int EXTERNAL_LIBRARY_ERROR;
}

void throwFromDiskANNFFIError(int64_t code, std::string_view context)
{
    /// Read the per-thread error message; the buffer is sized to fit any reasonable
    /// DiskANN error string (path, parameter values, downstream error). On overflow
    /// the C side guarantees null termination.
    std::array<char, 4096> buf{};
    diskann_last_error(buf.data(), buf.size());

    throw Exception(
        ErrorCodes::EXTERNAL_LIBRARY_ERROR,
        "DiskANN {} failed (code={}): {}",
        context,
        code,
        std::string_view(buf.data()));
}


DiskANNBuilderHandle::DiskANNBuilderHandle(
    uint32_t dim,
    DiskANNMetric metric,
    uint32_t pruned_degree,
    uint32_t max_degree,
    uint32_t l_build,
    float alpha,
    uint32_t num_threads,
    uint32_t pq_chunks,
    double build_ram_limit_gb)
{
    const int64_t result = diskann_create_disk_builder(
        dim,
        metric,
        pruned_degree,
        max_degree,
        l_build,
        alpha,
        num_threads,
        pq_chunks,
        build_ram_limit_gb);
    if (result < 0)
        throwFromDiskANNFFIError(result, "create_disk_builder");
    raw = result;
}

DiskANNBuilderHandle::~DiskANNBuilderHandle()
{
    if (raw >= 0)
        diskann_drop_builder(raw);
}

DiskANNBuilderHandle::DiskANNBuilderHandle(DiskANNBuilderHandle && other) noexcept
    : raw(std::exchange(other.raw, -1))
{
}

DiskANNBuilderHandle & DiskANNBuilderHandle::operator=(DiskANNBuilderHandle && other) noexcept
{
    if (this != &other)
    {
        if (raw >= 0)
            diskann_drop_builder(raw);
        raw = std::exchange(other.raw, -1);
    }
    return *this;
}

void DiskANNBuilderHandle::setDataPath(const std::string & data_path)
{
    checkDiskANNFFIResult(diskann_builder_set_data_path(raw, data_path.c_str()), "builder_set_data_path");
}

void DiskANNBuilderHandle::setIndexPrefix(const std::string & index_prefix)
{
    checkDiskANNFFIResult(diskann_builder_set_index_prefix(raw, index_prefix.c_str()), "builder_set_index_prefix");
}

void DiskANNBuilderHandle::setAssociatedDataPath(const std::string & associated_data_path, uint32_t record_size)
{
    checkDiskANNFFIResult(
        diskann_builder_set_associated_data_path(raw, associated_data_path.c_str(), record_size),
        "builder_set_associated_data_path");
}

void DiskANNBuilderHandle::build()
{
    checkDiskANNFFIResult(diskann_builder_build(raw), "builder_build");
}


DiskANNSearcherHandle::DiskANNSearcherHandle(
    const std::string & index_prefix,
    uint32_t dim,
    DiskANNMetric metric,
    uint32_t num_threads,
    uint32_t search_io_limit,
    uint32_t num_nodes_to_cache)
{
    const int64_t result = diskann_open_searcher(
        index_prefix.c_str(),
        dim,
        metric,
        num_threads,
        search_io_limit,
        num_nodes_to_cache);
    if (result < 0)
        throwFromDiskANNFFIError(result, "open_searcher");
    raw = result;
}

DiskANNSearcherHandle::~DiskANNSearcherHandle()
{
    if (raw >= 0)
        diskann_close_searcher(raw);
}

DiskANNSearcherHandle::DiskANNSearcherHandle(DiskANNSearcherHandle && other) noexcept
    : raw(std::exchange(other.raw, -1))
{
}

DiskANNSearcherHandle & DiskANNSearcherHandle::operator=(DiskANNSearcherHandle && other) noexcept
{
    if (this != &other)
    {
        if (raw >= 0)
            diskann_close_searcher(raw);
        raw = std::exchange(other.raw, -1);
    }
    return *this;
}

int64_t DiskANNSearcherHandle::numPoints() const
{
    const int64_t result = diskann_searcher_num_points(raw);
    if (result < 0)
        throwFromDiskANNFFIError(result, "searcher_num_points");
    return result;
}

int64_t DiskANNSearcherHandle::dimensions() const
{
    const int64_t result = diskann_searcher_dimensions(raw);
    if (result < 0)
        throwFromDiskANNFFIError(result, "searcher_dimensions");
    return result;
}

int64_t DiskANNSearcherHandle::memoryUsage() const
{
    const int64_t result = diskann_searcher_memory_usage(raw);
    if (result < 0)
        throwFromDiskANNFFIError(result, "searcher_memory_usage");
    return result;
}

int64_t DiskANNSearcherHandle::payloadRecordSize() const
{
    const int64_t result = diskann_searcher_payload_record_size(raw);
    if (result < 0)
        throwFromDiskANNFFIError(result, "searcher_payload_record_size");
    return result;
}

uint32_t DiskANNSearcherHandle::search(
    const float * query,
    uint32_t dim,
    uint32_t k,
    uint32_t search_list_size,
    uint32_t beam_width,
    uint64_t * results,
    float * distances) const
{
    const int64_t hits = diskann_search_disk_index(
        raw, query, dim, k, search_list_size, beam_width, results, distances);
    if (hits < 0)
        throwFromDiskANNFFIError(hits, "search_disk_index");
    return static_cast<uint32_t>(hits);
}

uint32_t DiskANNSearcherHandle::searchWithPayload(
    const float * query,
    uint32_t dim,
    uint32_t k,
    uint32_t search_list_size,
    uint32_t beam_width,
    uint64_t * results,
    float * distances,
    uint8_t * payload,
    uint32_t payload_record_size) const
{
    const int64_t hits = diskann_search_disk_index_with_payload(
        raw, query, dim, k, search_list_size, beam_width, results, distances, payload, payload_record_size);
    if (hits < 0)
        throwFromDiskANNFFIError(hits, "search_disk_index_with_payload");
    return static_cast<uint32_t>(hits);
}

void computeDiskANNDistances(
    DiskANNMetric metric,
    uint32_t dim,
    const float * query,
    const float * candidates,
    uint64_t n,
    float * out)
{
    checkDiskANNFFIResult(
        diskann_compute_distances(metric, dim, query, candidates, n, out),
        "compute_distances");
}

}

#endif
