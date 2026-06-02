#pragma once

#include "config.h"

#if USE_DISKANN

#include <string>
#include <string_view>
#include <cstdint>

#include <diskann_ffi.h>

namespace DB
{

/// Translate a DiskANN FFI return code into a ClickHouse exception when it is negative.
/// Reads the per-thread error message via `diskann_last_error` and embeds it into the
/// thrown exception message.
///
/// `code >= 0` returns silently. The function never returns when `code < 0`.
[[noreturn]] void throwFromDiskANNFFIError(int64_t code, std::string_view context);

inline void checkDiskANNFFIResult(int64_t code, std::string_view context)
{
    if (code < 0)
        throwFromDiskANNFFIError(code, context);
}

/// Move-only RAII wrapper around a DiskANN disk-builder handle.
/// Owns the FFI handle for its lifetime; destructor releases it.
class DiskANNBuilderHandle
{
public:
    DiskANNBuilderHandle(
        uint32_t dim,
        DiskANNMetric metric,
        uint32_t pruned_degree,
        uint32_t max_degree,
        uint32_t l_build,
        float alpha,
        uint32_t num_threads,
        uint32_t pq_chunks,
        double build_ram_limit_gb);

    ~DiskANNBuilderHandle();

    DiskANNBuilderHandle(DiskANNBuilderHandle && other) noexcept;
    DiskANNBuilderHandle & operator=(DiskANNBuilderHandle && other) noexcept;

    DiskANNBuilderHandle(const DiskANNBuilderHandle &) = delete;
    DiskANNBuilderHandle & operator=(const DiskANNBuilderHandle &) = delete;

    void setDataPath(const std::string & data_path);
    void setIndexPrefix(const std::string & index_prefix);
    void setAssociatedDataPath(const std::string & associated_data_path, uint32_t record_size);

    /// Synchronous build. Throws ErrorCodes::EXTERNAL_LIBRARY_ERROR on failure.
    /// Not cancellable once invoked: the underlying DiskANN routine has no
    /// cancel callback, so callers must check cancellation strictly before
    /// calling `build`.
    void build();

    int64_t handle() const { return raw; }

private:
    int64_t raw = -1;
};


/// Move-only RAII wrapper around a DiskANN searcher handle (an open index).
class DiskANNSearcherHandle
{
public:
    DiskANNSearcherHandle(
        const std::string & index_prefix,
        uint32_t dim,
        DiskANNMetric metric,
        uint32_t num_threads,
        uint32_t search_io_limit,
        uint32_t num_nodes_to_cache);

    ~DiskANNSearcherHandle();

    DiskANNSearcherHandle(DiskANNSearcherHandle && other) noexcept;
    DiskANNSearcherHandle & operator=(DiskANNSearcherHandle && other) noexcept;

    DiskANNSearcherHandle(const DiskANNSearcherHandle &) = delete;
    DiskANNSearcherHandle & operator=(const DiskANNSearcherHandle &) = delete;

    int64_t numPoints() const;
    int64_t dimensions() const;
    int64_t memoryUsage() const;
    int64_t payloadRecordSize() const;

    /// Returns the number of valid hits filled into `results` / `distances`.
    /// Throws on FFI error.
    uint32_t search(
        const float * query,
        uint32_t dim,
        uint32_t k,
        uint32_t search_list_size,
        uint32_t beam_width,
        uint64_t * results,
        float * distances) const;

    uint32_t searchWithPayload(
        const float * query,
        uint32_t dim,
        uint32_t k,
        uint32_t search_list_size,
        uint32_t beam_width,
        uint64_t * results,
        float * distances,
        uint8_t * payload,
        uint32_t payload_record_size) const;

    int64_t handle() const { return raw; }

private:
    int64_t raw = -1;
};

/// Stateless exact distance kernel with the same metric semantics DiskANN uses
/// internally. `candidates` is a flat row-major array of `n * dim` floats.
void computeDiskANNDistances(
    DiskANNMetric metric,
    uint32_t dim,
    const float * query,
    const float * candidates,
    uint64_t n,
    float * out);

}

#endif
