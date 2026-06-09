#pragma once
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    DISKANN_METRIC_L2 = 0,
    DISKANN_METRIC_COSINE = 1,
    DISKANN_METRIC_INNER_PRODUCT = 2,
    DISKANN_METRIC_COSINE_NORMALIZED = 3,
} DiskANNMetric;

typedef enum {
    DISKANN_BUILD_STAGE_NOT_STARTED = 0,
    DISKANN_BUILD_STAGE_START = 1,
    DISKANN_BUILD_STAGE_TRAIN_BUILD_QUANTIZER = 2,
    DISKANN_BUILD_STAGE_QUANTIZE_FPV = 3,
    DISKANN_BUILD_STAGE_IN_MEM_INDEX_BUILD = 4,
    DISKANN_BUILD_STAGE_PARTITION_DATA = 5,
    DISKANN_BUILD_STAGE_BUILD_INDICES_ON_SHARDS = 6,
    DISKANN_BUILD_STAGE_MERGE_INDICES = 7,
    DISKANN_BUILD_STAGE_WRITE_DISK_LAYOUT = 8,
    DISKANN_BUILD_STAGE_END = 9,
    DISKANN_BUILD_STAGE_UNKNOWN = 255,
} DiskANNBuildStage;

typedef struct {
    DiskANNBuildStage stage;
    DiskANNBuildStage next_stage;
    uint64_t progress;
    uint64_t current_shard;
    uint64_t num_shards;
    uint8_t started;
    uint8_t finished;
    uint8_t failed;
    uint8_t checkpoint_invalid;
    char error_message[256];
} DiskANNBuildStatus;

int64_t diskann_create_disk_builder(
    uint32_t dim,
    DiskANNMetric metric,
    uint32_t pruned_degree,
    uint32_t max_degree,
    uint32_t l_build,
    float alpha,
    uint32_t num_threads,
    uint32_t pq_chunks,
    const char * build_quantization,
    double build_ram_limit_gb);

void diskann_drop_builder(int64_t handle);

int64_t diskann_builder_set_data_path(
    int64_t handle,
    const char * data_path);

int64_t diskann_builder_set_index_prefix(
    int64_t handle,
    const char * index_prefix);

int64_t diskann_builder_set_associated_data_path(
    int64_t handle,
    const char * associated_data_path,
    uint32_t record_size);

int64_t diskann_builder_build(int64_t handle);

int64_t diskann_builder_get_status(
    int64_t handle,
    DiskANNBuildStatus * status);

int64_t diskann_open_searcher(
    const char * index_prefix,
    uint32_t dim,
    DiskANNMetric metric,
    uint32_t num_threads,
    uint32_t search_io_limit,
    uint32_t num_nodes_to_cache);

void diskann_close_searcher(int64_t handle);

int64_t diskann_searcher_num_points(int64_t handle);

int64_t diskann_searcher_dimensions(int64_t handle);

int64_t diskann_searcher_memory_usage(int64_t handle);

int64_t diskann_searcher_payload_record_size(int64_t handle);

int64_t diskann_search_disk_index(
    int64_t handle,
    const float * query_ptr,
    uint32_t dim,
    uint32_t k,
    uint32_t search_list_size,
    uint32_t beam_width,
    uint64_t * results_ptr,
    float * distances_ptr);

int64_t diskann_search_disk_index_with_payload(
    int64_t handle,
    const float * query_ptr,
    uint32_t dim,
    uint32_t k,
    uint32_t search_list_size,
    uint32_t beam_width,
    uint64_t * results_ptr,
    float * distances_ptr,
    uint8_t * payload_ptr,
    uint32_t payload_record_size);

int64_t diskann_compute_distances(
    DiskANNMetric metric,
    uint32_t dim,
    const float * query_ptr,
    const float * candidates_ptr,
    uint64_t n,
    float * out_ptr);

int64_t diskann_index_file_exists(const char * index_prefix);

int64_t diskann_last_error(char * buf, uint64_t buf_size);

#ifdef __cplusplus
}
#endif
