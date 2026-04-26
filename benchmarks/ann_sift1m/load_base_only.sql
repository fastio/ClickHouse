-- Re-created once per sweep entry: the index hyperparameters change so the
-- table needs a fresh CREATE. Index `dim` is pinned to 128 (the SIFT-1M
-- dimensionality); everything else is templated and supplied by
-- `recall_qps.sh` via clickhouse-client `--param_*`.

DROP TABLE IF EXISTS sift_base;

CREATE TABLE sift_base
(
    id UInt64,
    v  Array(Float32),
    INDEX idx_v v TYPE ann(
        dim                     = 128,
        metric                  = 'L2',
        max_degree              = {max_degree:UInt64},
        build_search_list_size  = {build_search_list_size:UInt64},
        alpha                   = {alpha:Float64},
        search_list_size        = {search_list_size:UInt64},
        beam_width              = {beam_width:UInt64},
        search_io_limit         = {search_io_limit:UInt64}
    ) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    ann_group_min_rows = 1,
    ann_group_max_rows = 2000000,
    ann_group_max_parts = 256;
