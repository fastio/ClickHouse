-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_per_key_parallel;
DROP TABLE IF EXISTS t_per_key_parallel_off;

SELECT 'feature off stays serial';
CREATE TABLE t_per_key_parallel_off
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    enable_map_key_columns_parallel_merge = 0;

SYSTEM STOP MERGES t_per_key_parallel_off;
INSERT INTO t_per_key_parallel_off VALUES (1, {'a': 1, 'b': 2, 'c': 3});
INSERT INTO t_per_key_parallel_off VALUES (2, {'b': 4, 'd': 5});
SYSTEM START MERGES t_per_key_parallel_off;
OPTIMIZE TABLE t_per_key_parallel_off FINAL;

SELECT id, m, m['a'], m['b'], m['c'], m['d'] FROM t_per_key_parallel_off ORDER BY id;
CHECK TABLE t_per_key_parallel_off SETTINGS check_query_single_value_result = 1;

SELECT 'feature on merges keys in parallel';
CREATE TABLE t_per_key_parallel
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    enable_map_key_columns_parallel_merge = 1,
    map_key_columns_merge_max_threads = 4;

SYSTEM STOP MERGES t_per_key_parallel;
INSERT INTO t_per_key_parallel VALUES (1, {'a': 1, 'b': 2, 'c': 3});
INSERT INTO t_per_key_parallel VALUES (2, {'b': 4, 'd': 5});
INSERT INTO t_per_key_parallel VALUES (3, {'a': 6, 'e': 7});
SYSTEM START MERGES t_per_key_parallel;
OPTIMIZE TABLE t_per_key_parallel FINAL;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_per_key_parallel' AND active;
SELECT id, m, m['a'], m['b'], m['c'], m['d'], m['e'] FROM t_per_key_parallel ORDER BY id;
CHECK TABLE t_per_key_parallel SETTINGS check_query_single_value_result = 1;

DROP TABLE t_per_key_parallel;
DROP TABLE t_per_key_parallel_off;
