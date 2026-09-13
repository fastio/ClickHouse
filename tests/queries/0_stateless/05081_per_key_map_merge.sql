-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_per_key_merge;
DROP TABLE IF EXISTS t_per_key_merge_limit;
DROP TABLE IF EXISTS t_per_key_merge_over_write;

SELECT 'union merge';
CREATE TABLE t_per_key_merge
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
    min_rows_for_wide_part = 0;

SYSTEM STOP MERGES t_per_key_merge;
INSERT INTO t_per_key_merge VALUES (1, {'a': 1, 'b': 2});
INSERT INTO t_per_key_merge VALUES (2, {'b': 3, 'c': 4});

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_per_key_merge' AND active;

SYSTEM START MERGES t_per_key_merge;
OPTIMIZE TABLE t_per_key_merge FINAL;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_per_key_merge' AND active;
SELECT id, m, m['a'], m['b'], m['c'], mapKeys(m) FROM t_per_key_merge ORDER BY id;

SELECT 'deduplicate rejected';
OPTIMIZE TABLE t_per_key_merge DEDUPLICATE; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'merge over write limit';
CREATE TABLE t_per_key_merge_over_write
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
    max_keys_in_map = 1;

SYSTEM STOP MERGES t_per_key_merge_over_write;
INSERT INTO t_per_key_merge_over_write VALUES (1, {'a': 1});
INSERT INTO t_per_key_merge_over_write VALUES (2, {'b': 2});
SYSTEM START MERGES t_per_key_merge_over_write;
OPTIMIZE TABLE t_per_key_merge_over_write FINAL;
SELECT id, m, m['a'], m['b'] FROM t_per_key_merge_over_write ORDER BY id;

SELECT 'merge key limit';
CREATE TABLE t_per_key_merge_limit
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
    max_keys_in_map_for_merge = 2;

SYSTEM STOP MERGES t_per_key_merge_limit;
INSERT INTO t_per_key_merge_limit VALUES (1, {'a': 1, 'b': 2});
INSERT INTO t_per_key_merge_limit VALUES (2, {'c': 3});
SYSTEM START MERGES t_per_key_merge_limit;
OPTIMIZE TABLE t_per_key_merge_limit FINAL; -- { serverError LIMIT_EXCEEDED }

DROP TABLE t_per_key_merge;
DROP TABLE t_per_key_merge_over_write;
DROP TABLE t_per_key_merge_limit;
