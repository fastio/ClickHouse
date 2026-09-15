-- Tags: no-random-settings, no-random-merge-tree-settings

-- Compact `with_key_columns` coverage beyond 05113: multi-granule parts, Array and
-- Nullable(Array) values, per-key seek under PREWHERE / explicit subcolumns / skip
-- indexes, Compact + Wide coexistence in one merge, metadata and virtual columns
-- served from the sidecar manifest, and Compact inputs to key-union merges.

DROP TABLE IF EXISTS t_pk_compact_mg;
DROP TABLE IF EXISTS t_pk_compact_arr;
DROP TABLE IF EXISTS t_pk_compact_pw;
DROP TABLE IF EXISTS t_pk_compact_idx;
DROP TABLE IF EXISTS t_pk_compact_mix;
DROP TABLE IF EXISTS t_pk_compact_meta;
DROP TABLE IF EXISTS t_pk_compact_missing;

SELECT 'multi-granule compact, keys frozen across granules';
CREATE TABLE t_pk_compact_mg
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000,
    index_granularity = 2;

-- Key 'c' only appears in the last granule; key 'b' spans granules. The whole part
-- must freeze {a, b, c} before writing granule 0 and keep the same substreams per granule.
INSERT INTO t_pk_compact_mg VALUES (1, map('a', 1)), (2, map('a', 2, 'b', 10)), (3, map('b', 0)), (4, map()), (5, map('c', 5)), (6, map('a', 6, 'c', 7));
SELECT part_type, rows, marks FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_compact_mg' AND active;
SELECT id, m, m['a'], m['b'], m['c'], isNull(m['a']), isNull(m['c']) FROM t_pk_compact_mg ORDER BY id;
SELECT id, m['b'] FROM t_pk_compact_mg WHERE m['b'] IS NOT NULL ORDER BY id;
CHECK TABLE t_pk_compact_mg SETTINGS check_query_single_value_result = 1;

SELECT 'array and nullable-array values in compact';
CREATE TABLE t_pk_compact_arr
(
    id UInt64,
    s Map(String, Array(String)),
    n Map(String, Array(Nullable(Int32)))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    serialization_info_version = 'with_types',
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000,
    index_granularity = 2;

INSERT INTO t_pk_compact_arr VALUES (1, map('k', ['x']), map('k', [1, NULL, -2])), (2, map('k', [], 'z', ['y']), map('k', [], 'z', [NULL])), (3, map(), map()), (4, map('z', ['w']), map('z', [7]));
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_compact_arr' AND active;
SELECT toTypeName(s['k']), toTypeName(n['k']) FROM t_pk_compact_arr LIMIT 1;
SELECT id, s, s['k'], s['z'], n, n['k'], n['z'], mapContains(s, 'k'), mapContains(n, 'z') FROM t_pk_compact_arr ORDER BY id;
SELECT id, s.key_k, n.key_z FROM t_pk_compact_arr ORDER BY id;
CHECK TABLE t_pk_compact_arr SETTINGS check_query_single_value_result = 1;

SELECT 'prewhere and explicit subcolumns seek per key';
CREATE TABLE t_pk_compact_pw
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000,
    index_granularity = 1;

INSERT INTO t_pk_compact_pw VALUES (1, map('a', 20, 'b', 1)), (2, map('a', 5, 'b', 2)), (3, map('b', 3)), (4, map('a', 7));
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_compact_pw' AND active;
SELECT id, m['a'], m['b'] FROM t_pk_compact_pw PREWHERE m['a'] > 10 ORDER BY id;
SELECT id, m['b'] FROM t_pk_compact_pw PREWHERE m['a'] IS NULL ORDER BY id;
SELECT id, m.key_a FROM t_pk_compact_pw PREWHERE isNull(m.key_a) ORDER BY id;
SELECT toTypeName(m.key_a), sum(m.key_a), count(m.key_a), countIf(isNull(m.key_a)) FROM t_pk_compact_pw;

SELECT 'skip index on key column in compact';
CREATE TABLE t_pk_compact_idx
(
    id UInt64,
    m Map(String, UInt64),
    INDEX idx m.key_a TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000,
    index_granularity = 1;

INSERT INTO t_pk_compact_idx VALUES (1, map('a', 20, 'b', 1)), (2, map('a', 5, 'b', 2)), (3, map('b', 3));
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_compact_idx' AND active;
SELECT id, m['a'] FROM t_pk_compact_idx WHERE m['a'] > 10 ORDER BY id;

SELECT 'compact and wide coexist in one merge';
CREATE TABLE t_pk_compact_mix
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000,
    index_granularity = 2;

SYSTEM STOP MERGES t_pk_compact_mix;
-- First part: compact (tiny, under the byte threshold).
INSERT INTO t_pk_compact_mix VALUES (1, map('a', 1, 'b', 2)), (2, map('b', 3));
-- Flip the byte threshold off so the next zero-level part is written as wide.
ALTER TABLE t_pk_compact_mix MODIFY SETTING max_bytes_for_compact_map_key_columns = 0;
INSERT INTO t_pk_compact_mix VALUES (3, map('b', 4, 'c', 5)), (4, map('d', 6));
SELECT part_type, count() FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_compact_mix' AND active GROUP BY part_type ORDER BY part_type;
SYSTEM START MERGES t_pk_compact_mix;
OPTIMIZE TABLE t_pk_compact_mix FINAL;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_compact_mix' AND active;
SELECT id, m, m['a'], m['b'], m['c'], m['d'], mapKeys(m) FROM t_pk_compact_mix ORDER BY id;
CHECK TABLE t_pk_compact_mix SETTINGS check_query_single_value_result = 1;

SELECT 'metadata and virtual columns from compact sidecar';
CREATE TABLE t_pk_compact_meta
(
    p UInt32,
    id UInt32,
    m Map(String, UInt64)
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000,
    index_granularity = 2;

INSERT INTO t_pk_compact_meta VALUES (1, 1, map('b', 0, 'a', 1)), (1, 2, map('a', 2, 'c', 3)), (1, 3, map('d', 4));
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_compact_meta' AND active;
SELECT getMapKeys(currentDatabase(), 't_pk_compact_meta', 'm');
SELECT DISTINCT _map_column_keys FROM t_pk_compact_meta;

SELECT 'missing key source across compact inputs';
CREATE TABLE t_pk_compact_missing
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000,
    index_granularity = 2,
    map_key_columns_merge_skip_missing_key_readers = 1;

SYSTEM STOP MERGES t_pk_compact_missing;
INSERT INTO t_pk_compact_missing VALUES (1, map('a', 0)), (2, map('a', 1, 'b', 2));
INSERT INTO t_pk_compact_missing VALUES (3, map('b', 3, 'c', 4)), (4, map('c', 5));
INSERT INTO t_pk_compact_missing VALUES (5, map('a', 6));
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_compact_missing' AND active;
SYSTEM START MERGES t_pk_compact_missing;
OPTIMIZE TABLE t_pk_compact_missing FINAL;
SELECT id, m, m['a'], isNull(m['a']), m['b'], isNull(m['b']), m['c'], isNull(m['c']) FROM t_pk_compact_missing ORDER BY id;
CHECK TABLE t_pk_compact_missing SETTINGS check_query_single_value_result = 1;

DROP TABLE t_pk_compact_mg;
DROP TABLE t_pk_compact_arr;
DROP TABLE t_pk_compact_pw;
DROP TABLE t_pk_compact_idx;
DROP TABLE t_pk_compact_mix;
DROP TABLE t_pk_compact_meta;
DROP TABLE t_pk_compact_missing;
