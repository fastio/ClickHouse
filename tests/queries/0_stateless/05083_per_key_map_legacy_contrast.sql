-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_legacy_default;
DROP TABLE IF EXISTS t_legacy_compact;
DROP TABLE IF EXISTS t_legacy_buckets;
DROP TABLE IF EXISTS t_legacy_mixed;

SELECT 'default settings';
SELECT name, value
FROM system.merge_tree_settings
WHERE name IN ('map_serialization_version', 'map_serialization_version_for_zero_level_parts')
ORDER BY name;

SELECT 'default table is not with_key_columns';
CREATE TABLE t_legacy_default (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree
ORDER BY id;
INSERT INTO t_legacy_default VALUES (1, {'a': 1, 'b': 0}), (2, map());

SELECT position(engine_full, 'with_key_columns') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_legacy_default';

SELECT 'default type and missing';
SELECT toTypeName(m['a']), toTypeName(m['missing']) FROM t_legacy_default LIMIT 1;
SELECT id, m['a'], m['b'], m['missing'], mapContains(m, 'a'), mapContains(m, 'missing')
FROM t_legacy_default
ORDER BY id;

SELECT 'compact default type and missing';
CREATE TABLE t_legacy_compact (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = '200G', min_rows_for_wide_part = 1000000;
INSERT INTO t_legacy_compact VALUES (1, {'a': 1}), (2, map());
SELECT toTypeName(m['a']), id, m['a'], mapContains(m, 'a') FROM t_legacy_compact ORDER BY id;

SELECT 'with_buckets type and missing';
CREATE TABLE t_legacy_buckets (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    serialization_info_version = 'with_types';

INSERT INTO t_legacy_buckets VALUES (1, {'a': 1, 'b': 0}), (2, map());

SELECT toTypeName(m['a']) FROM t_legacy_buckets LIMIT 1;
SELECT id, m['a'], m['b'], m['missing'], mapContains(m, 'a') FROM t_legacy_buckets ORDER BY id;
SELECT has(substreams, 'm.buckets_info') AS uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_legacy_buckets' AND column = 'm' AND active
LIMIT 1;

SELECT 'basic zero-level then with_buckets merge';
CREATE TABLE t_legacy_mixed (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'basic',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    serialization_info_version = 'with_types';

INSERT INTO t_legacy_mixed SELECT number, map('key', number) FROM numbers(10);
SELECT min(has(substreams, 'm.buckets_info')) AS zero_level_uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_legacy_mixed' AND column = 'm' AND active AND level = 0;

OPTIMIZE TABLE t_legacy_mixed FINAL;
SELECT has(substreams, 'm.buckets_info') AS merged_uses_with_buckets
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_legacy_mixed' AND column = 'm' AND active
LIMIT 1;
SELECT toTypeName(m['key']), count() FROM t_legacy_mixed WHERE m['key'] = 0;

DROP TABLE t_legacy_default;
DROP TABLE t_legacy_compact;
DROP TABLE t_legacy_buckets;
DROP TABLE t_legacy_mixed;
