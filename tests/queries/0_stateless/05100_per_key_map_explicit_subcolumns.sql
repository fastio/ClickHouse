SET enable_analyzer = 1;

CREATE TABLE explicit_keys (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

SELECT 'empty table';
SELECT toTypeName(any(m.key_a)), count(m.key_a) FROM explicit_keys;

INSERT INTO explicit_keys VALUES (1, {'a': 11}), (2, {'b': 7}), (3, {'a': 0}), (4, {}), (5, {'a': 23});

SELECT 'types and values';
SELECT toTypeName(m.key_a), toTypeName(m.key_missing) FROM explicit_keys LIMIT 1;
SELECT id, m.key_a, m['a'], isNull(m.key_a) FROM explicit_keys ORDER BY id;
SELECT sum(m.key_a), count(m.key_a), countIf(isNull(m.key_a)), min(m.key_a), max(m.key_a) FROM explicit_keys;
SELECT count(m.key_missing), countIf(isNull(m.key_missing)) FROM explicit_keys;

SELECT 'without subcolumn optimization';
SELECT toTypeName(m.key_a), sum(m.key_a), count(m.key_a), countIf(isNull(m.key_a))
FROM explicit_keys GROUP BY toTypeName(m.key_a) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'with subcolumn optimization';
SELECT toTypeName(m.key_a), sum(m.key_a), count(m.key_a), countIf(isNull(m.key_a))
FROM explicit_keys GROUP BY toTypeName(m.key_a) SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'filters and groups';
SELECT id FROM explicit_keys WHERE m.key_a = 0 ORDER BY id;
SELECT id FROM explicit_keys PREWHERE isNull(m.key_a) ORDER BY id;
SELECT m.key_a, count() FROM explicit_keys GROUP BY m.key_a ORDER BY m.key_a NULLS LAST;

SELECT 'key absent in one part';
INSERT INTO explicit_keys VALUES (6, {'b': 8});
SELECT sum(m.key_a), count(m.key_a), countIf(isNull(m.key_a)) FROM explicit_keys;
OPTIMIZE TABLE explicit_keys FINAL;
SELECT sum(m.key_a), count(m.key_a), countIf(isNull(m.key_a)) FROM explicit_keys;
DROP TABLE explicit_keys;

SELECT 'ordinary map';
CREATE TABLE basic_keys (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';
INSERT INTO basic_keys VALUES (1, {'a': 11}), (2, {'b': 7}), (3, {'a': 0}), (4, {}), (5, {'a': 23});
SELECT toTypeName(m.key_a), toTypeName(m.key_missing) FROM basic_keys LIMIT 1;
SELECT id, m.key_a FROM basic_keys ORDER BY id;
SELECT sum(m.key_a), count(m.key_a), countIf(isNull(m.key_a)) FROM basic_keys;
DROP TABLE basic_keys;

SELECT 'bucketed map';
CREATE TABLE bucket_keys (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4, map_buckets_strategy = 'constant', map_buckets_min_avg_size = 0,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, serialization_info_version = 'with_types';
INSERT INTO bucket_keys VALUES (1, {'a': 11}), (2, {});
SELECT toTypeName(m.key_a), id, m.key_a FROM bucket_keys ORDER BY id;
DROP TABLE bucket_keys;
