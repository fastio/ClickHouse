SET enable_parallel_replicas = 0;
SET optimize_functions_to_subcolumns = 1;
SET max_threads = 1;
CREATE TABLE nullable_arrays (id UInt64, m Map(String, Array(Nullable(Int32))))
ENGINE = MergeTree ORDER BY id SETTINGS serialization_info_version = 'with_types', map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    index_granularity = 2, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO nullable_arrays VALUES (1, {}), (2, {'k': []}), (3, {'k': [NULL]}),
    (4, {'k': [1, NULL, -2, 3]}), (5, {'k': [-2147483648, 2147483647]});
SELECT toTypeName(m['k']), toTypeName(m.key_k) FROM nullable_arrays LIMIT 1;
SELECT id, m, m['k'], mapContains(m, 'k'), mapKeys(m) FROM nullable_arrays ORDER BY id
SETTINGS preferred_block_size_bytes = 1, max_block_size = 2;
SELECT id, mapContains(m, 'k'), m.key_k, m FROM nullable_arrays ORDER BY id
SETTINGS preferred_block_size_bytes = 1, max_block_size = 2;
SELECT id, m.key_k, mapContains(m, 'k') FROM nullable_arrays PREWHERE id >= 3 ORDER BY id;
SELECT id FROM nullable_arrays PREWHERE isNull(m.key_k) ORDER BY id;
SELECT count(), count(m.key_k), countIf(mapContains(m, 'k')) FROM nullable_arrays;
SELECT id, m, mapContains(m, 'k') FROM nullable_arrays ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
CHECK TABLE nullable_arrays SETTINGS check_query_single_value_result = 1;
DROP TABLE nullable_arrays;

CREATE TABLE nullable_nested (id UInt64, s Map(String, Array(Nullable(String))), n Map(String, Array(Array(Nullable(Int32)))))
ENGINE = MergeTree ORDER BY id SETTINGS serialization_info_version = 'with_types', map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns';
INSERT INTO nullable_nested VALUES (1, {'k':['',NULL,'x']}, {'k':[[],[NULL],[1,NULL,-2]]}), (2, {}, {});
SELECT id, s['k'], n['k'], mapContains(s, 'k'), mapContains(n, 'k') FROM nullable_nested ORDER BY id;
SELECT toTypeName(n['k']) FROM nullable_nested LIMIT 1;
DROP TABLE nullable_nested;

CREATE TABLE nullable_parts (id UInt64, m Map(String, Array(Nullable(Int32))))
ENGINE = MergeTree ORDER BY id SETTINGS serialization_info_version = 'with_types', map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns',
    index_granularity = 3, index_granularity_bytes = 0;
INSERT INTO nullable_parts SELECT number, if(number < 5, CAST(map(), 'Map(String, Array(Nullable(Int32)))'),
    map('late', CAST([number, NULL], 'Array(Nullable(Int32))'))) FROM numbers(9)
SETTINGS max_block_size = 2, max_insert_block_size = 2, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'nullable_parts' AND active;
SELECT id, m['late'], mapContains(m, 'late') FROM nullable_parts ORDER BY id SETTINGS max_block_size = 2;
CHECK TABLE nullable_parts SETTINGS check_query_single_value_result = 1;
DROP TABLE nullable_parts;

CREATE TABLE nullable_rejected (m Map(String, Nullable(Int32))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS serialization_info_version = 'with_types', map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE nullable_rejected (m Map(String, Array(Nullable(Array(Int32))))) ENGINE = MergeTree ORDER BY tuple()
SETTINGS serialization_info_version = 'with_types', map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_TABLES, ILLEGAL_COLUMN }
