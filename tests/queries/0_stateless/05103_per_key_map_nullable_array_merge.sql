SET enable_parallel_replicas = 0;
SET optimize_functions_to_subcolumns = 1;
-- `with_key_columns` forces vertical merging even when the general switch is disabled.
SELECT 'vertical_disabled';
CREATE TABLE nullable_merge_disabled (id UInt64, m Map(String, Array(Nullable(Int32)))) ENGINE = MergeTree ORDER BY id
SETTINGS serialization_info_version = 'with_types', map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    index_granularity = 2, index_granularity_bytes = 0, enable_vertical_merge_algorithm = 0,
    vertical_merge_algorithm_min_rows_to_activate = 0, vertical_merge_algorithm_min_columns_to_activate = 0,
    vertical_merge_algorithm_min_bytes_to_activate = 0;
SYSTEM STOP MERGES nullable_merge_disabled;
INSERT INTO nullable_merge_disabled VALUES (1, {}), (2, {'a':[]}), (3, {'a':[NULL]});
INSERT INTO nullable_merge_disabled VALUES (4, {'b':[1,NULL,-2]}), (5, {'a':[3,NULL],'b':[]});
SELECT id, m, m['a'], m['b'], mapKeys(m) FROM nullable_merge_disabled ORDER BY id;
SYSTEM START MERGES nullable_merge_disabled;
OPTIMIZE TABLE nullable_merge_disabled FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'nullable_merge_disabled' AND active;
SELECT id, m, m['a'], m['b'], mapKeys(m) FROM nullable_merge_disabled ORDER BY id SETTINGS max_block_size = 2, preferred_block_size_bytes = 1;
CHECK TABLE nullable_merge_disabled SETTINGS check_query_single_value_result = 1;
SYSTEM FLUSH LOGS;
SELECT merge_algorithm FROM system.part_log WHERE database = currentDatabase() AND table = 'nullable_merge_disabled' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;
DETACH TABLE nullable_merge_disabled;
ATTACH TABLE nullable_merge_disabled;
SELECT id, m['a'], m['b'] FROM nullable_merge_disabled PREWHERE id >= 3 ORDER BY id;
DROP TABLE nullable_merge_disabled;

SELECT 'vertical';
CREATE TABLE nullable_merge_vertical (id UInt64, m Map(String, Array(Nullable(Int32)))) ENGINE = MergeTree ORDER BY id
SETTINGS serialization_info_version = 'with_types', map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0,
    index_granularity = 2, index_granularity_bytes = 0, enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 0, vertical_merge_algorithm_min_columns_to_activate = 0,
    vertical_merge_algorithm_min_bytes_to_activate = 0;
SYSTEM STOP MERGES nullable_merge_vertical;
INSERT INTO nullable_merge_vertical VALUES (1, {}), (2, {'a':[]}), (3, {'a':[NULL]});
INSERT INTO nullable_merge_vertical VALUES (4, {'b':[1,NULL,-2]}), (5, {'a':[3,NULL],'b':[]});
SELECT id, m, m['a'], m['b'], mapKeys(m) FROM nullable_merge_vertical ORDER BY id;
SYSTEM START MERGES nullable_merge_vertical;
OPTIMIZE TABLE nullable_merge_vertical FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'nullable_merge_vertical' AND active;
SELECT id, m, m['a'], m['b'], mapKeys(m) FROM nullable_merge_vertical ORDER BY id SETTINGS max_block_size = 2, preferred_block_size_bytes = 1;
CHECK TABLE nullable_merge_vertical SETTINGS check_query_single_value_result = 1;
SYSTEM FLUSH LOGS;
SELECT merge_algorithm FROM system.part_log WHERE database = currentDatabase() AND table = 'nullable_merge_vertical' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;
DETACH TABLE nullable_merge_vertical;
ATTACH TABLE nullable_merge_vertical;
SELECT id, m['a'], m['b'] FROM nullable_merge_vertical PREWHERE id >= 3 ORDER BY id;
DROP TABLE nullable_merge_vertical;
