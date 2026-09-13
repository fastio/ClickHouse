CREATE TABLE map_virtual_limits (p UInt32, id UInt32, m Map(String, UInt64))
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO map_virtual_limits VALUES (1, 1, {'first': 1});
INSERT INTO map_virtual_limits SELECT 2, number + 2, map('last', number) FROM numbers(100);
SELECT DISTINCT _map_column_keys FROM map_virtual_limits SETTINGS early_limit_for_map_virtual_columns = 1, max_threads = 4;
SELECT DISTINCT _map_column_keys FROM map_virtual_limits SETTINGS early_limit_for_map_virtual_columns = 2, max_threads = 1;
SELECT _map_column_keys FROM map_virtual_limits WHERE _partition_id = '1' SETTINGS early_limit_for_map_virtual_columns = 1;
SELECT count(), uniqExact(_map_column_keys) FROM map_virtual_limits SETTINGS early_limit_for_map_virtual_columns = 1;
SELECT count(), uniqExact(_map_column_keys) FROM map_virtual_limits SETTINGS early_limit_for_map_virtual_columns = 2;
SELECT count(), uniqExact(_map_column_keys) FROM map_virtual_limits SETTINGS early_limit_for_map_virtual_columns = 0;
SELECT count(_map_column_keys) FROM map_virtual_limits SETTINGS early_limit_for_map_virtual_columns = 1;
SELECT count(), uniqExact(_map_column_keys) FROM map_virtual_limits WHERE _partition_id = 'missing' SETTINGS early_limit_for_map_virtual_columns = 1;
SELECT count(_map_column_keys) FROM map_virtual_limits WHERE _partition_id = '2' SETTINGS early_limit_for_map_virtual_columns = 1;
SELECT count(_map_column_keys) FROM map_virtual_limits SETTINGS early_limit_for_map_virtual_columns = 0;
SELECT id, _map_column_keys FROM map_virtual_limits SETTINGS early_limit_for_map_virtual_columns = 1; -- { serverError BAD_ARGUMENTS }
SELECT _part_map_files FROM map_virtual_limits SETTINGS early_limit_for_map_virtual_columns = 1; -- { serverError BAD_ARGUMENTS }
SELECT _map_column_keys FROM map_virtual_limits WHERE id > 10 SETTINGS early_limit_for_map_virtual_columns = 1; -- { serverError BAD_ARGUMENTS }
SELECT _map_column_keys FROM map_virtual_limits PREWHERE id > 10 SETTINGS early_limit_for_map_virtual_columns = 1; -- { serverError BAD_ARGUMENTS }
SELECT _map_column_keys FROM map_virtual_limits FINAL SETTINGS early_limit_for_map_virtual_columns = 1; -- { serverError ILLEGAL_FINAL }
SELECT _map_column_keys, _partition_id FROM map_virtual_limits SETTINGS early_limit_for_map_virtual_columns = 1; -- { serverError BAD_ARGUMENTS }
SELECT _map_column_keys FROM map_virtual_limits WHERE rand() % 2 = 0 SETTINGS early_limit_for_map_virtual_columns = 1; -- { serverError BAD_ARGUMENTS }
SELECT _map_column_keys, n.number FROM map_virtual_limits CROSS JOIN numbers(2) AS n SETTINGS early_limit_for_map_virtual_columns = 1; -- { serverError BAD_ARGUMENTS }
DROP TABLE map_virtual_limits;
