CREATE TABLE map_virtuals (p UInt32, id UInt32, m Map(String, UInt64))
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
SELECT count() FROM (SELECT _map_column_keys FROM map_virtuals);
INSERT INTO map_virtuals VALUES (1, 1, {'a': 1}), (1, 2, {'b': 2});
INSERT INTO map_virtuals VALUES (2, 3, {'a': 3, 'c': 4});
SELECT DISTINCT _map_column_keys FROM map_virtuals SETTINGS max_threads = 1;
SELECT DISTINCT _map_column_keys FROM map_virtuals SETTINGS max_threads = 4;
SELECT DISTINCT _map_column_keys FROM map_virtuals WHERE _partition_id = '1';
SELECT id, _map_column_keys FROM map_virtuals ORDER BY id;
SELECT count() FROM map_virtuals WHERE 0 AND notEmpty(_map_column_keys);
SELECT DISTINCT _partition_id, notEmpty(_part_map_files), arrayAll(f -> NOT endsWith(f, '.keys_info.bin') AND position(f, 'per_key_template') = 0, _part_map_files) FROM map_virtuals ORDER BY _partition_id;
SELECT toTypeName(_map_column_keys), toTypeName(_part_map_files) FROM map_virtuals LIMIT 1;
ALTER TABLE map_virtuals DROP PARTITION 2;
SELECT DISTINCT _map_column_keys FROM map_virtuals;
OPTIMIZE TABLE map_virtuals FINAL;
SELECT DISTINCT _map_column_keys FROM map_virtuals;
ALTER TABLE map_virtuals ADD COLUMN new_map Map(String, UInt64);
SELECT DISTINCT _map_column_keys FROM map_virtuals;
ALTER TABLE map_virtuals DROP COLUMN m;
SELECT DISTINCT _map_column_keys, _part_map_files FROM map_virtuals;
DROP TABLE map_virtuals;
CREATE TABLE map_virtuals_basic (m Map(String, UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';
INSERT INTO map_virtuals_basic VALUES ({'a': 1});
SELECT _map_column_keys, _part_map_files FROM map_virtuals_basic;
DROP TABLE map_virtuals_basic;
