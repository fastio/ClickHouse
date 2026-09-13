SET optimize_functions_to_subcolumns = 1;

CREATE TABLE map_paths_single (m Map(String, UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns',
    max_keys_in_map_for_merge = 1, enable_vertical_merge_algorithm = 0, index_granularity = 2, index_granularity_bytes = 0;
SYSTEM STOP MERGES map_paths_single;
INSERT INTO map_paths_single VALUES ({'a': 1}), ({});
INSERT INTO map_paths_single VALUES ({'b': 2});
SYSTEM START MERGES map_paths_single;
OPTIMIZE TABLE map_paths_single FINAL; -- { serverError LIMIT_EXCEEDED }
ALTER TABLE map_paths_single MODIFY SETTING max_keys_in_map_for_merge = 0;
OPTIMIZE TABLE map_paths_single FINAL;
SELECT m FROM map_paths_single ORDER BY m;
SELECT length(m) FROM map_paths_single ORDER BY length(m);
SELECT empty(m) FROM map_paths_single ORDER BY empty(m);
SELECT notEmpty(m) FROM map_paths_single ORDER BY notEmpty(m);
SELECT mapKeys(m) FROM map_paths_single ORDER BY mapKeys(m);
SELECT mapValues(m) FROM map_paths_single ORDER BY mapValues(m);
CHECK TABLE map_paths_single SETTINGS check_query_single_value_result = 1;
SYSTEM FLUSH LOGS;
SELECT merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND table = 'map_paths_single' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;
DETACH TABLE map_paths_single;
ATTACH TABLE map_paths_single;
SELECT count() FROM map_paths_single;
DROP TABLE map_paths_single;

CREATE TABLE map_paths_empty (m Map(String, UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns';
SYSTEM STOP MERGES map_paths_empty;
INSERT INTO map_paths_empty VALUES ({});
INSERT INTO map_paths_empty VALUES ({});
SYSTEM START MERGES map_paths_empty;
OPTIMIZE TABLE map_paths_empty FINAL;
SELECT m FROM map_paths_empty;
CHECK TABLE map_paths_empty SETTINGS check_query_single_value_result = 1;
SYSTEM FLUSH LOGS;
SELECT merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND table = 'map_paths_empty' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;
DROP TABLE map_paths_empty;

CREATE TABLE map_paths_ttl (id UInt64, ts DateTime, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
TTL ts + INTERVAL 1 DAY
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns',
    vertical_merge_optimize_ttl_delete = 0, enable_vertical_merge_algorithm = 0;
SYSTEM STOP MERGES map_paths_ttl;
INSERT INTO map_paths_ttl VALUES (1, '2000-01-01 00:00:00', {'expired': 1}), (2, '2100-01-01 00:00:00', {'a': 2});
INSERT INTO map_paths_ttl VALUES (3, '2100-01-01 00:00:00', {'b': 3});
SYSTEM START MERGES map_paths_ttl;
OPTIMIZE TABLE map_paths_ttl FINAL;
SELECT id, m FROM map_paths_ttl ORDER BY id;
CHECK TABLE map_paths_ttl SETTINGS check_query_single_value_result = 1;
SYSTEM FLUSH LOGS;
SELECT merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND table = 'map_paths_ttl' AND event_type = 'MergeParts' AND error = 0
ORDER BY event_time_microseconds DESC LIMIT 1;
DROP TABLE map_paths_ttl;

ATTACH TABLE map_paths_bad_projection UUID '703e65d5-65cd-4f69-b794-9c3af21d8e41'
(id UInt64, m Map(String, UInt64), PROJECTION p (SELECT id ORDER BY id))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }
ATTACH TABLE map_paths_bad_key UUID '703e65d5-65cd-4f69-b794-9c3af21d8e42'
(m Map(String, UInt64)) ENGINE = MergeTree ORDER BY length(m)
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE map_paths_column_ttl (id UInt64, ts DateTime, value UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns';
INSERT INTO map_paths_column_ttl VALUES (1, '2000-01-01 00:00:00', 1, {'a': 1});
ALTER TABLE map_paths_column_ttl MODIFY COLUMN value UInt64 TTL ts + INTERVAL 1 DAY
SETTINGS materialize_ttl_after_modify = 0;
OPTIMIZE TABLE map_paths_column_ttl FINAL; -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE map_paths_column_ttl;

CREATE TABLE map_paths_group_ttl (id UInt64, ts DateTime, value UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns';
INSERT INTO map_paths_group_ttl VALUES (1, '2000-01-01 00:00:00', 1, {'a': 1});
ALTER TABLE map_paths_group_ttl MODIFY TTL ts + INTERVAL 1 DAY GROUP BY id SET value = sum(value)
SETTINGS materialize_ttl_after_modify = 0;
OPTIMIZE TABLE map_paths_group_ttl FINAL; -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE map_paths_group_ttl;
