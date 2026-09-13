-- Adapted from ByConity tests/queries/4_cnch_stateless/00745_merge_tree_map_data_type.sql
-- Source revision: 0ec8e61acc287afe34f4700fe510661ae28415fa
-- `CnchMergeTree` becomes `MergeTree`; BYTE/default maps use `with_key_columns`, KV maps use `basic`.
-- Use typed keys for bracket access; omit the duplicate query ported from brace access.
-- Port only the BYTE section through `mapValues`; metadata functions are covered separately.
SET optimize_functions_to_subcolumns = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET max_threads = 1;
SET mutations_sync = 1;
SELECT 'test BYTE map';
CREATE TABLE t_00745_merge_tree_map_data_type (
    n UInt8,
    `string_map` Map(String, String),
    `string_array_map` Map(String, Array(String)),
    `fixed_string_map` Map(FixedString(2), FixedString(2)),
    `int_map` Map(UInt32, UInt32),
    `lowcardinality_map` Map(LowCardinality(String), LowCardinality(Nullable(String))),
    `float_map` Map(Float32, Float32),
    `date_map` Map(Date, Date),
    `datetime_map` Map(DateTime('Asia/Shanghai'), DateTime('Asia/Shanghai')),
    `uuid_map` Map(UUID, String),
    `enums_map` Map(Enum8('hello' = 0, 'world' = 1, 'foo' = -1), Int32))
Engine=MergeTree ORDER BY n settings min_bytes_for_wide_part = 0, map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0;

insert into t_00745_merge_tree_map_data_type values(1, {'s1': 's1', 's2': 's2'}, {'s1': ['v1', 'v2'], 's2': ['v1', 'v2']}, {'s1': 's1', 's2': 's2'}, {1: 1, 2:2}, {'s1': 's1', 's2': 's2'}, {0.5: 0.5, 1.9: 1.9}, {'2022-06-14': '2022-06-14', '2022-06-15': '2022-06-15'}, {'2022-06-14 12:00:00': '2022-06-14 12:00:00', '2022-06-14 14:00:00': '2022-06-14 14:00:00'}, {'93fd2f47-4567-4eb8-9e7b-0e17f4c77837': '93fd2f47-4567-4eb8-9e7b-0e17f4c77837'}, {'world': 4});
insert into t_00745_merge_tree_map_data_type values(2, {'s3': 's3'}, {'s3': ['v3', 'v4']}, {'s3': 's3'}, {3: 3}, {'s3': 's3'}, {3.5: 3.5, 4.9: 4.9}, {'2022-06-16': '2022-06-16'}, {'2022-06-16 12:00:00': '2022-06-16 12:00:00'}, {'13fd2f47-4567-4eb8-9e7b-0e17f4c77837': '13fd2f47-4567-4eb8-9e7b-0e17f4c77837'}, {'hello': 1, 'foo': 2});

select '';
select 'select * :';
select * from t_00745_merge_tree_map_data_type order by n;

select '';
-- Missing keys in `with_key_columns` maps return `NULL`.
select 'select [] :';
select string_map['s1'], string_map['s10'], string_array_map['s2'], string_array_map['s10'], fixed_string_map['s1'::FixedString(2)], fixed_string_map['s8'::FixedString(2)], int_map[1], int_map[10], lowcardinality_map['s1'], lowcardinality_map['s10'], float_map[0.5], float_map[0.4], date_map[toDate('2022-06-14')], date_map[toDate('2021-06-14')], datetime_map[toDateTime('2022-06-14 12:00:00', 'Asia/Shanghai')], datetime_map[toDateTime('2021-06-14 12:00:00', 'Asia/Shanghai')], uuid_map[toUUID('93fd2f47-4567-4eb8-9e7b-0e17f4c77837')], uuid_map[toUUID('53fd2f47-4567-4eb8-9e7b-0e17f4c77837')], enums_map['hello'] from t_00745_merge_tree_map_data_type order by n;

select '';
select 'select mapKeys :';
select mapKeys(string_map), mapKeys(string_array_map), mapKeys(fixed_string_map), mapKeys(int_map), mapKeys(lowcardinality_map), mapKeys(float_map), mapKeys(date_map), mapKeys(datetime_map), mapKeys(uuid_map), mapKeys(enums_map) from t_00745_merge_tree_map_data_type order by n;

select '';
select 'select mapValues :';
select mapValues(string_map), mapValues(string_array_map), mapValues(fixed_string_map), mapValues(int_map), mapValues(lowcardinality_map), mapValues(float_map), mapValues(date_map), mapValues(datetime_map), mapValues(uuid_map), mapValues(enums_map) from t_00745_merge_tree_map_data_type order by n;


DROP TABLE t_00745_merge_tree_map_data_type;
