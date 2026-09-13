-- Adapted from ByConity tests/queries/4_cnch_stateless/10027_test_query_map.sql
-- Source revision: 0ec8e61acc287afe34f4700fe510661ae28415fa
-- `CnchMergeTree` becomes `MergeTree`; BYTE/default maps use `with_key_columns`, KV maps use `basic`.
-- Brace access uses bracket access. Original reference results are retained to expose semantic differences.
SET optimize_functions_to_subcolumns = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET max_threads = 1;
SET mutations_sync = 1;
drop table if exists `t_10027_test_query_map`;

-- limit to read 1 row per block
set preferred_block_size_bytes = 1;

select 'insert a data part whose mark number is bigger than 1, all map columns are empty';
create table `t_10027_test_query_map`(
    id Int64,
    `string_params` Map(String, String),
    `string_list_params` Map(String, Array(String)),
    `int_params` Map(String, Int32),
    `int_list_params` Map(String, Array(Nullable(Int32)))
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0, map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0;

-- last mark is incomplete
insert into `t_10027_test_query_map` (id) select number from system.numbers limit 5;

select count(id) from `t_10027_test_query_map`;
select count(string_params) from `t_10027_test_query_map`;
select count(string_list_params) from `t_10027_test_query_map`;
select count(int_params) from `t_10027_test_query_map`;
select count(int_list_params) from `t_10027_test_query_map`;
select count(string_params) from `t_10027_test_query_map` prewhere id = 0;
select count(string_params) from `t_10027_test_query_map` prewhere id = 1;
select count(string_params) from `t_10027_test_query_map` prewhere id < 5;

drop table `t_10027_test_query_map`;

select '';
select 'insert a data part whose mark number is bigger than 1';
create table `t_10027_test_query_map`(
    id Int64,
    `string_params` Map(String, String),
    `string_list_params` Map(String, Array(String)),
    `int_params` Map(String, Int32),
    `int_list_params` Map(String, Array(Nullable(Int32)))
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0, map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0;

-- last mark is incomplete
insert into `t_10027_test_query_map` values (1, {'1': '1'}, {'1': ['1']}, {'1': 1}, {'1': [1, null]}) (2, {'2': '1'}, {'2': ['1']}, {'2': 1}, {'2': [1, null]}) (3, {'3': '1'}, {'3': ['1']}, {'3': 1}, {'3': [1, null]});

-- test reading map sequentially
select * from `t_10027_test_query_map` order by id;

drop table `t_10027_test_query_map`;
