-- Adapted from ByConity tests/queries/4_cnch_stateless/10027_test_map_check_data_part.sql
-- Source revision: 0ec8e61acc287afe34f4700fe510661ae28415fa
-- `CnchMergeTree` becomes `MergeTree`; BYTE/default maps use `with_key_columns`, KV maps use `basic`.
-- Brace access uses bracket access. Original reference results are retained to expose semantic differences.
-- Preserve `check_query_single_value_result`; force the original vertical-merge scenario to activate.
SET optimize_functions_to_subcolumns = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET max_threads = 1;
SET mutations_sync = 1;
SET check_query_single_value_result = 1;



drop table if exists t_10027_test_map_check_data_part1;
drop table if exists t_10027_test_map_check_data_part2;
drop table if exists t_10027_test_map_check_data_part3;
drop table if exists t_10027_test_map_check_data_part4;

-- create table contains map in wide part format use horizontal merge
create table t_10027_test_map_check_data_part1(
    id Int64,
    `string_params` Map(String, String),
    `string_list_params` Map(String, Array(String)),
    `int_params` Map(Int32, Int32),
    `int_list_params` Map(Int32, Array(Int32))
)
engine = MergeTree
order by id
settings enable_vertical_merge_algorithm = 0, map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

system start merges t_10027_test_map_check_data_part1;

-- create table contains map in wide part format use vertical merge
create table t_10027_test_map_check_data_part2(
    id Int64,
    `string_params` Map(String, String),
    `string_list_params` Map(String, Array(String)),
    `int_params` Map(Int32, Int32),
    `int_list_params` Map(Int32, Array(Int32))
)
engine = MergeTree
order by id
settings enable_vertical_merge_algorithm = 1, map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, vertical_merge_algorithm_min_rows_to_activate = 0, vertical_merge_algorithm_min_bytes_to_activate = 0, vertical_merge_algorithm_min_columns_to_activate = 0;

system start merges t_10027_test_map_check_data_part2;

-- create table contains kv map
create table t_10027_test_map_check_data_part3(
    id Int64,
    `string_params` Map(String, String),
    `string_list_params` Map(String, Array(String)),
    `int_params` Map(Int32, Int32),
    `int_list_params` Map(Int32, Array(Int32))
)
engine = MergeTree
order by id SETTINGS map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

system start merges t_10027_test_map_check_data_part3;

-- create table contains low cardinality map
create table t_10027_test_map_check_data_part4(
    id Int64,
    `string_params` Map(String, LowCardinality(Nullable(String))),
    `int_params` Map(Int32, LowCardinality(Nullable(Int32)))
)
engine = MergeTree
order by id SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

system start merges t_10027_test_map_check_data_part4;


insert into t_10027_test_map_check_data_part1 values(1000, {'s1':'s_v1', 's2':'s_v2'}, {'s_l1':['l_v1', 'l_v2', 'l_v3']}, {1: 1, 2: 2}, {1:[1, 2, 3], 2:[4,5,6]});
insert into t_10027_test_map_check_data_part1 values(1000, {'s1':'s_v1', 's2':'s_v2'}, {'s_l1':['l_v1', 'l_v2', 'l_v3']}, {1: 1, 2: 2}, {1:[1, 2, 3], 2:[4,5,6]});
insert into t_10027_test_map_check_data_part2 values(1000, {'s1':'s_v1', 's2':'s_v2'}, {'s_l1':['l_v1', 'l_v2', 'l_v3']}, {1: 1, 2: 2}, {1:[1, 2, 3], 2:[4,5,6]});
insert into t_10027_test_map_check_data_part2 values(1000, {'s1':'s_v1', 's2':'s_v2'}, {'s_l1':['l_v1', 'l_v2', 'l_v3']}, {1: 1, 2: 2}, {1:[1, 2, 3], 2:[4,5,6]});
insert into t_10027_test_map_check_data_part3 values(1000, {'s1':'s_v1', 's2':'s_v2'}, {'s_l1':['l_v1', 'l_v2', 'l_v3']}, {1: 1, 2: 2}, {1:[1, 2, 3], 2:[4,5,6]});
insert into t_10027_test_map_check_data_part3 values(1000, {'s1':'s_v1', 's2':'s_v2'}, {'s_l1':['l_v1', 'l_v2', 'l_v3']}, {1: 1, 2: 2}, {1:[1, 2, 3], 2:[4,5,6]});
insert into t_10027_test_map_check_data_part4 select number, map('k1', toString(number)), map(1, number) from system.numbers limit 10;
insert into t_10027_test_map_check_data_part4 select number, map('k1', toString(number)), map(1, number) from system.numbers limit 100001;

check table t_10027_test_map_check_data_part1;
check table t_10027_test_map_check_data_part2;
check table t_10027_test_map_check_data_part3;
check table t_10027_test_map_check_data_part4;

optimize table t_10027_test_map_check_data_part1 final settings mutations_sync=1;
optimize table t_10027_test_map_check_data_part2 final settings mutations_sync=1;
optimize table t_10027_test_map_check_data_part3 final settings mutations_sync=1;
optimize table t_10027_test_map_check_data_part4 final settings mutations_sync=1;



check table t_10027_test_map_check_data_part1;
check table t_10027_test_map_check_data_part2;
check table t_10027_test_map_check_data_part3;
check table t_10027_test_map_check_data_part4;

select * from t_10027_test_map_check_data_part1;
select * from t_10027_test_map_check_data_part2;
select * from t_10027_test_map_check_data_part3;
-- select * from t_10027_test_map_check_data_part4;

drop table t_10027_test_map_check_data_part1;
drop table t_10027_test_map_check_data_part2;
drop table t_10027_test_map_check_data_part3;
drop table t_10027_test_map_check_data_part4;
