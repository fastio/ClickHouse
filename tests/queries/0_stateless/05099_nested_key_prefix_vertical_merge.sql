-- A physical column name may legitimately contain `.key_`: a flattened `Nested` column such as
-- `n.key_id`, or a column literally named `m.key_a`. Neither is a `Map` key subcolumn, and a
-- vertical merge must not treat them as one. These tables use the default `basic` Map
-- serialization and have no `Map` column at all.

DROP TABLE IF EXISTS t_nested_key_prefix;

CREATE TABLE t_nested_key_prefix (id UInt64, n Nested(key_id UInt64, v String), pad String)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO t_nested_key_prefix VALUES (1, [10, 11], ['x', 'xx'], 'p1');
INSERT INTO t_nested_key_prefix VALUES (2, [20], ['y'], 'p2');

OPTIMIZE TABLE t_nested_key_prefix FINAL;

SELECT 'nested';
SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_nested_key_prefix' AND active;
SELECT id, n.key_id, n.v, pad FROM t_nested_key_prefix ORDER BY id;
CHECK TABLE t_nested_key_prefix;

DROP TABLE t_nested_key_prefix;

DROP TABLE IF EXISTS t_dotted_key_prefix;

CREATE TABLE t_dotted_key_prefix (id UInt64, `m.key_a` UInt64, pad String)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO t_dotted_key_prefix VALUES (1, 10, 'p1');
INSERT INTO t_dotted_key_prefix VALUES (2, 20, 'p2');

OPTIMIZE TABLE t_dotted_key_prefix FINAL;

SELECT 'dotted';
SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_dotted_key_prefix' AND active;
SELECT id, `m.key_a`, pad FROM t_dotted_key_prefix ORDER BY id;
CHECK TABLE t_dotted_key_prefix;

DROP TABLE t_dotted_key_prefix;
