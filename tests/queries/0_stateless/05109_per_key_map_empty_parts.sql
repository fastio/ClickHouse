DROP TABLE IF EXISTS t_per_key_empty_target;
DROP TABLE IF EXISTS t_per_key_empty_moved;
DROP TABLE IF EXISTS t_per_key_empty_source;

CREATE TABLE t_per_key_empty_source
(
    p UInt8,
    id UInt64,
    strings Map(String, LowCardinality(Nullable(String))),
    integers Map(String, Int64),
    floats Map(String, Float64),
    arrays Map(String, Array(String))
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', remove_empty_parts = 0;
CREATE TABLE t_per_key_empty_moved AS t_per_key_empty_source;
CREATE TABLE t_per_key_empty_target AS t_per_key_empty_source;
SYSTEM STOP MERGES t_per_key_empty_source;
SYSTEM STOP MERGES t_per_key_empty_moved;
SYSTEM STOP MERGES t_per_key_empty_target;

-- Keep the zero-row parts created by `MOVE PARTITION` for the subsequent merges.
INSERT INTO t_per_key_empty_source VALUES (1, 1, {'a':'one'}, {'a':1}, {'a':1.5}, {'a':['one']});
ALTER TABLE t_per_key_empty_source MOVE PARTITION 1 TO TABLE t_per_key_empty_moved;
INSERT INTO t_per_key_empty_source VALUES (2, 1, {'a':'one'}, {'a':1}, {'a':1.5}, {'a':['one']});
ALTER TABLE t_per_key_empty_source MOVE PARTITION 2 TO TABLE t_per_key_empty_moved;
INSERT INTO t_per_key_empty_source VALUES (3, 1, {'a':'one'}, {'a':1}, {'a':1.5}, {'a':['one']});
ALTER TABLE t_per_key_empty_source MOVE PARTITION 3 TO TABLE t_per_key_empty_moved;
INSERT INTO t_per_key_empty_source VALUES (4, 1, {'a':'one'}, {'a':1}, {'a':1.5}, {'a':['one']});
ALTER TABLE t_per_key_empty_source MOVE PARTITION 4 TO TABLE t_per_key_empty_moved;

-- Empty part first, between nonempty parts, and last.
ALTER TABLE t_per_key_empty_target ATTACH PARTITION 1 FROM t_per_key_empty_source;
INSERT INTO t_per_key_empty_target VALUES (1, 11, {'a':'one'}, {'a':1}, {'a':1.5}, {'a':['one']});
INSERT INTO t_per_key_empty_target VALUES (2, 21, {'a':'one'}, {'a':1}, {'a':1.5}, {'a':['one']});
ALTER TABLE t_per_key_empty_target ATTACH PARTITION 2 FROM t_per_key_empty_source;
INSERT INTO t_per_key_empty_target VALUES (2, 22, {'b':'two'}, {'b':2}, {'b':2.5}, {'b':['two']});
INSERT INTO t_per_key_empty_target VALUES (3, 31, {'a':'one'}, {'a':1}, {'a':1.5}, {'a':['one']});
ALTER TABLE t_per_key_empty_target ATTACH PARTITION 3 FROM t_per_key_empty_source;

-- An all-empty merge and nonempty parts whose `Map` values are empty.
ALTER TABLE t_per_key_empty_target ATTACH PARTITION 4 FROM t_per_key_empty_source;
ALTER TABLE t_per_key_empty_target ATTACH PARTITION 4 FROM t_per_key_empty_source;
INSERT INTO t_per_key_empty_target VALUES (5, 51, {}, {}, {}, {});
INSERT INTO t_per_key_empty_target VALUES (5, 52, {}, {}, {}, {});

SELECT partition, countIf(rows = 0), countIf(rows > 0) FROM system.parts WHERE database = currentDatabase() AND table = 't_per_key_empty_target' AND active GROUP BY partition ORDER BY partition;
SYSTEM START MERGES t_per_key_empty_target;
OPTIMIZE TABLE t_per_key_empty_target FINAL;
SELECT p, id, strings, integers, floats, arrays FROM t_per_key_empty_target ORDER BY p, id;
SELECT count() FROM t_per_key_empty_moved;

DROP TABLE t_per_key_empty_target;
DROP TABLE t_per_key_empty_moved;
DROP TABLE t_per_key_empty_source;
