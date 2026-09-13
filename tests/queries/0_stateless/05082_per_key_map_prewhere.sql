-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_per_key_prewhere;
DROP TABLE IF EXISTS t_per_key_idx;

SELECT 'prewhere';
CREATE TABLE t_per_key_prewhere
(
    id UInt64,
    modality String,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    index_granularity = 1;

INSERT INTO t_per_key_prewhere VALUES
    (1, 'x', {'a': 20, 'b': 1}),
    (2, 'x', {'a': 5, 'b': 2}),
    (3, 'y', {'b': 3});

SELECT id, m['a'], m['b'] FROM t_per_key_prewhere PREWHERE m['a'] > 10 ORDER BY id;
SELECT id, m['b'] FROM t_per_key_prewhere PREWHERE m['a'] IS NULL ORDER BY id;

SELECT 'skip index';
CREATE TABLE t_per_key_idx
(
    id UInt64,
    m Map(String, UInt64),
    INDEX idx m.key_a TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    index_granularity = 1;

INSERT INTO t_per_key_idx VALUES (1, {'a': 20, 'b': 1}), (2, {'a': 5, 'b': 2}), (3, {'b': 3});

SELECT id, m['a'] FROM t_per_key_idx WHERE m['a'] > 10 ORDER BY id;

SELECT
    name,
    type
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_per_key_idx';

DROP TABLE t_per_key_prewhere;
DROP TABLE t_per_key_idx;
