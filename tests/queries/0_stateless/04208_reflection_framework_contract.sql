-- Tags: no-fasttest

SET allow_experimental_ann_index = 1;
SET allow_ann_index_engine_mismatch = 1;

DROP REFLECTION IF EXISTS refl_idx SYNC;
DROP TABLE IF EXISTS refl_src;

CREATE TABLE refl_src (id UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, assign_part_uuids = 1;

CREATE REFLECTION refl_idx
ON refl_src (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2',
         ann_dimension = 4,
         diskann_pruned_degree = 32,
         diskann_max_degree = 64;

SELECT family, impl, engine
FROM system.reflections
WHERE database = currentDatabase() AND name = 'refl_idx';

SELECT backlog_parts, backlog_bytes, backlog_rows
FROM system.reflections
WHERE database = currentDatabase() AND name = 'refl_idx';

SELECT family, impl, engine
FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'refl_idx';

SELECT create_table_query LIKE '%CREATE REFLECTION%' AS has_create_reflection,
       create_table_query LIKE '%ENGINE = ANNIndex(diskann)%' AS has_ann_index_engine
FROM system.tables
WHERE database = currentDatabase() AND name = 'refl_idx';

SYSTEM REFRESH REFLECTION refl_idx;
SYSTEM STOP REFLECTION BUILDS refl_idx;
SYSTEM START REFLECTION BUILDS refl_idx;
SYSTEM STOP REFLECTION REMAPS refl_idx;
SYSTEM START REFLECTION REMAPS refl_idx;
SYSTEM SYNC REFLECTION refl_idx;

SELECT 'reflection commands completed';

DROP REFLECTION refl_idx SYNC;
DROP TABLE refl_src;
