-- Tags: no-fasttest

SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_system_tables_idx SYNC;
DROP TABLE IF EXISTS mi_system_tables_src;

CREATE TABLE mi_system_tables_src (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_system_tables_idx
ON mi_system_tables_src (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

SELECT metric, dimension, uncovered_rows, pending_task_count
FROM system.reflection_ann_indexes
WHERE database = currentDatabase() AND reflection_name = 'mi_system_tables_idx';

SELECT count()
FROM system.reflection_jobs
WHERE database = currentDatabase() AND reflection_name = 'mi_system_tables_idx';

DROP TABLE mi_system_tables_idx SYNC;
DROP TABLE mi_system_tables_src;
