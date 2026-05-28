-- Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
-- D-07: A REFLECTION must be created on a source table that has
-- assign_part_uuids = 1; otherwise CREATE should fail with BAD_ARGUMENTS so
-- callers cannot accidentally produce an index that depends on UUIDHelpers::Nil.

SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_guard_src;
DROP TABLE IF EXISTS mi_guard SYNC;

CREATE TABLE mi_guard_src (k UInt64, v Array(Float32))
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_guard
ON mi_guard_src (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError BAD_ARGUMENTS }

DROP TABLE mi_guard_src;

CREATE TABLE mi_guard_src (k UInt64, v Array(Float32))
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, assign_part_uuids = 1;

CREATE REFLECTION mi_guard
ON mi_guard_src (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

SELECT 'guard ok';

DROP TABLE IF EXISTS mi_guard SYNC;
DROP TABLE IF EXISTS mi_guard_src;

