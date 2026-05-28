-- Tags: no-fasttest
-- Validates that CREATE REFLECTION rejects each of the nine
-- prerequisite violations with the expected error class. Assertions use
-- error codes, not error text, so future wording tweaks stay decoupled.

SET allow_experimental_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_src;
DROP TABLE IF EXISTS mi_src_plain;
DROP TABLE IF EXISTS mi_src_no_block_number;
DROP TABLE IF EXISTS mi_src_no_block_offset;
DROP TABLE IF EXISTS mi_src_ok;
DROP TABLE IF EXISTS mi_idx SYNC;
DROP TABLE IF EXISTS mi_collision;

-- 1. Source table does not exist.
CREATE REFLECTION mi_idx
ON mi_missing_src (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError UNKNOWN_TABLE }

-- 2. Source engine is not in the MergeTree family.
CREATE TABLE mi_src_plain (k UInt64, v Array(Float32)) ENGINE = Memory;
CREATE REFLECTION mi_idx
ON mi_src_plain (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError BAD_ARGUMENTS }

-- 3. Source MergeTree has _block_number disabled.
CREATE TABLE mi_src_no_block_number (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1;
CREATE REFLECTION mi_idx
ON mi_src_no_block_number (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError BAD_ARGUMENTS }

-- 4. Source MergeTree has _block_offset disabled.
CREATE TABLE mi_src_no_block_offset (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE REFLECTION mi_idx
ON mi_src_no_block_offset (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError BAD_ARGUMENTS }

-- Baseline source that passes checks 1 through 4.
CREATE TABLE mi_src_ok (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, assign_part_uuids = 1;

-- 5. Required build settings are missing.
CREATE REFLECTION mi_idx ON mi_src_ok (v) ENGINE = ANNIndex(diskann); -- { serverError BAD_ARGUMENTS }

-- 6. Algorithm is not registered.
CREATE REFLECTION mi_idx
ON mi_src_ok (v)
ENGINE = ANNIndex(nosuchalgorithm)
SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError BAD_ARGUMENTS }

-- 7. Family is registered but impl is not.
CREATE REFLECTION mi_idx
ON mi_src_ok (v)
ENGINE = ANNIndex(NoSuchImpl); -- { serverError BAD_ARGUMENTS }

-- 8. Indexed columns missing — rejected by the parser.
CREATE REFLECTION mi_idx ON mi_src_ok ENGINE = ANNIndex(diskann) SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { clientError SYNTAX_ERROR }

-- 9. Target REFLECTION name collides with an existing table.
CREATE TABLE mi_collision (x UInt64) ENGINE = Memory;
CREATE REFLECTION mi_collision
ON mi_src_ok (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError INCORRECT_QUERY }

-- Replication mismatch: source is plain MergeTree, engine asks for replication.
CREATE REFLECTION mi_idx
ON mi_src_ok (v)
ENGINE = ReplicatedANNIndex(diskann, '/clickhouse/{database}/mi_idx/{uuid}', '{replica}')
SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError INCORRECT_QUERY }

-- Happy path: with all prerequisites satisfied, the index is created.
CREATE REFLECTION mi_idx
ON mi_src_ok (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

SELECT family, impl, engine FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_idx';

DROP TABLE mi_idx SYNC;
DROP TABLE mi_collision;
DROP TABLE mi_src_ok;
DROP TABLE mi_src_no_block_offset;
DROP TABLE mi_src_no_block_number;
DROP TABLE mi_src_plain;
