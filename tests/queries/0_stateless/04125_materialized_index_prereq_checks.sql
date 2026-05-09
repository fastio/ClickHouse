-- Validates that CREATE MATERIALIZED INDEX rejects each of the nine
-- prerequisite violations with the expected error class. Assertions use
-- error codes, not error text, so future wording tweaks stay decoupled.

DROP TABLE IF EXISTS mi_src;
DROP TABLE IF EXISTS mi_src_plain;
DROP TABLE IF EXISTS mi_src_no_block_number;
DROP TABLE IF EXISTS mi_src_no_block_offset;
DROP TABLE IF EXISTS mi_src_ok;
DROP TABLE IF EXISTS mi_idx SYNC;
DROP TABLE IF EXISTS mi_collision;

-- 1. Source table does not exist.
CREATE MATERIALIZED INDEX mi_idx
ON mi_missing_src (v)
TYPE ann('MockAnn')
ENGINE = MaterializedIndex; -- { serverError UNKNOWN_TABLE }

-- 2. Source engine is not in the MergeTree family.
CREATE TABLE mi_src_plain (k UInt64, v Array(Float32)) ENGINE = Memory;
CREATE MATERIALIZED INDEX mi_idx
ON mi_src_plain (v)
TYPE ann('MockAnn')
ENGINE = MaterializedIndex; -- { serverError BAD_ARGUMENTS }

-- 3. Source MergeTree has _block_number disabled.
CREATE TABLE mi_src_no_block_number (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1;
CREATE MATERIALIZED INDEX mi_idx
ON mi_src_no_block_number (v)
TYPE ann('MockAnn')
ENGINE = MaterializedIndex; -- { serverError BAD_ARGUMENTS }

-- 4. Source MergeTree has _block_offset disabled.
CREATE TABLE mi_src_no_block_offset (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0;
CREATE MATERIALIZED INDEX mi_idx
ON mi_src_no_block_offset (v)
TYPE ann('MockAnn')
ENGINE = MaterializedIndex; -- { serverError BAD_ARGUMENTS }

-- Baseline source that passes checks 1 through 4.
CREATE TABLE mi_src_ok (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, assign_part_uuids = 1;

-- 5. TYPE clause missing — rejected by the parser before the validator runs.
-- Hint must sit on the same line as the parse-fail origin because ClientBase
-- only scans the first line when a multi-query statement fails to parse.
CREATE MATERIALIZED INDEX mi_idx ON mi_src_ok (v) ENGINE = MaterializedIndex; -- { clientError SYNTAX_ERROR }

-- 6. Algorithm family is not registered.
CREATE MATERIALIZED INDEX mi_idx
ON mi_src_ok (v)
TYPE nosuchfamily('Foo')
ENGINE = MaterializedIndex; -- { serverError BAD_ARGUMENTS }

-- 7. Family is registered but impl is not.
CREATE MATERIALIZED INDEX mi_idx
ON mi_src_ok (v)
TYPE ann('NoSuchImpl')
ENGINE = MaterializedIndex; -- { serverError BAD_ARGUMENTS }

-- 8. Indexed columns missing — rejected by the parser.
CREATE MATERIALIZED INDEX mi_idx ON mi_src_ok TYPE ann('MockAnn') ENGINE = MaterializedIndex; -- { clientError SYNTAX_ERROR }

-- 9. Target MATERIALIZED INDEX name collides with an existing table.
CREATE TABLE mi_collision (x UInt64) ENGINE = Memory;
CREATE MATERIALIZED INDEX mi_collision
ON mi_src_ok (v)
TYPE ann('MockAnn')
ENGINE = MaterializedIndex; -- { serverError INCORRECT_QUERY }

-- Replication mismatch: source is plain MergeTree, engine asks for replication.
CREATE MATERIALIZED INDEX mi_idx
ON mi_src_ok (v)
TYPE ann('MockAnn')
ENGINE = ReplicatedMaterializedIndex('/clickhouse/{database}/mi_idx/{uuid}', '{replica}'); -- { serverError INCORRECT_QUERY }

-- Happy path: with all prerequisites satisfied, the index is created.
CREATE MATERIALIZED INDEX mi_idx
ON mi_src_ok (v)
TYPE ann('MockAnn')
ENGINE = MaterializedIndex;

SELECT family, impl, engine FROM system.materialized_indexes
WHERE database = currentDatabase() AND name = 'mi_idx';

DROP TABLE mi_idx SYNC;
DROP TABLE mi_collision;
DROP TABLE mi_src_ok;
DROP TABLE mi_src_no_block_offset;
DROP TABLE mi_src_no_block_number;
DROP TABLE mi_src_plain;
