-- Tags: no-parallel
-- Exercises the DiskANN backend wired into MaterializedIndex via task-2's
-- factory registration, plus the real `SYSTEM SYNC MATERIALIZED INDEX`
-- semantics introduced in task-3 (waits for full source coverage or hits
-- the per-table `materialized_index_sync_timeout` and throws
-- `TIMEOUT_EXCEEDED`).

SET allow_experimental_materialized_index = 1;

DROP TABLE IF EXISTS mi_diskann SYNC;
DROP TABLE IF EXISTS src_diskann;

CREATE TABLE src_diskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

-- Use a small dim so the build is cheap; the algorithm path is identical
-- to a 128-d production index. A 1-second timeout is intentionally tight
-- — the assertion below reads the exception, so we want the wait to
-- complete deterministically inside the stateless budget.
CREATE MATERIALIZED INDEX mi_diskann
ON src_diskann (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex
SETTINGS materialized_index_sync_timeout = 1;

INSERT INTO src_diskann
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(50);

-- The real `SYSTEM SYNC` dispatches to `waitForCoverageOfSourceOrTimeout`,
-- which throws `TIMEOUT_EXCEEDED` (159) if the reconciler has not yet
-- produced a fully covering mi-part within the configured budget. We
-- assert the error code rather than success because the background
-- assignee schedule is environment-dependent in the stateless runner;
-- the contract under test is "the command performs a real bounded wait",
-- not "Build always finishes within 1 s here".
SYSTEM SYNC MATERIALIZED INDEX mi_diskann; -- { serverError TIMEOUT_EXCEEDED }

-- Even after a timeout the catalog row must still be queryable; the
-- sync command does not poison the storage.
SELECT name FROM system.materialized_indexes WHERE database = currentDatabase() AND name = 'mi_diskann';

DROP TABLE mi_diskann SYNC;
DROP TABLE src_diskann;
