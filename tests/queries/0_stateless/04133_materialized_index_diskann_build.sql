-- Tags: no-parallel
-- Exercises the DiskANN backend wired into MaterializedIndex via task-2's
-- factory registration, plus the real `SYSTEM SYNC MATERIALIZED INDEX`
-- semantics introduced in task-3: wait until the source table is fully covered.

SET allow_experimental_materialized_index = 1;

DROP TABLE IF EXISTS mi_diskann SYNC;
DROP TABLE IF EXISTS src_diskann;

CREATE TABLE src_diskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

-- Use a small dim so the build is cheap; the algorithm path is identical
-- to a 128-d production index.
CREATE MATERIALIZED INDEX mi_diskann
ON src_diskann (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex
SETTINGS materialized_index_sync_timeout = 20;

INSERT INTO src_diskann
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(50);

-- The build is intentionally tiny, so `SYSTEM SYNC` should complete and make
-- the materialized index visible through `system.materialized_indexes`.
SYSTEM SYNC MATERIALIZED INDEX mi_diskann;

SELECT name FROM system.materialized_indexes WHERE database = currentDatabase() AND name = 'mi_diskann';

DROP TABLE mi_diskann SYNC;
DROP TABLE src_diskann;
