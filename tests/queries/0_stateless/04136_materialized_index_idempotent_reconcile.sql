-- Tags: no-parallel
-- `SYSTEM SYNC MATERIALIZED INDEX` is idempotent: invoking it repeatedly
-- on a stable source schedule must not multiply build attempts in
-- `system.materialized_index_log`. Plan T18 / T21.

SET allow_experimental_materialized_index = 1;

DROP TABLE IF EXISTS mi_idem SYNC;
DROP TABLE IF EXISTS src_idem;

CREATE TABLE src_idem (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE MATERIALIZED INDEX mi_idem
ON src_idem (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex
SETTINGS materialized_index_sync_timeout = 1;

INSERT INTO src_idem
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(5);

-- Five back-to-back syncs. Each one performs an independent bounded wait
-- (timeout=1s here so the test stays deterministic when the assignee
-- has not yet completed Build). What we assert is that the storage and
-- the SYNC pipeline survive the repetition with no accumulated state.
SYSTEM SYNC MATERIALIZED INDEX mi_idem; -- { serverError TIMEOUT_EXCEEDED }
SYSTEM SYNC MATERIALIZED INDEX mi_idem; -- { serverError TIMEOUT_EXCEEDED }
SYSTEM SYNC MATERIALIZED INDEX mi_idem; -- { serverError TIMEOUT_EXCEEDED }
SYSTEM SYNC MATERIALIZED INDEX mi_idem; -- { serverError TIMEOUT_EXCEEDED }
SYSTEM SYNC MATERIALIZED INDEX mi_idem; -- { serverError TIMEOUT_EXCEEDED }

SELECT name FROM system.materialized_indexes
WHERE database = currentDatabase() AND name = 'mi_idem';

DROP TABLE mi_idem SYNC;
DROP TABLE src_idem;
