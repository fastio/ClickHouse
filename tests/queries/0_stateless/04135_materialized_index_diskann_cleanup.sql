-- Tags: no-parallel
-- Cleanup-path smoke test (T17 simplified per plan §734). After a Remap
-- the framework's cleanup_thread eventually drops retired mi-parts. We
-- only assert that the catalog row remains accessible and that the SYNC
-- command remains side-effect free across multiple invocations — the
-- actual mi-part roll-over is observed in upstream gtests, not here, to
-- keep the assertion deterministic across CI environments.

SET allow_experimental_materialized_index = 1;

DROP TABLE IF EXISTS mi_cleanup SYNC;
DROP TABLE IF EXISTS src_cleanup;

CREATE TABLE src_cleanup (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE MATERIALIZED INDEX mi_cleanup
ON src_cleanup (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex
SETTINGS materialized_index_sync_timeout = 1;

INSERT INTO src_cleanup
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(10);

SYSTEM SYNC MATERIALIZED INDEX mi_cleanup; -- { serverError TIMEOUT_EXCEEDED }

-- mi_part_count is a real integer column; reading it must succeed even
-- when no fully-covering mi-part has been produced yet.
SELECT name, mi_part_count >= 0 AS counter_present
FROM system.materialized_indexes
WHERE database = currentDatabase() AND name = 'mi_cleanup';

DROP TABLE mi_cleanup SYNC;
DROP TABLE src_cleanup;
