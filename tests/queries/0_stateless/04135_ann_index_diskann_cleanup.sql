-- Tags: no-parallel
-- Cleanup-path smoke test (T17 simplified per plan §734). Build is
-- deterministically rejected by the input-row limit, so the timeout assertion
-- does not depend on background completion timing.

SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_cleanup SYNC;
DROP TABLE IF EXISTS src_cleanup;

CREATE TABLE src_cleanup (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_cleanup
ON src_cleanup (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 1, ann_index_task_max_input_rows = 1;

INSERT INTO src_cleanup
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(10);

SYSTEM SYNC REFLECTION mi_cleanup; -- { serverError TIMEOUT_EXCEEDED }

-- ann_index_part_count is a real integer column; reading it must succeed even
-- when no fully-covering materialized-index-part has been produced yet.
SELECT name, ann_index_part_count >= 0 AS counter_present
FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_cleanup';

DROP TABLE mi_cleanup SYNC;
DROP TABLE src_cleanup;
