-- Tags: no-parallel
-- After a source-side delta_out (mutation drops rows belonging to specific
-- source parts), `SYSTEM SYNC REFLECTION` must again perform a
-- bounded wait. Build is deterministically rejected by the input-row limit,
-- so the test does not depend on machine speed.

SET allow_experimental_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_remap SYNC;
DROP TABLE IF EXISTS src_remap;

CREATE TABLE src_remap (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_remap
ON src_remap (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1, auxiliary_index_task_max_input_rows = 1;

INSERT INTO src_remap
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(20);

INSERT INTO src_remap
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(20, 20);

SYSTEM SYNC REFLECTION mi_remap; -- { serverError TIMEOUT_EXCEEDED }

-- Trigger source UUID churn: a mutation rewrites parts, producing new
-- UUIDs and retiring the originals. The materialized-index storage observes
-- `delta_in` / `delta_out` on the next reconciler tick.
ALTER TABLE src_remap DELETE WHERE k > 25 SETTINGS mutations_sync = 1;

SYSTEM SYNC REFLECTION mi_remap; -- { serverError TIMEOUT_EXCEEDED }

SELECT name FROM system.auxiliary_indexes WHERE database = currentDatabase() AND name = 'mi_remap';

DROP TABLE mi_remap SYNC;
DROP TABLE src_remap;
