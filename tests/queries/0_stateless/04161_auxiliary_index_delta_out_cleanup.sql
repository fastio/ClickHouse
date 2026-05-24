-- Tags: no-parallel
-- A source-side DROP PARTITION produces delta_out without delta_in. Build is
-- deterministically rejected by the input-row limit, so the timeout assertion
-- does not depend on background completion timing.

SET allow_experimental_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_delta_out_cleanup SYNC;
DROP TABLE IF EXISTS src_delta_out_cleanup;

CREATE TABLE src_delta_out_cleanup (p UInt64, k UInt64, embedding Array(Float32))
ENGINE = MergeTree
PARTITION BY p
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE AUXILIARY INDEX mi_delta_out_cleanup
ON src_delta_out_cleanup (embedding)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1, auxiliary_index_task_max_input_rows = 1;

INSERT INTO src_delta_out_cleanup
SELECT 0, number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(10);

INSERT INTO src_delta_out_cleanup
SELECT 1, number + 10, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(10);

SYSTEM SYNC AUXILIARY INDEX mi_delta_out_cleanup; -- { serverError TIMEOUT_EXCEEDED }

ALTER TABLE src_delta_out_cleanup DROP PARTITION 0;

SYSTEM SYNC AUXILIARY INDEX mi_delta_out_cleanup; -- { serverError TIMEOUT_EXCEEDED }

SELECT name FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_delta_out_cleanup';

DROP TABLE mi_delta_out_cleanup SYNC;
DROP TABLE src_delta_out_cleanup;
