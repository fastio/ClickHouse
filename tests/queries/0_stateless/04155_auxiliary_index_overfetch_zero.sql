-- Tags: no-fasttest, no-parallel
-- Zero overfetch factor disables the MI fast path (same handling as oversize);
-- query still returns the correct top-10.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;
SET auxiliary_index_overfetch_factor = 0;

DROP TABLE IF EXISTS mi_of_zero SYNC;
DROP TABLE IF EXISTS src_of_zero;

CREATE TABLE src_of_zero (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_of_zero
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(100);

CREATE AUXILIARY INDEX mi_of_zero
ON src_of_zero (embedding)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

SELECT k FROM src_of_zero
ORDER BY L2Distance(embedding, [41.7, 0, 0, 0])
LIMIT 10;

DROP TABLE mi_of_zero SYNC;
DROP TABLE src_of_zero;
