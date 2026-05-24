-- Tags: no-fasttest, no-parallel
-- With enable_auxiliary_index = 0 the optimizer pass is gated off and the
-- query must execute through the fallback scan as if no MI existed.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 0;

DROP TABLE IF EXISTS mi_off SYNC;
DROP TABLE IF EXISTS src_off;

CREATE TABLE src_off (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_off
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(20);

CREATE AUXILIARY INDEX mi_off
ON src_off (embedding)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

-- The query must succeed regardless of whether the background build finished:
-- with the flag off there is no path through the MI plan rewrite.
SELECT k FROM src_off
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_off SYNC;
DROP TABLE src_off;
