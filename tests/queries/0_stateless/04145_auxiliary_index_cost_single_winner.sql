-- Tags: no-fasttest, no-parallel
-- Single MI on a large source: algorithm cost (100*k = 500) plus verify cost
-- (k = 5) is well below the fallback full-scan (10000 rows). The optimizer
-- should choose the MI path whenever the background build has committed,
-- and the fallback scan otherwise. Both paths produce the same top-5 result.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_one SYNC;
DROP TABLE IF EXISTS src_one;

CREATE TABLE src_one (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_one
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10000);

CREATE REFLECTION mi_one
ON src_one (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

SELECT k FROM src_one
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_one SYNC;
DROP TABLE src_one;
