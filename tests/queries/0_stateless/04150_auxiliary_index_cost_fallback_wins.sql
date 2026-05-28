-- Tags: no-fasttest, no-parallel
-- 10-row source: the algorithm cost (100*k = 500) plus the verify cost is
-- larger than the full-scan cost (10 rows). The cost block returns nullopt
-- and the plan stays untouched (no Union step). The query result is still
-- correct because the fallback scan computes the same ranking. This case is
-- distinct from 04143 (match declined): here the algorithm matches but the
-- framework prefers the full scan.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_small SYNC;
DROP TABLE IF EXISTS src_small;

CREATE TABLE src_small (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_small
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10);

CREATE REFLECTION mi_small
ON src_small (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

SELECT k FROM src_small
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_small SYNC;
DROP TABLE src_small;
