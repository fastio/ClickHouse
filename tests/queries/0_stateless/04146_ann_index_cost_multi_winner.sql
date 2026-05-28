-- Tags: no-fasttest, no-parallel
-- Two MIs on the same source: both match the query (same algorithm, same k)
-- so their algorithm-side costs tie; the framework breaks the tie by MI name
-- lexicographic order. The query path is identical under either MI, so this
-- test asserts result correctness; the tie-break rule itself is covered by
-- the CostTie.StableSelection gtest.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_multi_a SYNC;
DROP TABLE IF EXISTS mi_multi_b SYNC;
DROP TABLE IF EXISTS src_multi;

CREATE TABLE src_multi (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_multi
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10000);

CREATE REFLECTION mi_multi_a
ON src_multi (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 1;

CREATE REFLECTION mi_multi_b
ON src_multi (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 1;

SELECT k FROM src_multi
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_multi_a SYNC;
DROP TABLE mi_multi_b SYNC;
DROP TABLE src_multi;
