-- Tags: no-fasttest, no-parallel
-- Locks the "no refetch on partial verify" contract: WHERE filtering after
-- the MI search may shrink the result below LIMIT, and the engine must not
-- top it up by re-running the search. The query vector places the smallest
-- distances near k = 42, so WHERE k > 50 keeps only the ~5 candidates with
-- k = 51..56 inside the default overfetch window. A regression that adds a
-- silent refetch would change this row count and fail against .reference.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_pv SYNC;
DROP TABLE IF EXISTS src_pv;

CREATE TABLE src_pv (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_pv
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(100);

CREATE REFLECTION mi_pv
ON src_pv (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

SELECT k FROM src_pv
WHERE k > 50
ORDER BY L2Distance(embedding, [42.0, 0, 0, 0])
LIMIT 10;

DROP TABLE mi_pv SYNC;
DROP TABLE src_pv;
