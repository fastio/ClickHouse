-- Tags: no-fasttest, no-parallel
-- auxiliary_index_overfetch_factor defaults to 4: candidate_limit = top_k * 4.
-- The overfetched candidates still produce the correct top-10 ranking regardless
-- of whether the MI fast path or the fallback scan executes the query.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_of_def SYNC;
DROP TABLE IF EXISTS src_of_def;

CREATE TABLE src_of_def (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_of_def
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(100);

CREATE AUXILIARY INDEX mi_of_def
ON src_of_def (embedding)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

SELECT k FROM src_of_def
ORDER BY L2Distance(embedding, [41.7, 0, 0, 0])
LIMIT 10;

DROP TABLE mi_of_def SYNC;
DROP TABLE src_of_def;
