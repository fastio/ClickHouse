-- Tags: no-fasttest, no-parallel
-- WHERE filtering removes candidates after the MI search; D-09 says do not
-- refetch to top up. With top_k = 10 and a query vector that puts the lowest
-- distances near k = 42 the predicate WHERE k > 50 keeps only ~5 candidates
-- (k = 51..56 in the default overfetch window). The exact number must be
-- locked by the reference file so a future regression is caught.

SET allow_experimental_materialized_index = 1;
SET enable_materialized_index = 1;

DROP TABLE IF EXISTS mi_pv SYNC;
DROP TABLE IF EXISTS src_pv;

CREATE TABLE src_pv (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_pv
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(100);

CREATE MATERIALIZED INDEX mi_pv
ON src_pv (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex
SETTINGS materialized_index_sync_timeout = 1;

SELECT k FROM src_pv
WHERE k > 50
ORDER BY L2Distance(embedding, [42.0, 0, 0, 0])
LIMIT 10;

DROP TABLE mi_pv SYNC;
DROP TABLE src_pv;
