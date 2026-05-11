-- Tags: no-fasttest, no-parallel
-- An explicit PREWHERE clause sets PrewhereInfo on the RFMT step; the MI
-- optimizer must early-return because the hint pipeline is not wired to
-- compose with PREWHERE.

SET allow_experimental_materialized_index = 1;
SET enable_materialized_index = 1;

DROP TABLE IF EXISTS mi_pw SYNC;
DROP TABLE IF EXISTS src_pw;

CREATE TABLE src_pw (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_pw
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10);

CREATE MATERIALIZED INDEX mi_pw
ON src_pw (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex
SETTINGS materialized_index_sync_timeout = 1;

SELECT k FROM src_pw
PREWHERE k > 0
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_pw SYNC;
DROP TABLE src_pw;
