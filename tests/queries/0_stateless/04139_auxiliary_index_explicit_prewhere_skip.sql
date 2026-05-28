-- Tags: no-fasttest, no-parallel
-- An explicit PREWHERE clause sets PrewhereInfo on the RFMT step; a predicate
-- that does not depend on the vector column can compose with the MI hint path.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_pw SYNC;
DROP TABLE IF EXISTS src_pw;

CREATE TABLE src_pw (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_pw
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10);

CREATE REFLECTION mi_pw
ON src_pw (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

SELECT k FROM src_pw
PREWHERE k > 0
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_pw SYNC;
DROP TABLE src_pw;
