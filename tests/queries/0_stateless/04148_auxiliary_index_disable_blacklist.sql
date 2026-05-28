-- Tags: no-fasttest, no-parallel
-- disable_auxiliary_index drops the named MI from the candidate list before
-- cost-based selection. The remaining MI is then chosen if its cost beats the
-- fallback. Both paths produce the same top-K result.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_disable_a SYNC;
DROP TABLE IF EXISTS mi_disable_b SYNC;
DROP TABLE IF EXISTS src_disable;

CREATE TABLE src_disable (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_disable
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10000);

CREATE REFLECTION mi_disable_a
ON src_disable (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

CREATE REFLECTION mi_disable_b
ON src_disable (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

SET disable_auxiliary_index = 'mi_disable_a';

SELECT k FROM src_disable
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_disable_a SYNC;
DROP TABLE mi_disable_b SYNC;
DROP TABLE src_disable;
