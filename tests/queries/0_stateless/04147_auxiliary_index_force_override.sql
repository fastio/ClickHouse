-- Tags: no-fasttest, no-parallel
-- force_auxiliary_index pins the optimizer to a specific MI even when its
-- cost is worse than another candidate (and even when its cost would lose to
-- the fallback full-scan). The result is still correct because both paths
-- produce the same top-K.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_force_a SYNC;
DROP TABLE IF EXISTS mi_force_b SYNC;
DROP TABLE IF EXISTS src_force;

CREATE TABLE src_force (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_force
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10000);

CREATE REFLECTION mi_force_a
ON src_force (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

CREATE REFLECTION mi_force_b
ON src_force (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

SET force_auxiliary_index = 'mi_force_b';

SELECT k FROM src_force
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_force_a SYNC;
DROP TABLE mi_force_b SYNC;
DROP TABLE src_force;
