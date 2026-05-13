-- Tags: no-fasttest, no-parallel
-- Result smoke test for the vector_similarity_index + MaterializedIndex
-- coexistence case with force_using_materialized_index = 1. Event-based tests
-- pin the actual yield/preempt behavior.

SET allow_experimental_materialized_index = 1;
SET enable_materialized_index = 1;
SET force_using_materialized_index = 1;

DROP TABLE IF EXISTS mi_pre SYNC;
DROP TABLE IF EXISTS src_pre;

CREATE TABLE src_pre (
    k UInt64,
    embedding Array(Float32),
    INDEX vsi embedding TYPE vector_similarity('hnsw', 'L2Distance', 4))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_pre
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(20);

CREATE MATERIALIZED INDEX mi_pre
ON src_pre (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex
SETTINGS materialized_index_sync_timeout = 1;

SELECT k FROM src_pre
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_pre SYNC;
DROP TABLE src_pre;
