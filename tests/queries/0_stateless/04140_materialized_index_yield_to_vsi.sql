-- Tags: no-fasttest, no-parallel
-- When the source table carries a vector_similarity_index *and* a
-- MaterializedIndex over the same column, the MI optimizer yields whenever
-- force_using_materialized_index = 0 so the vector path stays in charge.

SET allow_experimental_materialized_index = 1;
SET enable_materialized_index = 1;
SET force_using_materialized_index = 0;

DROP TABLE IF EXISTS mi_yield SYNC;
DROP TABLE IF EXISTS src_yield;

CREATE TABLE src_yield (
    k UInt64,
    embedding Array(Float32),
    INDEX vsi embedding TYPE vector_similarity('hnsw', 'L2Distance', 4))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_yield
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(20);

CREATE MATERIALIZED INDEX mi_yield
ON src_yield (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex
SETTINGS materialized_index_sync_timeout = 1;

-- Result correctness only — both paths must produce the same 5 nearest rows.
SELECT k FROM src_yield
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_yield SYNC;
DROP TABLE src_yield;
