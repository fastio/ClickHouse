-- Tags: no-fasttest, no-parallel
-- With parallel replicas active the MI optimizer must early-return so the
-- ReadHints channel is not used (it cannot reach the followers).

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_pr SYNC;
DROP TABLE IF EXISTS src_pr;

CREATE TABLE src_pr (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_pr
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10);

CREATE AUXILIARY INDEX mi_pr
ON src_pr (embedding)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

SELECT k FROM src_pr
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5
SETTINGS parallel_replicas_local_plan = 1, max_parallel_replicas = 2,
         parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE mi_pr SYNC;
DROP TABLE src_pr;
