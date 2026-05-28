-- Tags: no-fasttest, no-parallel
-- no-parallel because this test asserts per-query profile events via query_log.
-- AuxiliaryIndex fast path must not rewrite reads that need FINAL or on-the-fly mutations.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS mi_final_mutations_skip SYNC;
DROP TABLE IF EXISTS src_final_mutations_skip;

CREATE TABLE src_final_mutations_skip (k UInt64, ver UInt64, embedding Array(Float32))
ENGINE = ReplacingMergeTree(ver)
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_final_mutations_skip
SELECT number, 1, [number * 1.0, 0, 0, 0]
FROM numbers(2048);

CREATE REFLECTION mi_final_mutations_skip
ON src_final_mutations_skip (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 20;

SYSTEM SYNC REFLECTION mi_final_mutations_skip;

CREATE TEMPORARY TABLE mi_final_mutations_skip_start AS SELECT now64(6) AS ts;

SELECT k FROM src_final_mutations_skip
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS log_comment = 'mi_positive_control';

SELECT k FROM src_final_mutations_skip FINAL
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS log_comment = 'mi_final_skip';

SYSTEM STOP MERGES src_final_mutations_skip;
ALTER TABLE src_final_mutations_skip
    UPDATE embedding = [10000., 0., 0., 0.]
    WHERE k = 3
    SETTINGS mutations_sync = 0;

SELECT k FROM src_final_mutations_skip
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS apply_mutations_on_fly = 1, log_comment = 'mi_mutation_skip';

SYSTEM START MERGES src_final_mutations_skip;
SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['AuxiliaryIndexDiskANNSearchStarted'] > 0) AS used_diskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_final_mutations_skip_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment IN ('mi_positive_control', 'mi_final_skip', 'mi_mutation_skip')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_final_mutations_skip SYNC;
DROP TABLE src_final_mutations_skip;
