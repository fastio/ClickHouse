-- Tags: no-fasttest, no-parallel
-- no-parallel because this test asserts per-query profile events via query_log.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS mi_path_events SYNC;
DROP TABLE IF EXISTS src_path_events;

CREATE TABLE src_path_events (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_path_events
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(1024);

CREATE REFLECTION mi_path_events
ON src_path_events (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 20;

SYSTEM SYNC REFLECTION mi_path_events;

CREATE TEMPORARY TABLE mi_path_events_start AS SELECT now64(6) AS ts;

SELECT k FROM src_path_events
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS log_comment = 'mi_path_events_fast';

SELECT k FROM src_path_events
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS enable_auxiliary_index = 0, log_comment = 'mi_path_events_disabled';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['AuxiliaryIndexDiskANNSearchStarted'] > 0) AS used_diskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_path_events_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment IN ('mi_path_events_fast', 'mi_path_events_disabled')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_path_events SYNC;
DROP TABLE src_path_events;
