-- Tags: no-fasttest

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET force_ann_index = 'mi_lazy_hint';
SET log_queries = 1;

DROP TABLE IF EXISTS mi_lazy_hint SYNC;
DROP TABLE IF EXISTS src_lazy_hint;

CREATE TABLE src_lazy_hint (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_lazy_hint
SELECT number, arrayMap(d -> toFloat32(cityHash64(number, d) % 1000000) / 1000000.0, range(32))
FROM numbers(4096);

CREATE REFLECTION mi_lazy_hint
ON src_lazy_hint (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 32,
         ann_index_sync_timeout = 60;

SYSTEM SYNC REFLECTION mi_lazy_hint;

CREATE TEMPORARY TABLE mi_lazy_hint_start AS SELECT now64(6) AS ts;

WITH (SELECT embedding FROM src_lazy_hint WHERE k = 1000) AS q
SELECT k, round(L2Distance(embedding, q), 6) AS d
FROM src_lazy_hint
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS
    allow_experimental_analyzer = 1,
    query_plan_optimize_lazy_materialization = 1,
    ann_index_require_match = 1,
    log_comment = 'mi_lazy_hint_query';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['ANNIndexDiskANNSearchStarted'] > 0) AS used_diskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_lazy_hint_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = 'mi_lazy_hint_query'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_lazy_hint SYNC;
DROP TABLE src_lazy_hint;
