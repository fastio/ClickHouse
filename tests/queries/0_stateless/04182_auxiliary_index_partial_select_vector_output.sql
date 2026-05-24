-- Tags: no-fasttest, no-parallel
-- no-parallel because this test asserts per-query profile events via query_log.
-- Partial coverage must keep the vector column in both Union branches when
-- SELECT outputs depend on it.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;
SET force_auxiliary_index = 'mi_partial_select_vector';
SET log_queries = 1;

DROP TABLE IF EXISTS mi_partial_select_vector SYNC;
DROP TABLE IF EXISTS src_partial_select_vector;

CREATE TABLE src_partial_select_vector (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_partial_select_vector
SELECT number, arrayMap(d -> toFloat32(cityHash64(number, d) % 1000000) / 1000000.0, range(32))
FROM numbers(4096);

CREATE AUXILIARY INDEX mi_partial_select_vector
ON src_partial_select_vector (embedding)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 32,
         auxiliary_index_sync_timeout = 60;

SYSTEM SYNC AUXILIARY INDEX mi_partial_select_vector;

INSERT INTO src_partial_select_vector
SELECT 100000 + number, arrayMap(d -> toFloat32(cityHash64(100000 + number, d) % 1000000) / 1000000.0, range(32))
FROM numbers(16);

CREATE TEMPORARY TABLE mi_partial_select_vector_start AS SELECT now64(6) AS ts;

WITH (SELECT embedding FROM src_partial_select_vector WHERE k = 100000) AS q
SELECT length(embedding) AS dim, isFinite(L2Distance(embedding, q)) AS finite_distance
FROM src_partial_select_vector
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS log_comment = 'mi_partial_select_vector';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['AuxiliaryIndexDiskANNSearchStarted'] > 0) AS used_diskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_partial_select_vector_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = 'mi_partial_select_vector'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_partial_select_vector SYNC;
DROP TABLE src_partial_select_vector;
