-- Tags: no-fasttest, no-parallel
-- no-parallel because this test asserts a profile event via query_log.
-- End-to-end correctness smoke for the DiskANN-backed ANNIndex
-- query path: build → SYSTEM SYNC → optimizer rewrite → DiskANN search →
-- reverse lookup to source row. Uses dim=32 deterministic-hash vectors
-- (non-degenerate, so DiskANN actually returns the exact neighbour) and
-- self-queries with a stored embedding, which must come back top-1 with
-- distance 0.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET force_ann_index = 'mi_smoke';
SET log_queries = 1;

DROP TABLE IF EXISTS mi_smoke SYNC;
DROP TABLE IF EXISTS src_smoke;

CREATE TABLE src_smoke (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_smoke
SELECT number, arrayMap(d -> toFloat32(cityHash64(number, d) % 1000000) / 1000000.0, range(32))
FROM numbers(4096);

CREATE REFLECTION mi_smoke
ON src_smoke (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 32,
         ann_index_sync_timeout = 60;

SYSTEM SYNC REFLECTION mi_smoke;

CREATE TEMPORARY TABLE mi_smoke_start AS SELECT now64(6) AS ts;

-- Self-query: pull k=1000's stored embedding and look it up. DiskANN on
-- non-degenerate data must return it as the top-1 with distance ~ 0.
WITH (SELECT embedding FROM src_smoke WHERE k = 1000) AS q
SELECT k, round(L2Distance(embedding, q), 6) AS d
FROM src_smoke
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS log_comment = 'mi_smoke_self_query';

WITH (SELECT embedding FROM src_smoke WHERE k = 1000) AS q
SELECT k
FROM src_smoke
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS diskann_search_num_threads = 65; -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS query_log;

-- Assert the fast path actually fired (profile event must be set).
SELECT
    log_comment,
    max(ProfileEvents['ANNIndexDiskANNSearchStarted'] > 0) AS used_diskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_smoke_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = 'mi_smoke_self_query'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_smoke SYNC;
DROP TABLE src_smoke;
