-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-sptag
-- SPANN requires Linux x86_64 binaries built with USE_SPTAG (see
-- `materialized-index` / Algorithm availability and `CREATE AUXILIARY INDEX` / spann).
-- no-fasttest: long build; no-cpu-aarch64: SPTAG is not built on ARM CI images.
-- no-parallel because this test asserts profile events via query_log.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS mi_spann_smoke SYNC;
DROP TABLE IF EXISTS mi_spann_cosine SYNC;
DROP TABLE IF EXISTS src_spann_smoke;
DROP TABLE IF EXISTS src_spann_cosine;

-- L2 path -----------------------------------------------------------------

CREATE TABLE src_spann_smoke
(
    k UInt64,
    embedding Array(Float32)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_spann_smoke
SELECT
    number,
    arrayMap(d -> if(d = 0, toFloat32(number * 1000), toFloat32(cityHash64(number, d) % 100) / 100.0), range(16))
FROM numbers(512);

CREATE AUXILIARY INDEX mi_spann_smoke
ON src_spann_smoke (embedding)
ENGINE = ANN(spann)
SETTINGS ann_metric = 'L2', ann_dimension = 16,
         auxiliary_index_sync_timeout = 60,
         auxiliary_index_build_min_rows = 1,
         auxiliary_index_build_min_parts = 1;

SYSTEM SYNC AUXILIARY INDEX mi_spann_smoke;

CREATE TEMPORARY TABLE mi_spann_start AS SELECT now64(6) AS ts;

SET force_auxiliary_index = 'mi_spann_smoke';

-- Self-query: the nearest neighbour of row k=100 must be itself (distance 0).
WITH (SELECT embedding FROM src_spann_smoke WHERE k = 100) AS q
SELECT k, round(L2Distance(embedding, q), 6) AS d
FROM src_spann_smoke
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS log_comment = 'mi_spann_self_query';

-- Non-self query: probe with the row-250 embedding shifted; row 250 must remain
-- among the top-3 nearest. Validates the index returns real ANN candidates,
-- not just exact matches.
WITH arrayMap(x -> x + toFloat32(0.001), (SELECT embedding FROM src_spann_smoke WHERE k = 250)) AS q
SELECT countIf(k = 250) AS hit_top3
FROM
(
    SELECT k
    FROM src_spann_smoke
    ORDER BY L2Distance(embedding, q)
    LIMIT 3
)
SETTINGS log_comment = 'mi_spann_neighbour_query';

-- Remap path: after source parts merge, the new auxiliary-index part must keep
-- the directory-shaped `algorithm_private_spann` payload.
INSERT INTO src_spann_smoke
SELECT
    number,
    arrayMap(d -> if(d = 0, toFloat32(number * 1000), toFloat32(cityHash64(number, d) % 100) / 100.0), range(16))
FROM numbers(512, 128);

SYSTEM SYNC AUXILIARY INDEX mi_spann_smoke;
OPTIMIZE TABLE src_spann_smoke FINAL;
SYSTEM SYNC AUXILIARY INDEX mi_spann_smoke;

WITH (SELECT embedding FROM src_spann_smoke WHERE k = 100) AS q
SELECT k, round(L2Distance(embedding, q), 6) AS d
FROM src_spann_smoke
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS log_comment = 'mi_spann_after_remap';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['AuxiliaryIndexSPANNSearchStarted'] > 0) AS used_spann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_spann_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment IN ('mi_spann_self_query', 'mi_spann_neighbour_query', 'mi_spann_after_remap')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_spann_smoke SYNC;
DROP TABLE src_spann_smoke;

-- Cosine path -------------------------------------------------------------

CREATE TABLE src_spann_cosine
(
    k UInt64,
    embedding Array(Float32)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

-- Embeddings drawn from a deterministic pseudo-random source. We do *not*
-- normalise on the source side — SPANN's cosine path normalises internally
-- and the exact-distance fallback (`__materializedIndexSPANNDistance`) uses
-- the standard `1 - dot/(|a||b|)` formula that matches `cosineDistance`.
INSERT INTO src_spann_cosine
SELECT
    number,
    arrayMap(d -> (toFloat32(cityHash64(number * 31, d) % 1000) - 500.0) / 500.0, range(16))
FROM numbers(512);

CREATE AUXILIARY INDEX mi_spann_cosine
ON src_spann_cosine (embedding)
ENGINE = ANN(spann)
SETTINGS ann_metric = 'cosine', ann_dimension = 16,
         auxiliary_index_sync_timeout = 60,
         auxiliary_index_build_min_rows = 1,
         auxiliary_index_build_min_parts = 1;

SYSTEM SYNC AUXILIARY INDEX mi_spann_cosine;

CREATE TEMPORARY TABLE mi_spann_cosine_start AS SELECT now64(6) AS ts;

SET force_auxiliary_index = 'mi_spann_cosine';

-- Cosine self-query: distance to itself must round to 0.
WITH (SELECT embedding FROM src_spann_cosine WHERE k = 333) AS q
SELECT k, round(cosineDistance(embedding, q), 6) AS d
FROM src_spann_cosine
ORDER BY cosineDistance(embedding, q)
LIMIT 1
SETTINGS log_comment = 'mi_spann_cosine_self_query';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['AuxiliaryIndexSPANNSearchStarted'] > 0) AS used_spann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_spann_cosine_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = 'mi_spann_cosine_self_query'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_spann_cosine SYNC;
DROP TABLE src_spann_cosine;
