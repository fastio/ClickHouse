-- Test: text_index_lazy_apply_mode
-- Verifies that lazy posting list apply mode produces the same results as materialize mode.

SET allow_experimental_text_index_lazy_apply = 1;

DROP TABLE IF EXISTS t_text_idx_lazy;

CREATE TABLE t_text_idx_lazy
(
    id UInt64,
    text String,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 128;

-- Insert enough data to create multiple segments.
INSERT INTO t_text_idx_lazy
SELECT
    number,
    concat(
        'The quick brown fox jumps over the lazy dog. ',
        'Document number ', toString(number), ' contains ',
        if(number % 3 = 0, 'alpha ', ''),
        if(number % 5 = 0, 'beta ', ''),
        if(number % 7 = 0, 'gamma ', ''),
        if(number % 11 = 0, 'delta ', ''),
        'end'
    )
FROM numbers(10000);

-- Force the index to be built with V2 format (.idx2).
OPTIMIZE TABLE t_text_idx_lazy FINAL;

-- Query 1: hasToken with materialize mode (baseline).
SELECT 'hasToken materialize';
SELECT count()
FROM t_text_idx_lazy
WHERE hasToken(text, 'alpha')
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1;

-- Query 2: hasToken with lazy mode.
SELECT 'hasToken lazy';
SELECT count()
FROM t_text_idx_lazy
WHERE hasToken(text, 'alpha')
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         query_plan_direct_read_from_text_index = 1;

-- Query 3: hasAllTokens with materialize mode.
SELECT 'hasAllTokens materialize';
SELECT count()
FROM t_text_idx_lazy
WHERE hasAllTokens(text, ['alpha', 'beta'])
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1;

-- Query 4: hasAllTokens with lazy mode.
SELECT 'hasAllTokens lazy';
SELECT count()
FROM t_text_idx_lazy
WHERE hasAllTokens(text, ['alpha', 'beta'])
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         query_plan_direct_read_from_text_index = 1;

-- Query 5: hasAnyTokens with materialize mode.
SELECT 'hasAnyTokens materialize';
SELECT count()
FROM t_text_idx_lazy
WHERE hasAnyTokens(text, ['alpha', 'beta', 'gamma'])
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1;

-- Query 6: hasAnyTokens with lazy mode.
SELECT 'hasAnyTokens lazy';
SELECT count()
FROM t_text_idx_lazy
WHERE hasAnyTokens(text, ['alpha', 'beta', 'gamma'])
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         query_plan_direct_read_from_text_index = 1;

-- Query 7: Token not in index — both modes should return 0.
SELECT 'missing token materialize';
SELECT count()
FROM t_text_idx_lazy
WHERE hasToken(text, 'nonexistent_token_xyz')
SETTINGS text_index_posting_list_apply_mode = 'materialize',
         query_plan_direct_read_from_text_index = 1;

SELECT 'missing token lazy';
SELECT count()
FROM t_text_idx_lazy
WHERE hasToken(text, 'nonexistent_token_xyz')
SETTINGS text_index_posting_list_apply_mode = 'lazy',
         query_plan_direct_read_from_text_index = 1;

DROP TABLE t_text_idx_lazy;


-- ============================================================================
-- Test 2: Multi-part scenario (no OPTIMIZE — multiple parts stay separate)
-- ============================================================================
DROP TABLE IF EXISTS t_text_idx_multipart;

CREATE TABLE t_text_idx_multipart
(
    id UInt64,
    text String,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 128;

-- Insert 3 separate parts.
INSERT INTO t_text_idx_multipart SELECT number, concat(if(number % 3 = 0, 'alpha ', ''), if(number % 5 = 0, 'beta ', ''), 'end') FROM numbers(1000);
INSERT INTO t_text_idx_multipart SELECT 1000 + number, concat(if(number % 3 = 0, 'alpha ', ''), if(number % 7 = 0, 'gamma ', ''), 'end') FROM numbers(1000);
INSERT INTO t_text_idx_multipart SELECT 2000 + number, concat(if(number % 5 = 0, 'beta ', ''), if(number % 7 = 0, 'gamma ', ''), 'end') FROM numbers(1000);

SELECT 'multipart hasToken materialize';
SELECT count() FROM t_text_idx_multipart WHERE hasToken(text, 'alpha') SETTINGS text_index_posting_list_apply_mode = 'materialize', query_plan_direct_read_from_text_index = 1;
SELECT 'multipart hasToken lazy';
SELECT count() FROM t_text_idx_multipart WHERE hasToken(text, 'alpha') SETTINGS text_index_posting_list_apply_mode = 'lazy', query_plan_direct_read_from_text_index = 1;

SELECT 'multipart hasAllTokens materialize';
SELECT count() FROM t_text_idx_multipart WHERE hasAllTokens(text, ['alpha', 'beta']) SETTINGS text_index_posting_list_apply_mode = 'materialize', query_plan_direct_read_from_text_index = 1;
SELECT 'multipart hasAllTokens lazy';
SELECT count() FROM t_text_idx_multipart WHERE hasAllTokens(text, ['alpha', 'beta']) SETTINGS text_index_posting_list_apply_mode = 'lazy', query_plan_direct_read_from_text_index = 1;

SELECT 'multipart hasAnyTokens materialize';
SELECT count() FROM t_text_idx_multipart WHERE hasAnyTokens(text, ['alpha', 'gamma']) SETTINGS text_index_posting_list_apply_mode = 'materialize', query_plan_direct_read_from_text_index = 1;
SELECT 'multipart hasAnyTokens lazy';
SELECT count() FROM t_text_idx_multipart WHERE hasAnyTokens(text, ['alpha', 'gamma']) SETTINGS text_index_posting_list_apply_mode = 'lazy', query_plan_direct_read_from_text_index = 1;

DROP TABLE t_text_idx_multipart;


-- ============================================================================
-- Test 3: Large granule with many blocks per segment
-- ============================================================================
DROP TABLE IF EXISTS t_text_idx_large;

CREATE TABLE t_text_idx_large
(
    id UInt64,
    text String,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO t_text_idx_large
SELECT number, concat(if(number % 2 = 0, 'frequent ', ''), if(number % 100 = 0, 'rare ', ''), 'base') FROM numbers(50000);

OPTIMIZE TABLE t_text_idx_large FINAL;

-- hasToken with a frequent token (high density).
SELECT 'large hasToken frequent materialize';
SELECT count() FROM t_text_idx_large WHERE hasToken(text, 'frequent') SETTINGS text_index_posting_list_apply_mode = 'materialize', query_plan_direct_read_from_text_index = 1;
SELECT 'large hasToken frequent lazy';
SELECT count() FROM t_text_idx_large WHERE hasToken(text, 'frequent') SETTINGS text_index_posting_list_apply_mode = 'lazy', query_plan_direct_read_from_text_index = 1;

-- hasAllTokens with high selectivity difference.
SELECT 'large hasAllTokens frequent+rare materialize';
SELECT count() FROM t_text_idx_large WHERE hasAllTokens(text, ['frequent', 'rare']) SETTINGS text_index_posting_list_apply_mode = 'materialize', query_plan_direct_read_from_text_index = 1;
SELECT 'large hasAllTokens frequent+rare lazy';
SELECT count() FROM t_text_idx_large WHERE hasAllTokens(text, ['frequent', 'rare']) SETTINGS text_index_posting_list_apply_mode = 'lazy', query_plan_direct_read_from_text_index = 1;

DROP TABLE t_text_idx_large;


-- ============================================================================
-- Test 4: Mixed AND/OR combination
-- ============================================================================
DROP TABLE IF EXISTS t_text_idx_mixed;

CREATE TABLE t_text_idx_mixed
(
    id UInt64,
    text String,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 128;

INSERT INTO t_text_idx_mixed
SELECT number, concat(if(number % 3 = 0, 'x ', ''), if(number % 5 = 0, 'y ', ''), if(number % 7 = 0, 'z ', ''), 'w') FROM numbers(10000);

OPTIMIZE TABLE t_text_idx_mixed FINAL;

-- Combined hasAllTokens + hasAnyTokens in one WHERE clause.
SELECT 'mixed AND+OR materialize';
SELECT count() FROM t_text_idx_mixed WHERE hasAllTokens(text, ['x', 'y']) AND hasAnyTokens(text, ['z', 'w']) SETTINGS text_index_posting_list_apply_mode = 'materialize', query_plan_direct_read_from_text_index = 1;
SELECT 'mixed AND+OR lazy';
SELECT count() FROM t_text_idx_mixed WHERE hasAllTokens(text, ['x', 'y']) AND hasAnyTokens(text, ['z', 'w']) SETTINGS text_index_posting_list_apply_mode = 'lazy', query_plan_direct_read_from_text_index = 1;

-- hasToken OR hasToken — verifies union across conditions.
SELECT 'mixed OR materialize';
SELECT count() FROM t_text_idx_mixed WHERE hasToken(text, 'x') OR hasToken(text, 'y') SETTINGS text_index_posting_list_apply_mode = 'materialize', query_plan_direct_read_from_text_index = 1;
SELECT 'mixed OR lazy';
SELECT count() FROM t_text_idx_mixed WHERE hasToken(text, 'x') OR hasToken(text, 'y') SETTINGS text_index_posting_list_apply_mode = 'lazy', query_plan_direct_read_from_text_index = 1;

DROP TABLE t_text_idx_mixed;


-- ============================================================================
-- Test 5: PREWHERE interaction
-- ============================================================================
DROP TABLE IF EXISTS t_text_idx_prewhere;

CREATE TABLE t_text_idx_prewhere
(
    id UInt64,
    text String,
    val UInt32,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 128;

INSERT INTO t_text_idx_prewhere
SELECT number, concat(if(number % 3 = 0, 'alpha ', ''), if(number % 5 = 0, 'beta ', ''), 'end'), number % 100 FROM numbers(10000);

OPTIMIZE TABLE t_text_idx_prewhere FINAL;

-- PREWHERE with text index filter, WHERE with additional non-index predicate.
SELECT 'prewhere materialize';
SELECT count() FROM t_text_idx_prewhere PREWHERE hasToken(text, 'alpha') WHERE val < 50 SETTINGS text_index_posting_list_apply_mode = 'materialize', query_plan_direct_read_from_text_index = 1;
SELECT 'prewhere lazy';
SELECT count() FROM t_text_idx_prewhere PREWHERE hasToken(text, 'alpha') WHERE val < 50 SETTINGS text_index_posting_list_apply_mode = 'lazy', query_plan_direct_read_from_text_index = 1;

-- hasAllTokens in PREWHERE.
SELECT 'prewhere hasAllTokens materialize';
SELECT count() FROM t_text_idx_prewhere PREWHERE hasAllTokens(text, ['alpha', 'beta']) WHERE val < 30 SETTINGS text_index_posting_list_apply_mode = 'materialize', query_plan_direct_read_from_text_index = 1;
SELECT 'prewhere hasAllTokens lazy';
SELECT count() FROM t_text_idx_prewhere PREWHERE hasAllTokens(text, ['alpha', 'beta']) WHERE val < 30 SETTINGS text_index_posting_list_apply_mode = 'lazy', query_plan_direct_read_from_text_index = 1;

DROP TABLE t_text_idx_prewhere;
