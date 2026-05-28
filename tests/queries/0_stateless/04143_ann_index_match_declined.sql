-- Tags: no-fasttest, no-parallel
-- When the query's reference vector dimension does not match the MI dim, the
-- algorithm's match() returns std::nullopt and the optimizer must yield to
-- the fallback scan instead of attaching empty hints.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_decl SYNC;
DROP TABLE IF EXISTS src_decl;

CREATE TABLE src_decl (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_decl
SELECT number, [number * 1.0, 0]
FROM numbers(10);

CREATE REFLECTION mi_decl
ON src_decl (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 1;

-- Reference vector is 2-dim while the MI was built for dim = 4.
SELECT k FROM src_decl
ORDER BY L2Distance(embedding, [3.7, 0])
LIMIT 5;

DROP TABLE mi_decl SYNC;
DROP TABLE src_decl;
