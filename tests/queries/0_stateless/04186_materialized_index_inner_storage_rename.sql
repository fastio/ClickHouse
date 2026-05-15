-- Tags: no-fasttest
-- `Ordinary` databases force the `.inner.<name>` path instead of `.inner_id.<uuid>`.
-- This pins the non-UUID rename path for `StorageMaterializedIndex::renameInMemory`.

SET allow_experimental_materialized_index = 1;

SET send_logs_level = 'fatal';
SET allow_deprecated_database_ordinary = 1;
DROP DATABASE IF EXISTS mi_inner_rename_db;
CREATE DATABASE mi_inner_rename_db ENGINE = Ordinary;
SET send_logs_level = 'warning';

CREATE TABLE mi_inner_rename_db.src (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE MATERIALIZED INDEX mi_inner_rename_db.idx
ON mi_inner_rename_db.src (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex;

SELECT count()
FROM system.tables
WHERE database = 'mi_inner_rename_db' AND name = '.inner.idx';

RENAME TABLE mi_inner_rename_db.idx TO mi_inner_rename_db.idx2;

SELECT
    countIf(name = '.inner.idx') AS old_inner_name,
    countIf(name = '.inner.idx2') AS new_inner_name
FROM system.tables
WHERE database = 'mi_inner_rename_db' AND name IN ('.inner.idx', '.inner.idx2');

DROP TABLE mi_inner_rename_db.idx2 SYNC;

SELECT count()
FROM system.tables
WHERE database = 'mi_inner_rename_db' AND startsWith(name, '.inner');

DROP TABLE mi_inner_rename_db.src;
DROP DATABASE mi_inner_rename_db;
