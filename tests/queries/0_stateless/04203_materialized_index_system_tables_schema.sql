-- Tags: no-fasttest
-- Guards the documented `system.materialized_indexes` and
-- `system.materialized_index_parts` schemas against accidental column renames or
-- type changes in `StorageSystemMaterializedIndexes` /
-- `StorageSystemMaterializedIndexParts`.

SELECT name, type
FROM system.columns
WHERE database = 'system'
    AND table = 'materialized_indexes'
ORDER BY position;

SELECT name, type
FROM system.columns
WHERE database = 'system'
    AND table = 'materialized_index_parts'
ORDER BY position;
