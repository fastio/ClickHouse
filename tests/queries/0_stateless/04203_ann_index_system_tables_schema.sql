-- Tags: no-fasttest
-- Guards the documented `system.ann_indexes` and
-- `system.ann_index_parts` schemas against accidental column renames or
-- type changes in `StorageSystemANNIndexes` /
-- `StorageSystemANNIndexParts`.

SELECT name, type
FROM system.columns
WHERE database = 'system'
    AND table = 'ann_indexes'
ORDER BY position;

SELECT name, type
FROM system.columns
WHERE database = 'system'
    AND table = 'ann_index_parts'
ORDER BY position;
