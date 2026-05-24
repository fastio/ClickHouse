-- Tags: no-fasttest
-- Guards the documented `system.auxiliary_indexes` and
-- `system.auxiliary_index_parts` schemas against accidental column renames or
-- type changes in `StorageSystemAuxiliaryIndexes` /
-- `StorageSystemAuxiliaryIndexParts`.

SELECT name, type
FROM system.columns
WHERE database = 'system'
    AND table = 'auxiliary_indexes'
ORDER BY position;

SELECT name, type
FROM system.columns
WHERE database = 'system'
    AND table = 'auxiliary_index_parts'
ORDER BY position;
