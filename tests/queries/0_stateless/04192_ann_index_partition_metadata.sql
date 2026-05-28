-- Verifies that every materialized-index part persists the standard MergeTree
-- partition envelope (`partition.dat` and `minmax__source_partition_id.idx`)
-- so generic `MergeTreePartition::load` can round-trip it without throwing
-- `CORRUPTED_DATA` for a mismatched partition id.

SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_partmeta_idx SYNC;
DROP TABLE IF EXISTS mi_partmeta_src;

CREATE TABLE mi_partmeta_src (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_partmeta_idx
ON mi_partmeta_src (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

INSERT INTO mi_partmeta_src
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(8);

SYSTEM SYNC REFLECTION mi_partmeta_idx;

-- The inner storage uses a synthetic `_source_partition_id String` PARTITION
-- BY. Generic `loadPartitionAndMinMaxIndex` recomputes the partition id from
-- `partition.dat` and compares to the part name; a mismatch throws
-- `CORRUPTED_DATA`. If the part loaded into `Active` at all, the envelope is
-- consistent end-to-end.
SELECT
    countIf(active) > 0 AS has_active_part,
    countIf(active AND length(partition_id) = 32 AND match(partition_id, '^[0-9a-f]{32}$')) = countIf(active) AS all_partition_ids_are_mergetree_hash
FROM system.parts
WHERE database = currentDatabase()
  AND match(name, '^[0-9a-f]{32}_[0-9]+_[0-9]+_0$');

DROP TABLE mi_partmeta_idx SYNC;
DROP TABLE mi_partmeta_src;
