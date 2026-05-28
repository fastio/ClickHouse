-- Verifies `system.ann_index_parts` surfaces all expected fields,
-- with `source_partition_id` parsed from `header.json` rather than from any
-- column read.

SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_systable_idx SYNC;
DROP TABLE IF EXISTS mi_systable_src;

CREATE TABLE mi_systable_src (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_systable_idx
ON mi_systable_src (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

INSERT INTO mi_systable_src
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(8);

SYSTEM SYNC REFLECTION mi_systable_idx;

-- Every Active MI part should be listed with non-empty identity and
-- a non-empty source provenance field read from `header.json`.
SELECT
    count() > 0 AS has_rows,
    countIf(database = currentDatabase()) = count() AS db_filter_matches,
    countIf(index_name = 'mi_systable_idx') = count() AS index_name_matches,
    countIf(active = 1) = count() AS all_active,
    countIf(match(physical_partition_id, '^[0-9a-f]{32}$')) = count() AS phys_id_is_mergetree_hash,
    countIf(source_partition_id = 'all') = count() AS source_partition_id_matches_default,
    countIf(rows > 0) = count() AS rows_populated,
    countIf(bytes_on_disk > 0) = count() AS bytes_populated,
    countIf(level = 0) = count() AS all_level_zero,
    countIf(source_min_block <= source_max_block) = count() AS source_block_range_valid
FROM system.ann_index_parts
WHERE database = currentDatabase() AND index_name = 'mi_systable_idx';

-- physical_partition_id must equal the partition_id encoded in the MI part
-- name (the `MergeTreePartition::getID` round-trip).
SELECT
    countIf(mi.physical_partition_id != p.partition_id) AS mismatched_physical_id
FROM system.ann_index_parts AS mi
INNER JOIN system.parts AS p
    ON mi.part_name = p.name AND p.database = currentDatabase()
WHERE mi.database = currentDatabase() AND mi.index_name = 'mi_systable_idx';

DROP TABLE mi_systable_idx SYNC;
DROP TABLE mi_systable_src;
