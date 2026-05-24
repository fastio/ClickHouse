-- Verifies that materialized-index parts produced from a partitioned source
-- table carry distinct `source_partition_id`s and distinct
-- `physical_partition_id`s (one MI part per source partition), and that
-- `MergeTreePartition::getID` round-trips through every part's `partition.dat`.

SET allow_experimental_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_partsrc_idx SYNC;
DROP TABLE IF EXISTS mi_partsrc_src;

CREATE TABLE mi_partsrc_src (dt Date, k UInt64, embedding Array(Float32))
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE AUXILIARY INDEX mi_partsrc_idx
ON mi_partsrc_src (embedding)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

INSERT INTO mi_partsrc_src
SELECT toDate('2026-01-01') + (number % 2) * 31 AS dt,
       number AS k,
       [number * 1.0, number * 2.0, number * 3.0, number * 4.0] AS embedding
FROM numbers(8);

SYSTEM SYNC AUXILIARY INDEX mi_partsrc_idx;

-- The source table has two YYYYMM partitions (202601 and 202602), so the MI
-- should expose at least one Active part per source partition, each with a
-- different `source_partition_id` and `physical_partition_id`.
SELECT
    countDistinct(source_partition_id) >= 2 AS distinct_source_partitions,
    countDistinct(physical_partition_id) >= 2 AS distinct_physical_partitions,
    countIf(source_partition_id IN ('202601', '202602')) = count() AS source_ids_match_yyyymm
FROM system.auxiliary_index_parts
WHERE database = currentDatabase() AND index_name = 'mi_partsrc_idx';

-- physical_partition_id of every MI part must equal the partition_id encoded
-- in its name (the on-disk `partition.dat` round-trip through
-- `MergeTreePartition::getID`).
SELECT
    countIf(mi.physical_partition_id != p.partition_id) AS mismatched_physical_id
FROM system.auxiliary_index_parts AS mi
INNER JOIN system.parts AS p
    ON mi.part_name = p.name AND p.database = currentDatabase()
WHERE mi.database = currentDatabase() AND mi.index_name = 'mi_partsrc_idx';

DROP TABLE mi_partsrc_idx SYNC;
DROP TABLE mi_partsrc_src;
