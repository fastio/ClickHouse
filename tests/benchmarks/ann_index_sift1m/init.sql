-- Schema for the SIFT-1M ANNIndex recall benchmark.
-- Tables match the shape of the existing ann_sift1m HDF5 -> RowBinary
-- converter (hdf5_to_rowbinary.py --schema base|query|gt).

DROP TABLE IF EXISTS sift_base SYNC;
DROP TABLE IF EXISTS sift_query;
DROP TABLE IF EXISTS sift_gt;

CREATE TABLE sift_base
(
    id UInt64,
    v Array(Float32)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE sift_query
(
    id UInt32,
    v Array(Float32)
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE sift_gt
(
    query_id UInt32,
    neighbors Array(UInt32)
)
ENGINE = MergeTree
ORDER BY query_id;
