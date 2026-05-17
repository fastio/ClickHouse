"""
Dataset registry and loader for the ANN benchmark harness.

A `DatasetSpec` is purely metadata (dim, metric, HDF5 path, schema row
counts). Loading is delegated to the existing `hdf5_to_rowbinary.py`
streamer shipped with the ann_sift1m benchmark infra — invoking it as a
subprocess avoids re-implementing the HDF5-to-RowBinary path.

The harness only requires three tables in the target database, with the
schema the streamer expects:
  sift_base  (id UInt64, v Array(Float32))
  sift_query (id UInt32, v Array(Float32))
  sift_gt    (query_id UInt32, neighbors Array(UInt32))
"""
from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict


log = logging.getLogger(__name__)


INIT_SQL = """
DROP TABLE IF EXISTS sift_base SYNC;
DROP TABLE IF EXISTS sift_query SYNC;
DROP TABLE IF EXISTS sift_gt SYNC;

CREATE TABLE sift_base (id UInt64, v Array(Float32))
ENGINE = MergeTree ORDER BY id
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE sift_query (id UInt32, v Array(Float32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE sift_gt (query_id UInt32, neighbors Array(UInt32)) ENGINE = MergeTree ORDER BY query_id;
"""


@dataclass
class DatasetSpec:
    name: str
    dim: int
    metric: str            # "L2" | "cosine"
    hdf5_path: Path        # absolute path to the HDF5 file
    converter_path: Path   # absolute path to hdf5_to_rowbinary.py
    base_count: int        # nrows in `train`
    query_count: int       # nrows in `test`


SIFT1M = DatasetSpec(
    name="sift1m",
    dim=128,
    metric="L2",
    hdf5_path=Path("/data/data/sift-128-euclidean.hdf5"),
    converter_path=Path("/data/hdf5_to_rowbinary.py"),
    base_count=1_000_000,
    query_count=10_000,
)


DATASETS: Dict[str, DatasetSpec] = {
    SIFT1M.name: SIFT1M,
}


def load_dataset(server, dataset: DatasetSpec) -> None:
    """Recreate sift_base/sift_query/sift_gt and stream the HDF5 into them."""
    if not dataset.hdf5_path.is_file():
        raise FileNotFoundError(
            f"dataset HDF5 missing at {dataset.hdf5_path} — mount the dataset directory into the container"
        )
    if not dataset.converter_path.is_file():
        raise FileNotFoundError(
            f"hdf5_to_rowbinary.py missing at {dataset.converter_path}"
        )
    log.info("init schema for dataset %s", dataset.name)
    server.query(INIT_SQL, multiquery=True)

    for schema, table in (("base", "sift_base"), ("query", "sift_query"), ("gt", "sift_gt")):
        log.info("streaming HDF5 '%s' -> %s", schema, table)
        with subprocess.Popen(
            ["python3", str(dataset.converter_path), str(dataset.hdf5_path),
             "--schema", schema],
            stdout=subprocess.PIPE,
        ) as conv:
            argv = server.handle.client_argv() + ["-q", f"INSERT INTO {table} FORMAT RowBinary"]
            client = subprocess.Popen(argv, stdin=conv.stdout)
            conv.stdout.close()
            rc = client.wait()
            conv.wait()
            if rc != 0 or conv.returncode != 0:
                raise RuntimeError(
                    f"load {table}: converter rc={conv.returncode} client rc={rc}"
                )

    counts = server.query("SELECT count() FROM sift_base").strip()
    if int(counts) != dataset.base_count:
        raise RuntimeError(f"sift_base row count {counts} != expected {dataset.base_count}")
