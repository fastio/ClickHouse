"""
Dataset registry and loader for the ANN benchmark harness.

A `DatasetSpec` is purely metadata: dim, metric, on-disk format (HDF5 or
Big-ANN bin), and the file paths that hold base / query / ground-truth.
Loading is delegated to one of two streamers shipped under the dataset
mount (default `/data` inside the container):

  * `hdf5_to_rowbinary.py` — ann-benchmarks single-HDF5 packaging
        (`train`, `test`, `neighbors`, `distances`)
  * `bin_to_rowbinary.py`  — Big-ANN-Benchmarks (NeurIPS '21+) three-file
        packaging (.fbin/.u8bin/.i8bin/.ibin)

Both streamers emit ClickHouse RowBinary for the same three target tables:
  sift_base  (id UInt64, v Array(Float32))
  sift_query (id UInt32, v Array(Float32))
  sift_gt    (query_id UInt32, neighbors Array(UInt32))

Table names are sift_* for backwards compatibility with the SIFT-1M-era
records in results.jsonl; the data they hold is arbitrary now.
"""
from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional


log = logging.getLogger(__name__)


# Default granularity for sift_base. Override per-run via load_dataset(..., index_granularity=N).
# ANN groups are granule-bound, so this knob directly affects per-granule index payload size and
# the per-search granule fan-out. Smaller granules = more, smaller ANN groups; larger granules =
# fewer, fatter groups. See bench.py --index-granularity.
DEFAULT_BASE_INDEX_GRANULARITY = 1024


def init_sql(index_granularity: int = DEFAULT_BASE_INDEX_GRANULARITY) -> str:
    if not isinstance(index_granularity, int) or index_granularity <= 0:
        raise ValueError(f"index_granularity must be a positive int, got {index_granularity!r}")
    return f"""
DROP TABLE IF EXISTS sift_base SYNC;
DROP TABLE IF EXISTS sift_query SYNC;
DROP TABLE IF EXISTS sift_gt SYNC;

CREATE TABLE sift_base (id UInt64, v Array(Float32))
ENGINE = MergeTree ORDER BY id
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1, index_granularity = {index_granularity};

CREATE TABLE sift_query (id UInt32, v Array(Float32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE sift_gt (query_id UInt32, neighbors Array(UInt32)) ENGINE = MergeTree ORDER BY query_id;
"""


@dataclass
class DatasetSpec:
    name: str
    dim: int
    metric: str            # "L2" | "cosine"
    base_count: int        # rows in the base set actually loaded
    query_count: int       # rows in the query set actually loaded
    format: str            # "hdf5" | "bin"

    # HDF5 packaging: one file holds train / test / neighbors / distances.
    hdf5_path: Optional[Path] = None

    # Big-ANN packaging: three separate files. Element dtype is independent
    # per file (queries can be float32 even when base is uint8/int8 in some
    # datasets, though in practice they match).
    base_path: Optional[Path] = None
    query_path: Optional[Path] = None
    gt_path: Optional[Path] = None
    base_dtype: str = "float32"     # "float32" | "uint8" | "int8"
    query_dtype: str = "float32"

    # For Big-ANN-style files only: if non-zero, read only the first N rows
    # of the base file. The corresponding GT file must already match this N.
    base_rows_limit: int = 0

    # Converter scripts live next to the dataset mount.
    hdf5_converter: Path = field(default_factory=lambda: Path("/data/hdf5_to_rowbinary.py"))
    bin_converter: Path = field(default_factory=lambda: Path("/data/bin_to_rowbinary.py"))


# ---- ann-benchmarks (HDF5) -------------------------------------------------

SIFT1M = DatasetSpec(
    name="sift1m",
    dim=128,
    metric="L2",
    base_count=1_000_000,
    query_count=10_000,
    format="hdf5",
    hdf5_path=Path("/data/data/sift-128-euclidean.hdf5"),
)

# 1M × 960 float32 L2. The classic "high-dim" ann-benchmarks test —
# distance ratios are noisier than SIFT, so any PQ chunking too coarse
# collapses recall. Use this to surface high-dim regressions.
GIST_960 = DatasetSpec(
    name="gist-960",
    dim=960,
    metric="L2",
    base_count=1_000_000,
    query_count=1_000,
    format="hdf5",
    hdf5_path=Path("/data/data/gist-960-euclidean.hdf5"),
)

# 1.18M × 100 float32 cosine. ann-benchmarks 'angular' = vectors pre-
# normalised to unit length, so cosine on the index lines up with the
# 'neighbors' GT computed under angular distance. GloVe is a known-hard
# ANN dataset (NLP embeddings have uneven density), so this is the
# 'recall ceiling' stressor in the suite.
GLOVE_100 = DatasetSpec(
    name="glove-100",
    dim=100,
    metric="cosine",
    base_count=1_183_514,
    query_count=10_000,
    format="hdf5",
    hdf5_path=Path("/data/data/glove-100-angular.hdf5"),
)


# ---- Big-ANN-Benchmarks (NeurIPS '21+, bin) --------------------------------
#
# Layout under the dataset mount (the operator is responsible for placing the
# files there; see README for download URLs):
#
#   /data/data/deep1B/
#       base.10M.fbin              (deep-10M base, 96d float32, ~3.8 GB)
#       base.1B.fbin               (deep-1B base, 96d float32, ~360 GB)
#       query.public.10K.fbin      (10 000 queries, 96d float32)
#       deep-10M-gt100.ibin        (top-100 GT for the 10M slice)
#       deep-1B-gt100.ibin         (top-100 GT for the full 1B)
#
# big-ann-benchmarks DEEP is Yandex's image-feature dataset. Vectors are NOT
# pre-normalised at publication and the official GT files (deep-*-gt100.ibin)
# are computed under Euclidean / L2 — so metric MUST be 'L2' for recall to
# line up with GT. (The ann-benchmarks-mirrored `deep-image-96-angular.hdf5`
# is a different release: pre-normalised + cosine GT. Don't mix them.)

DEEP_10M = DatasetSpec(
    name="deep-10M",
    dim=96,
    metric="L2",
    base_count=10_000_000,
    query_count=10_000,
    format="bin",
    base_path=Path("/data/data/deep1B/base.10M.fbin"),
    query_path=Path("/data/data/deep1B/query.public.10K.fbin"),
    gt_path=Path("/data/data/deep1B/deep-10M-gt100.ibin"),
    base_dtype="float32",
    query_dtype="float32",
)

DEEP_1B = DatasetSpec(
    name="deep-1B",
    dim=96,
    metric="L2",
    base_count=1_000_000_000,
    query_count=10_000,
    format="bin",
    base_path=Path("/data/data/deep1B/base.1B.fbin"),
    query_path=Path("/data/data/deep1B/query.public.10K.fbin"),
    gt_path=Path("/data/data/deep1B/deep-1B-gt100.ibin"),
    base_dtype="float32",
    query_dtype="float32",
)


# big-ann-benchmarks BIGANN-1B: same distribution as SIFT-1M but at 1B scale,
# stored as uint8 (128d). bin_to_rowbinary.py widens uint8 -> float32 exactly
# (no scaling) so the backend, which only supports Array(Float32), sees the
# same numeric values. Official GT uses L2 distance over uint8 values.
BIGANN_1B = DatasetSpec(
    name="bigann-1B",
    dim=128,
    metric="L2",
    base_count=1_000_000_000,
    query_count=10_000,
    format="bin",
    base_path=Path("/data/data/bigann/base.1B.u8bin"),
    query_path=Path("/data/data/bigann/query.public.10K.u8bin"),
    gt_path=Path("/data/data/bigann/GT.public.1B.ibin"),
    base_dtype="uint8",
    query_dtype="uint8",
)


# ---- HuggingFace dbpedia-entities-openai-1M (bin, pre-converted) -----------
#
# The HF dataset ships as Parquet (KShivendu/dbpedia-entities-openai-1M, 1M ×
# 1536d, OpenAI text-embedding-ada-002). It is converted offline into the
# Big-ANN bin layout so this loader stays uniform:
#
#   /data/data/openai-dbpedia-1m/
#       base.1M.fbin               (1 000 000 × 1536 float32, ~5.9 GB)
#       query.10K.fbin             (10 000 × 1536 float32, ~58 MB)
#       gt100.ibin                 (top-100 GT computed against full base)
#
# See README for the one-time conversion script (HF parquet -> .fbin/.ibin).
# `metric=cosine` matches both the dataset semantics and the ann backends'
# queryFunctionMatchesMetric check against `cosineDistance`.

OPENAI_DBPEDIA_1M = DatasetSpec(
    name="openai-dbpedia-1M",
    dim=1536,
    metric="cosine",
    base_count=990_000,
    query_count=10_000,
    format="bin",
    base_path=Path("/data/data/openai-dbpedia-1m/base.990K.fbin"),
    query_path=Path("/data/data/openai-dbpedia-1m/query.10K.fbin"),
    gt_path=Path("/data/data/openai-dbpedia-1m/gt100.ibin"),
    base_dtype="float32",
    query_dtype="float32",
)


DATASETS: Dict[str, DatasetSpec] = {
    SIFT1M.name: SIFT1M,
    GIST_960.name: GIST_960,
    GLOVE_100.name: GLOVE_100,
    DEEP_10M.name: DEEP_10M,
    DEEP_1B.name: DEEP_1B,
    BIGANN_1B.name: BIGANN_1B,
    OPENAI_DBPEDIA_1M.name: OPENAI_DBPEDIA_1M,
}


def _stream_into_table(server, table: str, converter_argv: list[str]) -> None:
    """Pipe a converter's stdout into `INSERT INTO {table} FORMAT RowBinary`."""
    insert_sql = f"INSERT INTO {table} FORMAT RowBinary"
    log.info("SQL (stream) >>>\n%s\n<<<", insert_sql)
    with subprocess.Popen(converter_argv, stdout=subprocess.PIPE) as conv:
        argv = server.handle.client_argv() + ["-q", insert_sql]
        client = subprocess.Popen(argv, stdin=conv.stdout)
        conv.stdout.close()
        rc = client.wait()
        conv.wait()
        if rc != 0 or conv.returncode != 0:
            raise RuntimeError(
                f"load {table}: converter rc={conv.returncode} client rc={rc} "
                f"argv={converter_argv}"
            )


def _load_hdf5(server, dataset: DatasetSpec) -> None:
    if not dataset.hdf5_path or not dataset.hdf5_path.is_file():
        raise FileNotFoundError(
            f"dataset HDF5 missing at {dataset.hdf5_path} — mount the dataset directory into the container"
        )
    if not dataset.hdf5_converter.is_file():
        raise FileNotFoundError(
            f"hdf5_to_rowbinary.py missing at {dataset.hdf5_converter}"
        )
    for schema, table in (("base", "sift_base"), ("query", "sift_query"), ("gt", "sift_gt")):
        log.info("streaming HDF5 '%s' -> %s", schema, table)
        _stream_into_table(server, table, [
            "python3", str(dataset.hdf5_converter), str(dataset.hdf5_path),
            "--schema", schema,
        ])


def _load_bin(server, dataset: DatasetSpec) -> None:
    if not dataset.bin_converter.is_file():
        raise FileNotFoundError(f"bin_to_rowbinary.py missing at {dataset.bin_converter}")
    for path in (dataset.base_path, dataset.query_path, dataset.gt_path):
        if not path or not path.is_file():
            raise FileNotFoundError(f"bin dataset file missing: {path}")

    def _argv(schema: str, path: Path, dtype: Optional[str], rows_limit: int) -> list[str]:
        argv = ["python3", str(dataset.bin_converter), str(path), "--schema", schema]
        if dtype is not None:
            argv += ["--dtype", dtype]
        if rows_limit > 0:
            argv += ["--rows-limit", str(rows_limit)]
        return argv

    plan = [
        ("base",  "sift_base",  dataset.base_path,  dataset.base_dtype,  dataset.base_rows_limit),
        ("query", "sift_query", dataset.query_path, dataset.query_dtype, 0),
        ("gt",    "sift_gt",    dataset.gt_path,    None,                0),
    ]
    for schema, table, path, dtype, rows_limit in plan:
        log.info("streaming bin '%s' (%s) -> %s", schema, path, table)
        _stream_into_table(server, table, _argv(schema, path, dtype, rows_limit))


def load_dataset(server, dataset: DatasetSpec, *,
                 index_granularity: int = DEFAULT_BASE_INDEX_GRANULARITY) -> None:
    """Recreate sift_base/sift_query/sift_gt and stream the dataset into them."""
    log.info("init schema for dataset %s (format=%s, %dd, %s, sift_base index_granularity=%d)",
             dataset.name, dataset.format, dataset.dim, dataset.metric, index_granularity)
    server.query(init_sql(index_granularity), multiquery=True)

    if dataset.format == "hdf5":
        _load_hdf5(server, dataset)
    elif dataset.format == "bin":
        _load_bin(server, dataset)
    else:
        raise ValueError(f"unknown dataset format {dataset.format!r}")

    counts = server.query("SELECT count() FROM sift_base").strip()
    if int(counts) != dataset.base_count:
        raise RuntimeError(f"sift_base row count {counts} != expected {dataset.base_count}")
