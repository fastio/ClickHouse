# AuxiliaryIndex ANN benchmark harness

Algorithm-agnostic recall + QPS harness for `AuxiliaryIndex` ANN backends
(`diskann`, `spann`). One invocation = one (algorithm, dataset, build params,
search params) point, emitted as a JSON record.

This supersedes the DiskANN-only `tests/benchmarks/auxiliary_index_sift1m/`
script: it covers both algorithms via the same code path, parametrises build
and search separately, and produces machine-readable output suitable for
appending across many runs (parameter sweeps, A/B comparisons).

## Datasets

| dataset name        | dim  | metric | base       | query  | format | source                                                  |
|---------------------|------|--------|------------|--------|--------|---------------------------------------------------------|
| `sift1m`            | 128  | L2     | 1 M        | 10 k   | HDF5   | ann-benchmarks (`sift-128-euclidean.hdf5`)              |
| `gist-960`          | 960  | L2     | 1 M        | 1 k    | HDF5   | ann-benchmarks (`gist-960-euclidean.hdf5`)              |
| `glove-100`         | 100  | cosine | 1.18 M     | 10 k   | HDF5   | ann-benchmarks (`glove-100-angular.hdf5`)               |
| `deep-10M`          | 96   | L2     | 10 M       | 10 k   | bin    | big-ann-benchmarks DEEP (10M slice)                      |
| `deep-1B`           | 96   | L2     | 1 B        | 10 k   | bin    | big-ann-benchmarks DEEP (full 1B)                        |
| `bigann-1B`         | 128  | L2     | 1 B        | 10 k   | bin    | big-ann-benchmarks BIGANN (SIFT-1B, uint8)              |
| `openai-dbpedia-1M` | 1536 | cosine | 1 M        | 10 k   | bin    | HuggingFace `KShivendu/dbpedia-entities-openai-1M`      |

Coverage rationale: SIFT/BIGANN at 1M and 1B scale (same distribution,
size-scaling stress); GIST high-dim L2; GloVe known-hard NLP-embedding
recall-ceiling stress; DEEP at 10M and 1B scale (Yandex image features,
billion-scale baseline); OpenAI dbpedia 1M for the high-dim text-embedding
RAG workload shape.

Yandex Text2Image-1B is deliberately omitted — its native metric is inner
product (MIPS), which the DiskANN/SPANN backends do not yet support; running
it against `cosineDistance` would give recall numbers that don't match the
official GT.

The harness loads any of these into the same three tables (`sift_base`,
`sift_query`, `sift_gt` — the names are kept for backwards compatibility with
the SIFT-1M-era `results.jsonl`; the data they hold is arbitrary).

### Data preparation

Files live under `<ANN_DATA_DIR>/data/` (default
`/data/test/benchmarks/ann_sift1m/data/`). Layout expected by the harness:

```
data/
  sift-128-euclidean.hdf5                       # sift1m,  from ann-benchmarks.com
  gist-960-euclidean.hdf5                       # gist-960,  from ann-benchmarks.com
  glove-100-angular.hdf5                        # glove-100, from ann-benchmarks.com
  deep1B/
    base.10M.fbin                               # deep-10M  base (~3.8 GB)
    base.1B.fbin                                # deep-1B   base (~360 GB)
    query.public.10K.fbin                       # shared queries (both deep-* datasets)
    deep-10M-gt100.ibin                         # deep-10M  ground truth
    deep-1B-gt100.ibin                          # deep-1B   ground truth
  bigann/
    base.1B.u8bin                               # bigann-1B base (~128 GB, uint8)
    query.public.10K.u8bin                      # 10 000 queries (uint8)
    GT.public.1B.ibin                           # bigann-1B ground truth
  openai-dbpedia-1m/
    base.1M.fbin                                # 1M × 1536 float32, ~5.9 GB
    query.10K.fbin                              # 10k × 1536 float32
    gt100.ibin                                  # top-100 GT
```

* **SIFT-1M / GIST-960 / GloVe-100**: ann-benchmarks single-HDF5 files.
  `ann_sift1m/download.sh --dataset {sift-128-euclidean,gist-960-euclidean,glove-100-angular}`.
* **DEEP-10M / DEEP-1B / BIGANN-1B**: download from big-ann-benchmarks
  (https://big-ann-benchmarks.com/neurips21.html). Files are public on the
  Yandex / Facebook AI mirrors; the project's `create_dataset.py` is the
  canonical fetcher. BIGANN is the 1B extension of SIFT, stored as uint8 —
  `bin_to_rowbinary.py` widens to float32 at load.
* **OpenAI dbpedia 1M**: the source is HuggingFace Parquet, not bin. Convert
  once with the helper at the end of this README ("Converting
  dbpedia-entities-openai-1M").

## Quick start

`docker-bench.sh` reads `CH_BIN` / `ANN_DATA_DIR` / `RESULTS_DIR` /
`SERVER_DATA_DIR` from env; see the [Mounts](#mounts) section for defaults.
Each command emits one JSON record to `results/results.jsonl`.

### 1M-class datasets (single host, ~20 min – 1 h)

```bash
# SIFT-1M  (128d L2, ann-benchmarks)
./docker-bench.sh --algo diskann --dataset sift1m            --preset /presets/diskann_sift1m.json            --query-count 10000
./docker-bench.sh --algo spann   --dataset sift1m            --preset /presets/spann_sift1m.json              --query-count 10000

# GIST-960  (960d L2, high-dim stress, only 1 000 queries in upstream GT)
./docker-bench.sh --algo diskann --dataset gist-960          --preset /presets/diskann_gist960.json           --query-count 1000
./docker-bench.sh --algo spann   --dataset gist-960          --preset /presets/spann_gist960.json             --query-count 1000

# GloVe-100  (100d cosine angular, known-hard NLP embedding)
./docker-bench.sh --algo diskann --dataset glove-100         --preset /presets/diskann_glove100.json          --query-count 10000
./docker-bench.sh --algo spann   --dataset glove-100         --preset /presets/spann_glove100.json            --query-count 10000

# OpenAI dbpedia 1M  (1536d cosine, text-embedding-ada-002 RAG shape)
./docker-bench.sh --algo diskann --dataset openai-dbpedia-1M --preset /presets/diskann_openai_dbpedia.json    --query-count 10000
./docker-bench.sh --algo spann   --dataset openai-dbpedia-1M --preset /presets/spann_openai_dbpedia.json      --query-count 10000
```

### 10M-class datasets (single host, ~1 – 4 h)

```bash
# DEEP-10M  (96d L2, big-ann-benchmarks slice)
./docker-bench.sh --algo diskann --dataset deep-10M          --preset /presets/diskann_deep10m.json           --query-count 10000
./docker-bench.sh --algo spann   --dataset deep-10M          --preset /presets/spann_deep10m.json             --query-count 10000
```

### 1B-class datasets (quarterly gate, 1 – 3 days)

> Requires 16+ cores, ≥ 256 GB RAM, ≥ 1.5 TB NVMe.
> `--optimize-timeout-sec` defaults to 24 h to survive the single-part merge.
> Use `--keep-data-dir` to avoid wiping the multi-day-built index on a sweep
> restart.

```bash
# BIGANN-1B  (128d L2, SIFT-1B / uint8 widened to float32 on load)
./docker-bench.sh --algo diskann --dataset bigann-1B         --preset /presets/diskann_bigann1b.json          --query-count 10000
./docker-bench.sh --algo spann   --dataset bigann-1B         --preset /presets/spann_bigann1b.json            --query-count 10000

# DEEP-1B  (96d L2, Yandex image features, big-ann-benchmarks)
./docker-bench.sh --algo diskann --dataset deep-1B           --preset /presets/diskann_deep1b.json            --query-count 10000
./docker-bench.sh --algo spann   --dataset deep-1B           --preset /presets/spann_deep1b.json              --query-count 10000
```

### Sweep recall–QPS Pareto

Build params unchanged across the sweep, so the index is rebuilt only when
they change — the inner loop only flips a search session setting.

```bash
for L in 64 100 200 400 800; do
  ./docker-bench.sh --algo diskann --dataset sift1m --preset /presets/diskann_sift1m.json \
      --search diskann_search_list_size=$L --query-count 10000
done
jq -r '[.search_settings.diskann_search_list_size, .recall_at_10, .qps.c1.qps, .qps.c16.qps] | @tsv' \
    results/results.jsonl
```

## Mounts

| host path                                       | container path | rw | purpose                                  |
|-------------------------------------------------|----------------|----|------------------------------------------|
| `build/programs/clickhouse`                     | `/ch/clickhouse` | ro | server + client + benchmark in one binary |
| `<ANN_DATA_DIR>` (default `/data/test/benchmarks/ann_sift1m`) | `/data`        | ro | HDF5 + `hdf5_to_rowbinary.py`             |
| `<RESULTS_DIR>` (default `./results`)           | `/out`         | rw | `.jsonl` output                          |
| `<SERVER_DATA_DIR>` (default `<RESULTS_DIR>/server-data`) | `/tmp/ch-bench` | rw | clickhouse-server data dir (MUST be a real fs that supports `O_DIRECT`) |

Override via env: `CH_BIN`, `ANN_DATA_DIR`, `RESULTS_DIR`, `SERVER_DATA_DIR`.

> **Why bind-mount `SERVER_DATA_DIR`?** Two Docker defaults conspire against
> DiskANN's aligned graph reader:
> 1. The `overlay2` writable layer does not support `O_DIRECT`; index files
>    must live on a real `ext4` / `xfs` / `btrfs` filesystem.
> 2. Docker's default seccomp profile blocks the `io_uring_*` syscalls
>    DiskANN uses to read graph pages. `docker-bench.sh` adds
>    `--security-opt seccomp=unconfined` so `io_uring_setup` succeeds.
>
> Without (1), open fails with `EPERM`. Without (2), `io_uring_setup` fails
> with `EPERM`. Either symptom surfaces as ClickHouse
> `EXTERNAL_LIBRARY_ERROR` (code 391, exit 135 = `391 & 0xff`) at search
> time.

## CLI

| flag                     | default                                | meaning                                                      |
|--------------------------|----------------------------------------|--------------------------------------------------------------|
| `--algo`                 | (required)                             | `diskann` \| `spann`                                         |
| `--dataset`              | (required)                             | `sift1m` \| `gist-960` \| `glove-100` \| `deep-10M` \| `deep-1B` \| `bigann-1B` \| `openai-dbpedia-1M` |
| `--preset`               | none                                   | JSON with `build` / `search` sections                        |
| `--build k=v`            | (repeatable)                           | CLI override of a build param (wins over preset)             |
| `--search k=v`           | (repeatable)                           | CLI override of a search session setting                     |
| `--query-count`          | `1000`                                 | recall sweep size                                            |
| `--qps-iterations`       | `2000`                                 | iterations per concurrency level                              |
| `--qps-concurrencies`    | `1,16`                                 | comma-separated concurrency list for QPS                     |
| `--sync-timeout-sec`     | `3600`                                 | `SYSTEM SYNC AUXILIARY INDEX` timeout                     |
| `--no-optimize`          | (off)                                  | skip `OPTIMIZE TABLE sift_base FINAL` before index build     |
| `--optimize-timeout-sec` | `86400`                                | `OPTIMIZE TABLE FINAL` timeout (1B merges can take hours)    |
| `--binary`               | `/ch/clickhouse`                       | path inside the container                                    |
| `--data-dir`             | `/tmp/ch-bench`                        | server data dir (wiped before start unless `--keep-data-dir`) |
| `--output`               | `/out/results.jsonl`                   | append the JSON record here                                  |

## Pipeline (load → OPTIMIZE → CREATE → SYNC)

1. **Load**: stream the dataset via `hdf5_to_rowbinary.py` (HDF5 datasets) or
   `bin_to_rowbinary.py` (big-ann bin datasets) into `sift_base` / `sift_query` /
   `sift_gt`. Streamed INSERTs at 10M+ rows produce many `MergeTree` parts.
2. **OPTIMIZE FINAL**: collapse `sift_base` to a single active part. ANN groups
   in `AuxiliaryIndex` are part-bound — without this step a 1B-row table
   carries hundreds of small per-part indexes, fan-out search has to do
   `per_part_top_K' → merge → top_10` and per-part `K'` has to be widened
   enough to survive the merge or recall drops. Skipping the merge to measure
   that exact effect is possible via `--no-optimize`.
3. **CREATE AUXILIARY INDEX** + **SYSTEM SYNC**: build the index over the
   single consolidated part. Wall-clock dominates the run on 10M+ datasets.
4. **Recall sweep** + **QPS measurement**: same as before; the distance function
   in both the recall and QPS queries is selected from `dataset.metric`
   (`L2Distance` for `L2`, `cosineDistance` for `cosine`).

## Output record

```json
{
  "ts": "2026-05-17T22:30:01+00:00",
  "binary": "/ch/clickhouse",
  "binary_mtime": "2026-05-17T21:11:00+00:00",
  "git_commit": "76033ade",
  "clickhouse_version": "26.4.1.1",
  "algo": "diskann",
  "dataset": "sift1m",
  "build_params": { "metric": "L2", "dim": 128, "pq_chunks": 32, ... },
  "search_settings": { "diskann_search_list_size": 200, ... },
  "query_count": 10000,
  "load_seconds": 5.1,
  "optimize_seconds": 12.4,
  "parts_before_optimize": 4,
  "parts_after_optimize": 1,
  "build_seconds": 1167.0,
  "recall_seconds": 500.4,
  "index_bytes": 887340891,
  "recall_at_10": 0.9924,
  "qps": {
    "c1":  { "qps": 27.9,  "latency_ms_p50": 35.1, "latency_ms_p99": 78.2 },
    "c16": { "qps": 333.8, "latency_ms_p50": 44.5, "latency_ms_p99": 110.7 }
  }
}
```

## Parameter scope per algorithm

### DiskANN

Build params (passed to `ann('diskann', ...)`): `metric`, `dim`, `pq_chunks`,
`pruned_degree`, `max_degree`, `l_build`, `alpha`, `num_threads`,
`build_ram_limit_gb`.

Search session settings (per-query). Pass `0` to use the built-in default.

| setting                                                     | default | meaning                                                       |
|-------------------------------------------------------------|---------|---------------------------------------------------------------|
| `diskann_search_list_size`               | `200`   | `L_search` candidate list size                                |
| `diskann_search_beam_width`              | `16`    | graph neighbours dispatched per step (I/O concurrency)        |
| `diskann_search_num_threads`             | `8`     | per-part searcher worker pool (open-time, in cache key)       |
| `diskann_search_io_limit`                | `256`   | per-part in-flight I/O cap (open-time, in cache key)          |
| `diskann_search_nodes_to_cache`          | `1024`  | per-part hot-node cache size (open-time, in cache key)        |

### SPANN

Build params (passed to `ann('spann', ...)`): `metric`, `dim`, `head_ratio`,
`posting_page_limit`, `posting_vector_limit`, `search_posting_page_limit`,
`internal_result_num`, `replica_count`, `num_threads`, `io_threads`,
`max_check`, `max_dist_ratio`, `hash_table_exponent`, `io_timeout_us`.

Search session settings (per-query). Pass `0` to fall back to the value baked
into the index by `CREATE AUXILIARY INDEX`. SPTAG re-reads these from
`m_options` (workspace-pool / per-search reads) or, for the BKT-side keys,
re-applies them through SPTAG's section-aware `SetParameter` routing — so a
session-level change re-opens the per-part `Searcher` and the new values
take effect without a DROP/CREATE.

| setting                                                       | overrides build param | routing                                        |
|---------------------------------------------------------------|-----------------------|------------------------------------------------|
| `spann_search_posting_page_limit`          | `search_posting_page_limit` | SSD workspace (`m_options.m_searchPostingPageLimit`) |
| `spann_search_internal_result_num`         | `internal_result_num`       | SSD workspace (`m_options.m_searchInternalResultNum`) |
| `spann_search_max_dist_ratio`              | `max_dist_ratio`            | per-search read (`m_options.m_maxDistRatio`) |
| `spann_search_max_check`                   | `max_check`                 | head BKT (`m_index->SetParameter("MaxCheck", ...)`) |
| `spann_search_hash_table_exponent`         | `hash_table_exponent`       | head BKT (`m_index->SetParameter("HashTableExponent", ...)`) |

`io_timeout_us` is **not** a session setting — SPTAG stores it in
`Helper::AIOTimeout` (process-global), so per-query override would race
other concurrent searches. Treat it as build-only.

The remaining SPANN params (`head_ratio`, `posting_page_limit`,
`posting_vector_limit`, `replica_count`, `num_threads`, `io_threads`) are
build-time only — changing any of them forces `DROP INDEX` + `CREATE INDEX`
(and a full rebuild). To sweep them, vary via `--build`.

## Why this is not part of PR CI

Build wall-clock on SIFT-1M is dominated by Vamana / SPANN graph construction
(~20 min for DiskANN, ~tbd for SPANN on the same hardware). The cheap chain
correctness gate `04179_auxiliary_index_diskann_self_query_smoke.sql` lives
in `tests/queries/0_stateless/` and runs in PR CI. This harness is a manual /
nightly gate for "the chain is wired, but recall regressed" or "QPS regressed
under load".

## Implementation notes

* `harness/server.py` writes a stripped-down `config.xml`, launches
  `clickhouse server --daemon`, waits for `SELECT 1` to succeed. Each run
  uses its own data directory so concurrent runs do not collide.
* `harness/datasets.py` reuses two existing streamers under
  `<ANN_DATA_DIR>/` via subprocess instead of re-implementing the formats:
  `hdf5_to_rowbinary.py` for ann-benchmarks HDF5 and `bin_to_rowbinary.py`
  for big-ann-benchmarks `.fbin`/`.u8bin`/`.i8bin`/`.ibin`. The bin
  converter mmaps the body, so the loader's RSS stays at chunk size even
  for the 360 GB DEEP-1B file.
* `harness/algorithms.py` carries per-algorithm DDL templates, defaults, and
  the `ProfileEvent` name to assert fired (so a silent brute-force fallback
  cannot pass).
* Recall correctness uses the same per-query intersect template as the
  DiskANN-only `auxiliary_index_sift1m/run.sh` and runs under
  `auxiliary_index_require_match = 1`.
* QPS uses `clickhouse benchmark` with a representative query (the first
  test vector) repeated `--qps-iterations` times at each concurrency in
  `--qps-concurrencies`.

## Converting `dbpedia-entities-openai-1M` from HuggingFace Parquet

The HF dataset (`KShivendu/dbpedia-entities-openai-1M`) is published as
Parquet, not as big-ann bin files. A one-time offline conversion is needed
before the harness can load it. The simplest path uses `datasets` + `numpy`
to emit the three bin files in-place:

```python
# tools/dbpedia_openai_to_bin.py  (one-off, not shipped with the harness)
import numpy as np, struct, os
from datasets import load_dataset
from pathlib import Path

OUT = Path("/data/test/benchmarks/ann_sift1m/data/openai-dbpedia-1m")
OUT.mkdir(parents=True, exist_ok=True)

ds = load_dataset("KShivendu/dbpedia-entities-openai-1M", split="train")
vecs = np.asarray(ds["openai"], dtype=np.float32)        # (1_000_000, 1536)

# Split: first 990 000 as base, last 10 000 as queries.
base, queries = vecs[:990_000], vecs[990_000:1_000_000]

def write_fbin(path, arr):
    n, d = arr.shape
    with open(path, "wb") as f:
        f.write(struct.pack("<ii", n, d))
        arr.astype(np.float32, copy=False).tofile(f)

write_fbin(OUT / "base.1M.fbin",   vecs[:1_000_000])     # full 1M as base if you want
write_fbin(OUT / "query.10K.fbin", queries)

# Brute-force GT: for each query, top-100 nearest in base by cosine.
# Pick a chunked GEMM or faiss; left as an exercise — must match the
# `base.1M.fbin` chosen above. Emit as .ibin:
#   header [int32 Nq, int32 K], body Nq*K int32 ids, then Nq*K float32 dists.
```

The `dataset_count` in `datasets.py:OPENAI_DBPEDIA_1M` assumes the **first
1 000 000 rows are the base** (no holdout); the 10 000-query set must come
from disjoint embeddings (e.g., a held-out chunk, or queries from MS MARCO
re-embedded with the same model). Make sure the GT file you generate matches
exactly which rows you committed to `base.1M.fbin`.
