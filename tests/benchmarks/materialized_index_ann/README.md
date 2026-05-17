# MaterializedIndex ANN benchmark harness

Algorithm-agnostic recall + QPS harness for `MaterializedIndex` ANN backends
(`diskann`, `spann`). One invocation = one (algorithm, dataset, build params,
search params) point, emitted as a JSON record.

This supersedes the DiskANN-only `tests/benchmarks/materialized_index_sift1m/`
script: it covers both algorithms via the same code path, parametrises build
and search separately, and produces machine-readable output suitable for
appending across many runs (parameter sweeps, A/B comparisons).

## Quick start

```bash
# (1) Reproduce the DiskANN SIFT-1M tuned baseline (~30 min: 19 min build, 8 min recall on full 10k, 1 min QPS)
./docker-bench.sh --algo diskann --dataset sift1m --preset /presets/diskann_sift1m.json --query-count 10000

# (2) SPANN with code defaults + num_threads=16
./docker-bench.sh --algo spann --dataset sift1m --preset /presets/spann_sift1m.json --query-count 10000

# (3) Sweep DiskANN search list size against recall/QPS — only the search settings change,
#     so the index is rebuilt only once per build_params set.
for L in 64 100 200 400; do
  ./docker-bench.sh --algo diskann --dataset sift1m --preset /presets/diskann_sift1m.json \
      --search materialized_index_diskann_search_list_size=$L
done
jq -r '[.search_settings.materialized_index_diskann_search_list_size, .recall_at_10, .qps.c1.qps, .qps.c16.qps] | @tsv' results/results.jsonl
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

| flag                  | default                                | meaning                                                      |
|-----------------------|----------------------------------------|--------------------------------------------------------------|
| `--algo`              | (required)                             | `diskann` \| `spann`                                         |
| `--dataset`           | (required)                             | `sift1m`                                                     |
| `--preset`            | none                                   | JSON with `build` / `search` sections                        |
| `--build k=v`         | (repeatable)                           | CLI override of a build param (wins over preset)             |
| `--search k=v`        | (repeatable)                           | CLI override of a search session setting                     |
| `--query-count`       | `1000`                                 | recall sweep size; the full SIFT-1M test set is `10000`      |
| `--qps-iterations`    | `2000`                                 | iterations per concurrency level                              |
| `--qps-concurrencies` | `1,16`                                 | comma-separated concurrency list for QPS                     |
| `--sync-timeout-sec`  | `3600`                                 | `SYSTEM SYNC MATERIALIZED INDEX` timeout                     |
| `--binary`            | `/ch/clickhouse`                       | path inside the container                                    |
| `--data-dir`          | `/tmp/ch-bench`                        | server data dir (wiped before start unless `--keep-data-dir`) |
| `--output`            | `/out/results.jsonl`                   | append the JSON record here                                  |

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
  "search_settings": { "materialized_index_diskann_search_list_size": 200, ... },
  "query_count": 10000,
  "load_seconds": 5.1,
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

Search session settings (per-query, exposed in 26.4):
* `materialized_index_diskann_search_list_size` (default `200`, set `0` to use built-in default)
* `materialized_index_diskann_search_beam_width` (default `4`, set `0` to use built-in default)

Hardcoded open-time tunables (would require keying the searcher cache on the
value, follow-up): `SEARCHER_NUM_THREADS=1`, `SEARCHER_IO_LIMIT=100`,
`SEARCHER_NODES_TO_CACHE=0`. Tracked in `DiskANNAlgorithm.cpp`.

### SPANN

Build params (passed to `ann('spann', ...)`): `metric`, `dim`, `head_ratio`,
`posting_page_limit`, `search_posting_page_limit`, `internal_result_num`,
`replica_count`, `num_threads`, `io_threads`, `max_check`.

Search params are baked into the SPTAG index at CREATE time and are not
overridable per query. To sweep search behaviour for SPANN, vary
`search_posting_page_limit`, `internal_result_num`, or `max_check` via
`--build` — each change forces a full rebuild.

## Why this is not part of PR CI

Build wall-clock on SIFT-1M is dominated by Vamana / SPANN graph construction
(~20 min for DiskANN, ~tbd for SPANN on the same hardware). The cheap chain
correctness gate `04179_materialized_index_diskann_self_query_smoke.sql` lives
in `tests/queries/0_stateless/` and runs in PR CI. This harness is a manual /
nightly gate for "the chain is wired, but recall regressed" or "QPS regressed
under load".

## Implementation notes

* `harness/server.py` writes a stripped-down `config.xml`, launches
  `clickhouse server --daemon`, waits for `SELECT 1` to succeed. Each run
  uses its own data directory so concurrent runs do not collide.
* `harness/datasets.py` reuses the existing `hdf5_to_rowbinary.py` streamer
  via subprocess rather than re-implementing HDF5 → RowBinary.
* `harness/algorithms.py` carries per-algorithm DDL templates, defaults, and
  the `ProfileEvent` name to assert fired (so a silent brute-force fallback
  cannot pass).
* Recall correctness uses the same per-query intersect template as the
  DiskANN-only `materialized_index_sift1m/run.sh` and runs under
  `materialized_index_require_match = 1`.
* QPS uses `clickhouse benchmark` with a representative query (the first
  test vector) repeated `--qps-iterations` times at each concurrency in
  `--qps-concurrencies`.
