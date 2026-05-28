# AuxiliaryIndex × DiskANN — SIFT-1M recall@10 benchmark

End-to-end correctness gate for the `AuxiliaryIndex` query fast path on
real ANN data: build a DiskANN-backed index over SIFT-1M, run the full 10k
query workload through the MI rewrite, and require `recall@10 >= 0.95` against
the dataset's ground truth.

This is **not** part of PR CI. It is a manual / nightly gate that complements
the cheap `04179_auxiliary_index_diskann_self_query_smoke.sql` smoke in
`tests/queries/0_stateless/`. The smoke catches "the chain is wired wrong";
this benchmark catches "the chain is wired, but DiskANN's recall regressed".

## Prerequisites

- A running ClickHouse server with `AuxiliaryIndex` available
  (e.g. `cc:deploy`-style local deploy).
- The SIFT-1M HDF5 already downloaded under `ANN_SIFT_DIR` (defaults to
  `/data/test/benchmarks/ann_sift1m/data/sift-128-euclidean.hdf5`). If
  missing, `load.sh` calls `download.sh` from that directory.
- `python3` with `h5py` available (used by the converter).

## One-shot run

```bash
cd tests/benchmarks/auxiliary_index_sift1m
CH_BIN=/data/develop/feat-mi/build/programs/clickhouse \
CH_PORT=9101 \
./run.sh
```

Expected last line on success:

```
[HH:MM:SS] PASS: recall@10 = 0.9924 >= 0.95
```

Wall-clock on a workstation (16-thread build, sequential 10 k recall):

| step                      | time             |
|---------------------------|------------------|
| dataset load              | ~30 s            |
| DiskANN graph build       | ~20 min          |
| 10 000 recall SELECTs     | ~8 min           |
| **total**                 | **~30 min**      |

The 10 k recall step used to take ~80 min because the Rust FFI held a
global `Mutex<HashMap>` over the entire `searcher.search` call. After
`rust/workspace/diskann-clickhouse/src/lib.rs` was changed to release the
HashMap lock immediately and `DiskANNAlgorithm` started caching one
`DiskIndexSearcher` per AuxiliaryIndex part, search throughput scales
near-linearly with concurrency (`clickhouse-benchmark` on this hardware:
27.9 QPS at conc=1 → 333.8 QPS at conc=16). The single-threaded recall
sweep still benefits ~9.7× from the cache alone (skipping the open/close
per query).

## Environment knobs

| variable             | default                                  | meaning                                              |
|----------------------|------------------------------------------|------------------------------------------------------|
| `CH_BIN`             | `clickhouse`                             | binary on `$PATH` or absolute                        |
| `CH_PORT`            | `9000`                                   | TCP port                                             |
| `CH_DB`              | `default`                                | target database                                      |
| `ANN_SIFT_DIR`       | `/data/test/benchmarks/ann_sift1m`       | source of HDF5 + `hdf5_to_rowbinary.py`              |
| `RECALL_THRESHOLD`   | `0.95`                                   | fail floor (`recall@10 >= threshold`)                |
| `SYNC_TIMEOUT_SEC`   | `3600`                                   | `SYSTEM SYNC REFLECTION` timeout (60 min)    |
| `QUERY_COUNT`        | `10000`                                  | queries used for recall (full SIFT-1M query set)     |
| `KEEP_TABLES`        | `0`                                      | `1` skips `DROP/CREATE/INSERT` (re-use loaded data)  |

## What the script does

1. `init.sql` (re)creates `sift_base (id, v Array(Float32))`,
   `sift_query (id, v)`, `sift_gt (query_id, neighbors Array(UInt32))`.
2. `load.sh` streams the HDF5 file through
   `/data/test/benchmarks/ann_sift1m/hdf5_to_rowbinary.py` and bulk-inserts
   into the three tables (`RowBinary`, ~30 s for 1 M base + 10 k query +
   10 k GT rows).
3. `CREATE REFLECTION mi_sift ... ENGINE = ANNIndex(diskann)` with
   **SIFT-1M-tuned build params** (see "Tuning rationale" below). `SYSTEM SYNC`
   waits for coverage to reach 100 %.
4. Generates `QUERY_COUNT` per-query SELECTs of the form

   ```sql
   SELECT length(arrayIntersect(
       (SELECT groupArray(id) FROM (
           SELECT id FROM sift_base
           ORDER BY L2Distance(v, (SELECT v FROM sift_query WHERE id = N))
           LIMIT 10
       )),
       (SELECT arraySlice(neighbors, 1, 10) FROM sift_gt WHERE query_id = N)
   ));
   ```

   The scalar subquery `(SELECT v FROM sift_query WHERE id = N)` is folded
   to a `ColumnConst` at planning time, which is what
   `optimizeAuxiliaryIndex.cpp:extractQueryParams` requires.
   `force_auxiliary_index = 'mi_sift'` is set so the MI fast path is the
   only path taken, and `auxiliary_index_require_match = 1` makes the
   server **throw** if any per-query SELECT would silently fall back to a
   brute-force source scan.
5. Sums the 10 k per-query intersect counts, divides by `QUERY_COUNT * 10`,
   compares against `RECALL_THRESHOLD`.
6. As a defence in depth on top of `auxiliary_index_require_match`, also
   asserts via `system.query_log` that every recall SELECT logged
   `AuxiliaryIndexDiskANNSearchStarted > 0`.

## Tuning rationale

The in-code DiskANN defaults (`pq_chunks=4`, `pruned_degree=32`, `max_degree=64`,
`SEARCH_LIST_SIZE=100`, `SEARCHER_IO_LIMIT=8`) are smoke-test sized and
produce **`recall@10 = 0%` on SIFT-1M**: a 4-byte PQ code can't approximate
a 128-d L2 distance closely enough to guide graph traversal, and a 8-IO cap
stops the search before it reaches the true nearest neighbours.

The values baked into `run.sh` and `DiskANNAlgorithm.cpp` for this benchmark:

| knob               | in-code default | benchmark value | source                          |
|--------------------|-----------------|-----------------|---------------------------------|
| `pq_chunks`        | 4               | **32**          | DiskANN paper Table 2 SIFT-1M   |
| `pruned_degree` (R) | 32             | **64**          | DiskANN paper Table 2 SIFT-1M   |
| `max_degree`       | 64              | **96**          | 1.5×R, slack for diversity prune |
| `l_build`          | 128             | 128             | unchanged                       |
| `alpha`            | 1.2             | 1.2             | unchanged                       |
| `num_threads`      | 4               | **16**          | build wall-clock                |
| `SEARCH_LIST_SIZE` | 100             | **200** (code)  | recall@10 → 0.99 region         |
| `SEARCHER_IO_LIMIT` | 8              | **100** (code)  | 8 was a typo-grade default      |

`SEARCH_LIST_SIZE` and `SEARCHER_IO_LIMIT` are still `constexpr` in
`src/Storages/AuxiliaryIndex/DiskANNAlgorithm.cpp` — exposing them as
per-table or session settings is a follow-up. Measured `recall@10 = 0.9924`
on the full 10 000-query SIFT-1M test set with the values above; the 0.95
floor leaves ~4 pp of slack for build-param drift and PQ noise.

## Files

```
init.sql                     -- CREATE TABLE base/query/gt
load.sh                      -- HDF5 -> RowBinary -> INSERT
queries/per_query_recall.sql.template
                             -- documentation copy of the generated SELECT shape
run.sh                       -- orchestrator: init -> load -> build -> recall -> assert
settings.json                -- session settings (allow_experimental_auxiliary_index)
```
