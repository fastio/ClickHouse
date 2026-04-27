# SIFT-1M ANN Benchmark

Reproducible Recall@K / QPS sweep for the table-level `ann` (DiskANN/Vamana)
secondary index against the SIFT-1M dataset from the INRIA TEXMEX corpus.

This suite is **not** wired into CI. It downloads ~525 MB, builds an index that
takes minutes, and reports user-visible numbers - run it manually after sizeable
changes to the ANN code path.

## Why SIFT-1M?

SIFT-1M is the smallest dataset on the canonical
[ann-benchmarks](http://ann-benchmarks.com) and
[big-ann-benchmarks](https://big-ann-benchmarks.com) lists, and the same dataset
DiskANN's NeurIPS 2019 paper uses for its in-memory Vamana evaluation. 128-d L2
fits the index implementation today (DDL only allows
`metric ∈ {L2, Cosine}`, vector column must be `Array(Float32)`), and 1,000,000
base vectors keep build time under a few minutes on a workstation.

## Layout

```
download.sh                fetch & verify the HDF5 dataset under data/
hdf5_to_rowbinary.py       stream `train` / `test` / `neighbors` from HDF5 as RowBinary
run.sh                     thin wrapper: builds cmd/sweep/sweep on first use, forwards args
cmd/sweep/
    main.go                top-level Go driver: load → sanity → sweep → report
    go.mod                 standalone module, stdlib-only (uses ClickHouse HTTP interface)
configs/
    build_paper.env        DiskANN paper baseline build params (default)
    build_fast.env         smaller / faster-building alternate profile
scenarios/
    single_group.env       1M rows in a single ANN group (clean numbers)
    multi_group.env        4 ANN groups (exercises ANNIndexManager paths)
report/
    pareto.py              Pareto frontier + Δ-vs-baseline + iso-recall report
data/                      downloaded files (gitignored)
results/                   per-run TSVs and ProfileEvents snapshots (gitignored)
```

## One-time setup

```bash
pip install h5py             # only if missing
./download.sh
```

Downloads `sift-128-euclidean.hdf5` (~525 MB) from the ann-benchmarks mirror
over HTTPS. The original INRIA TEXMEX corpus is FTP-only, and FTP data
connections are routinely blocked in CI / sandbox environments; the HTTPS
mirror serves the same vectors. On the first run, the printed `sha256` should
be exported as `SIFT1M_SHA256` in subsequent invocations to lock the dataset
version.

## Running

```bash
./run.sh --http-port=8124 --tcp-port=9100
```

`run.sh` builds `cmd/sweep/sweep` on first use (or whenever `main.go` is
newer than the binary) and forwards all flags to it. Run
`./run.sh --help` for the full list.

Default: 2 scenarios × 1 build_cfg × 5 `search_list_size` × 2 concurrency × 3
runs = 60 measurement cells across 10 builds. Wall-clock ~80 min on a 16-core
workstation; ~50 min of that is the 10 index builds.

### Flags

| flag                   | default                  | meaning |
| ---------------------- | ------------------------ | --- |
| `--host`               | `127.0.0.1`              | ClickHouse HTTP host |
| `--http-port`          | `8123`                   | ClickHouse HTTP port (the driver talks HTTP, not TCP) |
| `--tcp-port`           | `9000`                   | TCP port (recorded into `server_meta.txt` only) |
| `--clickhouse-binary`  | `""`                     | path to the `clickhouse` binary (recorded into `server_meta.txt` only; not invoked) |
| `--db`                 | `sift`                   | target database (created if missing) |
| `--dir`                | (alongside the binary)   | harness root holding `configs/`, `scenarios/`, `data/` |
| `--hdf5`               | `<dir>/data/sift-128-euclidean.hdf5` | HDF5 file path |
| `--results-dir`        | `<dir>/results`          | where to write the per-run directory |
| `--k`                  | `10`                     | top-K for Recall@K |
| `--queries-per-cell`   | `1000`                   | queries used for both recall AND QPS in one cell |
| `--warmup-queries`     | `200`                    | throwaway queries before each measurement |
| `--runs`               | `3`                      | repetitions per cell (folded into mean ± stddev by `report/pareto.py`) |
| `--sls-list`           | `10,30,50,100,200`       | comma-separated `search_list_size` sweep |
| `--beam-width`         | `4`                      | constant per-query knob |
| `--search-io-limit`    | `4`                      | constant per-query knob |
| `--concurrencies`      | `1,<nproc>`              | comma-separated benchmark concurrency levels |
| `--scenarios`          | `single_group,multi_group` | comma-separated scenario subset |
| `--build-cfgs`         | `paper`                  | comma-separated build-config subset (also: `fast`) |
| `--sanity-queries`     | `5`                      | random qids to probe before the sweep |
| `--sanity-miss-budget` | `1`                      | per-query miss tolerance for the brute-force sanity check |

The ProfileEvents medians come from `system.query_log` filtered by
`log_comment`, so the server must have `<query_log>` configured (it almost
always is by default — most installs include `config.d/query-log.xml`).

Quick sanity-only run:

```bash
./run.sh --http-port=8124 --tcp-port=9100 \
    --scenarios=single_group --sls-list=100 --runs=1 --concurrencies=1
```

## Design notes (what this suite does that the v1 script did not) {#design-notes}

1. **Index rebuild noise eliminated.** `hash_seed` is pinned per build_cfg,
   so two builds with identical `(max_degree, build_search_list_size, alpha,
   pq_chunks)` produce a bit-identical graph. Recall variation between cells
   no longer includes graph-randomness. (Only the four params above feed
   `params_hash`; `search_list_size` / `beam_width` / `search_io_limit` do
   not, but the DDL bakes them into `DiskANNSearchOptions::default_*`, so
   today they still require a `DROP TABLE` + rebuild to vary. If a future
   change exposes them as per-query `Settings`, drop the inner build call
   from `run.sh` and you get a 5× speed-up.)
2. **Recall and QPS measured on the same query stream.** Both pull from the
   first `QUERIES_PER_CELL` rows of `sift_query` ordered by id. v1 computed
   recall on the first 200 and QPS on the first 1000.
3. **ProfileEvents extraction is by `log_comment`, not by event_time.** Every
   benchmark and recall query carries `--log_comment="sift1m/$RUN_ID/..."`,
   so back-to-back runs (or two parallel users on the same server) cannot
   alias each other's ProfileEvents windows. v1 used
   `event_time > now() - INTERVAL 5 MINUTE`.
4. **Two scenarios cover both single-group and multi-group code paths.**
   v1's defaults put 1M rows into one ANN group; the
   `ANNIndexManager` coordination path (group lifecycle,
   `clearRetiredANNIndexGroups` vs `tryReserveBuildSlot` interaction) was
   never exercised. `multi_group.env` splits 1M into ~4 groups of 250k - if
   anyone re-introduces a coordination bug like the one fixed below, the
   multi_group sweep is the探针 that will fail first.
5. **Brute-force sanity check** (`runSanity` in `cmd/sweep/main.go`). Before the real sweep,
   pick 5 random query ids, run a no-index `ORDER BY L2Distance(...) LIMIT K
   SETTINGS try_use_ann_search = 0`, and assert that the result matches the
   first K of `sift_gt.neighbors`. This catches "we're benchmarking against
   a stale or scaled oracle" before three hours of sweep time gets burnt.
6. **Concurrency is its own axis.** Every cell is measured at concurrency=1
   AND concurrency=`nproc`. Single-stream QPS is essentially `1 / latency`;
   parallel QPS exposes lock contention and I/O-cache thrashing.
7. **Variance is measured.** `--runs=3` repetitions per cell; the report tool
   folds them into mean ± stddev and warns if recall stddev is non-zero
   (which would indicate `hash_seed` is no longer effective).

## Output schema

`results/<utc>/sweep.tsv` (tab-separated):

| column | meaning |
| --- | --- |
| `run_id` | UTC timestamp; matches the `results/<utc>/` directory |
| `git_commit` | SHA being benchmarked (resolved from this checkout) |
| `scenario` | `single_group` / `multi_group` |
| `build_cfg` | `paper` / `fast` (config file name minus `build_` prefix) |
| `sls` | `search_list_size` value for the cell |
| `beam` | `beam_width` value (constant per default sweep) |
| `io_limit` | `search_io_limit` value (constant per default sweep) |
| `concurrency` | `clickhouse benchmark --concurrency` |
| `run_idx` | repetition number (1..`--runs`) |
| `queries` | query stream size (`--queries-per-cell`) |
| `k` | Recall@K |
| `recall` | fraction of `(top-K ∩ ground-truth top-K) / K`, averaged |
| `qps` | `total queries / wall-clock` measured by the Go runner |
| `p50_us`, `p95_us`, `p99_us` | latency percentiles in microseconds |
| `build_seconds` | wall-clock for `SYSTEM BUILD ANN INDEX` + coverage wait |
| `index_size_mb` | `secondary_indices_compressed_bytes / 1 MiB` from `system.parts` |
| `ann_groups` | total groups reported by `tableANNCoverage` |
| `diskann_search_count_p50` | per-query median of `ProfileEvents['DiskANNSearchCount']` |
| `diskann_search_us_p50` | per-query median of `ProfileEvents['DiskANNSearchMicroseconds']` |
| `diskann_results_returned_p50` | per-query median of `DiskANNSearchResultsReturned` |
| `notes` | comma-separated warnings: `index_did_not_fire`, `low_recall`, ... |

Plus, in the same directory:

```
server_meta.txt        run id, server version, K, sweep params, host info
sanity.txt             sanity-check log
build/<cell>.kv        per-build kv files: build_seconds, index_size_mb, params hash
queries/queries_*.sql  rendered query stream (cached, reused across cells)
bench_logs/<cell>.json per-cell raw timings, QPS and percentiles
```

## Fair-kernel baseline mode {#fair-kernel-baseline}

When comparing the index against a brute-force baseline, the published numbers can be
contaminated by a kernel-implementation difference: indexed parts use DiskANN's internal
SIMD distance kernel (Rust), while unindexed parts and the `try_use_ann_search = 0`
baseline use ClickHouse's SQL `L2Distance` / `cosineDistance` (C++). Two query-level
settings let you isolate the algorithmic speed-up from this confound:

| setting | values | effect |
| --- | --- | --- |
| `vector_search_force_brute_force` | `0` (default) / `1` | when `1`, skip the table-level ANN index lookup and push every part through the unindexed-parts dispatch — gives a brute-force baseline without disabling the entire optimizer |
| `vector_search_unindexed_metric_source` | `'sql'` (default) / `'index'` | distance kernel used by the unindexed dispatch; `'index'` borrows the same SIMD kernel that DiskANN uses internally so both legs of an A/B comparison share one kernel |

Recommended A/B configurations:

| label | settings | what it measures |
| --- | --- | --- |
| index path | (defaults) | end-to-end ANN, kernel-mixed |
| brute-force, same kernel as index | `vector_search_force_brute_force = 1, vector_search_unindexed_metric_source = 'index'` | exhaustive scan with DiskANN's SIMD kernel — best apples-to-apples baseline |
| brute-force, SQL kernel | `vector_search_force_brute_force = 1` (or `try_use_ann_search = 0`) | exhaustive scan with ClickHouse's SQL distance — measures the user-visible "no index" performance |

The ratio of the first two QPS numbers is the algorithmic speed-up of the graph search
itself. The ratio of the first and third is the end-to-end speed-up users observe; the gap
between the two ratios is the kernel-implementation contribution.

Notes:

- Index kernels follow the metric's mathematical definition as used internally by the
  index (DiskANN: `L2` returns squared L2; `Cosine` returns `1 − cosine_similarity`).
  Top-K ordering is preserved relative to the SQL function, but absolute distance values
  may differ — do not compare distances across the two kernel sources numerically.
- `vector_search_unindexed_metric_source = 'index'` requires that at least one ANN group
  is built (`SYSTEM BUILD ANN INDEX <table>`); on a fresh table the path silently falls
  back to the SQL kernel, so `force_brute_force = 1, source = 'index'` is safe to set
  before the first build completes.
- The two settings are orthogonal. `force_brute_force = 0, source = 'index'` is the
  natural mode for **mixed tables** (some parts indexed, others not yet) — both legs of
  the query then use one consistent kernel, removing the asymmetry from latency and
  recall numbers without disabling the index.
- `run.sh` does not yet sweep these settings as an axis. The simplest way to obtain the
  three baselines is to run `./run.sh` once normally, then issue a separate
  `clickhouse benchmark --settings vector_search_force_brute_force=1,vector_search_unindexed_metric_source=index ...`
  invocation against the same `sift_base` table for the second and third configurations,
  using the same `queries/queries_*.sql` stream that `run.sh` emits under
  `results/<utc>/queries/` for cross-comparable QPS numbers.

## Reading the numbers

```bash
python3 report/pareto.py results/<utc>/sweep.tsv
```

prints, per `(scenario, build_cfg, concurrency)` curve:

- a frontier table with Recall@K, QPS (mean ± stddev), p99, index size, build
  seconds, and a `★` mark on Pareto-optimal points;
- iso-recall slices at Recall ∈ {0.90, 0.95, 0.99} (interpolated QPS);
- iso-QPS slices at QPS ∈ {1000, 3000, 8000} (interpolated recall).

For PR review, the canonical invocation is:

```bash
python3 report/pareto.py \
    --baseline results/<prev>/sweep.tsv \
    --current  results/<curr>/sweep.tsv
```

which adds a Δ table per cell with `verdict ∈ {neutral, improvement,
regression, mixed}` based on:

- `|ΔRecall| < 0.005` AND `|ΔQPS%| < 3%` → **neutral**
- `ΔRecall ≥ 0` AND `ΔQPS% ≥ 3%` (or symmetric) → **improvement**
- `ΔRecall ≤ -0.005` OR `ΔQPS% ≤ -3%` (without offsetting gain) → **regression**
- Mixed sign → **mixed**

DiskANN's NeurIPS 2019 paper reports `Recall@10 ≥ 0.95` on SIFT-1M at
moderate `search_list_size`. If a sweep here drops below 0.90 at
`search_list_size = 100`, look at the per-query DiskANN ProfileEvents
medians before suspecting the test - low `DiskANNSearchMicroseconds` paired
with low recall usually means the search bailed out early; high values
paired with low recall hint at a build-time defect.

## Previously known issue (fixed) {#known-issue}

Earlier runs of this suite against SIFT-1M failed mid-build with
`DiskANN builder_build failed: ANNError: DiskANN(IOError) -- (pq_storage.rs:98)`.
Root cause: `MergeTreeCleanupThread::clearRetiredANNIndexGroups` swept every
`tmp_ann_<uuid>/` it found on disk, including the directory of the build
currently in progress, because the `ANNIndexManager` did not track in-flight
builds in memory and the cleanup pass had to fall back to a prefix-based
heuristic. Any build longer than
`merge_tree_clear_retired_ann_groups_interval_seconds` (default 300 s) hit it
— SIFT-1M's ~7 min `vectors.fbin` write was over the threshold.

Fixed by replacing the prefix heuristic with a manager-tracked in-flight set:
`ANNIndexManager::tryReserveBuildSlot` now returns a `BuildReservation` RAII
handle that registers the `tmp_ann_<uuid>` name with the manager for the
build's entire lifetime, and `clearRetiredANNIndexGroups` only sweeps
directories the manager does not know about. See `bug-1.md` at the repository
root for the full analysis. The `multi_group` scenario in this suite is the
regression探针 for this class of bug: it routinely creates >= 4 in-flight
build slots and depends on the manager getting the bookkeeping right.
