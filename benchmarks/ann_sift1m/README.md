# SIFT-1M ANN Benchmark

Reproducible Recall@10 / QPS sweep for the table-level `ann` (DiskANN/Vamana)
index against the SIFT-1M dataset from the INRIA TEXMEX corpus.

This suite is **not** wired into CI. It downloads ~167 MB, builds an index that
takes minutes, and reports user-visible numbers - run it manually after sizeable
changes to the ANN code path.

## Why SIFT-1M?

SIFT-1M is the smallest dataset on the canonical
[ann-benchmarks](http://ann-benchmarks.com) and
[big-ann-benchmarks](https://big-ann-benchmarks.com) lists, and the same
dataset DiskANN's NeurIPS 2019 paper uses for its in-memory Vamana
evaluation. 128-d L2 fits the index implementation today (DDL only allows
`metric ∈ {L2, Cosine}`, vector column must be `Array(Float32)`), and
1,000,000 base vectors keep build time under a few minutes on a workstation.
GloVe / Cosine and Deep1B are reasonable next datasets once the suite is in
place.

## Files

```
download.sh             fetch & verify the HDF5 dataset under data/
hdf5_to_rowbinary.py    stream `train` / `test` / `neighbors` from the HDF5
                        file as ClickHouse RowBinary
recall_qps.sh           drive the sweep, write results/<utc>/sweep.tsv
data/                   downloaded files (gitignored)
results/                per-run TSVs and ProfileEvents snapshots (gitignored)
```

## One-time setup

```bash
pip install h5py             # only if missing
./download.sh
```

Downloads `sift-128-euclidean.hdf5` (~525 MB) from the ann-benchmarks mirror
over HTTPS. The original INRIA TEXMEX corpus is FTP-only, and FTP data
connections are routinely blocked in CI / sandbox environments; the HTTPS
mirror serves the same vectors. On the first run, the printed `sha256`
should be exported as `SIFT1M_SHA256` in subsequent invocations to lock the
dataset version.

## Running

```bash
CLICKHOUSE_BINARY=/path/to/build/programs/clickhouse \
CLICKHOUSE_PORT_TCP=9100 \
./recall_qps.sh
```

Environment knobs:

| variable | default | meaning |
| --- | --- | --- |
| `CLICKHOUSE_BINARY` | `clickhouse` (PATH) | binary used for both `clickhouse client` and `clickhouse benchmark` |
| `CLICKHOUSE_PORT_TCP` | `9000` | server TCP port |
| `CLICKHOUSE_DB` | `sift` | target database (created if missing) |
| `K` | `10` | top-k for Recall@K |
| `SEARCH_LIST_SIZES` | `10 30 50 100 200` | values of the per-query knob to sweep |
| `QUERIES_FOR_QPS` | `1000` | queries used in the QPS measurement (set to 10000 for the published baseline) |
| `MAX_DEGREE` | `64` | constant build-side knob |
| `BUILD_SEARCH_LIST_SIZE` | `100` | constant build-side knob |
| `ALPHA` | `1.2` | constant build-side knob |
| `BEAM_WIDTH` | `4` | constant per-query knob |
| `SEARCH_IO_LIMIT` | `4` | constant per-query knob |

## Output

`results/<utc-timestamp>/sweep.tsv`:

```
search_list_size  recall@10  queries  qps     p50_us  p95_us  p99_us  index_size_mb  build_seconds  diskann_search_count_p50  diskann_search_us_p50
10                ...        1000     ...     ...     ...     ...     ...            ...            ...                       ...
30                ...        1000     ...
...
```

Recall is computed in pure SQL via `arrayIntersect(arraySlice(gt, 1, K), ann_top_k)`.
Latency percentiles come from `clickhouse benchmark --json`. The DiskANN
ProfileEvents medians come from `system.query_log` and serve as a sanity check
that the index actually fired (`diskann_search_count_p50` should be >= 1 for
every row).

## Reading the numbers

DiskANN's NeurIPS 2019 paper reports `recall@10 ≥ 0.95` on SIFT-1M at
moderate `search_list_size`. If a sweep here drops below 0.90 at
`search_list_size=100`, look at the per-query DiskANN ProfileEvents medians
before suspecting the test - low `DiskANNSearchMicroseconds` paired with low
recall usually means the search bailed out early; high values paired with low
recall hint at a build-time defect.

## Known issue (as of `feat-knn-step-3` commit `83448a1`) {#known-issue}

The first end-to-end run of this suite against SIFT-1M does **not** produce a
recall number: `SYSTEM BUILD ANN INDEX sift_base` writes `vectors.fbin`
(~512 MB) successfully, then the DiskANN builder fails inside the PQ pivot
storage path:

```
DiskANN builder_build failed: ANNError: DiskANN(IOError)
No such file or directory (os error 2)
  -- (contrib/diskann/diskann-providers/src/storage/pq_storage.rs:98)
```

Reproducer: download the dataset and run `./recall_qps.sh`. The CPU stays
pegged for ~7 min while `vectors.fbin` is written, the `tmp_ann_*` directory
is then cleaned up, and `BackgroundANNBuildPoolTask` keeps re-queueing the
task until `wait_for_full_coverage` times out at 30 minutes.

Smaller groups exercise the same code path successfully — the existing
stateless tests `04102`–`04107` build ANN groups of 1,000 rows × 16 dim
without issue — so the regression appears at the SIFT-1M shape (1M rows,
128 dim) rather than at any specific DDL knob. `pq_chunks` is *not* a way
out: the DDL parser only overrides `DiskANNBuildOptions::pq_chunks` when the
user passes a non-zero value, and the default of `4` is what triggers the
failing path.

A captured log excerpt is checked in as `data/build_failure.log` (gitignored
together with the rest of `data/`). Raise this with the DiskANN integration
owners before relying on the sweep numbers.
