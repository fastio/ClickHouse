# SIFT-1M ANN Benchmark — Cookbook

Copy-paste commands for every workflow. Each block runs as-is on this
checkout; if your ClickHouse is on a different host/port, edit the
**connection defaults** block below and re-run.

---

## Connection defaults (set once per shell)

```bash
cd /data/develop/feature-knn-3/benchmarks/ann_sift1m
export CH_BIN=/data/develop/feature-knn-3/build/programs/clickhouse
export CH_HTTP=8124   # HTTP port (sweep driver uses this)
export CH_TCP=9100    # TCP port (recorded into server_meta only)
mkdir -p tmp
```

The sweep driver itself is built on demand by `./run.sh`. Pre-build it
explicitly (optional):

```bash
( cd cmd/sweep && go build -o sweep . )
```

---

## 1. First-time setup

Download the SIFT-1M dataset (~525 MB) and verify it:

```bash
pip install h5py matplotlib   # h5py is required, matplotlib only for --plot
./download.sh
```

Make sure your ClickHouse server is reachable:

```bash
curl -fsS "http://127.0.0.1:${CH_HTTP}/?query=SELECT+version()"
```

If that fails, start a server. The recommended setup is `cc:deploy` (or
any deploy whose `config.xml` exposes both TCP and HTTP and has
`<query_log>` enabled — most installs do by default).

---

## 2. Smoke test (≈ 6 minutes wall-clock)

One scenario, one `sls`, one run, low query count. Useful to confirm
end-to-end plumbing before launching a real sweep.

```bash
./run.sh \
    --http-port="$CH_HTTP" --tcp-port="$CH_TCP" \
    --clickhouse-binary="$CH_BIN" \
    --scenarios=single_group \
    --sls-list=100 \
    --runs=1 --concurrencies=1 \
    --queries-per-cell=200 --warmup-queries=50 \
    2>&1 | tee tmp/smoke_$(date +%s).log
```

Expected last line: `cell single_group_paper_sls=100_conc=1_r=1: recall=0.999... qps=...`

---

## 3. Full sweep (≈ 70 minutes wall-clock)

Default cell matrix: 2 scenarios × 1 build_cfg × 5 sls × 2 conc × 3 runs
= 60 cells across 10 builds.

```bash
LOG=tmp/full_sweep_$(date +%s).log
./run.sh \
    --http-port="$CH_HTTP" --tcp-port="$CH_TCP" \
    --clickhouse-binary="$CH_BIN" \
    --sls-list=10,30,50,100,200 \
    > "$LOG" 2>&1 &
echo "PID=$!  LOG=$LOG"
```

Tail progress:

```bash
tail -f tmp/full_sweep_*.log
```

---

## 4. Single-scenario sweep (≈ 35 minutes)

Skip `multi_group`, sweep all 5 `sls` on `single_group` only:

```bash
LOG=tmp/single_sweep_$(date +%s).log
./run.sh \
    --http-port="$CH_HTTP" --tcp-port="$CH_TCP" \
    --clickhouse-binary="$CH_BIN" \
    --scenarios=single_group \
    --sls-list=10,30,50,100,200 \
    > "$LOG" 2>&1 &
echo "PID=$!  LOG=$LOG"
```

---

## 5. Use the `fast` build config (smaller graph, faster build)

```bash
./run.sh \
    --http-port="$CH_HTTP" --tcp-port="$CH_TCP" \
    --clickhouse-binary="$CH_BIN" \
    --build-cfgs=paper,fast \
    --scenarios=single_group \
    --sls-list=10,30,50,100,200
```

Useful when you want a second curve on the Pareto plot to investigate a
build-time-vs-recall regression.

---

## 6. Generate the markdown report (single run)

```bash
RUN_DIR=$(ls -td results/*/ | head -1)
python3 report/pareto.py "$RUN_DIR/sweep.tsv"
```

Output: Frontier table per `(scenario, build_cfg, conc)` curve +
iso-recall / iso-QPS slices.

---

## 7. Generate the markdown report + plots

One command, three modes covered (frontier curves, iso slices overlaid
as guide lines, optional Δ-vs-baseline bars).

```bash
RUN_DIR=$(ls -td results/*/ | head -1)
python3 report/pareto.py --plot "$RUN_DIR/plots" "$RUN_DIR/sweep.tsv"
```

Outputs:

```
$RUN_DIR/plots/frontier.png    # Recall vs QPS Pareto, faceted by conc
$RUN_DIR/plots/delta.png       # only if --baseline is supplied
```

Suppress markdown, plots only:

```bash
python3 report/pareto.py --plot-only --plot "$RUN_DIR/plots" "$RUN_DIR/sweep.tsv"
```

---

## 8. Compare two runs (Δ-vs-baseline + delta.png)

The canonical PR-review invocation:

```bash
PREV=results/<previous_run_id>/sweep.tsv
CURR=$(ls -td results/*/ | head -1)/sweep.tsv

python3 report/pareto.py \
    --baseline "$PREV" \
    --current  "$CURR" \
    --plot     "$(dirname "$CURR")/plots"
```

The Δ table prints one verdict per cell:
`improvement` (green) / `neutral` (gray) / `regression` (red) / `mixed` (yellow).
Same color coding shows up in `delta.png`.

---

## 9. Inspect a single cell's raw timings

Per-cell latency arrays + JSON QPS dump (for histogram plotting outside
this harness):

```bash
RUN_DIR=$(ls -td results/*/ | head -1)
ls "$RUN_DIR/bench_logs/"
jq '.qps, .p99_us, (.latencies | length)' \
    "$RUN_DIR/bench_logs/single_group_paper_sls=100_beam=4_io=4_conc=1_r=1.json"
```

---

## 10. Troubleshooting

**Connection refused on the HTTP port:**

```bash
curl -fsS "http://127.0.0.1:${CH_HTTP}/?query=SELECT+1"
```

If this fails, the sweep can't start. Either the server isn't running or
HTTP isn't enabled in `config.xml` (`<http_port>8123</http_port>` block).

**`UNKNOWN_TABLE: system.query_log`** during measurement:

The server isn't logging queries; ProfileEvents extraction fails. Check:

```bash
"$CH_BIN" client --port="$CH_TCP" --query "EXISTS TABLE system.query_log"
```

If it returns `0`, add `<query_log>` to your `config.xml` and restart:

```xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <flush_interval_milliseconds>1000</flush_interval_milliseconds>
</query_log>
```

**HDF5 file missing:**

```bash
ls data/sift-128-euclidean.hdf5  || ./download.sh
```

**Build hangs at `ANN coverage 0/1`:**

Normal — `SYSTEM BUILD ANN INDEX` on 1M × 128 takes ~5 min on a 64-core
box. The driver prints progress every 30s; if elapsed exceeds 30 min,
check the server log:

```bash
ls -t tmp/ch_deploy/logs/clickhouse-server.log | head -1 | xargs tail -100
```

**Force rebuild of the Go binary:**

```bash
rm cmd/sweep/sweep
./run.sh --help    # auto-rebuilds
```
