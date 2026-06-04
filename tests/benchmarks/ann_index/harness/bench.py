#!/usr/bin/env python3
"""
ANNIndex ANN benchmark harness.

One invocation = one (algorithm, dataset, params) point. Starts a fresh
clickhouse-server in a tmp data directory, loads the dataset, builds the
index, runs the recall sweep, measures search QPS, emits one JSON record.

The same script handles both `diskann` and `spann` — algorithm-specific
shape lives in `algorithms.py`. Build and search parameters are layered:

  algorithm defaults  <  --preset file  <  --build k=v / --search k=v CLI

Append multiple runs to a single `.jsonl` file (--output) for cross-run
comparison with `jq` / pandas.

Usage:
  python3 bench.py --algo diskann --dataset sift1m --preset presets/diskann_sift1m.json
  python3 bench.py --algo spann   --dataset sift1m --preset presets/spann_sift1m.json \\
                   --build head_ratio=0.3 --build replica_count=16
"""
from __future__ import annotations

import argparse
import datetime
import json
import logging
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from algorithms import ALGOS, AlgoSpec  # noqa: E402
from datasets import DATASETS, DatasetSpec, load_dataset  # noqa: E402
from server import ClickHouseServer  # noqa: E402


def parse_kv(values: list[str]) -> dict:
    out: dict = {}
    for v in values or []:
        if "=" not in v:
            raise SystemExit(f"--build/--search expects key=value, got {v!r}")
        k, val = v.split("=", 1)
        # Try int → float → str
        try:
            out[k] = int(val)
            continue
        except ValueError:
            pass
        try:
            out[k] = float(val)
            continue
        except ValueError:
            pass
        out[k] = val
    return out


def merge_params(defaults: dict, preset_section: dict | None, overrides: dict) -> dict:
    merged = dict(defaults)
    merged.update(preset_section or {})
    merged.update(overrides)
    return merged


_METRIC_TO_DISTANCE_FN = {
    "L2": "L2Distance",
    "cosine": "cosineDistance",
}


def _distance_fn(metric: str) -> str:
    try:
        return _METRIC_TO_DISTANCE_FN[metric]
    except KeyError:
        raise SystemExit(f"unsupported metric {metric!r}; expected one of {sorted(_METRIC_TO_DISTANCE_FN)}")


def run_recall(server: ClickHouseServer, dataset: DatasetSpec, algo: AlgoSpec,
               index_name: str, query_count: int, search_settings: dict,
               log_comment_prefix: str) -> tuple[int, int]:
    """Returns (matched_total, queries_ran). recall@10 = matched_total / (queries_ran * 10)."""
    distance_fn = _distance_fn(dataset.metric)
    # Generate per-query recall SELECTs server-side, redirect to a tmpfile.
    gen_sql = (
        "SELECT 'SELECT length(arrayIntersect("
        f"(SELECT groupArray(id) FROM (SELECT id FROM sift_base ORDER BY {distance_fn}(v, "
        "(SELECT v FROM sift_query WHERE id = ' || toString(id) || ')) LIMIT 10)), "
        "(SELECT arraySlice(neighbors, 1, 10) FROM sift_gt WHERE query_id = ' || toString(id) || '))) "
        f"SETTINGS log_comment = ''{log_comment_prefix}' || toString(id) || ''';' "
        "FROM sift_query "
        f"WHERE id < {query_count} ORDER BY id FORMAT TSVRaw"
    )

    with tempfile.TemporaryDirectory() as td:
        tdp = Path(td)
        sql_file = tdp / "recall.sql"
        prelude_lines = [
            "SET allow_experimental_ann_index = 1;",
            "SET enable_ann_index = 1;",
            f"SET force_ann_index = '{index_name}';",
            "SET ann_index_require_match = 1;",
            "SET log_queries = 1;",
        ]
        for k, v in search_settings.items():
            prelude_lines.append(f"SET {k} = {v};")
        # Echo the prelude (session settings + meta-generator) so the run log shows
        # everything except the unrolled per-query recall SELECTs themselves.
        logging.info("SQL (recall prelude, fed to --multiquery) >>>\n%s\n<<<",
                     "\n".join(prelude_lines))
        with sql_file.open("w") as f:
            for line in prelude_lines:
                f.write(line + "\n")
            f.write(server.query(gen_sql))

        argv = server.handle.client_argv() + ["--multiquery"]
        with sql_file.open("rb") as fin:
            proc = subprocess.run(argv, stdin=fin, capture_output=True, text=True)
        if proc.returncode != 0:
            head = "\n".join(proc.stdout.splitlines()[:10])
            tail = "\n".join(proc.stdout.splitlines()[-10:])
            raise RuntimeError(
                f"recall --multiquery failed (rc={proc.returncode})\n"
                f"--- first 10 stdout lines ---\n{head}\n"
                f"--- last 10 stdout lines ---\n{tail}\n"
                f"--- stderr ---\n{proc.stderr}"
            )

    matched = 0
    ran = 0
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        matched += int(line)
        ran += 1
    return matched, ran


_BENCH_QPS_RE = re.compile(r"\bQPS:\s*([0-9]+(?:\.[0-9]+)?)")
_BENCH_PCT_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)%\s+([0-9]+(?:\.[0-9]+)?)\s*sec\.")


def _parse_clickhouse_benchmark_output(stderr_text: str) -> dict:
    """Parse the human-readable report emitted by `clickhouse benchmark`.

    The relevant lines look like:

      ..., queries: 2000, QPS: 1234.567, RPS: ..., MiB/s: ..., ...
      0%       0.001 sec.
      ...
      50%      0.012 sec.
      ...
      99%      0.045 sec.
      99.9%    0.060 sec.

    `--delay 0` is passed so only the final report block is emitted; we still
    take the *last* QPS / percentile values seen as a defensive measure in
    case an intermediate report leaks through.
    """
    qps: float | None = None
    percentiles_sec: dict[float, float] = {}
    for line in stderr_text.splitlines():
        m = _BENCH_QPS_RE.search(line)
        if m:
            qps = float(m.group(1))
            continue
        m = _BENCH_PCT_RE.match(line)
        if m:
            percentiles_sec[float(m.group(1))] = float(m.group(2))
    if qps is None:
        raise RuntimeError(
            "could not find 'QPS:' line in clickhouse-benchmark output:\n" + stderr_text
        )
    needed = {50.0, 95.0, 99.0}
    missing = needed - percentiles_sec.keys()
    if missing:
        raise RuntimeError(
            f"clickhouse-benchmark output missing percentiles {sorted(missing)}:\n{stderr_text}"
        )
    return {
        "qps": round(qps, 2),
        "latency_ms_p50": round(percentiles_sec[50.0] * 1000.0, 3),
        "latency_ms_p95": round(percentiles_sec[95.0] * 1000.0, 3),
        "latency_ms_p99": round(percentiles_sec[99.0] * 1000.0, 3),
    }


def run_qps(server: ClickHouseServer, dataset: DatasetSpec, algo: AlgoSpec, index_name: str,
            search_settings: dict, concurrency: int, iterations: int,
            log_comment: str) -> dict:
    """Replay a representative query at the given concurrency and return QPS + latency percentiles.

    Drives `clickhouse benchmark` over the native TCP protocol: each worker
    keeps a persistent connection (no per-query reconnect) and the server
    reports its own per-query timing, so the measurement is not skewed by
    Python GIL or `http.client` parsing overhead.
    """
    distance_fn = _distance_fn(dataset.metric)
    representative_query = (
        "SELECT id FROM sift_base "
        f"ORDER BY {distance_fn}(v, (SELECT v FROM sift_query WHERE id = 0)) LIMIT 10"
    )
    # Settings supplied via --<name>=<value> are forwarded into the SETTINGS
    # clause by clickhouse-benchmark.
    settings = {
        "allow_experimental_ann_index": 1,
        "enable_ann_index": 1,
        "force_ann_index": index_name,
        "ann_index_require_match": 1,
        "log_queries": 1,
        "log_comment": log_comment,
        **search_settings,
    }

    argv = [
        str(server.handle.binary), "benchmark",
        "--host", "127.0.0.1",
        "--port", str(server.tcp_port),
        "--database", "default",
        "--concurrency", str(concurrency),
        "--iterations", str(iterations),
        "--delay", "0",
        "--query", representative_query,
    ]
    for k, v in settings.items():
        argv.extend([f"--{k}", str(v)])

    logging.info("clickhouse benchmark: concurrency=%d iterations=%d", concurrency, iterations)
    wall_t0 = time.monotonic()
    proc = subprocess.run(argv, capture_output=True, text=True, check=False)
    wall_seconds = time.monotonic() - wall_t0
    if proc.returncode != 0:
        raise RuntimeError(
            f"clickhouse benchmark failed (rc={proc.returncode})\n"
            f"argv: {argv}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )

    # `clickhouse benchmark` writes its report to stdout; older builds wrote
    # to stderr — check both to be safe.
    parsed = _parse_clickhouse_benchmark_output(proc.stdout + "\n" + proc.stderr)
    parsed["wall_seconds"] = round(wall_seconds, 3)
    return parsed


def verify_path_fired(server: ClickHouseServer, algo: AlgoSpec, log_comment_prefix: str,
                      expected_count: int) -> int:
    server.query("SYSTEM FLUSH LOGS query_log")
    out = server.query(f"""
SELECT countIf(ProfileEvents['{algo.search_profile_event}'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND startsWith(log_comment, '{log_comment_prefix}')
  AND event_time > now() - INTERVAL 1 HOUR
""").strip()
    return int(out)


def get_git_commit() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"],
                                       cwd=HERE, text=True).strip()
    except Exception:
        return None


def get_binary_mtime(binary: Path) -> str:
    return datetime.datetime.fromtimestamp(binary.stat().st_mtime,
                                           tz=datetime.timezone.utc).isoformat()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--algo", required=True, choices=sorted(ALGOS.keys()))
    ap.add_argument("--dataset", required=True, choices=sorted(DATASETS.keys()))
    ap.add_argument("--preset", type=Path, help="JSON file with build/search overrides")
    ap.add_argument("--build", action="append", default=[], help="build param override k=v (repeatable)")
    ap.add_argument("--search", action="append", default=[], help="search session setting override k=v (repeatable)")
    ap.add_argument("--query-count", type=int, default=1000, help="queries used for recall (full = 10000)")
    ap.add_argument("--qps-iterations", type=int, default=2000)
    ap.add_argument("--qps-concurrencies", default="1,16", help="comma-separated concurrency list for QPS")
    ap.add_argument("--sync-timeout-sec", type=int, default=3600)
    ap.add_argument("--no-optimize", action="store_true",
                    help="skip OPTIMIZE TABLE FINAL between INSERT and CREATE MI. "
                         "By default the base table is merged to a single part before "
                         "index build, so ANN groups are not fragmented across parts.")
    ap.add_argument("--optimize-timeout-sec", type=int, default=24 * 3600,
                    help="receive_timeout for OPTIMIZE TABLE FINAL (default 24h, for 1B-class merges)")
    ap.add_argument("--index-granularity", type=int, default=1024,
                    help="index_granularity for the sift_base table (default 1024). "
                         "ANN groups are granule-bound, so this directly affects per-granule "
                         "index payload size and per-search granule fan-out. Try 512 / 1024 / 2048 / 8192.")
    ap.add_argument("--binary", type=Path, default=Path("/ch/clickhouse"))
    ap.add_argument("--data-dir", type=Path, default=Path("/tmp/ch-bench"))
    ap.add_argument("--output", type=Path, help="append one JSON line to this path")
    ap.add_argument("--keep-data-dir", action="store_true")
    ap.add_argument("--reuse-index", action="store_true",
                    help="skip load/optimize/CREATE INDEX — connect to an existing data dir "
                         "that already holds sift_base + the ann index. Used for "
                         "search-side sweeps where build params do not change. Implies "
                         "--keep-data-dir.")
    ap.add_argument("--skip-recall", action="store_true",
                    help="skip the per-query recall sweep AND the verify_path_fired check. "
                         "Use when iterating purely on QPS — the JSON record will have "
                         "recall_at_10 / recall_seconds set to null. The path-fired check "
                         "is also skipped, so the operator is responsible for confirming "
                         "the ANN index path is being exercised.")
    args = ap.parse_args()
    if args.reuse_index:
        args.keep_data_dir = True

    algo = ALGOS[args.algo]
    dataset = DATASETS[args.dataset]

    preset = {}
    if args.preset is not None:
        preset = json.loads(args.preset.read_text())

    build_params = merge_params(algo.default_build_params, preset.get("build"), parse_kv(args.build))
    search_settings = merge_params(algo.default_search_settings, preset.get("search"), parse_kv(args.search))

    # Reject build params that disagree with dataset metadata.
    if int(build_params.get("dim", dataset.dim)) != dataset.dim:
        raise SystemExit(f"build dim {build_params.get('dim')} != dataset dim {dataset.dim}")
    build_params["dim"] = dataset.dim
    build_params["metric"] = dataset.metric

    args.data_dir.mkdir(parents=True, exist_ok=True)
    if not args.keep_data_dir:
        # Wipe the *contents* of data_dir, not the directory itself: when the
        # caller bind-mounts a host path here (the recommended workaround for
        # Docker overlayfs not supporting O_DIRECT), the mount point cannot be
        # rmdir'd from inside the container.
        for child in args.data_dir.iterdir():
            if child.is_dir() and not child.is_symlink():
                shutil.rmtree(child)
            else:
                child.unlink()

    # SQL identifier: substitute '-' (e.g. 'gist-960') with '_' so the name does
    # not need backticking everywhere it lands in raw DDL / SYSTEM commands.
    index_name = f"mi_{dataset.name.replace('-', '_')}_{algo.name}"
    log_comment_prefix = f"bench_{algo.name}_q"

    with ClickHouseServer(binary=args.binary, data_dir=args.data_dir,
                          keep_data_dir=args.keep_data_dir) as srv:
        version = srv.query("SELECT version()").strip()
        logging.info("clickhouse %s on port %d", version, srv.tcp_port)

        if args.reuse_index:
            # Sanity-check the data dir already holds the table + index, then skip
            # load / optimize / CREATE. Build params still go into the JSON record
            # for traceability — the operator is responsible for matching them to
            # how the index was actually built.
            row_count = int(srv.query("SELECT count() FROM sift_base").strip() or 0)
            if row_count != dataset.base_count:
                raise SystemExit(
                    f"--reuse-index: sift_base has {row_count} rows, expected {dataset.base_count} "
                    f"({dataset.name}). Was the data dir built for this dataset?")
            idx_count = int(srv.query(
                f"SELECT count() FROM system.ann_indexes "
                f"WHERE database = currentDatabase() AND name = '{index_name}'"
            ).strip() or 0)
            if idx_count == 0:
                raise SystemExit(f"--reuse-index: no ann index named {index_name!r}")
            parts_after_optimize = int(srv.query(
                "SELECT count() FROM system.parts "
                "WHERE database = currentDatabase() AND table = 'sift_base' AND active"
            ).strip() or 0)
            parts_before_optimize = parts_after_optimize
            load_seconds = 0.0
            optimize_seconds = 0.0
            build_seconds = 0.0
            logging.info("reusing existing index %s (rows=%d, parts=%d)",
                         index_name, row_count, parts_after_optimize)
        else:
            t0 = time.monotonic()
            load_dataset(srv, dataset, index_granularity=args.index_granularity)
            load_seconds = time.monotonic() - t0
            logging.info("load: %.1fs", load_seconds)

            # Streaming 1B-class INSERTs lands in hundreds of parts. ANN groups are
            # part-bound, so without consolidation each part would carry its own
            # tiny ANN index and search would fan out across all of them — both a
            # latency hit and a recall hit (per-part top-K' has to be widened to
            # survive the merge). OPTIMIZE FINAL collapses the table to a single
            # part before CREATE REFLECTION so the index covers all rows in
            # one ANN group. Sweepable with --no-optimize for A/B comparison.
            parts_before_optimize = int(srv.query(
                "SELECT count() FROM system.parts "
                "WHERE database = currentDatabase() AND table = 'sift_base' AND active"
            ).strip() or 0)
            if args.no_optimize:
                optimize_seconds = 0.0
                parts_after_optimize = parts_before_optimize
                logging.info("skipping OPTIMIZE (--no-optimize); parts=%d", parts_before_optimize)
            else:
                logging.info("OPTIMIZE TABLE sift_base FINAL (%d active parts before)", parts_before_optimize)
                t0 = time.monotonic()
                srv.query("OPTIMIZE TABLE sift_base FINAL", receive_timeout=args.optimize_timeout_sec)
                optimize_seconds = time.monotonic() - t0
                parts_after_optimize = int(srv.query(
                    "SELECT count() FROM system.parts "
                    "WHERE database = currentDatabase() AND table = 'sift_base' AND active"
                ).strip() or 0)
                logging.info("optimize: %.1fs (parts %d -> %d)",
                             optimize_seconds, parts_before_optimize, parts_after_optimize)

            create_sql = algo.build_create_sql(
                index_name=index_name, source_table="sift_base", indexed_column="v",
                build_params=build_params, sync_timeout_sec=args.sync_timeout_sec,
            )
            logging.info("CREATE REFLECTION %s (%s)", index_name, algo.name)
            srv.query(create_sql, multiquery=True)

            t0 = time.monotonic()
            srv.query(f"SYSTEM SYNC REFLECTION {index_name}",
                      receive_timeout=args.sync_timeout_sec)
            build_seconds = time.monotonic() - t0
            logging.info("build: %.1fs", build_seconds)

        index_bytes_str = srv.query(f"""
SELECT total_bytes_on_disk FROM system.ann_indexes
WHERE database = currentDatabase() AND name = '{index_name}' FORMAT TSV
""").strip()
        index_bytes = int(index_bytes_str) if index_bytes_str else 0

        logging.info("running recall on %d queries", args.query_count)
        if args.skip_recall:
            logging.info("--skip-recall: skipping recall sweep AND verify_path_fired")
            recall_at_10 = None
            recall_seconds = 0.0
        else:
            t0 = time.monotonic()
            matched, ran = run_recall(srv, dataset, algo, index_name, args.query_count,
                                      search_settings, log_comment_prefix)
            recall_seconds = time.monotonic() - t0
            if ran != args.query_count:
                raise SystemExit(f"recall ran={ran} expected={args.query_count}")
            recall_at_10 = matched / (args.query_count * 10.0)
            logging.info("recall@10 = %.4f in %.1fs", recall_at_10, recall_seconds)

            fired = verify_path_fired(srv, algo, log_comment_prefix, args.query_count)
            if fired < args.query_count:
                raise SystemExit(f"only {fired}/{args.query_count} queries fired {algo.search_profile_event}")

        qps_results = {}
        for conc_str in args.qps_concurrencies.split(","):
            conc = int(conc_str.strip())
            logging.info("QPS measurement: concurrency=%d iterations=%d", conc, args.qps_iterations)
            qps_results[f"c{conc}"] = run_qps(srv, dataset, algo, index_name, search_settings,
                                              concurrency=conc, iterations=args.qps_iterations,
                                              log_comment=f"bench_qps_{algo.name}_c{conc}")

        record = {
            "ts": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
            "binary": str(args.binary),
            "binary_mtime": get_binary_mtime(args.binary),
            "git_commit": get_git_commit(),
            "clickhouse_version": version,
            "algo": algo.name,
            "dataset": dataset.name,
            "build_params": build_params,
            "search_settings": search_settings,
            "index_granularity": args.index_granularity,
            "query_count": args.query_count,
            "load_seconds": round(load_seconds, 2),
            "optimize_seconds": round(optimize_seconds, 2),
            "parts_before_optimize": parts_before_optimize,
            "parts_after_optimize": parts_after_optimize,
            "build_seconds": round(build_seconds, 2),
            "recall_seconds": round(recall_seconds, 2),
            "index_bytes": index_bytes,
            "recall_at_10": None if recall_at_10 is None else round(recall_at_10, 6),
            "qps": qps_results,
        }

    print(json.dumps(record, indent=2))
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("a") as f:
            f.write(json.dumps(record) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
