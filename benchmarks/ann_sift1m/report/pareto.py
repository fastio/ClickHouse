#!/usr/bin/env python3
"""Generate Pareto / Δ-vs-baseline / iso-recall reports from sweep.tsv.

Three output modes (only the first is shown if `--current` is the only input):

  1. Single-run frontier table - per (scenario, build_cfg, concurrency) prints
     a Recall ↑ ordered table with QPS / latency / index size, marking each
     row that lies on the Pareto frontier (no other row dominates it on
     both Recall and QPS).

  2. Δ-vs-baseline table - given two TSVs, joins on
     (scenario, build_cfg, sls, beam, io_limit, concurrency) and prints
     Δrecall, ΔQPS%, Δp99%, with a textual verdict per row.

  3. Iso-recall / iso-QPS slices - linear-interpolates each curve to the
     interesting recall (0.90, 0.95, 0.99) and QPS (1000, 3000, 8000) levels;
     emits a one-row-per-anchor table for the headline numbers a PR
     description should quote.

Run-idx folding: rows with the same (scenario, build_cfg, sls, beam, io_limit,
concurrency) but different `run_idx` are folded into mean ± stddev. Recall is
expected to be deterministic if hash_seed is pinned; QPS is not, so any
recall stddev > 0.001 is flagged.

Usage:
    pareto.py <sweep.tsv>                                  # mode 1 only
    pareto.py --baseline prev.tsv --current curr.tsv       # modes 1 + 2 + 3
    pareto.py --current curr.tsv                           # mode 1 + 3
    pareto.py --current curr.tsv --json                    # machine-readable
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from dataclasses import dataclass, field
from statistics import mean, stdev
from typing import Dict, Iterable, List, Optional, Tuple

# Group key: everything that is NOT recall/qps/latency/run_idx. Two TSV rows
# with the same key are different repetitions of the same cell.
GROUP_KEYS = ("scenario", "build_cfg", "sls", "beam", "io_limit", "concurrency")
# Curve key: everything that varies along the trade-off curve goes into the
# ROW key (`sls`); everything else identifies which curve we're on.
CURVE_KEYS = ("scenario", "build_cfg", "concurrency")

# Verdict thresholds. Tuned for SIFT-1M's typical run-to-run noise.
RECALL_NOISE = 0.005
QPS_NOISE_PCT = 3.0


@dataclass
class Cell:
    scenario: str
    build_cfg: str
    sls: int
    beam: int
    io_limit: int
    concurrency: int
    runs: int = 0
    recall: float = 0.0
    recall_std: float = 0.0
    qps: float = 0.0
    qps_std: float = 0.0
    p50_us: float = 0.0
    p95_us: float = 0.0
    p99_us: float = 0.0
    build_seconds: float = 0.0
    index_size_mb: float = 0.0
    ann_groups: int = 0
    diskann_search_count_p50: float = 0.0
    diskann_search_us_p50: float = 0.0
    notes: List[str] = field(default_factory=list)

    @property
    def group_key(self) -> Tuple:
        return (self.scenario, self.build_cfg, self.sls, self.beam, self.io_limit, self.concurrency)

    @property
    def curve_key(self) -> Tuple:
        return (self.scenario, self.build_cfg, self.concurrency)


def _to_float(value: str, default: float = 0.0) -> float:
    if value is None or value == "" or value == "\\N":
        return default
    try:
        return float(value)
    except ValueError:
        return default


def load_tsv(path: str) -> List[Cell]:
    """Load a sweep.tsv and fold (run_idx) repetitions into mean / stddev."""
    raw: Dict[Tuple, List[Dict[str, str]]] = {}
    with open(path, newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            key = (
                row["scenario"], row["build_cfg"], int(row["sls"]),
                int(row["beam"]), int(row["io_limit"]), int(row["concurrency"]),
            )
            raw.setdefault(key, []).append(row)

    out: List[Cell] = []
    for key, rows in raw.items():
        scenario, build_cfg, sls, beam, io_limit, concurrency = key
        recalls = [_to_float(r["recall"]) for r in rows]
        qpss = [_to_float(r["qps"]) for r in rows]
        cell = Cell(
            scenario=scenario,
            build_cfg=build_cfg,
            sls=sls,
            beam=beam,
            io_limit=io_limit,
            concurrency=concurrency,
            runs=len(rows),
            recall=mean(recalls) if recalls else 0.0,
            recall_std=stdev(recalls) if len(recalls) > 1 else 0.0,
            qps=mean(qpss) if qpss else 0.0,
            qps_std=stdev(qpss) if len(qpss) > 1 else 0.0,
            p50_us=mean(_to_float(r["p50_us"]) for r in rows),
            p95_us=mean(_to_float(r["p95_us"]) for r in rows),
            p99_us=mean(_to_float(r["p99_us"]) for r in rows),
            build_seconds=mean(_to_float(r["build_seconds"]) for r in rows),
            index_size_mb=mean(_to_float(r["index_size_mb"]) for r in rows),
            ann_groups=int(_to_float(rows[0].get("ann_groups", "0"))),
            diskann_search_count_p50=mean(_to_float(r["diskann_search_count_p50"]) for r in rows),
            diskann_search_us_p50=mean(_to_float(r["diskann_search_us_p50"]) for r in rows),
            notes=sorted({n for r in rows for n in (r.get("notes") or "").split(",") if n}),
        )
        out.append(cell)
    out.sort(key=lambda c: (c.curve_key, c.sls))
    return out


def pareto_frontier(curve: List[Cell]) -> List[bool]:
    """Mark rows that lie on the Recall-up / QPS-up Pareto frontier.

    A row is on the frontier iff no other row in the same curve has both
    recall >= self.recall AND qps >= self.qps with at least one strictly >.
    """
    on_frontier: List[bool] = []
    for i, c in enumerate(curve):
        dominated = False
        for j, other in enumerate(curve):
            if i == j:
                continue
            if (other.recall >= c.recall and other.qps >= c.qps
                    and (other.recall > c.recall or other.qps > c.qps)):
                dominated = True
                break
        on_frontier.append(not dominated)
    return on_frontier


def linear_interp_x_for_y(curve: List[Tuple[float, float]], y_target: float) -> Optional[float]:
    """Given a list of (x, y) points sorted by x, return the x at which a
    piecewise-linear interpolation crosses y = y_target. Returns None if
    y_target is outside the [min y, max y] range of the curve.
    """
    pts = sorted(curve, key=lambda p: p[0])
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        lo, hi = (y0, y1) if y0 <= y1 else (y1, y0)
        if lo <= y_target <= hi:
            if y1 == y0:
                return (x0 + x1) / 2.0
            t = (y_target - y0) / (y1 - y0)
            return x0 + t * (x1 - x0)
    return None


def by_curve(cells: Iterable[Cell]) -> Dict[Tuple, List[Cell]]:
    out: Dict[Tuple, List[Cell]] = {}
    for c in cells:
        out.setdefault(c.curve_key, []).append(c)
    for v in out.values():
        v.sort(key=lambda c: c.sls)
    return out


# --------------------------------------------------------------------- mode 1
def render_frontier(cells: List[Cell]) -> str:
    lines = ["## Frontier per curve", ""]
    for curve_key, curve in sorted(by_curve(cells).items()):
        scenario, build_cfg, conc = curve_key
        lines.append(f"### scenario=`{scenario}`  build=`{build_cfg}`  conc={conc}")
        lines.append("")
        # Mean ± stddev compaction; columns aligned for grep-ability.
        header = ("sls", "recall@K", "qps", "p99_us", "index_mb",
                  "ann_grps", "search_cnt", "build_s", "pareto", "notes")
        lines.append("| " + " | ".join(header) + " |")
        lines.append("|" + "|".join("---" for _ in header) + "|")
        on = pareto_frontier(curve)
        for c, mark in zip(curve, on):
            lines.append("| " + " | ".join([
                str(c.sls),
                f"{c.recall:.4f}" + (f" ± {c.recall_std:.4f}" if c.recall_std else ""),
                f"{c.qps:.1f}" + (f" ± {c.qps_std:.1f}" if c.qps_std else ""),
                f"{int(c.p99_us)}",
                f"{c.index_size_mb:.1f}",
                str(c.ann_groups),
                f"{c.diskann_search_count_p50:.1f}",
                f"{int(c.build_seconds)}",
                "★" if mark else "",
                ",".join(c.notes) or "-",
            ]) + " |")
        lines.append("")
    return "\n".join(lines)


# --------------------------------------------------------------------- mode 2
def verdict(d_recall: float, d_qps_pct: float) -> str:
    if abs(d_recall) < RECALL_NOISE and abs(d_qps_pct) < QPS_NOISE_PCT:
        return "neutral"
    if d_recall < -RECALL_NOISE or d_qps_pct < -QPS_NOISE_PCT:
        if d_recall > RECALL_NOISE or d_qps_pct > QPS_NOISE_PCT:
            return "mixed"
        return "regression"
    return "improvement"


def render_delta(baseline: List[Cell], current: List[Cell]) -> str:
    base_by_key = {c.group_key: c for c in baseline}
    cur_by_key = {c.group_key: c for c in current}
    common = sorted(set(base_by_key) & set(cur_by_key))
    only_base = sorted(set(base_by_key) - set(cur_by_key))
    only_cur = sorted(set(cur_by_key) - set(base_by_key))

    lines = ["## Δ vs baseline", ""]
    if only_base or only_cur:
        if only_base:
            lines.append(f"_baseline-only cells (skipped):_ {len(only_base)}")
        if only_cur:
            lines.append(f"_current-only cells (skipped):_ {len(only_cur)}")
        lines.append("")

    header = ("scenario", "build", "sls", "conc", "ΔRecall", "ΔQPS%", "Δp99%", "Δindex_mb", "verdict", "notes")
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|" + "|".join("---" for _ in header) + "|")
    for key in common:
        b = base_by_key[key]
        c = cur_by_key[key]
        d_recall = c.recall - b.recall
        d_qps_pct = (c.qps - b.qps) / b.qps * 100.0 if b.qps else 0.0
        d_p99_pct = (c.p99_us - b.p99_us) / b.p99_us * 100.0 if b.p99_us else 0.0
        d_index = c.index_size_mb - b.index_size_mb
        v = verdict(d_recall, d_qps_pct)
        notes = ",".join(sorted(set(b.notes) | set(c.notes))) or "-"
        lines.append("| " + " | ".join([
            c.scenario, c.build_cfg, str(c.sls), str(c.concurrency),
            f"{d_recall:+.4f}",
            f"{d_qps_pct:+.1f}%",
            f"{d_p99_pct:+.1f}%",
            f"{d_index:+.1f}",
            v, notes,
        ]) + " |")
    lines.append("")
    return "\n".join(lines)


# --------------------------------------------------------------------- mode 3
ISO_RECALL_TARGETS = (0.90, 0.95, 0.99)
ISO_QPS_TARGETS = (1000.0, 3000.0, 8000.0)


def render_iso(cells_curr: List[Cell], cells_base: Optional[List[Cell]]) -> str:
    lines = ["## Iso-recall / iso-QPS slices", ""]
    cur_by_curve = by_curve(cells_curr)
    base_by_curve = by_curve(cells_base) if cells_base else {}

    for curve_key in sorted(cur_by_curve):
        scenario, build_cfg, conc = curve_key
        cur = cur_by_curve[curve_key]
        base = base_by_curve.get(curve_key, [])
        lines.append(f"### scenario=`{scenario}`  build=`{build_cfg}`  conc={conc}")
        lines.append("")
        # iso-recall: x = QPS, y = recall
        cur_xy_qps = [(c.qps, c.recall) for c in cur]
        base_xy_qps = [(c.qps, c.recall) for c in base]
        # iso-qps:    x = recall, y = qps
        cur_xy_recall = [(c.recall, c.qps) for c in cur]
        base_xy_recall = [(c.recall, c.qps) for c in base]

        rows: List[List[str]] = []
        for r_target in ISO_RECALL_TARGETS:
            cur_qps = linear_interp_x_for_y(cur_xy_qps, r_target)
            base_qps = linear_interp_x_for_y(base_xy_qps, r_target) if base else None
            rows.append([
                f"iso-recall@{r_target:.2f}",
                f"{cur_qps:.0f} qps" if cur_qps is not None else "n/a",
                f"{base_qps:.0f} qps" if base_qps is not None else "-",
                f"{(cur_qps - base_qps) / base_qps * 100:+.1f}%"
                    if (base_qps is not None and cur_qps is not None and base_qps > 0)
                    else "-",
            ])
        for q_target in ISO_QPS_TARGETS:
            cur_r = linear_interp_x_for_y(cur_xy_recall, q_target)
            base_r = linear_interp_x_for_y(base_xy_recall, q_target) if base else None
            rows.append([
                f"iso-qps@{int(q_target)}",
                f"{cur_r:.4f}" if cur_r is not None else "n/a",
                f"{base_r:.4f}" if base_r is not None else "-",
                f"{(cur_r - base_r):+.4f}"
                    if (base_r is not None and cur_r is not None)
                    else "-",
            ])

        header = ("anchor", "current", "baseline", "Δ")
        lines.append("| " + " | ".join(header) + " |")
        lines.append("|" + "|".join("---" for _ in header) + "|")
        for r in rows:
            lines.append("| " + " | ".join(r) + " |")
        lines.append("")
    return "\n".join(lines)


# --------------------------------------------------------------------- main
def main() -> int:
    p = argparse.ArgumentParser(description="ANN SIFT-1M Pareto report")
    p.add_argument("current", nargs="?", help="path to sweep.tsv (positional shortcut for --current)")
    p.add_argument("--current", dest="current_flag", help="path to sweep.tsv (current run)")
    p.add_argument("--baseline", help="path to a previous sweep.tsv to diff against")
    p.add_argument("--json", action="store_true", help="emit machine-readable JSON instead of markdown")
    args = p.parse_args()

    current_path = args.current_flag or args.current
    if not current_path:
        p.error("must provide a current sweep.tsv (positional or via --current)")

    cells_curr = load_tsv(current_path)
    cells_base = load_tsv(args.baseline) if args.baseline else None

    # Determinism warning - if hash_seed is pinned, recall_std should be 0.
    flaky = [c for c in cells_curr if c.recall_std > 0.001]
    if flaky:
        print(f"# warning: {len(flaky)} cells have recall stddev > 0.001 across runs (hash_seed not effective?)", file=sys.stderr)

    if args.json:
        payload = {
            "current": [c.__dict__ for c in cells_curr],
            "baseline": [c.__dict__ for c in cells_base] if cells_base else None,
        }
        json.dump(payload, sys.stdout, indent=2, default=str)
        return 0

    print(f"# SIFT-1M ANN sweep report")
    print(f"")
    print(f"_current:_ `{current_path}`  ({len(cells_curr)} cells)")
    if args.baseline:
        print(f"_baseline:_ `{args.baseline}`  ({len(cells_base or [])} cells)")
    print(f"")

    print(render_frontier(cells_curr))
    if cells_base:
        print(render_delta(cells_base, cells_curr))
    print(render_iso(cells_curr, cells_base))
    return 0


if __name__ == "__main__":
    sys.exit(main())
