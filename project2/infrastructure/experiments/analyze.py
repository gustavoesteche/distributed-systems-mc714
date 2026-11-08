#!/usr/bin/env python3
"""Aggregate experiment results: percentiles, variability, message overhead and
plots. Requires matplotlib only for the optional figures; CSV/JSON output is
always produced with the standard library.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Iterable

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception:  # pragma: no cover - optional dependency
    plt = None


def percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * p
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo)


def mean_ci95(values: list[float]) -> tuple[float, float]:
    if not values:
        return float("nan"), float("nan")
    if len(values) == 1:
        return values[0], 0.0
    mean = statistics.fmean(values)
    sd = statistics.stdev(values)
    return mean, 1.96 * sd / math.sqrt(len(values))


def load_run(summary_path: Path) -> dict | None:
    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    run_dir = summary_path.parent
    csv_path = run_dir / "requests.csv"
    latencies: list[float] = []
    successes: list[float] = []
    statuses: dict[str, int] = {}
    if csv_path.exists():
        with csv_path.open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    latency = float(row["latency_ms"])
                except (KeyError, ValueError):
                    continue
                latencies.append(latency)
                status = row.get("status", "")
                statuses[status] = statuses.get(status, 0) + 1
                if status == "201":
                    successes.append(latency)

    counters: dict[str, int] = defaultdict(int)
    for metrics_path in run_dir.glob("metrics-node-*.json"):
        try:
            view = json.loads(metrics_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(view, dict):
            continue
        for name, value in (view.get("counters") or {}).items():
            counters[name] += int(value)

    load = summary.get("load") or {}
    recorded = int(load.get("recorded") or 0)
    conv = summary.get("convergence") or {}
    config = summary.get("config") or {}
    row = {
        "run_id": summary.get("run_id", run_dir.name),
        "suite": config.get("suite", summary.get("name", "").split("-")[0]),
        "nodes": config.get("nodes"),
        "interval": config.get("interval"),
        "rate": config.get("rate"),
        "seed": config.get("seed"),
        "fault": config.get("fault"),
        "fault_node": config.get("fault_node"),
        "latency_ms": config.get("latency_ms"),
        "recorded": recorded,
        "successful": len(successes),
        "errors": int(load.get("errors") or 0),
        "latency_mean_ms": statistics.fmean(latencies) if latencies else float("nan"),
        "latency_p50_ms": percentile(successes, 0.50),
        "latency_p95_ms": percentile(successes, 0.95),
        "latency_p99_ms": percentile(successes, 0.99),
        "convergence_ms": conv.get("convergence_ms"),
        "detection_ms": conv.get("detection_ms"),
        "converged": conv.get("converged"),
        "messages_total": counters.get("messages_sent_total", 0),
        "bytes_total": counters.get("bytes_sent_total", 0),
        "gossip_rounds_total": counters.get("gossip_rounds_total", 0),
        "ops_applied_total": counters.get("ops_applied_total", 0),
        "ops_rejected_total": counters.get("ops_rejected_total", 0),
        "concurrent_ops_total": counters.get("concurrent_ops_detected_total", 0),
        "messages_per_op": (counters.get("messages_sent_total", 0) / recorded) if recorded else None,
        "bytes_per_op": (counters.get("bytes_sent_total", 0) / recorded) if recorded else None,
        "statuses": json.dumps(statuses, sort_keys=True),
    }
    return row


def interval_seconds(value: str | None) -> float:
    if not value:
        return float("nan")
    if value.endswith("ms"):
        return float(value[:-2]) / 1000
    if value.endswith("s"):
        return float(value[:-1])
    if value.endswith("m"):
        return float(value[:-1]) * 60
    return float(value)


def write_csv(path: Path, rows: Iterable[dict]) -> None:
    rows = list(rows)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def group_key(row: dict) -> tuple:
    return (
        row.get("suite"),
        row["nodes"],
        row["interval"],
        row["rate"],
        row["fault"],
        row["latency_ms"],
    )


def aggregate(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        groups[group_key(row)].append(row)
    out = []
    for key, members in sorted(groups.items(), key=lambda kv: str(kv[0])):
        def values(field: str) -> list[float]:
            return [float(m[field]) for m in members if m.get(field) is not None]

        conv = values("convergence_ms")
        conv_mean, conv_ci = mean_ci95(conv)
        det = values("detection_ms")
        det_mean, det_ci = mean_ci95(det)
        msg = values("messages_per_op")
        msg_mean, msg_ci = mean_ci95(msg)
        lat = values("latency_p95_ms")
        lat_mean, lat_ci = mean_ci95(lat)
        out.append({
            "suite": key[0],
            "nodes": key[1],
            "interval": key[2],
            "rate": key[3],
            "fault": key[4],
            "latency_ms": key[5],
            "runs": len(members),
            "convergence_mean_ms": conv_mean,
            "convergence_ci95_ms": conv_ci,
            "convergence_p50_ms": percentile(conv, 0.50),
            "convergence_p95_ms": percentile(conv, 0.95),
            "convergence_p99_ms": percentile(conv, 0.99),
            "detection_mean_ms": det_mean,
            "detection_ci95_ms": det_ci,
            "detection_p50_ms": percentile(det, 0.50),
            "detection_p95_ms": percentile(det, 0.95),
            "detection_p99_ms": percentile(det, 0.99),
            "messages_per_op_mean": msg_mean,
            "messages_per_op_ci95": msg_ci,
            "latency_p95_mean_ms": lat_mean,
            "latency_p95_ci95_ms": lat_ci,
        })
    return out


def plot_series(xs, ys, yerr, xlabel, ylabel, title, path: Path) -> None:
    if plt is None:
        return
    fig, ax = plt.subplots(figsize=(7, 4.2))
    ax.errorbar(xs, ys, yerr=yerr, marker="o", capsize=4, linewidth=1.8)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def make_plots(rows: list[dict], out_dir: Path) -> None:
    if plt is None:
        print("matplotlib not installed; skipping plots")
        return
    plots = out_dir / "plots"
    plots.mkdir(parents=True, exist_ok=True)
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for row in rows:
        groups[group_key(row)].append(row)
    agg = aggregate(rows)

    def series(predicate, xfield, yfield, yerrfield):
        selected = [a for a in agg if predicate(a)]
        if len(selected) < 2:
            return None
        selected.sort(key=lambda a: xfield(a))
        xs = [xfield(a) for a in selected]
        ys = [a[yfield] for a in selected]
        errs = [a[yerrfield] for a in selected]
        return xs, ys, errs

    base = dict(fault="none", latency_ms=0)
    s = series(lambda a: a.get("suite") == "nodes" and all(a[k] == v for k, v in base.items()),
               lambda a: a["nodes"], "convergence_mean_ms", "convergence_ci95_ms")
    if s:
        plot_series(*s, "nodes", "convergence (ms)", "Convergence vs cluster size",
                    plots / "convergence_vs_nodes.png")

    s = series(lambda a: a.get("suite") == "interval",
               lambda a: interval_seconds(a["interval"]), "messages_per_op_mean", "messages_per_op_ci95")
    if s:
        plot_series(*s, "gossip interval (s)", "messages / op", "Message overhead vs gossip interval",
                    plots / "messages_vs_interval.png")

    s = series(lambda a: a.get("suite") == "load",
               lambda a: float(a["rate"]), "latency_p95_mean_ms", "latency_p95_ci95_ms")
    if s:
        plot_series(*s, "offered rate (ops/s)", "p95 latency (ms)", "Latency vs offered load",
                    plots / "latency_vs_load.png")

    fault_rows = [a for a in agg if a.get("suite") == "failure"]
    if fault_rows:
        labels = [str(a["fault"]) for a in fault_rows]
        conv = [a["convergence_mean_ms"] for a in fault_rows]
        det = [a["detection_mean_ms"] for a in fault_rows]
        fig, ax = plt.subplots(figsize=(7, 4.2))
        x = range(len(labels))
        ax.bar([i - 0.2 for i in x], conv, width=0.4, label="recovery/convergence")
        ax.bar([i + 0.2 for i in x], det, width=0.4, label="failure detection")
        ax.set_xticks(list(x), labels)
        ax.set_ylabel("ms")
        ax.set_title("Failure scenarios")
        ax.legend()
        ax.grid(True, axis="y", alpha=0.3)
        fig.tight_layout()
        fig.savefig(plots / "failure_scenarios.png", dpi=150)
        plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path,
                        default=Path(__file__).resolve().parent / "results")
    parser.add_argument("--out", type=Path,
                        default=Path(__file__).resolve().parent / "analysis")
    args = parser.parse_args()

    rows = []
    for summary_path in sorted(args.results.glob("*/summary.json")):
        row = load_run(summary_path)
        if row:
            rows.append(row)
    if not rows:
        raise SystemExit(f"no summaries under {args.results}")

    args.out.mkdir(parents=True, exist_ok=True)
    write_csv(args.out / "runs.csv", rows)
    agg = aggregate(rows)
    write_csv(args.out / "aggregate.csv", agg)
    (args.out / "aggregate.json").write_text(
        json.dumps(agg, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    make_plots(rows, args.out)
    print(f"wrote {len(rows)} runs and {len(agg)} groups to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
