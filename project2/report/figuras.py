#!/usr/bin/env python3
"""Gera as figuras (PDF) e a tabela LaTeX do relatório a partir de
experiments/analysis/runs.csv. Uso, a partir de project2/report:

    python3 figuras.py
"""

from __future__ import annotations

import csv
import math
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
RUNS = HERE.parent / "infrastructure" / "experiments" / "analysis" / "runs.csv"
OUT = HERE / "figs"

plt.rcParams.update({"font.size": 8, "figure.figsize": (3.4, 2.1), "axes.grid": True,
                     "grid.alpha": 0.3, "savefig.bbox": "tight"})


def num(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(x) else x


def stats(values):
    values = [v for v in values if v is not None]
    if not values:
        return float("nan"), float("nan")
    if len(values) == 1:
        return values[0], 0.0
    return statistics.fmean(values), 1.96 * statistics.stdev(values) / math.sqrt(len(values))


def suite_groups(rows, suite, key):
    groups = defaultdict(list)
    for r in rows:
        if r["suite"] == suite:
            groups[key(r)].append(r)
    return dict(sorted(groups.items()))


def series(groups, field, scale=1.0):
    xs, ys, es = [], [], []
    for x, members in groups.items():
        m, e = stats([num(r[field]) for r in members])
        xs.append(x)
        ys.append(m * scale)
        es.append(e * scale)
    return xs, ys, es


def save(fig, name):
    fig.savefig(OUT / name)
    plt.close(fig)


def interval_s(v: str) -> float:
    return float(v[:-2]) / 1000 if v.endswith("ms") else float(v[:-1])


def fmt(m, e, digits=1):
    return f"{m:.{digits}f} $\\pm$ {e:.{digits}f}".replace(".", "{,}")


def main() -> None:
    rows = list(csv.DictReader(RUNS.open(encoding="utf-8")))
    OUT.mkdir(exist_ok=True)

    nodes = suite_groups(rows, "nodes", lambda r: int(r["nodes"]))
    interval = suite_groups(rows, "interval", lambda r: interval_s(r["interval"]))
    load = suite_groups(rows, "load", lambda r: float(r["rate"]))
    delay = suite_groups(rows, "delay", lambda r: int(float(r["latency_ms"] or 0)))
    failure = suite_groups(rows, "failure", lambda r: r["fault"])

    # Fig. 1: convergência e bytes de relógio por operação vs N.
    fig, ax = plt.subplots()
    xs, ys, es = series(nodes, "convergence_ms", 1 / 1000)
    ax.errorbar(xs, ys, yerr=es, marker="o", capsize=3, label="convergência (s)")
    ax.set_xlabel("número de nós $N$")
    ax.set_ylabel("convergência (s)", color="C0")
    ax2 = ax.twinx()
    xs, ys, es = series(nodes, "op_clock_bytes_mean")
    ax2.errorbar(xs, ys, yerr=es, marker="s", color="C1", capsize=3, label="bytes do relógio / op")
    ax2.set_ylabel("bytes do relógio / op", color="C1")
    ax2.grid(False)
    ax.set_xticks(xs)
    save(fig, "nos.pdf")

    # Fig. 2: mensagens por operação e convergência vs intervalo.
    fig, ax = plt.subplots()
    xs, ys, es = series(interval, "messages_per_op")
    ax.errorbar(xs, ys, yerr=es, marker="o", capsize=3)
    ax.set_xscale("log", base=2)
    ax.set_xticks(xs, [f"{x:g}".replace(".", ",") for x in xs])
    ax.set_xlabel("intervalo de gossip (s)")
    ax.set_ylabel("mensagens / op", color="C0")
    ax2 = ax.twinx()
    xs, ys, es = series(interval, "convergence_ms", 1 / 1000)
    ax2.errorbar(xs, ys, yerr=es, marker="s", color="C1", capsize=3)
    ax2.set_ylabel("convergência (s)", color="C1")
    ax2.grid(False)
    save(fig, "intervalo.pdf")

    # Fig. 3: percentis de latência vs carga.
    fig, ax = plt.subplots()
    for field, label, marker in (("latency_p50_ms", "p50", "o"), ("latency_p95_ms", "p95", "s"),
                                 ("latency_p99_ms", "p99", "^")):
        xs, ys, es = series(load, field)
        ax.errorbar(xs, ys, yerr=es, marker=marker, capsize=3, label=label)
    ax.set_xlabel("carga oferecida (ops/s)")
    ax.set_ylabel("latência de escrita (ms)")
    ax.set_xticks(xs)
    ax.legend()
    save(fig, "carga.pdf")

    # Tabela: todas as configurações.
    lines = []

    def row(label, members):
        c = stats([num(r["convergence_ms"]) for r in members])
        mp = stats([num(r["messages_per_op"]) for r in members])
        p99 = stats([num(r["latency_p99_ms"]) for r in members])
        ce = stats([num(r["clock_entries_max"]) for r in members])
        cb = stats([num(r["op_clock_bytes_mean"]) for r in members])
        sh = stats([num(r["op_clock_share"]) for r in members])
        ok = sum(r.get("converged") == "True" for r in members)
        lines.append(f"{label} & {fmt(c[0] / 1000, c[1] / 1000, 2)} & {fmt(*mp)} & {fmt(*p99, 2)} & "
                     f"{fmt(*ce, 1)} & {fmt(*cb, 0)} & {fmt(sh[0] * 100, sh[1] * 100, 0)} & "
                     f"{ok}/{len(members)} \\\\")

    for x, m in nodes.items():
        row(f"$N={x}$", m)
    lines.append("\\midrule")
    for x, m in interval.items():
        row(f"int.\\ {x:g}".replace("0.", "0{,}") + "\\,s", m)
    lines.append("\\midrule")
    for x, m in load.items():
        row(f"{x:g} ops/s", m)
    lines.append("\\midrule")
    for x, m in delay.items():
        row(f"atraso {x}\\,ms", m)
    lines.append("\\midrule")
    names = {"partition": "partição", "restart": "reinício"}
    for x, m in failure.items():
        row(names.get(x, x), m)
    (OUT / "tabela.tex").write_text("\n".join(lines) + "\n", encoding="utf-8")

    # Falhas: detecção e recuperação, com percentis.
    flines = []
    for x, m in failure.items():
        det = [num(r["detection_ms"]) for r in m]
        det = [d / 1000 for d in det if d is not None]
        rec = [num(r["convergence_ms"]) / 1000 for r in m if num(r["convergence_ms"]) is not None]
        dm, de = stats(det)
        rm, re_ = stats(rec)
        flines.append(f"{names.get(x, x)} & {fmt(dm, de, 2)} & {fmt(rm, re_, 2)} & "
                      f"{max(rec):.2f} \\\\".replace(".", "{,}") if rec else "")
    (OUT / "falhas.tex").write_text("\n".join(flines) + "\n", encoding="utf-8")
    print(f"figuras e tabelas em {OUT}")


if __name__ == "__main__":
    main()
