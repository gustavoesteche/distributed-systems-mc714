#!/usr/bin/env python3

import argparse
import csv
from collections import defaultdict
from pathlib import Path

from confidence import mean_ci95

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


STRATEGIES = ("random", "round-robin", "shortest-queue")
COLORS = {
    "random": "#d62728",
    "round-robin": "#1f77b4",
    "shortest-queue": "#2ca02c",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate the simulation plots from the Go CSV output."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("out"),
        help="Directory containing runs.csv and queue_trace.csv.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Plot destination; defaults to --input-dir.",
    )
    return parser.parse_args()


def read_csv(path):
    try:
        with path.open(newline="", encoding="utf-8") as file:
            return list(csv.DictReader(file))
    except FileNotFoundError as error:
        raise SystemExit(f"missing {path}; run 'go run .' first") from error


def parse_service_rates(value):
    rates = [float(rate) for rate in value.split(";")]
    if len(rates) == 1:
        return rates * 3
    if len(rates) != 3:
        raise ValueError(f"expected one or three service rates, got {value!r}")
    return rates


def plot_response_time(run_rows, output_dir):
    stable_rows = [
        row
        for row in run_rows
        if float(row["lambda"]) / 3 < min(parse_service_rates(row["mu"]))
    ]
    if not stable_rows:
        print("response-time plot skipped: no stable configuration")
        return

    service_rates = parse_service_rates(stable_rows[0]["mu"])
    max_lambda = max(float(row["lambda"]) for row in stable_rows)

    figure, axis = plt.subplots(figsize=(9, 6))

    analytical_lambdas = [
        max_lambda * index / 300
        for index in range(301)
    ]
    analytical_response = [
        sum(1 / (rate - lambda_ / 3) for rate in service_rates) / 3
        for lambda_ in analytical_lambdas
    ]
    axis.plot(
        analytical_lambdas,
        analytical_response,
        color="#111111",
        linewidth=2,
        label="Analítico (aleatória)",
    )

    grouped = defaultdict(lambda: defaultdict(list))
    for row in stable_rows:
        grouped[row["strategy"]][float(row["lambda"])].append(float(row["ER"]))

    for strategy in STRATEGIES:
        groups = grouped.get(strategy)
        if not groups:
            continue
        lambdas = sorted(groups)
        statistics = [mean_ci95(groups[lambda_]) for lambda_ in lambdas]
        axis.errorbar(
            lambdas,
            [mean for mean, _ in statistics],
            yerr=[interval for _, interval in statistics],
            color=COLORS[strategy],
            marker="o",
            markersize=5,
            capsize=4,
            linewidth=1.5,
            label=f"{strategy} simulado",
        )

    axis.set(
        title="Tempo médio de resposta: modelo analítico e simulação",
        xlabel=r"Taxa de chegada $\lambda$",
        ylabel=r"$E[R]$",
        xlim=(0, max_lambda * 1.03),
        ylim=(0, None),
    )
    axis.grid(alpha=0.25)
    axis.legend()
    figure.tight_layout()

    path = output_dir / "response_time.png"
    figure.savefig(path, dpi=180)
    plt.close(figure)
    print(f"wrote {path}")


def plot_unstable_queues(trace_rows, output_dir):
    if not trace_rows:
        print("unstable-queue plot skipped: queue_trace.csv has no samples")
        return

    totals = defaultdict(lambda: defaultdict(lambda: [0.0, 0]))
    lambda_value = float(trace_rows[0]["lambda"])
    service_rates = parse_service_rates(trace_rows[0]["mu"])

    for row in trace_rows:
        strategy = row["strategy"]
        time = float(row["time"])
        accumulator = totals[strategy][time]
        accumulator[0] += float(row["N"])
        accumulator[1] += 1

    figure, axis = plt.subplots(figsize=(9, 6))
    max_time = 0.0
    for strategy in STRATEGIES:
        samples = totals.get(strategy)
        if not samples:
            continue
        times = sorted(samples)
        means = [
            samples[time][0] / samples[time][1]
            for time in times
        ]
        max_time = max(max_time, times[-1])
        axis.plot(
            times,
            means,
            color=COLORS[strategy],
            linewidth=1.5,
            label=f"{strategy} (média)",
        )

    fluid_growth = max(0.0, lambda_value - sum(service_rates))
    axis.plot(
        [0, max_time],
        [0, fluid_growth * max_time],
        color="#111111",
        linestyle="--",
        linewidth=2,
        label=f"Aproximação fluida, inclinação={fluid_growth:.2f}",
    )
    axis.set(
        title=f"Crescimento das filas no caso instável ($\\lambda={lambda_value:g}$)",
        xlabel="Tempo simulado",
        ylabel=r"$E[N(t)]$",
        xlim=(0, max_time),
        ylim=(0, None),
    )
    axis.grid(alpha=0.25)
    axis.legend()
    figure.tight_layout()

    path = output_dir / "unstable_queues.png"
    figure.savefig(path, dpi=180)
    plt.close(figure)
    print(f"wrote {path}")


def main():
    args = parse_args()
    output_dir = args.output_dir or args.input_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    run_rows = read_csv(args.input_dir / "runs.csv")
    trace_rows = read_csv(args.input_dir / "queue_trace.csv")
    plot_response_time(run_rows, output_dir)
    plot_unstable_queues(trace_rows, output_dir)


if __name__ == "__main__":
    main()
