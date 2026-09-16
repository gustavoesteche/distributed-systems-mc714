#!/usr/bin/env python3
"""Mean and 95% confidence interval of each metric across the replications.

Reads the per-run data (runs.csv) produced by the Go simulator and writes
results_ci.csv with one row per (strategy, lambda): the sample mean and the
Student-t 95% half-width of X, E[R], E[N], E[Tq], and U1..U3.
"""

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path

METRICS = ("X", "ER", "EN", "ETq", "U1", "U2", "U3")

# Two-sided Student-t critical values at 95% by degrees of freedom.
# 10 replications use t(9) = 2.262; larger samples fall back to the normal.
T_CRITICAL = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
    6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262, 10: 2.228,
    11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131,
    16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086,
    21: 2.080, 22: 2.074, 23: 2.069, 24: 2.064, 25: 2.060,
    26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042,
}


def mean_ci95(values):
    """Return (mean, half-width) of the 95% confidence interval."""
    mean = sum(values) / len(values)
    if len(values) == 1:
        return mean, 0.0

    variance = sum((value - mean) ** 2 for value in values) / (len(values) - 1)
    standard_error = math.sqrt(variance / len(values))
    t_critical = T_CRITICAL.get(len(values) - 1, 1.96)
    return mean, t_critical * standard_error


def parse_args():
    parser = argparse.ArgumentParser(
        description="Summarize runs.csv into means with 95% confidence intervals."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("out"),
        help="Directory containing runs.csv.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output CSV path; defaults to <input-dir>/results_ci.csv.",
    )
    return parser.parse_args()


def read_runs(path):
    try:
        with path.open(newline="", encoding="utf-8") as file:
            return list(csv.DictReader(file))
    except FileNotFoundError as error:
        raise SystemExit(f"missing {path}; run 'go run .' first") from error


def main():
    args = parse_args()
    output = args.output or args.input_dir / "results_ci.csv"
    rows = read_runs(args.input_dir / "runs.csv")

    groups = defaultdict(list)
    for row in rows:
        groups[(row["strategy"], float(row["lambda"]), row["mu"])].append(row)

    header = ["strategy", "lambda", "mu", "runs"]
    for metric in METRICS:
        header += [f"{metric}_mean", f"{metric}_ci95"]

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        for (strategy, lambda_, mu), group in sorted(
            groups.items(), key=lambda item: (item[0][1], item[0][0])
        ):
            record = [strategy, f"{lambda_:g}", mu, len(group)]
            for metric in METRICS:
                values = [
                    float(row[metric]) for row in group if row[metric] != ""
                ]
                if not values:
                    record += ["", ""]
                    continue
                mean, interval = mean_ci95(values)
                record += [f"{mean:.6g}", f"{interval:.3g}"]
            writer.writerow(record)

    print(f"wrote {output}")


if __name__ == "__main__":
    main()
