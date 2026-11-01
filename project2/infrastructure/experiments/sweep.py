#!/usr/bin/env python3
"""Run a predefined matrix of experiments and then aggregate the results.

Suites:
  smoke     one short 4-node run, useful as a harness sanity check
  nodes     convergence/messages with 4, 6 and 8 nodes
  interval  gossip interval 250ms, 1s and 4s
  load      offered load 5, 20 and 50 ops/s
  failure   partition and restart, with detection and recovery timing
  delay     toxiproxy latency 0, 50 and 150ms
  all       every suite above
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

EXPERIMENTS_DIR = Path(__file__).resolve().parent
INFRA_DIR = EXPERIMENTS_DIR.parent
IMAGE = "mc714-node:latest"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--suite", choices=["smoke", "nodes", "interval", "load", "failure", "delay", "all"],
                   default="smoke")
    p.add_argument("--reps", type=int, default=None, help="override repetitions per configuration")
    p.add_argument("--duration", default=None, help="override measured duration, e.g. 60s")
    p.add_argument("--warmup", default=None, help="override warm-up, e.g. 10s")
    p.add_argument("--results", type=Path, default=EXPERIMENTS_DIR / "results")
    p.add_argument("--analysis", type=Path, default=EXPERIMENTS_DIR / "analysis")
    p.add_argument("--no-build", action="store_true", help="reuse the current image")
    p.add_argument("--keep", action="store_true", help="keep each cluster up after its run")
    p.add_argument("--no-analyze", action="store_true")
    p.add_argument("--converge-timeout", type=float, default=120.0)
    return p.parse_args()


def build_image() -> None:
    print("+ docker build -t mc714-node:latest .", flush=True)
    subprocess.run(["docker", "build", "-t", IMAGE, "."], cwd=str(INFRA_DIR), check=True)


def run_one(config: dict, results: Path, *, keep: bool, converge_timeout: float) -> int:
    cmd = [sys.executable, str(EXPERIMENTS_DIR / "run.py")]
    for key, value in config.items():
        if value is None:
            continue
        flag = "--" + key.replace("_", "-")
        if isinstance(value, bool):
            if value:
                cmd.append(flag)
        elif isinstance(value, float):
            cmd.extend([flag, f"{value:g}"])
        else:
            cmd.extend([flag, str(value)])
    cmd.extend(["--results-dir", str(results), "--no-build", "--converge-timeout", str(converge_timeout)])
    if keep:
        cmd.append("--keep")
    print("\n" + "=" * 80, flush=True)
    print(" ".join(cmd), flush=True)
    return subprocess.run(cmd, cwd=str(INFRA_DIR)).returncode


def main() -> int:
    args = parse_args()
    if not args.no_build:
        build_image()

    reps = args.reps
    duration = args.duration
    warmup = args.warmup
    specs: list[dict] = []

    if args.suite in ("smoke", "all"):
        specs.append(dict(name="smoke", nodes=4, interval="1s", rate=10,
                          duration=duration or "15s", warmup=warmup or "5s",
                          repetitions=reps or 1))
    if args.suite in ("nodes", "all"):
        for n in (4, 6, 8):
            for rep in range(reps or 5):
                specs.append(dict(name=f"nodes-n{n}-rep{rep+1}", nodes=n, interval="1s", rate=20,
                                  duration=duration or "60s", warmup=warmup or "10s",
                                  seed=100 + rep))
    if args.suite in ("interval", "all"):
        for interval in ("250ms", "1s", "4s"):
            for rep in range(reps or 5):
                specs.append(dict(name=f"interval-{interval}-rep{rep+1}", nodes=6, interval=interval,
                                  rate=20, duration=duration or "60s", warmup=warmup or "10s",
                                  seed=200 + rep))
    if args.suite in ("load", "all"):
        for rate in (5, 20, 50):
            for rep in range(reps or 5):
                specs.append(dict(name=f"load-{rate}-rep{rep+1}", nodes=6, interval="1s", rate=rate,
                                  duration=duration or "60s", warmup=warmup or "10s",
                                  seed=300 + rep))
    if args.suite in ("failure", "all"):
        for fault in ("partition", "restart"):
            for rep in range(reps or 5):
                specs.append(dict(name=f"failure-{fault}-rep{rep+1}", nodes=6, interval="1s", rate=20,
                                  duration=duration or "60s", warmup=warmup or "10s",
                                  seed=400 + rep, fault=fault, fault_node=3,
                                  fault_at=20, heal_at=40))
    if args.suite in ("delay", "all"):
        for latency in (0, 50, 150):
            for rep in range(reps or 5):
                specs.append(dict(name=f"delay-{latency}ms-rep{rep+1}", nodes=6, interval="1s", rate=20,
                                  duration=duration or "60s", warmup=warmup or "10s",
                                  seed=500 + rep, toxiproxy=True, latency_ms=latency,
                                  jitter_ms=10))

    failures = 0
    for index, spec in enumerate(specs, start=1):
        repetition = spec.pop("repetitions", None)
        if repetition and repetition > 1:
            base = dict(spec)
            for rep in range(1, repetition + 1):
                one = dict(base)
                one["name"] = f"{base['name']}-rep{rep}"
                one.setdefault("seed", 100 + rep)
                if run_one(one, args.results, keep=args.keep,
                           converge_timeout=args.converge_timeout) != 0:
                    failures += 1
            continue
        spec.setdefault("seed", 1)
        if run_one(spec, args.results, keep=args.keep,
                   converge_timeout=args.converge_timeout) != 0:
            failures += 1

    if not args.no_analyze:
        subprocess.run([sys.executable, str(EXPERIMENTS_DIR / "analyze.py"),
                        "--results", str(args.results), "--out", str(args.analysis)],
                       cwd=str(INFRA_DIR), check=False)
    if failures:
        print(f"{failures} run(s) failed", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
