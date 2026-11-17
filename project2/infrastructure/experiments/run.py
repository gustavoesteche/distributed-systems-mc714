#!/usr/bin/env python3
"""Run one reproducible experiment against a Docker Compose cluster.

Outputs under experiments/results/<run-id>/:

  compose.yaml          generated cluster definition
  requests.csv          one row per client request in the measured window
  load-summary.json     workload window, status counts, last write timestamp
  metrics-node-k.json   final /debug/metrics for every node
  state-node-k.json     final /debug/state (full operation log)
  cluster.log           timestamped compose logs
  summary.json          experiment metadata, convergence, fault timing, result
  verification.json     result of verify.py

The script is deliberately dependency-free (Python standard library). Only
Docker, Docker Compose v2 and Go are required.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import compose_gen

INFRA_DIR = Path(__file__).resolve().parents[1]
EXPERIMENTS_DIR = Path(__file__).resolve().parent
BIN_DIR = EXPERIMENTS_DIR / ".bin"
LOADGEN = BIN_DIR / "loadgen"
IMAGE = compose_gen.IMAGE


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def http_json(url: str, timeout: float = 2.0, method: str = "GET", body: dict | None = None):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        if not raw:
            return None
        return json.loads(raw.decode())


def try_json(url: str, timeout: float = 1.0):
    try:
        return http_json(url, timeout=timeout)
    except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError, OSError):
        return None


def run(cmd: list[str], *, cwd: Path = INFRA_DIR, check: bool = True, quiet: bool = False):
    if not quiet:
        print("+ " + " ".join(cmd), flush=True)
    return subprocess.run(cmd, cwd=str(cwd), check=check, text=True, capture_output=True)


def compose(project: str, compose_file: Path, *args: str, check: bool = True):
    return run(
        ["docker", "compose", "-p", project, "-f", str(compose_file),
         "--project-directory", str(INFRA_DIR), *args],
        check=check,
    )


def ensure_image(build: bool) -> None:
    if build:
        run(["docker", "build", "-t", IMAGE, "."])
        return
    if run(["docker", "image", "inspect", IMAGE], check=False).returncode != 0:
        raise SystemExit(f"image {IMAGE} not found; run without --no-build")


def ensure_loadgen() -> None:
    BIN_DIR.mkdir(parents=True, exist_ok=True)
    run(["go", "build", "-o", str(LOADGEN), "./cmd/loadgen"])


def wait_health(ports: list[int], timeout: float) -> None:
    deadline = time.time() + timeout
    pending = set(ports)
    while pending and time.time() < deadline:
        for p in list(pending):
            if try_json(f"http://127.0.0.1:{p}/health") is not None:
                pending.remove(p)
        if pending:
            time.sleep(0.25)
    if pending:
        raise SystemExit(f"nodes did not become healthy: {sorted(pending)}")


def wait_toxiproxy(timeout: float) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if try_json("http://127.0.0.1:8474/version") is not None:
            return
        time.sleep(0.2)
    raise SystemExit("toxiproxy API did not become ready")


def setup_toxiproxy(nodes: int, latency_ms: int, jitter_ms: int) -> None:
    wait_toxiproxy(60)
    for k in range(1, nodes + 1):
        name = f"node-{k}"
        # Remove a stale proxy left by an interrupted previous run.
        try:
            http_json(f"http://127.0.0.1:8474/proxies/{name}", method="DELETE")
        except urllib.error.HTTPError:
            pass
        http_json(
            "http://127.0.0.1:8474/proxies",
            method="POST",
            body={
                "name": name,
                "listen": f"0.0.0.0:{10000 + k}",
                "upstream": f"{name}:8080",
                "enabled": True,
            },
        )
        if latency_ms > 0:
            http_json(
                f"http://127.0.0.1:8474/proxies/{name}/toxics",
                method="POST",
                body={
                    "name": "latency_downstream",
                    "type": "latency",
                    "stream": "downstream",
                    "attributes": {"latency": latency_ms, "jitter": jitter_ms},
                },
            )


def container_id(project: str, compose_file: Path, service: str) -> str:
    out = compose(project, compose_file, "ps", "-q", service).stdout.strip()
    if not out:
        raise SystemExit(f"no container for service {service}")
    return out.splitlines()[0]


def container_network(cid: str) -> str:
    out = run(
        ["docker", "inspect", "-f",
         "{{range $k,$v := .NetworkSettings.Networks}}{{$k}}\n{{end}}", cid],
        quiet=True,
    ).stdout
    names = [n.strip() for n in out.splitlines() if "gossip-net" in n]
    if not names:
        raise SystemExit(f"container {cid} is not attached to a gossip-net network")
    return names[0]


def apply_fault(kind: str, project: str, compose_file: Path, service: str) -> dict:
    if kind == "partition":
        cid = container_id(project, compose_file, service)
        network = container_network(cid)
        run(["docker", "network", "disconnect", network, cid])
        return {"kind": kind, "service": service, "container": cid, "network": network}
    if kind == "restart":
        compose(project, compose_file, "stop", service)
        return {"kind": kind, "service": service}
    raise ValueError(f"unknown fault {kind}")


def heal_fault(fault: dict, project: str, compose_file: Path) -> None:
    if not fault:
        return
    if fault["kind"] == "partition":
        run(["docker", "network", "connect", fault["network"], fault["container"]])
    elif fault["kind"] == "restart":
        compose(project, compose_file, "start", fault["service"])


def peer_unreachable(ports: list[int]) -> bool:
    for p in ports:
        view = try_json(f"http://127.0.0.1:{p}/debug/metrics")
        if not view:
            continue
        for peer in view.get("peers", []):
            if peer.get("state") == "unreachable":
                return True
    return False


def cluster_views(ports: list[int]):
    views = []
    for p in ports:
        view = try_json(f"http://127.0.0.1:{p}/debug/metrics")
        if view is None:
            return None
        views.append(view)
    return views


def is_converged(views) -> bool:
    if not views:
        return False
    first = views[0]
    for view in views:
        if view.get("pending_ops") != 0:
            return False
        if view.get("applied_ops") != first.get("applied_ops"):
            return False
        if view.get("clock") != first.get("clock"):
            return False
        if view.get("quantities") != first.get("quantities"):
            return False
    return True


def wait_converged(ports: list[int], timeout: float) -> tuple[bool, float | None]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        views = cluster_views(ports)
        if views and is_converged(views):
            return True, time.time()
        time.sleep(0.2)
    return False, None


def host_metadata() -> dict:
    def out(cmd: list[str]) -> str:
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout.strip()
        except Exception:
            return ""

    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "python": sys.version.split()[0],
        "go": out(["go", "version"]),
        "docker": out(["docker", "version", "--format", "{{.Server.Version}}"]),
        "compose": out(["docker", "compose", "version", "--short"]),
        "image_id": out(["docker", "image", "inspect", "-f", "{{.Id}}", IMAGE]),
        "uname": out(["uname", "-a"]),
    }


def duration_seconds(value: str) -> float:
    value = value.strip()
    if value.endswith("ms"):
        return float(value[:-2]) / 1000
    if value.endswith("s"):
        return float(value[:-1])
    if value.endswith("m"):
        return float(value[:-1]) * 60
    return float(value)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--name", default="run", help="short run name used in the directory")
    p.add_argument("--nodes", type=int, default=4)
    p.add_argument("--interval", default="1s", help="GOSSIP_INTERVAL, e.g. 250ms, 1s, 4s")
    p.add_argument("--extra-edges", type=int, default=1)
    p.add_argument("--topology-seed", type=int, default=42)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--fanout", type=int, default=1, help="neighbors contacted per round (0 = all)")
    p.add_argument("--gossip-seed", type=int, default=None, help="peer-selection seed (default: --seed)")
    p.add_argument("--rate", type=float, default=10)
    p.add_argument("--duration", default="20s")
    p.add_argument("--warmup", default="5s")
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--remove-ratio", type=float, default=0.2)
    p.add_argument("--products", type=int, default=50)
    p.add_argument("--initial-stock", type=int, default=1000)
    p.add_argument("--timeout", default="2s")
    p.add_argument("--bootstrap-settle", default=None, help="default: max(2s, interval*(nodes+1))")
    p.add_argument("--fault", choices=["none", "partition", "restart"], default="none")
    p.add_argument("--fault-node", type=int, default=3)
    p.add_argument("--fault-at", type=float, default=10.0, help="seconds after the workload window starts (warm-up included)")
    p.add_argument("--heal-at", type=float, default=20.0, help="seconds after the workload window starts (warm-up included)")
    p.add_argument("--toxiproxy", action="store_true", help="route gossip through toxiproxy")
    p.add_argument("--latency-ms", type=int, default=0)
    p.add_argument("--jitter-ms", type=int, default=0)
    p.add_argument("--results-dir", type=Path, default=EXPERIMENTS_DIR / "results")
    p.add_argument("--no-build", action="store_true", help="reuse the mc714-node:latest image")
    p.add_argument("--keep", action="store_true", help="keep the cluster up after the run")
    p.add_argument("--converge-timeout", type=float, default=120.0)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.fault != "none" and args.heal_at <= args.fault_at:
        raise SystemExit("--heal-at must be greater than --fault-at")
    if not 1 <= args.fault_node <= args.nodes:
        raise SystemExit("--fault-node must be within the cluster")

    if args.gossip_seed is None:
        args.gossip_seed = args.seed
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"{stamp}-{args.name}"
    run_dir = args.results_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    compose_file = run_dir / "compose.yaml"
    compose_gen.write(
        compose_file,
        nodes=args.nodes,
        interval=args.interval,
        extra_edges=args.extra_edges,
        topology_seed=args.topology_seed,
        fanout=args.fanout,
        gossip_seed=args.gossip_seed,
        toxiproxy=args.toxiproxy,
    )

    project = f"mc714-exp-{stamp.lower()}-{args.name.lower()}".replace("_", "-")
    ports = [8000 + k for k in range(1, args.nodes + 1)]
    targets = [f"http://127.0.0.1:{p}" for p in ports]
    started_at = now_iso()
    fault: dict = {}
    detection_at: float | None = None
    converged = False
    converged_at: float | None = None
    load_summary: dict = {}
    summary: dict = {}
    load_proc: subprocess.Popen | None = None

    ensure_image(build=not args.no_build)
    ensure_loadgen()

    try:
        compose(project, compose_file, "up", "-d")
        wait_health(ports, timeout=90)
        if args.toxiproxy:
            setup_toxiproxy(args.nodes, args.latency_ms, args.jitter_ms)

        bootstrap_settle = args.bootstrap_settle
        if bootstrap_settle is None:
            bootstrap_settle = f"{max(2.0, duration_seconds(args.interval) * (args.nodes + 1)):g}s"

        load_cmd = [
            str(LOADGEN),
            "--targets", ",".join(targets),
            "--rate", str(args.rate),
            "--duration", args.duration,
            "--warmup", args.warmup,
            "--concurrency", str(args.concurrency),
            "--seed", str(args.seed),
            "--remove-ratio", str(args.remove_ratio),
            "--products", str(args.products),
            "--initial-stock", str(args.initial_stock),
            "--timeout", args.timeout,
            "--bootstrap-settle", bootstrap_settle,
            "--out", str(run_dir / "requests.csv"),
            "--summary", str(run_dir / "load-summary.json"),
        ]
        print("+ " + " ".join(load_cmd), flush=True)
        load_proc = subprocess.Popen(load_cmd, cwd=str(INFRA_DIR), text=True,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        load_start = time.time()

        total_window = duration_seconds(args.warmup) + duration_seconds(args.duration)
        workload_start = load_start + duration_seconds(bootstrap_settle)
        if args.fault != "none" and args.fault_at < total_window and load_proc.poll() is None:
            wait_until = workload_start + args.fault_at
            time.sleep(max(0.0, wait_until - time.time()))
            fault = apply_fault(args.fault, project, compose_file, f"node-{args.fault_node}")
            fault["at_unix"] = time.time()
            fault["at_seconds"] = args.fault_at
            print(f"fault injected: {fault}", flush=True)

            heal_at = workload_start + args.heal_at
            while time.time() < heal_at:
                if detection_at is None and peer_unreachable(ports):
                    detection_at = time.time()
                time.sleep(0.2)
            heal_fault(fault, project, compose_file)
            fault["heal_unix"] = time.time()
            fault["heal_seconds"] = args.heal_at
            print(f"fault healed: {fault}", flush=True)
        elif args.fault != "none":
            print("fault skipped: workload already finished or --fault-at is outside the window", flush=True)

        load_timeout = (duration_seconds(bootstrap_settle) + duration_seconds(args.warmup)
                        + duration_seconds(args.duration) + 120.0)
        try:
            stdout, stderr = load_proc.communicate(timeout=max(30.0, load_timeout))
        except subprocess.TimeoutExpired:
            load_proc.kill()
            stdout, stderr = load_proc.communicate()
            raise SystemExit(f"loadgen timed out after {load_timeout:.0f}s")
        (run_dir / "loadgen.stdout.log").write_text(stdout, encoding="utf-8")
        (run_dir / "loadgen.stderr.log").write_text(stderr, encoding="utf-8")
        if load_proc.returncode != 0:
            raise SystemExit(f"loadgen failed with {load_proc.returncode}: {stderr[-2000:]}")

        load_summary = json.loads((run_dir / "load-summary.json").read_text(encoding="utf-8"))
        converged, converged_at = wait_converged(ports, args.converge_timeout)

        for k, port in enumerate(ports, start=1):
            view = try_json(f"http://127.0.0.1:{port}/debug/metrics")
            (run_dir / f"metrics-node-{k}.json").write_text(
                json.dumps(view, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            state = try_json(f"http://127.0.0.1:{port}/debug/state")
            (run_dir / f"state-node-{k}.json").write_text(
                json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        logs = compose(project, compose_file, "logs", "-t", "--no-color", check=False)
        (run_dir / "cluster.log").write_text(logs.stdout, encoding="utf-8")

        last_write = load_summary.get("last_write_unix_nano") or 0
        reference = max(last_write / 1e9, load_summary.get("recorded_to_unix_nano", 0) / 1e9)
        if fault.get("heal_unix"):
            reference = max(reference, fault["heal_unix"])
        convergence_ms = (converged_at - reference) * 1000 if converged and converged_at else None
        detection_ms = None
        if detection_at and fault.get("at_unix"):
            detection_ms = (detection_at - fault["at_unix"]) * 1000

        summary = {
            "run_id": run_id,
            "name": args.name,
            "suite": args.name.split("-")[0],
            "started_at": started_at,
            "finished_at": now_iso(),
            "config": {
                "nodes": args.nodes,
                "interval": args.interval,
                "extra_edges": args.extra_edges,
                "topology_seed": args.topology_seed,
                "seed": args.seed,
                "fanout": args.fanout,
                "gossip_seed": args.gossip_seed,
                "rate": args.rate,
                "duration": args.duration,
                "warmup": args.warmup,
                "concurrency": args.concurrency,
                "bootstrap_settle": bootstrap_settle,
                "remove_ratio": args.remove_ratio,
                "products": args.products,
                "toxiproxy": args.toxiproxy,
                "latency_ms": args.latency_ms,
                "jitter_ms": args.jitter_ms,
                "fault": args.fault,
                "fault_node": args.fault_node,
                "fault_at": args.fault_at,
                "heal_at": args.heal_at,
            },
            "host": host_metadata(),
            "load": load_summary,
            "fault": fault,
            "convergence": {
                "converged": converged,
                "converged_at_unix": converged_at,
                "convergence_ms": convergence_ms,
                "detection_ms": detection_ms,
            },
        }

        (run_dir / "summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        verify = run([sys.executable, str(EXPERIMENTS_DIR / "verify.py"), str(run_dir)],
                     check=False)
        (run_dir / "verification.json").write_text(
            json.dumps({
                "returncode": verify.returncode,
                "stdout": verify.stdout,
                "stderr": verify.stderr,
            }, indent=2) + "\n", encoding="utf-8")
        summary["verification"] = {"ok": verify.returncode == 0, "returncode": verify.returncode}
        (run_dir / "summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        print(json.dumps({
            "run_dir": str(run_dir),
            "converged": converged,
            "convergence_ms": convergence_ms,
            "detection_ms": detection_ms,
            "verified": verify.returncode == 0,
        }, indent=2), flush=True)
        return 0 if converged and verify.returncode == 0 else 2
    finally:
        if load_proc is not None and load_proc.poll() is None:
            load_proc.kill()
            load_proc.communicate()
        if not args.keep:
            compose(project, compose_file, "down", "-v", "--remove-orphans", check=False)


if __name__ == "__main__":
    raise SystemExit(main())
