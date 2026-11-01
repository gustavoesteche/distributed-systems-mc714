# Experiments

This directory contains the reproducible experimental harness for project 2.
It generates an N-node Compose cluster, runs a seeded client workload, injects
crashes/partitions (and optional network latency), measures convergence and
recovery, records raw data, and checks the correctness invariants from the
final node state.

The harness is plain Python 3 (standard library) plus Docker Compose v2 and Go.
`matplotlib` is optional, used only by `analyze.py` to draw the plots:

```bash
python3 -m pip install -r experiments/requirements.txt
```

## Quick start

Docker must be running. From `project2/infrastructure`:

```bash
# One short 4-node sanity run (about 25 s)
python3 experiments/sweep.py --suite smoke

# The full required matrix: nodes, interval, load, failure, delay
python3 experiments/sweep.py --suite all
```

`just exp-smoke`, `just exp-nodes`, `just exp-all` and `just analyze` are
shortcuts for the same commands.

A single configuration can be run directly:

```bash
python3 experiments/run.py \
  --name manual --nodes 6 --interval 1s --rate 20 \
  --duration 60s --warmup 10s --seed 7
```

To use network latency through Toxiproxy:

```bash
python3 experiments/run.py \
  --name delay-50 --nodes 6 --toxiproxy --latency-ms 50 --jitter-ms 10
```

To run a partition or a crash/restart:

```bash
python3 experiments/run.py --name partition --nodes 6 \
  --fault partition --fault-node 3 --fault-at 20 --heal-at 40
python3 experiments/run.py --name restart --nodes 6 \
  --fault restart --fault-node 2 --fault-at 20 --heal-at 40
```

## Suites

| Suite | Configurations | Default repetitions | Main question |
|---|---|---|---|
| `smoke` | 4 nodes, 1s, 10 ops/s, 15s | 1 | Harness works end to end |
| `nodes` | 4, 6, 8 nodes, 1s, 20 ops/s | 5 | Convergence and messages vs cluster size |
| `interval` | 250ms, 1s, 4s, 6 nodes, 20 ops/s | 5 | Message overhead vs convergence |
| `load` | 5, 20, 50 ops/s, 6 nodes, 1s | 5 | Client latency percentiles and backlog |
| `failure` | partition and restart, 6 nodes | 5 | Detection and recovery time |
| `delay` | 0, 50, 150ms latency, 6 nodes | 5 | Convergence under slow links |

Every run uses `--seed` for the workload and a fixed `--topology-seed 42` for
the random extra edge. The seed and every other parameter are stored in
`summary.json`.

## Output

Each run writes `experiments/results/<utc-timestamp>-<name>/`:

| File | Contents |
|---|---|
| `compose.yaml` | Generated cluster definition (N, interval, topology, toxiproxy) |
| `requests.csv` | One row per measured request: target, op, status, latency, timestamps |
| `load-summary.json` | Workload window, status counts, last successful write |
| `metrics-node-k.json` | Final `/debug/metrics` of node k |
| `state-node-k.json` | Final `/debug/state`, including the full operation log |
| `cluster.log` | Timestamped Compose logs |
| `summary.json` | Config, host metadata, fault timing, convergence, verification |
| `verification.json` | Output of `verify.py` |

`analyze.py` reads every `summary.json` and writes `experiments/analysis/`:

- `runs.csv` — one row per run with p50/p95/p99 latency, convergence,
  recovery/detection, messages per operation and bytes per operation;
- `aggregate.csv` and `aggregate.json` — mean and 95% confidence interval per
  configuration;
- `plots/*.png` — convergence vs nodes, messages vs interval, latency vs load,
  and failure scenarios (when matplotlib is installed).

## Metrics

`GET /debug/metrics` is the lightweight experiment endpoint. It returns:

- `counters`: operations created/applied/duplicated/buffered/rejected,
  concurrent operations, gossip rounds/exchanges/failures, messages and bytes
  sent/received, and peer reachability transitions;
- `pending_ops`, `applied_ops`, `tombstones`;
- `clock` and `quantities`;
- `peers` with each neighbor's monitor state.

Convergence is declared when every node has the same clock, the same applied
operation count, the same quantities and no pending operation. `run.py` polls
this endpoint every 200 ms after the workload. `verify.py` additionally checks
the full operation log: contiguous counters, causal predecessors delivered
before each operation, observed removes, reconstructed quantities and equality
across nodes.

Client latency is recorded by `cmd/loadgen` with an explicit per-request
timeout. The analysis reports p50/p95/p99 over successful writes (HTTP 201);
HTTP 409 responses from a remove are expected in a concurrent cart workload and
are counted separately in `statuses`.

## Failure injection

- **partition**: `docker network disconnect` removes the target node from the
  Compose network; `docker network connect` heals it. Peers see timeouts and
  the gossip monitor logs `gossip_peer_unreachable`.
- **restart**: `docker compose stop node-k` followed by `start node-k`. The node
  returns as a new incarnation with an empty replica and recovers from
  reachable peers.
- **delay**: Toxiproxy is added to the generated Compose file, one proxy per
  node, and a latency toxic is installed on every proxy before the workload.

`--fault-at` and `--heal-at` are seconds measured from the start of the
workload window: bootstrap settle first, then warm-up, then the measured
window. A fault at 20s with a 10s warm-up therefore hits 10s into the measured
window. Detection time is the first poll
at which some peer reports `unreachable`; recovery time is measured from the
heal/restart to cluster convergence.

## Reproducibility checklist

- Pin the image tag `mc714-node:latest` built from the current checkout.
- Keep `TOPOLOGY_SEED` and the workload seed in `summary.json`.
- Use the same machine for comparisons; absolute numbers are not portable.
- `summary.json` records OS, kernel, CPU count, Python, Go, Docker and Compose
  versions.
- Warm-up is excluded from `requests.csv` and from latency percentiles.
- Raw CSV/JSON and logs are kept under `results/` so the report can be rebuilt
  with `analyze.py` alone.
