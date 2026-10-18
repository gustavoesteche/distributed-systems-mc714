# MC714 Project 2: Vector Clocks + Epidemic Replication

A replicated shopping cart: N Go nodes in Docker Compose, each with an in-memory replica. Clients add and remove items at any node; nodes gossip with their neighbors until every cart is identical. Standard library only.

- **Vector clocks** order operations, gate causal delivery, summarize what a replica knows, and detect concurrent ops (`concurrent_ops_detected`).
- **Epidemic anti-entropy**: every `GOSSIP_INTERVAL` each node runs a push-pull exchange with all its neighbors in a static seeded topology (the chain `node-1 — … — node-N` plus `EXTRA_EDGES` random edges). State is only merged, never overwritten.

Details: [docs/protocol.md](docs/protocol.md), [docs/architecture.md](docs/architecture.md).

## Commands

Requires Go 1.26, Docker Compose v2 and [just](https://github.com/casey/just). Run from `project2/infrastructure`.

| Command | What it does |
|---|---|
| `just up` / `just down` | Start / remove the cluster |
| `just logs` | Follow every node's log |
| `just clean-state` | Print each node's cart (product → quantity) |
| `just test` / `just test-race` | Unit and in-process integration tests |
| `just e2e` | Real 4-node cluster: propagation, partition, restart. Uses ports 8001–8004, so `just down` first |

## Trying it out

Node `node-k` listens on `127.0.0.1:800k`.

```bash
just up && just logs                                   # in another terminal
curl -s -X POST localhost:8001/cart/add -d '{"product_id":"apple","quantity":3}'
curl -s -X POST localhost:8004/cart/remove -d '{"product_id":"apple","quantity":1}'   # 409 if too many
just clean-state
# {"items":{"apple":2},"node":"node-1"}
# ...
curl -s localhost:8002/debug/state                     # clock, ops, tombstones, pending, peers
docker compose stop node-2 && docker compose start node-2   # comes back empty, recovers the cart
```

## Configuration

Edit `compose.yaml` directly. To add a node, copy a service, change its `NODE_ID`, hostname and port, and raise `NODE_COUNT` on every node. The e2e test runs this file with `GOSSIP_INTERVAL=1s` and expects a chain (`EXTRA_EDGES: "0"`).

| Variable | Default | Meaning |
|---|---|---|
| `NODE_ID` | required | `node-k`, 1 ≤ k ≤ `NODE_COUNT` |
| `NODE_COUNT` | `4` | number of nodes (≥ 3) |
| `MAX_NEIGHBORS` | `3` | degree cap (2 … N-1) |
| `EXTRA_EDGES` | `1` | random non-chain edges |
| `TOPOLOGY_SEED` | `42` | seed for the random edges |
| `GOSSIP_INTERVAL` | `1s` | time between rounds |
| `HTTP_ADDR` | `:8080` | listen address |
| `HTTP_TIMEOUT` | `2s` | per-request timeout between nodes |
| `FAILURE_THRESHOLD` | `3` | failures before a peer is logged unreachable |
| `PEER_URL_TEMPLATE` | `http://{id}:8080` | `{id}` → `node-k`, `{index}` → `k` |
| `LOG_LEVEL` | `info` | `debug` also logs exchanges that moved nothing |
