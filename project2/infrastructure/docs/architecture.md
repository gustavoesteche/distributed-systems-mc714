# Architecture

Every node is the same static binary (`cmd/node`) in its own container, configured only by environment variables from `compose.yaml`. Nodes reach each other by Compose DNS (`http://node-k:8080`) on the `gossip-net` bridge.

## Packages

| Package | Responsibility |
|---|---|
| `clock` | Pure vector clocks: `Increment`, `Merge`, `Compare`, `Sum` |
| `topology` | Seeded neighbor graph |
| `config` | Reads and validates the environment |
| `cart` | The replica: ops, units, tombstones, causal delivery (`Apply`), deltas |
| `replication` | Anti-entropy rounds, neighbor check, peer monitor, `Transport` interface |
| `metrics` | Process-wide counters for operations, gossip traffic and peer transitions |
| `transport` | HTTP server (decode → delegate → encode), `/debug/metrics`, and the HTTP client |
| `node` | Wires everything and owns the lifecycle |
| `logging` | `slog` setup and event names |

## Topology

`topology.Generate(n, maxNeighbors, extraEdges, seed)` adds the chain `node-i — node-(i+1)`, shuffles the remaining pairs with `PCG(seed, 0)`, and adds pairs whose endpoints are both under the cap until `extraEdges` fit (or fails). Every node computes the same graph, logs `configured_neighbors`, and keeps it static. Gossip from a non-neighbor gets 403.

## Lifecycle

- `New` creates a fresh incarnation `node-k@<boot-nanos>` with an empty replica.
- `Start` serves HTTP and starts the gossip loop; the first round runs immediately.
- `Stop` waits for the current round, then shuts the server down.

## Failure model

- **Crash-recovery, no persistence.** A restarted node is a new incarnation with empty state and recovers every op a reachable replica still holds. An op that never left a crashed node is lost.
- **Partitions** are tolerated; both sides converge after healing.
- **No Byzantine nodes**, though malformed messages are rejected atomically.
- **No garbage collection** of tombstones, the op log, or dead clock entries.
- Timeouts only drive the reachability log; safety never depends on them.
