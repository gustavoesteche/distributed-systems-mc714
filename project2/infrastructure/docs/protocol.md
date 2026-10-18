# Replication Protocol

## Identities

| Concept | Format |
|---|---|
| Node name | `node-k` |
| Replica ID (one per boot) | `<node>@<boot-unix-nanos>` |
| Operation ID | `<replicaID>:<counter>` |
| Unit ID | `<opID>/<i>`, i = 0 … q-1 |

Vector clocks are keyed by replica ID, so a restarted node starts a fresh entry and never reissues an old op ID.

## Operations

```json
{"id": "node-1@17:3", "origin": "node-1@17", "node_id": "node-1", "counter": 3,
 "type": "REMOVE", "product_id": "apple", "quantity": 1,
 "clock": {"node-1@17": 3, "node-2@42": 1}, "removes": ["node-1@17:1/0"]}
```

A local op increments the replica's own clock entry and is applied immediately. Received ops are validated first, and one invalid op fails the whole batch with no state change: the ID must match origin and counter, `counter == clock[origin]`, `1 ≤ quantity ≤ 10000`, and a REMOVE names exactly `quantity` distinct unit IDs.

## Cart semantics (observed-remove, add-wins)

- **ADD(p, q)** creates q units.
- **REMOVE(p, q)** tombstones the q lowest live units of p and lists them in `removes`; it fails with 409 if fewer are visible.
- **Quantity** = live units from applied ADDs. Tombstones are permanent.
- A unit added concurrently with a remove survives it. Two concurrent removes may pick the same unit and collapse into one.

## Causal delivery

`Apply` drops duplicates (already applied or pending), buffers the rest, and delivers an op once it is the next from its origin and every other predecessor is applied. A REMOVE of a unit its origin could not have observed is rejected and logged as `op_rejected`. An op concurrent with the last op on the same product logs `concurrent_ops_detected`.

## Anti-entropy round

Every `GOSSIP_INTERVAL`, for each neighbor P in parallel:

1. Pull: send our clock; P returns its clock and the ops we lack.
2. Apply them.
3. Push the ops P lacks, if any.

Received state is only merged through `Apply`, never overwritten.

## Endpoints

| Method | Path | Response |
|---|---|---|
| GET | `/health` | `{"node","replica"}` |
| GET | `/cart` | `{"node","replica","items"}` |
| POST | `/cart/add` | `{"product_id","quantity"}` → 201 op · 400 |
| POST | `/cart/remove` | `{"product_id","quantity"}` → 201 op · 400 · 409 |
| POST | `/internal/gossip/pull` | `{"sender_node","sender_replica","clock"}` → `{"node","replica","clock","ops"}` · 403 |
| POST | `/internal/gossip/push` | `{"sender_node","sender_replica","ops"}` → `{"applied","duplicates","buffered","rejected"}` · 403 |
| GET | `/debug/state` | full snapshot plus `neighbors` and `peers` |
| GET | `/debug/clean-state` | `{"node","items":{"<product>":<qty>}}` |

Bodies are limited to 8 MiB (413) and must be one JSON value with exactly matching keys (400). Errors are `{"error": "..."}`.

## Logs

`slog` text lines, each with `node=` and `event=`.

| Event | Level | When |
|---|---|---|
| `op_created` | info | client add/remove, with the new quantity |
| `anti_entropy_success` | info / debug | exchange that moved ops / moved nothing |
| `ops_received` | info | a neighbor's push applied ops |
| `anti_entropy_failed` | warn | exchange error |
| `gossip_peer_connected`, `gossip_peer_reconnected` | info | peer becomes reachable |
| `gossip_peer_unreachable` | warn | `FAILURE_THRESHOLD` consecutive failures (logged once) |
| `concurrent_ops_detected`, `op_rejected` | info, warn | see causal delivery |
| `configured_neighbors`, `node_started`, `node_stopped`, `node_error` | info / error | lifecycle |

## Correctness

Safety: no op is applied before its causal predecessors; each op takes effect at most once; quantities are never negative and pending ops are invisible; a REMOVE only kills units its origin observed; an invalid message changes nothing; op IDs are never reused.

Liveness: connected live nodes converge once updates stop; a restarted node recovers every op a reachable replica holds; partitioned sides converge after healing.
