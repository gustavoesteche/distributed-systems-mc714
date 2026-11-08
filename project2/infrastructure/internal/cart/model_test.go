package cart

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"reflect"
	"sort"
	"testing"

	"mc714/project2/internal/clock"
)

// checkInvariants asserts the per-step safety properties on one replica:
// no negative quantity, every visible unit comes from an applied ADD and
// is not tombstoned (pending ops are invisible), and the clock is exactly the
// per-origin count of applied ops.
func checkInvariants(r *Replica) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	q := r.quantitiesLocked()
	for p, n := range q {
		if n <= 0 {
			return fmt.Errorf("%s: product %s listed with quantity %d", r.replicaID, p, n)
		}
	}
	for p, units := range r.live {
		if len(units) != q[p] {
			return fmt.Errorf("%s: %s has %d live units but quantity %d", r.replicaID, p, len(units), q[p])
		}
		for u := range units {
			info, ok := r.units[u]
			if !ok {
				return fmt.Errorf("%s: live unit %s was never created", r.replicaID, u)
			}
			if info.product != p {
				return fmt.Errorf("%s: unit %s listed under %s, belongs to %s", r.replicaID, u, p, info.product)
			}
			idx, ok := r.opIndex[info.opID]
			if !ok || r.log[idx].Type != Add {
				return fmt.Errorf("%s: unit %s does not come from an applied ADD", r.replicaID, u)
			}
			if _, dead := r.tombstones[u]; dead {
				return fmt.Errorf("%s: unit %s is both live and tombstoned", r.replicaID, u)
			}
		}
	}
	for id := range r.pending {
		if _, applied := r.opIndex[id]; applied {
			return fmt.Errorf("%s: op %s is both pending and applied", r.replicaID, id)
		}
	}
	// Each op was delivered right after its origin's
	// previous op and after every other causal predecessor.
	counts := clock.VC{}
	removed := map[string]struct{}{}
	for i, op := range r.log {
		if op.Clock[op.Origin] != counts[op.Origin]+1 {
			return fmt.Errorf("%s: log[%d]=%s delivered out of origin order", r.replicaID, i, op.ID)
		}
		for k, v := range op.Clock {
			if k != op.Origin && v > counts[k] {
				return fmt.Errorf("%s: log[%d]=%s delivered before a predecessor from %s", r.replicaID, i, op.ID, k)
			}
		}
		counts[op.Origin]++
		for _, u := range op.Removes {
			removed[u] = struct{}{}
		}
	}
	// Only applied REMOVEs tombstone units: a pending one has no effect.
	if len(removed) != len(r.tombstones) {
		return fmt.Errorf("%s: %d tombstones, applied removes name %d units", r.replicaID, len(r.tombstones), len(removed))
	}
	for u := range removed {
		if _, ok := r.tombstones[u]; !ok {
			return fmt.Errorf("%s: unit %s removed by an applied op but not tombstoned", r.replicaID, u)
		}
	}
	if r.clock.Compare(counts) != clock.Equal {
		return fmt.Errorf("%s: clock %v != applied counts %v", r.replicaID, r.clock, counts)
	}
	return nil
}

func idSet(ops []Operation) map[string]bool {
	s := map[string]bool{}
	for _, op := range ops {
		s[op.ID] = true
	}
	return s
}

func TestRandomizedConvergence(t *testing.T) {
	seeds := make([]uint64, 50)
	for i := range seeds {
		seeds[i] = uint64(1000 + i*7919)
	}
	products := []string{"apple", "pear", "plum"}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 99))
			fail := func(format string, args ...any) {
				t.Helper()
				t.Fatalf("seed %d: %s", seed, fmt.Sprintf(format, args...))
			}
			n := 3 + rng.IntN(4)
			reps := make([]*Replica, n)
			for i := range reps {
				reps[i] = NewReplica(fmt.Sprintf("node-%d", i+1), fmt.Sprintf("node-%d@%d", i+1, seed), discardLogger())
			}
			issued := map[string]bool{}               // every op any replica created
			prevApplied := make([]map[string]bool, n) // per replica, for monotonicity
			apply := func(r *Replica, ops []Operation) {
				if _, err := r.Apply(ops); err != nil {
					fail("apply on %s: %v", r.replicaID, err)
				}
			}

			for step := 0; step < 500; step++ {
				i, j := rng.IntN(n), rng.IntN(n)
				switch k := rng.IntN(10); {
				case k < 3:
					q := 1 + rng.IntN(3)
					op, err := reps[i].Add(products[rng.IntN(3)], q)
					if err != nil {
						fail("add: %v", err)
					}
					issued[op.ID] = true
				case k < 5:
					p := products[rng.IntN(3)]
					avail := reps[i].Quantities()[p]
					if avail > 0 {
						op, err := reps[i].Remove(p, 1+rng.IntN(avail))
						if err != nil {
							fail("remove: %v", err)
						}
						issued[op.ID] = true
					}
				case k < 7:
					if i != j {
						apply(reps[i], reps[j].Delta(reps[i].Clock()))
						apply(reps[j], reps[i].Delta(reps[j].Clock()))
					}
				default:
					ops := reps[j].Snapshot().Ops
					var batch []Operation
					for _, op := range ops {
						if rng.IntN(2) == 0 {
							batch = append(batch, op)
							if rng.IntN(4) == 0 {
								batch = append(batch, op)
							}
						}
					}
					rng.Shuffle(len(batch), func(a, b int) { batch[a], batch[b] = batch[b], batch[a] })
					apply(reps[i], batch)
				}
				for idx, r := range reps {
					if err := checkInvariants(r); err != nil {
						fail("step %d: %v", step, err)
					}
					// Received state never overwrites local state: the applied
					// set only grows.
					cur := r.appliedIDs()
					for id := range prevApplied[idx] {
						if !cur[id] {
							fail("step %d: %s lost applied op %s", step, r.replicaID, id)
						}
					}
					prevApplied[idx] = cur
				}
			}

			// Full pairwise sync until quiescent.
			for round := 0; ; round++ {
				if round > 3*n {
					fail("no quiescence after %d rounds", round)
				}
				changed := false
				for a := 0; a < n; a++ {
					for b := 0; b < n; b++ {
						if a == b {
							continue
						}
						res, err := reps[a].Apply(reps[b].Delta(reps[a].Clock()))
						if err != nil {
							fail("final sync: %v", err)
						}
						if res.Applied > 0 || res.Rejected > 0 {
							changed = true
						}
						// Pending ops are not covered by Delta; push them as well.
						res, err = reps[a].Apply(reps[b].Snapshot().Pending)
						if err != nil {
							fail("final sync: %v", err)
						}
						if res.Applied > 0 {
							changed = true
						}
					}
				}
				if !changed {
					break
				}
			}

			// Identical clock, op set, tombstones and quantities.
			ref := reps[0].Snapshot()
			for _, r := range reps {
				if err := checkInvariants(r); err != nil {
					fail("after sync: %v", err)
				}
				s := r.Snapshot()
				if s.Clock.Compare(ref.Clock) != clock.Equal {
					fail("%s clock %v != %v", r.replicaID, s.Clock, ref.Clock)
				}
				if !reflect.DeepEqual(idSet(s.Ops), idSet(ref.Ops)) {
					fail("%s op set differs", r.replicaID)
				}
				if !reflect.DeepEqual(s.Tombstones, ref.Tombstones) {
					fail("%s tombstones differ", r.replicaID)
				}
				if !reflect.DeepEqual(s.Quantities, ref.Quantities) {
					fail("%s quantities %v != %v", r.replicaID, s.Quantities, ref.Quantities)
				}
				if len(s.Pending) != 0 {
					fail("%s still has %d pending ops", r.replicaID, len(s.Pending))
				}
			}

			// Nothing issued was lost and nothing was invented.
			if !reflect.DeepEqual(idSet(ref.Ops), issued) {
				fail("converged op set (%d ops) differs from the issued ops (%d)", len(ref.Ops), len(issued))
			}

			// quantity = units created by ADDs − |union of tombstones| per product.
			created := map[string]int{}
			unitProduct := map[string]string{}
			for _, op := range ref.Ops {
				if op.Type == Add {
					created[op.ProductID] += op.Quantity
					for u := 0; u < op.Quantity; u++ {
						unitProduct[UnitID(op.ID, u)] = op.ProductID
					}
				}
			}
			dead := map[string]int{}
			union := map[string]bool{}
			for _, r := range reps {
				for _, op := range r.Snapshot().Ops {
					for _, u := range op.Removes {
						union[u] = true
					}
				}
			}
			for u := range union {
				p, ok := unitProduct[u]
				if !ok {
					fail("tombstone %s names no created unit", u)
				}
				dead[p]++
			}
			for _, p := range products {
				if want := created[p] - dead[p]; ref.Quantities[p] != want {
					fail("%s: quantity %d, want %d - %d", p, ref.Quantities[p], created[p], dead[p])
				}
			}

			// Re-applying every op changes nothing.
			for _, r := range reps {
				before := r.Snapshot()
				res, err := r.Apply(ref.Ops)
				if err != nil {
					fail("re-apply: %v", err)
				}
				if res.Applied != 0 || res.Buffered != 0 || res.Duplicates != len(ref.Ops) {
					fail("re-apply on %s: %+v", r.replicaID, res)
				}
				if !reflect.DeepEqual(before, r.Snapshot()) {
					fail("re-apply changed %s", r.replicaID)
				}
			}

			// Order independence: a fresh replica fed every op in a random
			// permutation, in random chunks, reaches the same state.
			all := append([]Operation(nil), ref.Ops...)
			rng.Shuffle(len(all), func(a, b int) { all[a], all[b] = all[b], all[a] })
			fresh := NewReplica("node-99", "node-99@1", discardLogger())
			for len(all) > 0 {
				k := 1 + rng.IntN(len(all))
				apply(fresh, all[:k])
				all = all[k:]
				if err := checkInvariants(fresh); err != nil {
					fail("fresh: %v", err)
				}
			}
			fs := fresh.Snapshot()
			if fs.Clock.Compare(ref.Clock) != clock.Equal || !reflect.DeepEqual(idSet(fs.Ops), idSet(ref.Ops)) ||
				!reflect.DeepEqual(fs.Quantities, ref.Quantities) || len(fs.Pending) != 0 {
				fail("fresh replica differs from the converged state")
			}
			ts := append([]string(nil), fs.Tombstones...)
			sort.Strings(ts)
			if !reflect.DeepEqual(ts, ref.Tombstones) {
				fail("fresh tombstones differ")
			}
		})
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// appliedIDs returns the set of applied op IDs without a deep snapshot.
func (r *Replica) appliedIDs() map[string]bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make(map[string]bool, len(r.opIndex))
	for id := range r.opIndex {
		ids[id] = true
	}
	return ids
}
