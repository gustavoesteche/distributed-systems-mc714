package cart

import "mc714/project2/internal/clock"

// Delta returns every applied op the peer's clock does not cover
// (op.Counter > peer[op.Origin]), sorted by (clock sum, ID). That order is a
// linear extension of causality, so a receiver covered by peer applies the
// delta in order with nothing left pending.
func (r *Replica) Delta(peer clock.VC) []Operation {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.deltaLocked(peer)
}

// DeltaAndClock returns Delta(peer) and the clock it was computed against,
// read atomically.
func (r *Replica) DeltaAndClock(peer clock.VC) ([]Operation, clock.VC) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.deltaLocked(peer), r.clock.Copy()
}

func (r *Replica) deltaLocked(peer clock.VC) []Operation {
	out := []Operation{}
	for _, op := range r.log {
		if op.Counter > peer[op.Origin] {
			out = append(out, op.Copy())
		}
	}
	sortCausal(out)
	return out
}
