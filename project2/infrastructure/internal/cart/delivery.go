package cart

import (
	"fmt"

	"mc714/project2/internal/clock"
	"mc714/project2/internal/logging"
)

// Apply merges received ops. One invalid op rejects the whole batch with no
// state change; known ops count as duplicates; the rest are buffered and
// delivered in causal order.
func (r *Replica) Apply(ops []Operation) (ApplyResult, error) {
	for i := range ops {
		if err := ops[i].Validate(); err != nil {
			return ApplyResult{}, err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	var res ApplyResult
	var fromBatch []string
	for _, op := range ops {
		if _, applied := r.opIndex[op.ID]; applied || op.Counter <= r.clock[op.Origin] {
			res.Duplicates++
			continue
		}
		if _, pending := r.pending[op.ID]; pending {
			res.Duplicates++
			continue
		}
		r.pending[op.ID] = op.Copy()
		fromBatch = append(fromBatch, op.ID)
	}
	res.Applied, res.Rejected = r.drain()
	for _, id := range fromBatch {
		if _, still := r.pending[id]; still {
			res.Buffered++
		}
	}
	return res, nil
}

// ready reports whether op is the next op from its origin and every other
// causal predecessor has been applied. The caller holds the lock.
func (r *Replica) ready(op Operation) bool {
	if op.Clock[op.Origin] != r.clock[op.Origin]+1 {
		return false
	}
	for k, v := range op.Clock {
		if k != op.Origin && v > r.clock[k] {
			return false
		}
	}
	return true
}

// drain delivers pending ops until none is causally ready.
func (r *Replica) drain() (applied, rejected int) {
	for progress := true; progress && len(r.pending) > 0; {
		progress = false
		batch := make([]Operation, 0, len(r.pending))
		for _, op := range r.pending {
			batch = append(batch, op)
		}
		sortCausal(batch)
		for _, op := range batch {
			if op.Counter <= r.clock[op.Origin] {
				// Another op with the same origin and counter got applied first.
				delete(r.pending, op.ID)
				continue
			}
			if !r.ready(op) {
				continue
			}
			delete(r.pending, op.ID)
			progress = true
			if r.deliver(op) {
				applied++
			} else {
				rejected++
			}
		}
	}
	return applied, rejected
}

// deliver applies one causally ready op, dropping a REMOVE of a unit it could
// not have observed. The caller holds the lock.
func (r *Replica) deliver(op Operation) bool {
	if op.Type == Remove {
		if reason := r.checkRemove(op); reason != "" {
			r.logger.Warn("op rejected", "event", logging.EventOpRejected, "op", op.ID, "reason", reason)
			return false
		}
	}
	if lastID, ok := r.lastByProduct[op.ProductID]; ok {
		last := r.log[r.opIndex[lastID]]
		if last.Clock.Compare(op.Clock) == clock.Concurrent {
			r.logger.Info("concurrent ops detected", "event", logging.EventConcurrentOps,
				"product", op.ProductID, "op", op.ID, "concurrent_with", last.ID)
		}
	}

	r.opIndex[op.ID] = len(r.log)
	r.log = append(r.log, op)
	switch op.Type {
	case Add:
		live := r.live[op.ProductID]
		if live == nil {
			live = map[string]struct{}{}
			r.live[op.ProductID] = live
		}
		for i := 0; i < op.Quantity; i++ {
			u := UnitID(op.ID, i)
			r.units[u] = unit{product: op.ProductID, opID: op.ID}
			if _, dead := r.tombstones[u]; !dead {
				live[u] = struct{}{}
			}
		}
	case Remove:
		for _, u := range op.Removes {
			r.tombstones[u] = struct{}{}
			delete(r.live[op.ProductID], u)
		}
		if len(r.live[op.ProductID]) == 0 {
			delete(r.live, op.ProductID)
		}
	}
	r.clock = r.clock.Merge(op.Clock)
	r.lastByProduct[op.ProductID] = op.ID
	return true
}

// checkRemove returns why a REMOVE is not acceptable, or "" if it is: every
// unit must exist, belong to the product, and come from an ADD that
// happened before the REMOVE.
func (r *Replica) checkRemove(op Operation) string {
	for _, u := range op.Removes {
		info, ok := r.units[u]
		if !ok {
			return fmt.Sprintf("unit %s does not exist", u)
		}
		if info.product != op.ProductID {
			return fmt.Sprintf("unit %s belongs to %s, not %s", u, info.product, op.ProductID)
		}
		add := r.log[r.opIndex[info.opID]]
		if o := add.Clock.Compare(op.Clock); o != clock.Before && o != clock.Equal {
			return fmt.Sprintf("unit %s was not observed by %s", u, op.Origin)
		}
	}
	return ""
}
