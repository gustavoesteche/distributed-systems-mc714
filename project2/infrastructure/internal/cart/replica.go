package cart

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"mc714/project2/internal/clock"
	"mc714/project2/internal/logging"
)

// ApplyResult counts what Apply did with a batch.
type ApplyResult struct {
	Applied, Duplicates, Buffered, Rejected int
}

// Snapshot is a deep copy of a replica's state.
type Snapshot struct {
	NodeID     string         `json:"node"`
	ReplicaID  string         `json:"replica"`
	Clock      clock.VC       `json:"clock"`
	Quantities map[string]int `json:"quantities"` // only products with quantity > 0
	Ops        []Operation    `json:"ops"`        // applied ops, in causal (delivery) order
	Tombstones []string       `json:"tombstones"` // sorted
	Pending    []Operation    `json:"pending"`    // sorted by (clock sum, ID)
}

type unit struct {
	product string
	opID    string
}

// Replica is one incarnation's cart state. A RWMutex makes every method
// atomic, and every returned value is a deep copy.
type Replica struct {
	mu        sync.RWMutex
	nodeID    string
	replicaID string
	logger    *slog.Logger

	clock         clock.VC
	log           []Operation                    // applied ops, in delivery order
	opIndex       map[string]int                 // op ID -> index in log
	units         map[string]unit                // every unit created by an applied ADD
	tombstones    map[string]struct{}            // dead units
	live          map[string]map[string]struct{} // product -> live unit IDs
	pending       map[string]Operation           // received, not yet causally ready
	lastByProduct map[string]string              // product -> ID of the latest applied op
}

// NewReplica returns an empty replica. A nil logger discards logs.
func NewReplica(nodeID, replicaID string, logger *slog.Logger) *Replica {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Replica{
		nodeID:        nodeID,
		replicaID:     replicaID,
		logger:        logger,
		clock:         clock.VC{},
		opIndex:       map[string]int{},
		units:         map[string]unit{},
		tombstones:    map[string]struct{}{},
		live:          map[string]map[string]struct{}{},
		pending:       map[string]Operation{},
		lastByProduct: map[string]string{},
	}
}

// NodeID returns the stable node name.
func (r *Replica) NodeID() string { return r.nodeID }

// ReplicaID returns this incarnation's ID.
func (r *Replica) ReplicaID() string { return r.replicaID }

func checkInput(product string, q int) error {
	if product == "" {
		return fmt.Errorf("%w: product_id must not be empty", ErrInvalidInput)
	}
	if q < 1 || q > MaxQuantity {
		return fmt.Errorf("%w: quantity %d outside 1 … %d", ErrInvalidInput, q, MaxQuantity)
	}
	return nil
}

// newLocalOp builds the next op of this replica. The caller holds the lock.
func (r *Replica) newLocalOp(t OpType, product string, q int, removes []string) Operation {
	clk := r.clock.Increment(r.replicaID)
	counter := clk[r.replicaID]
	return Operation{
		ID:        OpID(r.replicaID, counter),
		Origin:    r.replicaID,
		NodeID:    r.nodeID,
		Counter:   counter,
		Type:      t,
		ProductID: product,
		Quantity:  q,
		Clock:     clk,
		Removes:   removes,
	}
}

// Add creates q new units of product.
func (r *Replica) Add(product string, q int) (Operation, error) {
	if err := checkInput(product, q); err != nil {
		return Operation{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	op := r.newLocalOp(Add, product, q, nil)
	r.deliver(op)
	r.logCreated(op)
	return op.Copy(), nil
}

// Remove tombstones the q lexicographically lowest live units of product.
// It fails with ErrInsufficient, changing nothing, if fewer are visible.
func (r *Replica) Remove(product string, q int) (Operation, error) {
	if err := checkInput(product, q); err != nil {
		return Operation{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	live := r.live[product]
	if len(live) < q {
		return Operation{}, fmt.Errorf("%w: %s has %d, asked to remove %d", ErrInsufficient, product, len(live), q)
	}
	units := make([]string, 0, len(live))
	for u := range live {
		units = append(units, u)
	}
	sort.Strings(units)
	op := r.newLocalOp(Remove, product, q, units[:q:q])
	r.deliver(op)
	r.logCreated(op)
	return op.Copy(), nil
}

func (r *Replica) logCreated(op Operation) {
	r.logger.Info("op created", "event", logging.EventOpCreated, "op", op.ID, "type", op.Type,
		"product", op.ProductID, "quantity", op.Quantity, "now", len(r.live[op.ProductID]))
}

// Clock returns a copy of the replica's vector clock.
func (r *Replica) Clock() clock.VC {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.clock.Copy()
}

// Quantities returns the visible quantity of every product that has one.
func (r *Replica) Quantities() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.quantitiesLocked()
}

func (r *Replica) quantitiesLocked() map[string]int {
	q := make(map[string]int, len(r.live))
	for p, units := range r.live {
		if len(units) > 0 {
			q[p] = len(units)
		}
	}
	return q
}

// Snapshot returns a deep copy of the whole state.
func (r *Replica) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s := Snapshot{
		NodeID:     r.nodeID,
		ReplicaID:  r.replicaID,
		Clock:      r.clock.Copy(),
		Quantities: r.quantitiesLocked(),
		Ops:        make([]Operation, len(r.log)),
		Tombstones: make([]string, 0, len(r.tombstones)),
		Pending:    make([]Operation, 0, len(r.pending)),
	}
	for i, op := range r.log {
		s.Ops[i] = op.Copy()
	}
	for u := range r.tombstones {
		s.Tombstones = append(s.Tombstones, u)
	}
	sort.Strings(s.Tombstones)
	for _, op := range r.pending {
		s.Pending = append(s.Pending, op.Copy())
	}
	sortCausal(s.Pending)
	return s
}

// sortCausal sorts by (clock sum, ID): a linear extension of happened-before.
func sortCausal(ops []Operation) {
	sums := make(map[string]uint64, len(ops))
	for _, op := range ops {
		sums[op.ID] = op.Clock.Sum()
	}
	sort.Slice(ops, func(i, j int) bool {
		si, sj := sums[ops[i].ID], sums[ops[j].ID]
		if si != sj {
			return si < sj
		}
		return ops[i].ID < ops[j].ID
	})
}
