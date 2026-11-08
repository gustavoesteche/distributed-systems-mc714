package cart

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"testing"

	"mc714/project2/internal/clock"
	"mc714/project2/internal/logging"
	"mc714/project2/internal/testutil"
)

// newTestReplica builds a replica whose logs go into a capture.
func newTestReplica(nodeID, replicaID string) (*Replica, *testutil.LogCapture) {
	logs := testutil.NewLogCapture()
	return NewReplica(nodeID, replicaID, logs.Logger(nodeID)), logs
}

func mustAdd(t *testing.T, r *Replica, product string, q int) Operation {
	t.Helper()
	op, err := r.Add(product, q)
	if err != nil {
		t.Fatalf("Add(%s,%d) on %s: %v", product, q, r.replicaID, err)
	}
	return op
}

func mustRemove(t *testing.T, r *Replica, product string, q int) Operation {
	t.Helper()
	op, err := r.Remove(product, q)
	if err != nil {
		t.Fatalf("Remove(%s,%d) on %s: %v", product, q, r.replicaID, err)
	}
	return op
}

func mustApply(t *testing.T, r *Replica, ops []Operation) ApplyResult {
	t.Helper()
	res, err := r.Apply(ops)
	if err != nil {
		t.Fatalf("Apply on %s: %v", r.replicaID, err)
	}
	return res
}

// syncPair runs a full push-pull between a and b through Delta/Apply.
func syncPair(t *testing.T, a, b *Replica) {
	t.Helper()
	mustApply(t, a, b.Delta(a.Clock()))
	mustApply(t, b, a.Delta(b.Clock()))
}

func snapJSON(t *testing.T, r *Replica) string {
	t.Helper()
	b, err := json.Marshal(r.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func opIDs(ops []Operation) []string {
	ids := make([]string, len(ops))
	for i, op := range ops {
		ids[i] = op.ID
	}
	return ids
}

func sortedIDs(ops []Operation) []string {
	ids := opIDs(ops)
	sort.Strings(ids)
	return ids
}

// liveUnitsOf returns the sorted live unit IDs of a product.
func liveUnitsOf(r *Replica, product string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []string
	for u := range r.live[product] {
		out = append(out, u)
	}
	sort.Strings(out)
	return out
}

func TestAddCreatesOpAndUnits(t *testing.T) {
	r, _ := newTestReplica("node-1", "node-1@100")
	op := mustAdd(t, r, "apple", 3)
	if op.Counter != 1 || op.ID != "node-1@100:1" || op.Origin != "node-1@100" || op.NodeID != "node-1" {
		t.Fatalf("bad identity: %+v", op)
	}
	if op.Type != Add || op.ProductID != "apple" || op.Quantity != 3 || len(op.Removes) != 0 {
		t.Fatalf("bad payload: %+v", op)
	}
	if op.Clock.Compare(clock.VC{"node-1@100": 1}) != clock.Equal {
		t.Fatalf("clock %v, want {node-1@100:1}", op.Clock)
	}
	if err := op.Validate(); err != nil {
		t.Fatalf("own op invalid: %v", err)
	}
	want := []string{"node-1@100:1/0", "node-1@100:1/1", "node-1@100:1/2"}
	if got := liveUnitsOf(r, "apple"); !reflect.DeepEqual(got, want) {
		t.Fatalf("units %v, want %v", got, want)
	}
	if q := r.Quantities()["apple"]; q != 3 {
		t.Fatalf("quantity %d", q)
	}
	op2 := mustAdd(t, r, "pear", 1)
	if op2.Counter != 2 || op2.ID != "node-1@100:2" || op2.Clock.Get("node-1@100") != 2 {
		t.Fatalf("second op: %+v", op2)
	}
	// Returned ops are copies: mutating one leaves the replica alone.
	op2.Clock["node-1@100"] = 99
	if r.Clock().Get("node-1@100") != 2 {
		t.Fatalf("returned op aliases replica clock")
	}

	// Bad client input changes nothing.
	before := snapJSON(t, r)
	for _, c := range []struct {
		p string
		q int
	}{{"", 1}, {"x", 0}, {"x", -2}, {"x", MaxQuantity + 1}} {
		if _, err := r.Add(c.p, c.q); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("Add(%q,%d) = %v, want ErrInvalidInput", c.p, c.q, err)
		}
		if _, err := r.Remove(c.p, c.q); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("Remove(%q,%d) = %v, want ErrInvalidInput", c.p, c.q, err)
		}
	}
	if snapJSON(t, r) != before {
		t.Fatalf("invalid input changed state")
	}
}

func TestRemoveInsufficientLeavesStateUnchanged(t *testing.T) {
	r, _ := newTestReplica("node-1", "node-1@1")
	mustAdd(t, r, "apple", 2)
	before := snapJSON(t, r)
	clk := r.Clock()
	for _, c := range []struct {
		p string
		q int
	}{{"apple", 3}, {"banana", 1}} {
		if _, err := r.Remove(c.p, c.q); !errors.Is(err, ErrInsufficient) {
			t.Fatalf("Remove(%s,%d) = %v, want ErrInsufficient", c.p, c.q, err)
		}
	}
	if snapJSON(t, r) != before {
		t.Fatalf("failed remove changed the snapshot")
	}
	if r.Clock().Compare(clk) != clock.Equal {
		t.Fatalf("failed remove changed the clock")
	}
	// The counter did not move: the next op is :2.
	if op := mustAdd(t, r, "apple", 1); op.Counter != 2 {
		t.Fatalf("counter advanced by a failed remove: %d", op.Counter)
	}
}

func TestRemovePicksLowestUnits(t *testing.T) {
	r, _ := newTestReplica("r", "r@1")
	mustAdd(t, r, "apple", 1)  // r@1:1/0
	mustAdd(t, r, "banana", 2) // r@1:2/*
	mustAdd(t, r, "apple", 12) // r@1:3/0 … r@1:3/11
	op := mustRemove(t, r, "apple", 4)
	want := []string{"r@1:1/0", "r@1:3/0", "r@1:3/1", "r@1:3/10"}
	if !reflect.DeepEqual(op.Removes, want) {
		t.Fatalf("Removes = %v, want %v", op.Removes, want)
	}
	op = mustRemove(t, r, "apple", 1)
	if !reflect.DeepEqual(op.Removes, []string{"r@1:3/11"}) {
		t.Fatalf("second Removes = %v", op.Removes)
	}
	if op.Type != Remove || op.Quantity != 1 || op.Validate() != nil {
		t.Fatalf("bad remove op %+v", op)
	}
	if q := r.Quantities(); q["apple"] != 8 || q["banana"] != 2 {
		t.Fatalf("quantities %v", q)
	}
}

func TestAddThenRemoveQuantity(t *testing.T) {
	r, _ := newTestReplica("node-1", "node-1@1")
	mustAdd(t, r, "apple", 2)
	mustRemove(t, r, "apple", 1)
	if q := r.Quantities()["apple"]; q != 1 {
		t.Fatalf("quantity %d, want 1", q)
	}
	mustRemove(t, r, "apple", 1)
	q := r.Quantities()
	if _, ok := q["apple"]; ok {
		t.Fatalf("product with 0 units still listed: %v", q)
	}
	if s := r.Snapshot(); len(s.Tombstones) != 2 || len(s.Ops) != 3 {
		t.Fatalf("snapshot %+v", s)
	}
}

func TestApplyIdempotent(t *testing.T) {
	a, _ := newTestReplica("node-1", "node-1@1")
	mustAdd(t, a, "apple", 3)
	mustAdd(t, a, "pear", 1)
	mustRemove(t, a, "apple", 2)
	batch := a.Delta(clock.VC{})
	if len(batch) != 3 {
		t.Fatalf("batch has %d ops", len(batch))
	}

	once, _ := newTestReplica("node-2", "node-2@1")
	res := mustApply(t, once, batch)
	if res != (ApplyResult{Applied: 3}) {
		t.Fatalf("first apply %+v", res)
	}

	thrice, _ := newTestReplica("node-2", "node-2@1")
	for i := 0; i < 3; i++ {
		res := mustApply(t, thrice, batch)
		want := ApplyResult{Duplicates: 3}
		if i == 0 {
			want = ApplyResult{Applied: 3}
		}
		if res != want {
			t.Fatalf("apply #%d: %+v, want %+v", i+1, res, want)
		}
	}
	if snapJSON(t, once) != snapJSON(t, thrice) {
		t.Fatalf("3x apply differs from 1x:\n%s\n%s", snapJSON(t, once), snapJSON(t, thrice))
	}

	// A batch that repeats an op counts the copy as a duplicate.
	dup, _ := newTestReplica("node-3", "node-3@1")
	res = mustApply(t, dup, append(append([]Operation{}, batch...), batch[0]))
	if res != (ApplyResult{Applied: 3, Duplicates: 1}) {
		t.Fatalf("in-batch duplicate: %+v", res)
	}
	if !reflect.DeepEqual(dup.Quantities(), once.Quantities()) {
		t.Fatalf("in-batch duplicate changed quantities")
	}
}

func TestRemoveBeforeAddIsBuffered(t *testing.T) {
	a, _ := newTestReplica("node-1", "node-1@1")
	add := mustAdd(t, a, "apple", 2)
	rem := mustRemove(t, a, "apple", 1)

	b, _ := newTestReplica("node-2", "node-2@1")
	res := mustApply(t, b, []Operation{rem})
	if res != (ApplyResult{Buffered: 1}) {
		t.Fatalf("remove first: %+v", res)
	}
	if q, ok := b.Quantities()["apple"]; ok || q < 0 {
		t.Fatalf("pending remove affected visible state: %v", b.Quantities())
	}
	s := b.Snapshot()
	if len(s.Pending) != 1 || len(s.Ops) != 0 || len(s.Tombstones) != 0 || len(s.Clock) != 0 {
		t.Fatalf("snapshot after buffered remove: %+v", s)
	}
	// Receiving the remove again while it is pending is a duplicate.
	if res := mustApply(t, b, []Operation{rem}); res != (ApplyResult{Duplicates: 1}) {
		t.Fatalf("re-sent pending remove: %+v", res)
	}

	res = mustApply(t, b, []Operation{add})
	if res != (ApplyResult{Applied: 2}) {
		t.Fatalf("after add: %+v", res)
	}
	if q := b.Quantities()["apple"]; q != 1 {
		t.Fatalf("quantity %d, want 1", q)
	}
	s = b.Snapshot()
	if len(s.Pending) != 0 || !reflect.DeepEqual(opIDs(s.Ops), []string{add.ID, rem.ID}) {
		t.Fatalf("snapshot after add: %+v", s)
	}
}

func TestCounterGapBuffered(t *testing.T) {
	a, _ := newTestReplica("node-1", "node-1@1")
	op1 := mustAdd(t, a, "apple", 1)
	op2 := mustAdd(t, a, "banana", 1)
	op3 := mustAdd(t, a, "cherry", 1)

	b, _ := newTestReplica("node-2", "node-2@1")
	res := mustApply(t, b, []Operation{op1, op3})
	if res != (ApplyResult{Applied: 1, Buffered: 1}) {
		t.Fatalf("ops 1,3: %+v", res)
	}
	s := b.Snapshot()
	if !reflect.DeepEqual(opIDs(s.Pending), []string{op3.ID}) || b.Quantities()["cherry"] != 0 {
		t.Fatalf("op 3 not pending: %+v", s)
	}
	if b.Clock().Get("node-1@1") != 1 {
		t.Fatalf("clock %v", b.Clock())
	}
	res = mustApply(t, b, []Operation{op2})
	if res != (ApplyResult{Applied: 2}) {
		t.Fatalf("op 2: %+v", res)
	}
	s = b.Snapshot()
	if !reflect.DeepEqual(opIDs(s.Ops), []string{op1.ID, op2.ID, op3.ID}) || len(s.Pending) != 0 {
		t.Fatalf("delivery order %v, pending %d", opIDs(s.Ops), len(s.Pending))
	}
	if b.Clock().Get("node-1@1") != 3 {
		t.Fatalf("clock %v", b.Clock())
	}
}

func TestInvalidBatchIsAtomic(t *testing.T) {
	a, _ := newTestReplica("node-1", "node-1@1")
	add1 := mustAdd(t, a, "apple", 2)
	add2 := mustAdd(t, a, "pear", 1)
	rem := mustRemove(t, a, "apple", 1)
	add4 := mustAdd(t, a, "kiwi", 1)
	add5 := mustAdd(t, a, "plum", 1)

	b, _ := newTestReplica("node-2", "node-2@1")
	mustApply(t, b, []Operation{add1, add2})
	mustApply(t, b, []Operation{add5}) // leaves something pending too
	mustAdd(t, b, "lime", 1)
	before := snapJSON(t, b)

	mutations := map[string]func(op *Operation){
		"id mismatch":            func(op *Operation) { op.ID = op.Origin + ":77" },
		"origin without node":    func(op *Operation) { op.NodeID = "node-9" },
		"origin without @":       func(op *Operation) { op.Origin = "node-1"; op.ID = "node-1:3"; op.Clock = clock.VC{"node-1": 3} },
		"counter zero":           func(op *Operation) { op.Counter = 0; op.ID = op.Origin + ":0"; op.Clock[op.Origin] = 0 },
		"counter != clock":       func(op *Operation) { op.Clock[op.Origin] = op.Counter + 1 },
		"clock missing origin":   func(op *Operation) { delete(op.Clock, op.Origin) },
		"bad type":               func(op *Operation) { op.Type = "MULTIPLY" },
		"empty product":          func(op *Operation) { op.ProductID = "" },
		"quantity zero":          func(op *Operation) { op.Quantity = 0; op.Removes = nil },
		"quantity too large":     func(op *Operation) { op.Type = Add; op.Quantity = MaxQuantity + 1; op.Removes = nil },
		"add with removes":       func(op *Operation) { op.Type = Add },
		"remove count mismatch":  func(op *Operation) { op.Quantity = 2 },
		"remove duplicate units": func(op *Operation) { op.Quantity = 2; op.Removes = []string{op.Removes[0], op.Removes[0]} },
		"remove bad unit id":     func(op *Operation) { op.Removes = []string{"not-a-unit"} },
		"remove unit no index":   func(op *Operation) { op.Removes = []string{"node-1@1:1"} },
		"remove unit bad op id":  func(op *Operation) { op.Removes = []string{"node-1:1/0"} },
		"remove empty removes":   func(op *Operation) { op.Removes = nil },
	}
	for name, mutate := range mutations {
		bad := rem.Copy()
		mutate(&bad)
		if err := bad.Validate(); err == nil {
			t.Fatalf("%s: Validate accepted %+v", name, bad)
		}
		_, err := b.Apply([]Operation{add4.Copy(), bad, rem.Copy()})
		if !errors.Is(err, ErrInvalidOp) {
			t.Fatalf("%s: Apply error %v, want ErrInvalidOp", name, err)
		}
		if after := snapJSON(t, b); after != before {
			t.Fatalf("%s: state changed:\n before %s\n after  %s", name, before, after)
		}
	}
	if err := rem.Validate(); err != nil {
		t.Fatalf("valid remove rejected: %v", err)
	}
	if res := mustApply(t, b, []Operation{rem, add4}); res.Applied != 3 {
		t.Fatalf("valid batch: %+v", res)
	}
}

func TestRemoveUnknownUnitRejected(t *testing.T) {
	a, _ := newTestReplica("a", "a@1")
	mustAdd(t, a, "apple", 1)  // a@1:1/0
	mustAdd(t, a, "banana", 1) // a@1:2/0

	b, logs := newTestReplica("b", "b@1")
	syncPair(t, a, b)

	cases := []struct {
		name string
		op   Operation
	}{
		{"nonexistent unit", Operation{ID: "c@1:1", Origin: "c@1", NodeID: "c", Counter: 1, Type: Remove,
			ProductID: "apple", Quantity: 1, Clock: clock.VC{"c@1": 1, "a@1": 2}, Removes: []string{"a@1:9/0"}}},
		{"unit of another product", Operation{ID: "d@1:1", Origin: "d@1", NodeID: "d", Counter: 1, Type: Remove,
			ProductID: "apple", Quantity: 1, Clock: clock.VC{"d@1": 1, "a@1": 2}, Removes: []string{"a@1:2/0"}}},
		{"unit not observed by origin", Operation{ID: "e@1:1", Origin: "e@1", NodeID: "e", Counter: 1, Type: Remove,
			ProductID: "apple", Quantity: 1, Clock: clock.VC{"e@1": 1}, Removes: []string{"a@1:1/0"}}},
	}
	for _, c := range cases {
		before := b.Quantities()
		clk := b.Clock()
		res, err := b.Apply([]Operation{c.op})
		if err != nil {
			t.Fatalf("%s: unexpected error %v", c.name, err)
		}
		if res != (ApplyResult{Rejected: 1}) {
			t.Fatalf("%s: %+v, want Rejected=1", c.name, res)
		}
		if !reflect.DeepEqual(b.Quantities(), before) || b.Clock().Compare(clk) != clock.Equal {
			t.Fatalf("%s: state changed", c.name)
		}
		if len(b.Snapshot().Pending) != 0 {
			t.Fatalf("%s: rejected op left pending", c.name)
		}
		if !logs.HasEvent(logging.EventOpRejected, "op", c.op.ID) {
			t.Fatalf("%s: no op_rejected log:\n%s", c.name, logs)
		}
	}
	if q := b.Quantities(); q["apple"] != 1 || q["banana"] != 1 {
		t.Fatalf("quantities %v", q)
	}
}

func TestConcurrentAddSurvivesRemove(t *testing.T) {
	a, logsA := newTestReplica("a", "a@1")
	b, logsB := newTestReplica("b", "b@1")
	u1 := mustAdd(t, a, "apple", 1)
	syncPair(t, a, b)

	add := mustAdd(t, a, "apple", 1) // u2, concurrent with the remove
	rem := mustRemove(t, b, "apple", 1)
	if !reflect.DeepEqual(rem.Removes, []string{UnitID(u1.ID, 0)}) {
		t.Fatalf("remove picked %v", rem.Removes)
	}
	if add.Clock.Compare(rem.Clock) != clock.Concurrent {
		t.Fatalf("ops are not concurrent")
	}
	syncPair(t, a, b)

	u2 := UnitID(add.ID, 0)
	for _, r := range []*Replica{a, b} {
		if q := r.Quantities()["apple"]; q != 1 {
			t.Fatalf("%s: quantity %d, want 1", r.replicaID, q)
		}
		if got := liveUnitsOf(r, "apple"); !reflect.DeepEqual(got, []string{u2}) {
			t.Fatalf("%s: live units %v, want [%s]", r.replicaID, got, u2)
		}
		if got := r.Snapshot().Tombstones; !reflect.DeepEqual(got, []string{UnitID(u1.ID, 0)}) {
			t.Fatalf("%s: tombstones %v", r.replicaID, got)
		}
	}
	if !logsA.HasEvent(logging.EventConcurrentOps) || !logsB.HasEvent(logging.EventConcurrentOps) {
		t.Fatalf("concurrency not logged:\nA:\n%s\nB:\n%s", logsA, logsB)
	}
}

func TestConcurrentRemovesCollapse(t *testing.T) {
	a, _ := newTestReplica("a", "a@1")
	b, _ := newTestReplica("b", "b@1")
	add := mustAdd(t, a, "apple", 2)
	syncPair(t, a, b)

	ra := mustRemove(t, a, "apple", 1)
	rb := mustRemove(t, b, "apple", 1)
	u1 := UnitID(add.ID, 0)
	if !reflect.DeepEqual(ra.Removes, []string{u1}) || !reflect.DeepEqual(rb.Removes, []string{u1}) {
		t.Fatalf("removes picked %v and %v, want both %s", ra.Removes, rb.Removes, u1)
	}
	syncPair(t, a, b)
	for _, r := range []*Replica{a, b} {
		if q := r.Quantities()["apple"]; q != 1 {
			t.Fatalf("%s: quantity %d, want 1 (the two removes collapse)", r.replicaID, q)
		}
		if len(r.Snapshot().Ops) != 3 {
			t.Fatalf("%s: both removes should be recorded", r.replicaID)
		}
	}
}

func TestDeltaExact(t *testing.T) {
	a, _ := newTestReplica("a", "a@1")
	b, _ := newTestReplica("b", "b@1")
	mustAdd(t, a, "apple", 2)
	mustAdd(t, b, "pear", 1)
	syncPair(t, a, b)
	mustRemove(t, a, "pear", 1)
	partial := a.Clock()
	mustAdd(t, b, "plum", 3)
	syncPair(t, a, b)
	n1 := mustAdd(t, a, "kiwi", 1)
	n2 := mustRemove(t, a, "plum", 2)

	if d := a.Delta(a.Clock()); len(d) != 0 {
		t.Fatalf("delta for an equal clock: %v", opIDs(d))
	}
	full := a.Delta(clock.VC{})
	if !reflect.DeepEqual(sortedIDs(full), sortedIDs(a.Snapshot().Ops)) {
		t.Fatalf("delta({}) %v != log %v", sortedIDs(full), sortedIDs(a.Snapshot().Ops))
	}
	for i := 1; i < len(full); i++ {
		p, c := full[i-1], full[i]
		if p.Clock.Sum() > c.Clock.Sum() || (p.Clock.Sum() == c.Clock.Sum() && p.ID >= c.ID) {
			t.Fatalf("delta not sorted by (sum, id) at %d", i)
		}
	}
	// partial was a's clock before b's plum and a's two latest ops.
	got := sortedIDs(a.Delta(partial))
	plum := ""
	for _, op := range b.Snapshot().Ops {
		if op.ProductID == "plum" {
			plum = op.ID
		}
	}
	want := []string{plum, n1.ID, n2.ID}
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("partial delta %v, want %v", got, want)
	}

	// The full delta applied in order to a fresh replica never buffers.
	fresh, _ := newTestReplica("c", "c@1")
	for _, op := range full {
		res := mustApply(t, fresh, []Operation{op})
		if res.Buffered != 0 || res.Applied != 1 {
			t.Fatalf("op %s: %+v", op.ID, res)
		}
	}
	if len(fresh.Snapshot().Pending) != 0 || !reflect.DeepEqual(fresh.Quantities(), a.Quantities()) {
		t.Fatalf("fresh replica differs")
	}
	// DeltaAndClock is consistent with Delta and Clock.
	d, c := a.DeltaAndClock(partial)
	if !reflect.DeepEqual(sortedIDs(d), want) || c.Compare(a.Clock()) != clock.Equal {
		t.Fatalf("DeltaAndClock inconsistent")
	}
}

func TestNewIncarnationRecovers(t *testing.T) {
	n1, _ := newTestReplica("node-1", "node-1@1")
	old, _ := newTestReplica("node-2", "node-2@old")
	mustAdd(t, n1, "apple", 2)
	mustAdd(t, old, "pear", 3)
	mustRemove(t, old, "pear", 1)
	syncPair(t, n1, old)
	oldIDs := sortedIDs(old.Snapshot().Ops)

	// node-2 crashes; the new incarnation starts empty.
	fresh, _ := newTestReplica("node-2", "node-2@new")
	mustApply(t, fresh, n1.Delta(fresh.Clock()))
	if !reflect.DeepEqual(fresh.Quantities(), n1.Quantities()) {
		t.Fatalf("quantities %v, want %v", fresh.Quantities(), n1.Quantities())
	}
	if !reflect.DeepEqual(sortedIDs(fresh.Snapshot().Ops), sortedIDs(n1.Snapshot().Ops)) {
		t.Fatalf("op sets differ")
	}
	op := mustAdd(t, fresh, "kiwi", 1)
	if op.ID != "node-2@new:1" {
		t.Fatalf("new incarnation's first op is %s", op.ID)
	}
	for _, existing := range n1.Snapshot().Ops {
		if existing.ID == op.ID {
			t.Fatalf("op ID %s collides", op.ID)
		}
	}
	syncPair(t, n1, fresh)
	for _, r := range []*Replica{n1, fresh} {
		ids := map[string]bool{}
		for _, o := range r.Snapshot().Ops {
			ids[o.ID] = true
		}
		for _, id := range oldIDs {
			if !ids[id] {
				t.Fatalf("%s lost old op %s", r.replicaID, id)
			}
		}
		if !ids[op.ID] {
			t.Fatalf("%s lacks the new op", r.replicaID)
		}
		if r.Clock().Get("node-2@old") != 2 || r.Clock().Get("node-2@new") != 1 {
			t.Fatalf("%s clock %v", r.replicaID, r.Clock())
		}
	}
	if !reflect.DeepEqual(n1.Quantities(), fresh.Quantities()) {
		t.Fatalf("not converged")
	}
}

func TestUnitIDRoundTrip(t *testing.T) {
	id := UnitID("node-2@17:3", 4)
	if id != "node-2@17:3/4" {
		t.Fatalf("UnitID = %q", id)
	}
	op, i, err := ParseUnitID(id)
	if err != nil || op != "node-2@17:3" || i != 4 {
		t.Fatalf("ParseUnitID = %q %d %v", op, i, err)
	}
	for _, bad := range []string{"", "x", "a@1:1/", "a@1:1/-1", "a@1:1/x", "a@1/0", "a:1/0", "a@1:0/0", "a@1:x/0", "@1:1/0", "a@1:1/01"} {
		if _, _, err := ParseUnitID(bad); err == nil {
			t.Errorf("ParseUnitID(%q) accepted", bad)
		}
	}
}

func TestStatsMatchesSnapshotWithoutCopyingLog(t *testing.T) {
	r, _ := newTestReplica("node-1", "node-1@1")
	mustAdd(t, r, "apple", 2)
	mustRemove(t, r, "apple", 1)
	stats := r.Stats()
	snap := r.Snapshot()
	if stats.NodeID != snap.NodeID || stats.ReplicaID != snap.ReplicaID {
		t.Fatalf("identity: %+v", stats)
	}
	if !reflect.DeepEqual(stats.Clock, snap.Clock) {
		t.Fatalf("clock: %v vs %v", stats.Clock, snap.Clock)
	}
	if !reflect.DeepEqual(stats.Quantities, snap.Quantities) {
		t.Fatalf("quantities: %v vs %v", stats.Quantities, snap.Quantities)
	}
	if stats.Pending != len(snap.Pending) || stats.Applied != len(snap.Ops) || stats.Tombstones != len(snap.Tombstones) {
		t.Fatalf("gauges: %+v vs snapshot %+v", stats, snap)
	}
	stats.Clock["mutated"] = 99
	if _, ok := r.Stats().Clock["mutated"]; ok {
		t.Fatal("Stats clock aliases replica state")
	}
}
