package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"mc714/project2/internal/cart"
	"mc714/project2/internal/clock"
	"mc714/project2/internal/logging"
	"mc714/project2/internal/testutil"
	"mc714/project2/internal/topology"
)

// call is one recorded transport request.
type call struct{ from, to, kind string }

// fakeNet routes Pull/Push to engines in memory. Messages are JSON
// round-tripped so no state is shared between nodes.
type fakeNet struct {
	mu        sync.Mutex
	engines   map[string]*Engine
	calls     []call
	downLinks map[[2]string]bool
	downNodes map[string]bool
}

func newFakeNet() *fakeNet {
	return &fakeNet{engines: map[string]*Engine{}, downLinks: map[[2]string]bool{}, downNodes: map[string]bool{}}
}

func linkKey(a, b string) [2]string {
	if a > b {
		a, b = b, a
	}
	return [2]string{a, b}
}

func (n *fakeNet) setLink(a, b string, down bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.downLinks[linkKey(a, b)] = down
}

func (n *fakeNet) route(from, url, kind string) (*Engine, error) {
	to := strings.TrimPrefix(url, "fake://")
	n.mu.Lock()
	defer n.mu.Unlock()
	n.calls = append(n.calls, call{from, to, kind})
	if n.downLinks[linkKey(from, to)] || n.downNodes[to] || n.downNodes[from] {
		return nil, fmt.Errorf("fake: link %s-%s is down", from, to)
	}
	e, ok := n.engines[to]
	if !ok {
		return nil, fmt.Errorf("fake: no node at %s", url)
	}
	return e, nil
}

func (n *fakeNet) recorded() []call {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]call(nil), n.calls...)
}

func roundTrip[T any](t testing.TB, v T) T {
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out T
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

type fakeTransport struct {
	t    testing.TB
	net  *fakeNet
	from string
}

func (f *fakeTransport) Pull(ctx context.Context, url string, req PullRequest) (PullResponse, error) {
	e, err := f.net.route(f.from, url, "pull")
	if err != nil {
		return PullResponse{}, err
	}
	resp, err := e.HandlePull(ctx, roundTrip(f.t, req))
	return roundTrip(f.t, resp), err
}

func (f *fakeTransport) Push(ctx context.Context, url string, req PushRequest) (PushResponse, error) {
	e, err := f.net.route(f.from, url, "push")
	if err != nil {
		return PushResponse{}, err
	}
	resp, err := e.HandlePush(ctx, roundTrip(f.t, req))
	return roundTrip(f.t, resp), err
}

type fakeNode struct {
	id      string
	replica *cart.Replica
	engine  *Engine
	logs    *testutil.LogCapture
}

type fakeCluster struct {
	net   *fakeNet
	graph *topology.Graph
	nodes []*fakeNode // index k-1 is node-k
}

func newFakeCluster(t *testing.T, n, maxN, extra int, seed uint64, threshold int) *fakeCluster {
	t.Helper()
	g, err := topology.Generate(n, maxN, extra, seed)
	if err != nil {
		t.Fatalf("topology: %v", err)
	}
	c := &fakeCluster{net: newFakeNet(), graph: g}
	for k := 1; k <= n; k++ {
		id := topology.NodeName(k)
		logs := testutil.NewLogCapture()
		logger := logs.Logger(id)
		rep := cart.NewReplica(id, fmt.Sprintf("%s@%d", id, k), logger)
		var peers []Peer
		for _, p := range g.Neighbors(id) {
			peers = append(peers, Peer{ID: p, URL: "fake://" + p})
		}
		e := NewEngine(EngineConfig{
			NodeID: id, Replica: rep, Peers: peers,
			Transport: &fakeTransport{t: t, net: c.net, from: id},
			Interval:  time.Hour, FailureThreshold: threshold, Logger: logger,
		})
		c.net.engines[id] = e
		c.nodes = append(c.nodes, &fakeNode{id: id, replica: rep, engine: e, logs: logs})
	}
	return c
}

func (c *fakeCluster) node(k int) *fakeNode { return c.nodes[k-1] }

// tick runs one round on every node, in the given order (default 1..N).
func (c *fakeCluster) tick(order ...int) {
	if len(order) == 0 {
		for k := 1; k <= len(c.nodes); k++ {
			order = append(order, k)
		}
	}
	for _, k := range order {
		c.node(k).engine.Round(context.Background())
	}
}

func hasOp(r *cart.Replica, id string) bool {
	for _, op := range r.Snapshot().Ops {
		if op.ID == id {
			return true
		}
	}
	return false
}

func opSet(r *cart.Replica) []string {
	var ids []string
	for _, op := range r.Snapshot().Ops {
		ids = append(ids, op.ID)
	}
	sort.Strings(ids)
	return ids
}

func identical(nodes []*fakeNode) error {
	ref := nodes[0].replica
	for _, n := range nodes[1:] {
		if n.replica.Clock().Compare(ref.Clock()) != clock.Equal {
			return fmt.Errorf("%s clock %v != %v", n.id, n.replica.Clock(), ref.Clock())
		}
		if !reflect.DeepEqual(opSet(n.replica), opSet(ref)) {
			return fmt.Errorf("%s op set differs", n.id)
		}
		if !reflect.DeepEqual(n.replica.Quantities(), ref.Quantities()) {
			return fmt.Errorf("%s quantities %v != %v", n.id, n.replica.Quantities(), ref.Quantities())
		}
	}
	return nil
}

func TestRejectsNonNeighbor(t *testing.T) {
	c := newFakeCluster(t, 4, 3, 0, 42, 3)
	n1 := c.node(1)
	if _, err := n1.replica.Add("apple", 2); err != nil {
		t.Fatal(err)
	}
	stranger := cart.NewReplica("node-3", "node-3@x", nil)
	foreign, err := stranger.Add("pear", 1)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(n1.replica.Snapshot())

	for _, sender := range []string{"node-3", "node-4", "node-9", "node-1", ""} {
		_, err := n1.engine.HandlePull(context.Background(), PullRequest{SenderNode: sender, SenderReplica: sender + "@1", Clock: clock.VC{}})
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("pull from %q: err %v, want ErrForbidden", sender, err)
		}
		_, err = n1.engine.HandlePush(context.Background(), PushRequest{SenderNode: sender, SenderReplica: sender + "@1", Ops: []cart.Operation{foreign}})
		if !errors.Is(err, ErrForbidden) {
			t.Fatalf("push from %q: err %v, want ErrForbidden", sender, err)
		}
	}
	after, _ := json.Marshal(n1.replica.Snapshot())
	if string(before) != string(after) {
		t.Fatalf("forbidden requests changed state")
	}
	resp, err := n1.engine.HandlePull(context.Background(), PullRequest{SenderNode: "node-2", SenderReplica: "node-2@2", Clock: clock.VC{}})
	if err != nil || len(resp.Ops) != 1 || resp.Node != "node-1" || resp.Replica != "node-1@1" {
		t.Fatalf("neighbor pull: %+v %v", resp, err)
	}
	if got := n1.engine.Neighbors(); !reflect.DeepEqual(got, []string{"node-2"}) {
		t.Fatalf("neighbors %v", got)
	}
}

func TestPartitionAndHeal(t *testing.T) {
	const threshold = 3
	c := newFakeCluster(t, 4, 3, 0, 42, threshold)
	c.net.setLink("node-2", "node-3", true)

	left, err := c.node(1).replica.Add("apple", 2)
	if err != nil {
		t.Fatal(err)
	}
	right, err := c.node(4).replica.Add("pear", 1)
	if err != nil {
		t.Fatal(err)
	}
	for tick := 1; tick <= threshold+2; tick++ {
		c.tick()
		for k := 1; k <= 2; k++ {
			if hasOp(c.node(k).replica, right.ID) {
				t.Fatalf("tick %d: node-%d got an op across the partition", tick, k)
			}
		}
		for k := 3; k <= 4; k++ {
			if hasOp(c.node(k).replica, left.ID) {
				t.Fatalf("tick %d: node-%d got an op across the partition", tick, k)
			}
		}
	}
	n2 := c.node(2)
	if n2.logs.CountEvent(logging.EventPeerUnreachable, "peer", "node-3") != 1 {
		t.Fatalf("node-2 did not log unreachable for node-3 exactly once:\n%s", n2.logs)
	}
	if !c.node(3).logs.HasEvent(logging.EventPeerUnreachable, "peer", "node-2") {
		t.Fatalf("node-3 did not log unreachable for node-2")
	}
	if n2.logs.HasEvent(logging.EventPeerUnreachable, "peer", "node-1") {
		t.Fatalf("node-2 logged its healthy neighbor as unreachable")
	}
	if !n2.logs.HasEvent(logging.EventPeerConnected, "peer", "node-1") {
		t.Fatalf("node-2 did not log node-1 as connected")
	}

	c.net.setLink("node-2", "node-3", false)
	converged := false
	for tick := 0; tick < 5 && !converged; tick++ {
		c.tick()
		converged = identical(c.nodes) == nil
	}
	if err := identical(c.nodes); err != nil {
		t.Fatalf("no convergence after heal: %v", err)
	}
	for k := 1; k <= 4; k++ {
		if !hasOp(c.node(k).replica, left.ID) || !hasOp(c.node(k).replica, right.ID) {
			t.Fatalf("node-%d lacks an op after heal", k)
		}
	}
	if !n2.logs.HasEvent(logging.EventPeerReconnected, "peer", "node-3") {
		t.Fatalf("node-2 did not log reconnected:\n%s", n2.logs)
	}
	if !n2.logs.HasEvent(logging.EventAntiEntropyFailed, "peer", "node-3") {
		t.Fatalf("failed exchanges were not logged")
	}
	for _, st := range n2.engine.PeerStatuses() {
		if st.State != "reachable" || st.ConsecutiveFailures != 0 {
			t.Fatalf("peer status after heal: %+v", st)
		}
	}
}

func TestRoundPushesOnlyMissing(t *testing.T) {
	c := newFakeCluster(t, 3, 2, 0, 42, 3)
	n1, n2 := c.node(1), c.node(2)
	shared, err := n1.replica.Add("apple", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n2.replica.Apply([]cart.Operation{shared}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := n1.replica.Add("pear", 1); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := n2.replica.Add("plum", 2); err != nil {
		t.Fatal(err)
	}

	results := n1.engine.Round(context.Background())
	if len(results) != 1 || results[0].Peer != "node-2" {
		t.Fatalf("results %+v", results)
	}
	r := results[0]
	if r.Err != nil || r.Pulled != 1 || r.Applied != 1 || r.Pushed != 3 {
		t.Fatalf("round result %+v, want pulled=1 applied=1 pushed=3", r)
	}
	if err := identical([]*fakeNode{n1, n2}); err != nil {
		t.Fatal(err)
	}
	if !n1.logs.HasEvent(logging.EventAntiEntropySuccess, "peer", "node-2", "applied", "1", "pushed", "3") {
		t.Fatalf("success not logged with counts:\n%s", n1.logs)
	}

	before := len(c.net.recorded())
	results = n1.engine.Round(context.Background())
	if results[0].Err != nil || results[0].Pulled != 0 || results[0].Pushed != 0 {
		t.Fatalf("in-sync round %+v", results[0])
	}
	for _, cl := range c.net.recorded()[before:] {
		if cl.kind == "push" {
			t.Fatalf("in-sync round pushed")
		}
	}
}

func TestPauseStopsRun(t *testing.T) {
	c := newFakeCluster(t, 3, 2, 0, 42, 3)
	n1 := c.node(1)
	e := NewEngine(EngineConfig{
		NodeID: "node-1", Replica: n1.replica, Peers: []Peer{{ID: "node-2", URL: "fake://node-2"}},
		Transport: &fakeTransport{t: t, net: c.net, from: "node-1"},
		Interval:  5 * time.Millisecond, FailureThreshold: 3, Logger: n1.logs.Logger("node-1"),
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.Run(ctx); close(done) }()
	testutil.Eventually(t, 2*time.Second, func() bool { return len(c.net.recorded()) > 0 }, "Run never ran a round")

	e.Pause()
	n := len(c.net.recorded())
	time.Sleep(50 * time.Millisecond)
	if len(c.net.recorded()) != n {
		t.Fatalf("rounds ran while paused")
	}
	e.Resume()
	testutil.Eventually(t, 2*time.Second, func() bool { return len(c.net.recorded()) > n }, "Run did not resume")
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not return after cancel")
	}
}
