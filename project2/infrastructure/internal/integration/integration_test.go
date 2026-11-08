// Package integration runs full nodes over real HTTP on 127.0.0.1.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"mc714/project2/internal/cart"
	"mc714/project2/internal/clock"
	"mc714/project2/internal/config"
	"mc714/project2/internal/logging"
	"mc714/project2/internal/node"
	"mc714/project2/internal/replication"
	"mc714/project2/internal/testutil"
	"mc714/project2/internal/topology"
)

const convergeTimeout = 5 * time.Second

type cluster struct {
	t              *testing.T
	n, maxN, extra int
	seed           uint64
	base           int
	graph          *topology.Graph
	nodes          []*node.Node // index k-1 is node-k; nil while stopped
	logs           []*testutil.LogCapture
	client         *http.Client
}

// allocateBlock finds B, a multiple of 10, with ports B+1 … B+n all free.
func allocateBlock(t *testing.T, n int) int {
	t.Helper()
	if n > 9 {
		t.Fatalf("the {index} template supports at most 9 nodes")
	}
	for attempt := 0; attempt < 200; attempt++ {
		base := (2000 + rand.IntN(4000)) * 10
		var ls []net.Listener
		ok := true
		for k := 1; k <= n; k++ {
			l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+k))
			if err != nil {
				ok = false
				break
			}
			ls = append(ls, l)
		}
		for _, l := range ls {
			l.Close()
		}
		if ok {
			return base
		}
	}
	t.Fatalf("no free port block")
	return 0
}

func newCluster(t *testing.T, n, maxN, extra int, seed uint64) *cluster {
	t.Helper()
	g, err := topology.Generate(n, maxN, extra, seed)
	if err != nil {
		t.Fatal(err)
	}
	c := &cluster{t: t, n: n, maxN: maxN, extra: extra, seed: seed, base: allocateBlock(t, n), graph: g,
		nodes: make([]*node.Node, n), logs: make([]*testutil.LogCapture, n),
		client: &http.Client{Timeout: 2 * time.Second}}
	for k := 1; k <= n; k++ {
		c.logs[k-1] = testutil.NewLogCapture()
		c.start(k)
	}
	t.Cleanup(func() {
		for k := 1; k <= n; k++ {
			if c.nodes[k-1] != nil {
				c.stop(k)
			}
		}
	})
	return c
}

func (c *cluster) config(k int) config.Config {
	c.t.Helper()
	env := map[string]string{
		"NODE_ID":           topology.NodeName(k),
		"NODE_COUNT":        strconv.Itoa(c.n),
		"MAX_NEIGHBORS":     strconv.Itoa(c.maxN),
		"EXTRA_EDGES":       strconv.Itoa(c.extra),
		"TOPOLOGY_SEED":     strconv.FormatUint(c.seed, 10),
		"GOSSIP_INTERVAL":   "50ms",
		"HTTP_TIMEOUT":      "200ms",
		"FAILURE_THRESHOLD": "3",
		"HTTP_ADDR":         fmt.Sprintf("127.0.0.1:%d", c.base+k),
		"PEER_URL_TEMPLATE": fmt.Sprintf("http://127.0.0.1:%d{index}", c.base/10),
		"LOG_LEVEL":         "debug",
	}
	cfg, err := config.Load(func(key string) string { return env[key] })
	if err != nil {
		c.t.Fatalf("config node-%d: %v", k, err)
	}
	return cfg
}

// start boots a new incarnation of node-k on its port.
func (c *cluster) start(k int) *node.Node {
	c.t.Helper()
	n, err := node.New(c.config(k), c.logs[k-1].Logger(topology.NodeName(k)))
	if err != nil {
		c.t.Fatalf("node.New node-%d: %v", k, err)
	}
	if err := n.Start(); err != nil {
		c.t.Fatalf("Start node-%d: %v", k, err)
	}
	c.nodes[k-1] = n
	return n
}

func (c *cluster) stop(k int) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.nodes[k-1].Stop(ctx); err != nil {
		c.t.Errorf("Stop node-%d: %v", k, err)
	}
	c.nodes[k-1] = nil
}

func (c *cluster) url(k int) string { return fmt.Sprintf("http://127.0.0.1:%d", c.base+k) }

func (c *cluster) post(k int, path string, body any) (int, []byte) {
	c.t.Helper()
	b, _ := json.Marshal(body)
	resp, err := c.client.Post(c.url(k)+path, "application/json", bytes.NewReader(b))
	if err != nil {
		c.t.Fatalf("POST %s on node-%d: %v", path, k, err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

func (c *cluster) op(k int, path, product string, q int) cart.Operation {
	c.t.Helper()
	code, body := c.post(k, path, map[string]any{"product_id": product, "quantity": q})
	if code != http.StatusCreated {
		c.t.Fatalf("POST %s on node-%d: %d %s", path, k, code, body)
	}
	var op cart.Operation
	if err := json.Unmarshal(body, &op); err != nil {
		c.t.Fatal(err)
	}
	return op
}

func (c *cluster) add(k int, product string, q int) cart.Operation {
	return c.op(k, "/cart/add", product, q)
}

func (c *cluster) remove(k int, product string, q int) cart.Operation {
	return c.op(k, "/cart/remove", product, q)
}

// cart reads GET /cart; ok is false when the node does not answer.
func (c *cluster) cart(k int) (map[string]int, bool) {
	resp, err := c.client.Get(c.url(k) + "/cart")
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	var out struct {
		Items map[string]int `json:"items"`
	}
	if resp.StatusCode != 200 || json.NewDecoder(resp.Body).Decode(&out) != nil {
		return nil, false
	}
	return out.Items, true
}

func opIDs(r *cart.Replica) []string {
	var ids []string
	for _, op := range r.Snapshot().Ops {
		ids = append(ids, op.ID)
	}
	sort.Strings(ids)
	return ids
}

// converged reports whether every running node has the same clock, op set
// and quantities, and nothing pending.
func (c *cluster) converged() bool {
	var ref *cart.Snapshot
	var refIDs []string
	for _, n := range c.nodes {
		if n == nil {
			return false
		}
		s := n.Replica().Snapshot()
		if len(s.Pending) != 0 {
			return false
		}
		if ref == nil {
			ref, refIDs = &s, opIDs(n.Replica())
			continue
		}
		if s.Clock.Compare(ref.Clock) != clock.Equal || !reflect.DeepEqual(s.Quantities, ref.Quantities) ||
			!reflect.DeepEqual(opIDs(n.Replica()), refIDs) {
			return false
		}
	}
	return true
}

func (c *cluster) waitConverged() {
	c.t.Helper()
	testutil.Eventually(c.t, convergeTimeout, c.converged, "cluster convergence")
}

func (c *cluster) hasOp(k int, id string) bool {
	for _, op := range c.nodes[k-1].Replica().Snapshot().Ops {
		if op.ID == id {
			return true
		}
	}
	return false
}

func TestIntegrationChainPropagation(t *testing.T) {
	c := newCluster(t, 4, 3, 0, 42)
	if got := c.nodes[0].Neighbors(); !reflect.DeepEqual(got, []string{"node-2"}) {
		t.Fatalf("node-1 neighbors %v", got)
	}
	c.add(1, "apple", 3)
	testutil.Eventually(t, convergeTimeout, func() bool {
		items, ok := c.cart(4)
		return ok && items["apple"] == 3
	}, "node-4 sees the add")
	for k := 1; k <= 4; k++ {
		if !c.logs[k-1].HasEvent(logging.EventConfiguredNeighbors) || !c.logs[k-1].HasEvent(logging.EventNodeStarted) {
			t.Fatalf("node-%d did not log startup events", k)
		}
	}
	if !c.logs[0].HasEvent(logging.EventOpCreated, "product", "apple", "quantity", "3") {
		t.Fatalf("node-1 did not log the add:\n%s", c.logs[0])
	}
	// Every exchange that moved the op is logged at info.
	moved := false
	for _, r := range c.logs[3].Records() {
		ev := r.Attrs["event"]
		moved = moved || r.Level == slog.LevelInfo &&
			(ev == logging.EventOpsReceived || ev == logging.EventAntiEntropySuccess && r.Attrs["applied"] != "0")
	}
	if !moved {
		t.Fatalf("node-4 logged no info-level gossip:\n%s", c.logs[3])
	}
	// node-1 and node-4 are not neighbors, so they never talked directly.
	for _, r := range c.logs[0].Records() {
		if p, ok := r.Attrs["peer"]; ok && p != "node-2" {
			t.Fatalf("node-1 talked to %s", p)
		}
	}
}

func TestIntegrationDuplicatePush(t *testing.T) {
	c := newCluster(t, 4, 3, 0, 42)
	for _, n := range c.nodes {
		n.PauseGossip()
	}
	src := cart.NewReplica("node-1", "node-1@external", nil)
	src.Add("apple", 3)
	src.Add("pear", 1)
	src.Remove("apple", 1)
	req := replication.PushRequest{SenderNode: "node-1", SenderReplica: "node-1@external", Ops: src.Delta(clock.VC{})}

	for i := 0; i < 5; i++ {
		code, body := c.post(2, "/internal/gossip/push", req)
		if code != 200 {
			t.Fatalf("push #%d: %d %s", i+1, code, body)
		}
		var resp replication.PushResponse
		json.Unmarshal(body, &resp)
		want := replication.PushResponse{Duplicates: 3}
		if i == 0 {
			want = replication.PushResponse{Applied: 3}
		}
		if resp != want {
			t.Fatalf("push #%d: %+v, want %+v", i+1, resp, want)
		}
	}
	items, ok := c.cart(2)
	if !ok || !reflect.DeepEqual(items, src.Quantities()) {
		t.Fatalf("node-2 cart %v, want %v", items, src.Quantities())
	}
	if len(c.nodes[1].Replica().Snapshot().Ops) != 3 {
		t.Fatalf("duplicates were recorded")
	}
}

func TestIntegrationRemoval(t *testing.T) {
	c := newCluster(t, 4, 3, 0, 42)
	c.add(1, "apple", 3)
	c.waitConverged()
	c.remove(4, "apple", 2)
	testutil.Eventually(t, convergeTimeout, func() bool {
		for k := 1; k <= 4; k++ {
			items, ok := c.cart(k)
			if !ok || items["apple"] != 1 {
				return false
			}
		}
		return true
	}, "every node shows apple=1")
	c.remove(2, "apple", 1)
	testutil.Eventually(t, convergeTimeout, func() bool {
		for k := 1; k <= 4; k++ {
			items, ok := c.cart(k)
			if _, present := items["apple"]; !ok || present {
				return false
			}
		}
		return true
	}, "apple disappears everywhere")
	c.waitConverged()
}

func TestIntegrationConcurrentOps(t *testing.T) {
	c := newCluster(t, 4, 3, 0, 42)
	base := c.add(1, "apple", 2)
	c.waitConverged()
	for _, n := range c.nodes {
		n.PauseGossip()
	}

	ops := []cart.Operation{
		base,
		c.add(1, "apple", 1),
		c.remove(4, "apple", 1),
		c.remove(2, "apple", 1),
		c.add(3, "banana", 3),
	}
	// While paused, nothing crosses: every node only sees its own op.
	time.Sleep(150 * time.Millisecond)
	if c.hasOp(2, ops[1].ID) || c.hasOp(1, ops[2].ID) {
		t.Fatalf("an op crossed while gossip was paused")
	}
	if ops[2].Clock.Compare(ops[3].Clock) != clock.Concurrent || ops[1].Clock.Compare(ops[2].Clock) != clock.Concurrent {
		t.Fatalf("ops are not concurrent")
	}

	// Expected quantities: units added minus the union of tombstones.
	created := map[string]int{}
	unitProduct := map[string]string{}
	dead := map[string]bool{}
	for _, op := range ops {
		if op.Type == cart.Add {
			created[op.ProductID] += op.Quantity
			for i := 0; i < op.Quantity; i++ {
				unitProduct[cart.UnitID(op.ID, i)] = op.ProductID
			}
		}
		for _, u := range op.Removes {
			dead[u] = true
		}
	}
	want := map[string]int{}
	for p, n := range created {
		want[p] = n
	}
	for u := range dead {
		want[unitProduct[u]]--
	}
	if want["apple"] != 2 || want["banana"] != 3 {
		t.Fatalf("expected quantities %v: the two removes should collapse onto one unit", want)
	}

	for _, n := range c.nodes {
		n.ResumeGossip()
	}
	c.waitConverged()
	for k := 1; k <= 4; k++ {
		items, ok := c.cart(k)
		if !ok || !reflect.DeepEqual(items, want) {
			t.Fatalf("node-%d cart %v, want %v", k, items, want)
		}
	}
	logged := false
	for _, l := range c.logs {
		logged = logged || l.HasEvent(logging.EventConcurrentOps)
	}
	if !logged {
		t.Fatalf("concurrent_ops_detected was not logged")
	}
}

func TestIntegrationRestartRecovery(t *testing.T) {
	c := newCluster(t, 4, 3, 0, 42)
	c.add(1, "apple", 2)
	c.add(2, "pear", 1)
	c.add(4, "plum", 3)
	c.waitConverged()
	c.remove(3, "apple", 1)
	c.waitConverged()
	before := opIDs(c.nodes[0].Replica())
	oldID := c.nodes[1].ReplicaID()

	c.stop(2)
	testutil.Eventually(t, convergeTimeout, func() bool {
		return c.logs[0].HasEvent(logging.EventPeerUnreachable, "peer", "node-2")
	}, "node-1 logs node-2 unreachable")

	n2 := c.start(2)
	if n2.ReplicaID() == oldID {
		t.Fatalf("restart reused replica ID %s", oldID)
	}
	c.waitConverged()
	if got := opIDs(n2.Replica()); !reflect.DeepEqual(got, before) {
		t.Fatalf("node-2 recovered %v, want %v", got, before)
	}
	for _, k := range []int{1, 3} {
		if got := opIDs(c.nodes[k-1].Replica()); !reflect.DeepEqual(got, before) {
			t.Fatalf("node-%d lost ops: %v, want %v", k, got, before)
		}
	}
	testutil.Eventually(t, convergeTimeout, func() bool {
		return c.logs[0].HasEvent(logging.EventPeerReconnected, "peer", "node-2")
	}, "node-1 logs node-2 reconnected")

	op := c.add(2, "kiwi", 1)
	if op.Origin != n2.ReplicaID() || op.ID != n2.ReplicaID()+":1" {
		t.Fatalf("new op %s does not use the new replica ID %s", op.ID, n2.ReplicaID())
	}
	testutil.Eventually(t, convergeTimeout, func() bool { return c.hasOp(4, op.ID) }, "new op reaches node-4")
	c.waitConverged()
	if c.nodes[0].Replica().Clock().Get(oldID) != 1 {
		t.Fatalf("old incarnation's clock entry lost")
	}
}

func TestIntegrationUnavailableNeighbor(t *testing.T) {
	c := newCluster(t, 4, 3, 0, 42)
	c.add(1, "apple", 1)
	c.waitConverged()

	c.stop(3)
	for _, k := range []int{2, 4} {
		k := k
		testutil.Eventually(t, convergeTimeout, func() bool {
			return c.logs[k-1].HasEvent(logging.EventPeerUnreachable, "peer", "node-3")
		}, "node-%d logs node-3 unreachable", k)
	}
	op := c.add(1, "pear", 2)
	testutil.Eventually(t, convergeTimeout, func() bool { return c.hasOp(2, op.ID) }, "op reaches node-2")
	time.Sleep(200 * time.Millisecond)
	if c.hasOp(4, op.ID) {
		t.Fatalf("op reached node-4 while node-3 was down")
	}

	c.start(3)
	testutil.Eventually(t, convergeTimeout, func() bool { return c.hasOp(4, op.ID) }, "op reaches node-4 after node-3 returns")
	c.waitConverged()
	for _, k := range []int{2, 4} {
		k := k
		testutil.Eventually(t, convergeTimeout, func() bool {
			return c.logs[k-1].HasEvent(logging.EventPeerReconnected, "peer", "node-3")
		}, "node-%d logs node-3 reconnected", k)
	}
}

func TestIntegrationRandomTopologyConvergence(t *testing.T) {
	c := newCluster(t, 8, 3, 4, 7)
	rng := rand.New(rand.NewPCG(7, 7))
	products := []string{"apple", "pear", "plum", "kiwi"}
	for i := 0; i < 40; i++ {
		k := 1 + rng.IntN(8)
		p := products[rng.IntN(len(products))]
		items, _ := c.cart(k)
		if items[p] > 0 && rng.IntN(3) == 0 {
			code, body := c.post(k, "/cart/remove", map[string]any{"product_id": p, "quantity": 1})
			if code != http.StatusCreated && code != http.StatusConflict {
				t.Fatalf("remove: %d %s", code, body)
			}
		} else {
			c.add(k, p, 1+rng.IntN(3))
		}
		if i%10 == 0 {
			time.Sleep(30 * time.Millisecond)
		}
	}
	c.waitConverged()
	for k := 1; k <= 8; k++ {
		id := topology.NodeName(k)
		nb := map[string]bool{}
		for _, p := range c.graph.Neighbors(id) {
			nb[p] = true
		}
		if got := c.nodes[k-1].Neighbors(); !reflect.DeepEqual(got, c.graph.Neighbors(id)) {
			t.Fatalf("%s neighbors %v, want %v", id, got, c.graph.Neighbors(id))
		}
		for _, r := range c.logs[k-1].Records() {
			if p, ok := r.Attrs["peer"]; ok && !nb[p] {
				t.Fatalf("%s logged gossip with non-neighbor %s", id, p)
			}
		}
		if !c.logs[k-1].HasEvent(logging.EventAntiEntropySuccess) {
			t.Fatalf("%s never logged a successful exchange", id)
		}
	}
	ref, _ := c.cart(1)
	for k := 2; k <= 8; k++ {
		items, ok := c.cart(k)
		if !ok || !reflect.DeepEqual(items, ref) {
			t.Fatalf("node-%d cart %v, want %v", k, items, ref)
		}
	}
}
