package transport

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mc714/project2/internal/cart"
	"mc714/project2/internal/clock"
	"mc714/project2/internal/replication"
	"mc714/project2/internal/testutil"
)

type fixture struct {
	replica *cart.Replica
	engine  *replication.Engine
	srv     *httptest.Server
}

// newFixture serves node-1, whose only neighbor is node-2.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	logs := testutil.NewLogCapture()
	rep := cart.NewReplica("node-1", "node-1@1", logs.Logger("node-1"))
	eng := replication.NewEngine(replication.EngineConfig{
		NodeID: "node-1", Replica: rep,
		Peers:     []replication.Peer{{ID: "node-2", URL: "http://127.0.0.1:1"}},
		Transport: NewHTTPTransport(time.Second), Interval: time.Hour, FailureThreshold: 3,
		Logger: logs.Logger("node-1"),
	})
	srv := httptest.NewServer(NewServer(rep, eng))
	t.Cleanup(srv.Close)
	return &fixture{replica: rep, engine: eng, srv: srv}
}

func (f *fixture) do(t *testing.T, method, path, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, f.srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func snap(t *testing.T, r *cart.Replica) string { return mustJSON(t, r.Snapshot()) }

func expectError(t *testing.T, label string, code, want int, body []byte) {
	t.Helper()
	if code != want {
		t.Fatalf("%s: status %d, want %d (body %s)", label, code, want, body)
	}
	var e struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil || e.Error == "" {
		t.Fatalf("%s: error body %q is not {\"error\": ...}", label, body)
	}
}

func TestEndpointsStatusCodes(t *testing.T) {
	f := newFixture(t)

	code, body := f.do(t, "GET", "/health", "")
	var health map[string]string
	if code != 200 || json.Unmarshal(body, &health) != nil || health["node"] != "node-1" || health["replica"] != "node-1@1" {
		t.Fatalf("/health: %d %s", code, body)
	}

	code, body = f.do(t, "POST", "/cart/add", `{"product_id":"apple","quantity":2}`)
	var op cart.Operation
	if code != 201 || json.Unmarshal(body, &op) != nil || op.ID != "node-1@1:1" || op.Quantity != 2 || op.Type != cart.Add {
		t.Fatalf("/cart/add: %d %s", code, body)
	}

	code, body = f.do(t, "GET", "/cart", "")
	var c struct {
		Node    string         `json:"node"`
		Replica string         `json:"replica"`
		Items   map[string]int `json:"items"`
	}
	if code != 200 || json.Unmarshal(body, &c) != nil || c.Node != "node-1" || c.Replica != "node-1@1" || c.Items["apple"] != 2 || len(c.Items) != 1 {
		t.Fatalf("/cart: %d %s", code, body)
	}

	for label, b := range map[string]string{
		"bad json":      `{"product_id":`,
		"unknown field": `{"product_id":"apple","quantity":1,"color":"red"}`,
		"zero quantity": `{"product_id":"apple","quantity":0}`,
		"neg quantity":  `{"product_id":"apple","quantity":-1}`,
		"empty product": `{"product_id":"","quantity":1}`,
		"huge quantity": `{"product_id":"apple","quantity":10001}`,
		"trailing data": `{"product_id":"apple","quantity":1} {}`,
		"wrong type":    `{"product_id":"apple","quantity":"1"}`,
	} {
		before := snap(t, f.replica)
		code, body := f.do(t, "POST", "/cart/add", b)
		expectError(t, "add "+label, code, 400, body)
		code, body = f.do(t, "POST", "/cart/remove", b)
		expectError(t, "remove "+label, code, 400, body)
		if snap(t, f.replica) != before {
			t.Fatalf("%s: state changed", label)
		}
	}

	code, body = f.do(t, "POST", "/cart/remove", `{"product_id":"apple","quantity":5}`)
	expectError(t, "remove insufficient", code, 409, body)
	code, body = f.do(t, "POST", "/cart/remove", `{"product_id":"ghost","quantity":1}`)
	expectError(t, "remove unknown product", code, 409, body)
	code, body = f.do(t, "POST", "/cart/remove", `{"product_id":"apple","quantity":1}`)
	if code != 201 || json.Unmarshal(body, &op) != nil || op.Type != cart.Remove || len(op.Removes) != 1 {
		t.Fatalf("remove: %d %s", code, body)
	}

	// Gossip from a non-neighbor is forbidden; from a neighbor it works.
	pullFrom := func(sender string) string {
		return mustJSON(t, replication.PullRequest{SenderNode: sender, SenderReplica: sender + "@1", Clock: clock.VC{}})
	}
	before := snap(t, f.replica)
	code, body = f.do(t, "POST", "/internal/gossip/pull", pullFrom("node-3"))
	expectError(t, "pull non-neighbor", code, 403, body)
	other := cart.NewReplica("node-3", "node-3@1", nil)
	foreign, _ := other.Add("pear", 1)
	code, body = f.do(t, "POST", "/internal/gossip/push",
		mustJSON(t, replication.PushRequest{SenderNode: "node-3", SenderReplica: "node-3@1", Ops: []cart.Operation{foreign}}))
	expectError(t, "push non-neighbor", code, 403, body)
	if snap(t, f.replica) != before {
		t.Fatalf("forbidden gossip changed state")
	}
	code, body = f.do(t, "POST", "/internal/gossip/pull", pullFrom("node-2"))
	var pr replication.PullResponse
	if code != 200 || json.Unmarshal(body, &pr) != nil || len(pr.Ops) != 2 || pr.Clock.Get("node-1@1") != 2 {
		t.Fatalf("pull from neighbor: %d %s", code, body)
	}
	code, body = f.do(t, "POST", "/internal/gossip/push",
		mustJSON(t, replication.PushRequest{SenderNode: "node-2", SenderReplica: "node-2@1", Ops: []cart.Operation{foreign}}))
	var push replication.PushResponse
	if code != 200 || json.Unmarshal(body, &push) != nil || push.Applied != 1 {
		t.Fatalf("push from neighbor: %d %s", code, body)
	}

	// Leave one op pending so /debug/state shows it.
	gap := cart.NewReplica("node-4", "node-4@1", nil)
	gap.Add("kiwi", 1)
	second, _ := gap.Add("kiwi", 1)
	f.do(t, "POST", "/internal/gossip/push",
		mustJSON(t, replication.PushRequest{SenderNode: "node-2", SenderReplica: "node-2@1", Ops: []cart.Operation{second}}))

	code, body = f.do(t, "GET", "/debug/state", "")
	var dbg struct {
		Node      string                   `json:"node"`
		Replica   string                   `json:"replica"`
		Clock     clock.VC                 `json:"clock"`
		Pending   []cart.Operation         `json:"pending"`
		Ops       []cart.Operation         `json:"ops"`
		Neighbors []string                 `json:"neighbors"`
		Peers     []replication.PeerStatus `json:"peers"`
	}
	if code != 200 || json.Unmarshal(body, &dbg) != nil {
		t.Fatalf("/debug/state: %d %s", code, body)
	}
	if dbg.Node != "node-1" || dbg.Clock.Get("node-1@1") != 2 || dbg.Clock.Get("node-3@1") != 1 || len(dbg.Pending) != 1 ||
		len(dbg.Ops) != 3 || len(dbg.Neighbors) != 1 || dbg.Neighbors[0] != "node-2" ||
		len(dbg.Peers) != 1 || dbg.Peers[0].ID != "node-2" || dbg.Peers[0].State != "unknown" {
		t.Fatalf("/debug/state content: %s", body)
	}

	code, body = f.do(t, "GET", "/debug/clean-state", "")
	var clean map[string]any
	if code != 200 || json.Unmarshal(body, &clean) != nil || len(clean) != 2 || clean["node"] != "node-1" {
		t.Fatalf("/debug/clean-state: %d %s", code, body)
	}
	if items, _ := clean["items"].(map[string]any); len(items) == 0 {
		t.Fatalf("/debug/clean-state items: %s", body)
	}

	if code, _ := f.do(t, "GET", "/cart/add", ""); code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /cart/add: %d", code)
	}
	if code, _ := f.do(t, "GET", "/nope", ""); code != 404 {
		t.Fatalf("unknown path: %d", code)
	}
}

func TestMalformedGossipLeavesStateUnchanged(t *testing.T) {
	f := newFixture(t)
	f.do(t, "POST", "/cart/add", `{"product_id":"apple","quantity":2}`)

	src := cart.NewReplica("node-2", "node-2@1", nil)
	good, _ := src.Add("pear", 1)
	bad, _ := src.Add("plum", 1)
	bad.Counter = 7 // ID and clock no longer match
	before := snap(t, f.replica)

	for label, body := range map[string]string{
		"invalid op":       mustJSON(t, replication.PushRequest{SenderNode: "node-2", SenderReplica: "node-2@1", Ops: []cart.Operation{good, bad}}),
		"bad json":         `{"sender_node":"node-2","ops":[`,
		"unknown op field": `{"sender_node":"node-2","sender_replica":"node-2@1","ops":[{"id":"x","bogus":1}]}`,
	} {
		code, resp := f.do(t, "POST", "/internal/gossip/push", body)
		expectError(t, label, code, 400, resp)
		if snap(t, f.replica) != before {
			t.Fatalf("%s: state changed", label)
		}
	}
	code, resp := f.do(t, "POST", "/internal/gossip/pull", `{"sender_node":"node-2","clock":{"x":-1}}`)
	expectError(t, "bad pull", code, 400, resp)
}

func TestOversizedBodyRejected(t *testing.T) {
	f := newFixture(t)
	before := snap(t, f.replica)
	huge := strings.Repeat("a", MaxBodyBytes+1024)
	for _, path := range []string{"/cart/add", "/cart/remove", "/internal/gossip/push", "/internal/gossip/pull"} {
		var body string
		switch path {
		case "/internal/gossip/push":
			body = `{"sender_node":"node-2","sender_replica":"node-2@1","ops":[{"product_id":"` + huge + `"}]}`
		case "/internal/gossip/pull":
			body = `{"sender_node":"node-2","sender_replica":"` + huge + `","clock":{}}`
		default:
			body = `{"product_id":"` + huge + `","quantity":1}`
		}
		code, _ := f.do(t, "POST", path, body)
		if code != 400 && code != http.StatusRequestEntityTooLarge {
			t.Fatalf("%s: status %d, want 400 or 413", path, code)
		}
	}
	if snap(t, f.replica) != before {
		t.Fatalf("oversized bodies changed state")
	}
}

func TestClientTimeout(t *testing.T) {
	const timeout = 200 * time.Millisecond
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(3 * timeout):
		case <-r.Context().Done():
		}
		w.Write([]byte(`{}`))
	}))
	defer slow.Close()
	tr := NewHTTPTransport(timeout)

	start := time.Now()
	_, err := tr.Pull(context.Background(), slow.URL, replication.PullRequest{SenderNode: "node-2"})
	if err == nil {
		t.Fatalf("pull against a slow peer succeeded")
	}
	if el := time.Since(start); el > timeout+500*time.Millisecond {
		t.Fatalf("pull took %v", el)
	}
	start = time.Now()
	if _, err := tr.Push(context.Background(), slow.URL, replication.PushRequest{SenderNode: "node-2"}); err == nil {
		t.Fatalf("push against a slow peer succeeded")
	}
	if el := time.Since(start); el > timeout+500*time.Millisecond {
		t.Fatalf("push took %v", el)
	}
}

func TestClientNon2xxIsError(t *testing.T) {
	status := 200
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		if r.URL.Path == "/internal/gossip/pull" {
			w.Write([]byte(`{"node":"node-2","replica":"node-2@1","clock":{"node-2@1":1},"ops":[]}`))
		} else {
			w.Write([]byte(`{"applied":1,"duplicates":0,"buffered":0,"rejected":0}`))
		}
	}))
	defer srv.Close()
	tr := NewHTTPTransport(time.Second)
	ctx := context.Background()

	pr, err := tr.Pull(ctx, srv.URL, replication.PullRequest{SenderNode: "node-1"})
	if err != nil || pr.Node != "node-2" || pr.Clock.Get("node-2@1") != 1 {
		t.Fatalf("200 pull: %+v %v", pr, err)
	}
	ps, err := tr.Push(ctx, srv.URL, replication.PushRequest{SenderNode: "node-1"})
	if err != nil || ps.Applied != 1 {
		t.Fatalf("200 push: %+v %v", ps, err)
	}
	for _, s := range []int{400, 403, 404, 409, 500, 503} {
		status = s
		if _, err := tr.Pull(ctx, srv.URL, replication.PullRequest{}); err == nil {
			t.Fatalf("pull with status %d returned no error", s)
		}
		if _, err := tr.Push(ctx, srv.URL, replication.PushRequest{}); err == nil {
			t.Fatalf("push with status %d returned no error", s)
		}
	}
	// A refused connection is also an error.
	if _, err := tr.Pull(ctx, "http://127.0.0.1:1", replication.PullRequest{}); err == nil {
		t.Fatalf("pull to a closed port returned no error")
	}
}
