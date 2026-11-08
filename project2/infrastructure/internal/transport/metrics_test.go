package transport

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"mc714/project2/internal/cart"
	"mc714/project2/internal/metrics"
	"mc714/project2/internal/replication"
	"mc714/project2/internal/testutil"
)

func TestMetricsEndpoint(t *testing.T) {
	logs := testutil.NewLogCapture()
	reg := metrics.New()
	rep := cart.NewReplica("node-1", "node-1@1", logs.Logger("node-1"))
	rep.SetMetrics(reg)
	eng := replication.NewEngine(replication.EngineConfig{
		NodeID: "node-1", Replica: rep,
		Peers:     []replication.Peer{{ID: "node-2", URL: "http://127.0.0.1:1"}},
		Transport: NewHTTPTransport(time.Second), Interval: time.Hour, FailureThreshold: 3,
		Logger: logs.Logger("node-1"), Metrics: reg,
	})
	srv := httptest.NewServer(NewServer(rep, eng, reg))
	defer srv.Close()

	do := func(method, path, body string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out json.RawMessage
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, out
	}

	if code, body := do("POST", "/cart/add", `{"product_id":"apple","quantity":2}`); code != 201 {
		t.Fatalf("add: %d %s", code, body)
	}

	code, body := do("GET", "/debug/metrics", "")
	if code != 200 {
		t.Fatalf("metrics: %d %s", code, body)
	}
	var got struct {
		Node       string            `json:"node"`
		Replica    string            `json:"replica"`
		Counters   map[string]uint64 `json:"counters"`
		PendingOps int               `json:"pending_ops"`
		AppliedOps int               `json:"applied_ops"`
		Quantities map[string]int    `json:"quantities"`
		Peers      []struct {
			ID string `json:"id"`
		} `json:"peers"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Node != "node-1" || got.Replica != "node-1@1" {
		t.Fatalf("identity: %+v", got)
	}
	if got.Counters[metrics.OpsCreated] != 1 || got.Counters[metrics.OpsApplied] != 1 {
		t.Fatalf("counters: %v", got.Counters)
	}
	if got.PendingOps != 0 || got.AppliedOps != 1 || got.Quantities["apple"] != 2 {
		t.Fatalf("gauges: pending=%d applied=%d quantities=%v", got.PendingOps, got.AppliedOps, got.Quantities)
	}
	if len(got.Peers) != 1 || got.Peers[0].ID != "node-2" {
		t.Fatalf("peers: %+v", got.Peers)
	}

	if code, body := do("POST", "/debug/metrics", ""); code != 405 {
		t.Fatalf("wrong method: %d %s", code, body)
	}
}
