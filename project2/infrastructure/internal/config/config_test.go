package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	c, err := Load(env(map[string]string{"NODE_ID": "node-1"}))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		NodeID:           "node-1",
		NodeIndex:        1,
		NodeCount:        4,
		MaxNeighbors:     3,
		ExtraEdges:       1,
		TopologySeed:     42,
		GossipInterval:   time.Second,
		HTTPAddr:         ":8080",
		HTTPTimeout:      2 * time.Second,
		FailureThreshold: 3,
		PeerURLTemplate:  "http://{id}:8080",
		LogLevel:         "info",
	}
	if c != want {
		t.Fatalf("defaults:\n got  %+v\n want %+v", c, want)
	}

	// Every value can be overridden; NODE_COUNT is configurable.
	c, err = Load(env(map[string]string{
		"NODE_ID": "node-7", "NODE_COUNT": "8", "MAX_NEIGHBORS": "4", "EXTRA_EDGES": "3",
		"TOPOLOGY_SEED": "7", "GOSSIP_INTERVAL": "50ms", "HTTP_ADDR": "127.0.0.1:0",
		"HTTP_TIMEOUT": "200ms", "FAILURE_THRESHOLD": "5",
		"PEER_URL_TEMPLATE": "http://127.0.0.1:90{index}", "LOG_LEVEL": "debug",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.NodeIndex != 7 || c.NodeCount != 8 || c.MaxNeighbors != 4 || c.ExtraEdges != 3 ||
		c.TopologySeed != 7 || c.GossipInterval != 50*time.Millisecond || c.HTTPAddr != "127.0.0.1:0" ||
		c.HTTPTimeout != 200*time.Millisecond || c.FailureThreshold != 5 || c.LogLevel != "debug" {
		t.Fatalf("overrides not applied: %+v", c)
	}
}

func TestLoadRequiresNodeID(t *testing.T) {
	_, err := Load(env(map[string]string{}))
	if err == nil || !strings.Contains(err.Error(), "NODE_ID") {
		t.Fatalf("want an error naming NODE_ID, got %v", err)
	}
}

func TestLoadRejectsNodeOutOfRange(t *testing.T) {
	for _, id := range []string{"node-0", "node-5", "foo", "node-", "node-01"} {
		_, err := Load(env(map[string]string{"NODE_ID": id, "NODE_COUNT": "4"}))
		if err == nil || !strings.Contains(err.Error(), "NODE_ID") {
			t.Errorf("NODE_ID=%q: want an error naming NODE_ID, got %v", id, err)
		}
	}
}

func TestLoadRejectsBadValues(t *testing.T) {
	cases := []struct{ key, val string }{
		{"NODE_COUNT", "abc"},
		{"NODE_COUNT", "2"},
		{"MAX_NEIGHBORS", "1"},
		{"MAX_NEIGHBORS", "4"}, // > NODE_COUNT-1 with N=4
		{"MAX_NEIGHBORS", "x"},
		{"EXTRA_EDGES", "-1"},
		{"EXTRA_EDGES", "3.5"},
		{"EXTRA_EDGES", "10"}, // unreachable for N=4, MAX=3
		{"TOPOLOGY_SEED", "-3"},
		{"TOPOLOGY_SEED", "seed"},
		{"GOSSIP_INTERVAL", "soon"},
		{"GOSSIP_INTERVAL", "0s"},
		{"GOSSIP_INTERVAL", "-1s"},
		{"HTTP_TIMEOUT", "2"},
		{"HTTP_TIMEOUT", "0ms"},
		{"FAILURE_THRESHOLD", "0"},
		{"FAILURE_THRESHOLD", "three"},
		{"LOG_LEVEL", "verbose"},
		{"PEER_URL_TEMPLATE", "http://peer:8080"},
	}
	for _, c := range cases {
		_, err := Load(env(map[string]string{"NODE_ID": "node-1", c.key: c.val}))
		if err == nil || !strings.Contains(err.Error(), c.key) {
			t.Errorf("%s=%q: want an error naming %s, got %v", c.key, c.val, c.key, err)
		}
	}
	// HTTP_ADDR must be non-empty; an unset value takes the default, so an
	// explicit whitespace-only value is the invalid case.
	_, err := Load(env(map[string]string{"NODE_ID": "node-1", "HTTP_ADDR": "  "}))
	if err == nil || !strings.Contains(err.Error(), "HTTP_ADDR") {
		t.Errorf("blank HTTP_ADDR: got %v", err)
	}
}

func TestPeerURLTemplate(t *testing.T) {
	c, err := Load(env(map[string]string{"NODE_ID": "node-1"}))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.PeerURL("node-3"); got != "http://node-3:8080" {
		t.Fatalf("default template: %q", got)
	}

	c, err = Load(env(map[string]string{"NODE_ID": "node-1", "PEER_URL_TEMPLATE": "http://{id}.local:7{index}"}))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.PeerURL("node-2"); got != "http://node-2.local:72" {
		t.Fatalf("id+index template: %q", got)
	}

	c, err = Load(env(map[string]string{"NODE_ID": "node-1", "PEER_URL_TEMPLATE": "http://127.0.0.1:80{index}"}))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.PeerURL("node-4"); got != "http://127.0.0.1:804" {
		t.Fatalf("index-only template: %q", got)
	}

	for _, bad := range []string{"http://peer:8080", "http://{node}:8080"} {
		if _, err := Load(env(map[string]string{"NODE_ID": "node-1", "PEER_URL_TEMPLATE": bad})); err == nil ||
			!strings.Contains(err.Error(), "PEER_URL_TEMPLATE") {
			t.Errorf("template %q accepted (err=%v)", bad, err)
		}
	}
}
