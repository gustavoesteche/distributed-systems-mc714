//go:build e2e

// Package e2e runs the real Docker Compose cluster. It reports pass/fail only.
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const project = "mc714-e2e"

type harness struct {
	t        *testing.T
	infraDir string
	file     string
	client   *http.Client
}

func (h *harness) compose(args ...string) (string, error) {
	full := append([]string{"compose", "-p", project, "-f", h.file, "--project-directory", h.infraDir}, args...)
	cmd := exec.Command("docker", full...)
	cmd.Env = append(os.Environ(), "GOSSIP_INTERVAL=1s")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return out.String(), err
}

func (h *harness) mustCompose(args ...string) string {
	h.t.Helper()
	out, err := h.compose(args...)
	if err != nil {
		h.t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}
	return out
}

func (h *harness) url(k int) string { return fmt.Sprintf("http://127.0.0.1:%d", 8000+k) }

func (h *harness) health(k int) (string, bool) {
	resp, err := h.client.Get(h.url(k) + "/health")
	if err != nil {
		return "", false
	}
	defer resp.Body.Close()
	var out struct {
		Replica string `json:"replica"`
	}
	if resp.StatusCode != 200 || json.NewDecoder(resp.Body).Decode(&out) != nil {
		return "", false
	}
	return out.Replica, true
}

func (h *harness) cart(k int) (map[string]int, bool) {
	resp, err := h.client.Get(h.url(k) + "/cart")
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

func (h *harness) op(k int, path, product string, q int) {
	h.t.Helper()
	body, _ := json.Marshal(map[string]any{"product_id": product, "quantity": q})
	resp, err := h.client.Post(h.url(k)+path, "application/json", bytes.NewReader(body))
	if err != nil {
		h.t.Fatalf("POST %s node-%d: %v", path, k, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		h.t.Fatalf("POST %s node-%d: status %d", path, k, resp.StatusCode)
	}
}

func (h *harness) eventually(timeout time.Duration, what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			logs, _ := h.compose("logs", "--no-color", "--tail", "60")
			h.t.Fatalf("timed out waiting for: %s\n%s", what, logs)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func TestE2EClusterLifecycle(t *testing.T) {
	infraDir, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, infraDir: infraDir, file: filepath.Join(infraDir, "compose.yaml"),
		client: &http.Client{Timeout: 3 * time.Second}}

	// 1. Start the committed compose.yaml, a 4-node chain, with 1s gossip.
	t.Cleanup(func() {
		if out, err := h.compose("down", "--remove-orphans"); err != nil {
			t.Logf("compose down: %v\n%s", err, out)
		}
	})
	h.mustCompose("up", "-d", "--build")

	// 2. Every node answers /health.
	ids := map[int]string{}
	for k := 1; k <= 4; k++ {
		k := k
		h.eventually(60*time.Second, fmt.Sprintf("node-%d health", k), func() bool {
			id, ok := h.health(k)
			ids[k] = id
			return ok
		})
	}

	// 3. An add on node-1 reaches node-4 over the chain.
	h.op(1, "/cart/add", "apple", 3)
	h.eventually(30*time.Second, "node-4 sees apple=3", func() bool {
		items, ok := h.cart(4)
		return ok && items["apple"] == 3
	})

	// 4. A remove on node-3 reaches node-1.
	h.op(3, "/cart/remove", "apple", 1)
	h.eventually(30*time.Second, "node-1 sees apple=2", func() bool {
		items, ok := h.cart(1)
		return ok && items["apple"] == 2
	})

	// 5. Stopping node-2 partitions node-1; node-1 notices.
	h.mustCompose("stop", "node-2")
	h.eventually(60*time.Second, "node-1 logs gossip_peer_unreachable", func() bool {
		out, _ := h.compose("logs", "--no-color", "node-1")
		return strings.Contains(out, "event=gossip_peer_unreachable peer=node-2")
	})

	// 6. Ops on either side of the partition do not cross.
	h.op(1, "/cart/add", "left", 1)
	h.op(3, "/cart/add", "right", 1)
	time.Sleep(4 * time.Second)
	if items, _ := h.cart(1); items["right"] != 0 {
		t.Fatalf("node-1 saw node-3's op across the partition: %v", items)
	}
	for _, k := range []int{3, 4} {
		if items, _ := h.cart(k); items["left"] != 0 {
			t.Fatalf("node-%d saw node-1's op across the partition: %v", k, items)
		}
	}

	// 7. node-2 comes back as a new incarnation and everything converges.
	h.mustCompose("start", "node-2")
	want := map[string]int{"apple": 2, "left": 1, "right": 1}
	h.eventually(60*time.Second, "all nodes converge", func() bool {
		for k := 1; k <= 4; k++ {
			items, ok := h.cart(k)
			if !ok || !reflect.DeepEqual(items, want) {
				return false
			}
		}
		return true
	})
	newID, ok := h.health(2)
	if !ok || newID == ids[2] || !strings.HasPrefix(newID, "node-2@") {
		t.Fatalf("node-2 replica ID %q, old %q: want a new incarnation", newID, ids[2])
	}
	h.eventually(30*time.Second, "node-1 logs gossip_peer_reconnected", func() bool {
		out, _ := h.compose("logs", "--no-color", "node-1")
		return strings.Contains(out, "event=gossip_peer_reconnected peer=node-2")
	})

	// 8. The runtime image is Alpine.
	out := h.mustCompose("exec", "-T", "node-1", "cat", "/etc/alpine-release")
	if strings.TrimSpace(out) == "" {
		t.Fatalf("empty /etc/alpine-release")
	}
}
