package replication

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"mc714/project2/internal/cart"
)

func fanoutEngine(fanout int, seed uint64, n int) *Engine {
	var peers []Peer
	for k := 1; k <= n; k++ {
		peers = append(peers, Peer{ID: fmt.Sprintf("node-%d", k+1), URL: fmt.Sprintf("fake://node-%d", k+1)})
	}
	return NewEngine(EngineConfig{
		NodeID: "node-1", Replica: cart.NewReplica("node-1", "node-1@1", nil), Peers: peers,
		Interval: time.Hour, FailureThreshold: 3, Fanout: fanout, Seed: seed,
	})
}

func peerIDs(ps []Peer) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = p.ID
	}
	return out
}

// Fanout 0 or ≥ degree contacts every neighbor, as before.
func TestSelectPeersAllWhenFanoutCoversDegree(t *testing.T) {
	for _, f := range []int{0, 3, 5} {
		e := fanoutEngine(f, 1, 3)
		if got := peerIDs(e.selectPeers()); !reflect.DeepEqual(got, []string{"node-2", "node-3", "node-4"}) {
			t.Fatalf("fanout %d: got %v", f, got)
		}
	}
}

// Fanout k picks k distinct neighbors, every neighbor roughly equally often,
// and the same seed reproduces the same sequence.
func TestSelectPeersRandomUniformReproducible(t *testing.T) {
	const rounds = 6000
	a, b := fanoutEngine(2, 7, 4), fanoutEngine(2, 7, 4)
	counts := map[string]int{}
	for i := 0; i < rounds; i++ {
		got := peerIDs(a.selectPeers())
		if len(got) != 2 || got[0] == got[1] {
			t.Fatalf("round %d: want 2 distinct peers, got %v", i, got)
		}
		if other := peerIDs(b.selectPeers()); !reflect.DeepEqual(got, other) {
			t.Fatalf("round %d: same seed diverged: %v vs %v", i, got, other)
		}
		for _, id := range got {
			counts[id]++
		}
	}
	want := rounds * 2 / 4
	for id, c := range counts {
		if c < want*9/10 || c > want*11/10 {
			t.Fatalf("%s chosen %d times, want about %d (%v)", id, c, want, counts)
		}
	}
	if len(counts) != 4 {
		t.Fatalf("not every neighbor was chosen: %v", counts)
	}
	c := fanoutEngine(2, 8, 4)
	same := true
	for i := 0; i < 50 && same; i++ {
		same = reflect.DeepEqual(peerIDs(fanoutEngine(2, 7, 4).selectPeers()), peerIDs(c.selectPeers()))
	}
	if same {
		t.Fatalf("different seeds gave the same choices")
	}
}

// With one random neighbor per round the cluster still converges (L2).
func TestFanoutOneConverges(t *testing.T) {
	c := newFakeCluster(t, 8, 3, 1, 42, 3)
	for k, n := range c.nodes {
		n.engine.fanout = 1
		n.engine.rng = fanoutEngine(1, uint64(k+1), 1).rng
		if _, err := n.replica.Add(fmt.Sprintf("p%d", k), k+1); err != nil {
			t.Fatal(err)
		}
	}
	for r := 1; r <= 200; r++ {
		for _, n := range c.nodes {
			n.engine.Round(context.Background())
		}
		if identical(c.nodes) == nil {
			t.Logf("converged after %d rounds", r)
			return
		}
	}
	t.Fatalf("no convergence after 200 rounds: %v", identical(c.nodes))
}
