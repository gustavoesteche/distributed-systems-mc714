package topology

import (
	"fmt"
	"reflect"
	"testing"
)

func isChainEdge(a, b string) bool {
	i, err1 := ParseNodeName(a)
	j, err2 := ParseNodeName(b)
	if err1 != nil || err2 != nil {
		return false
	}
	return i-j == 1 || j-i == 1
}

// checkGraph asserts the topology invariants.
func checkGraph(t *testing.T, g *Graph, n, max, extra int, label string) {
	t.Helper()
	if g.Size() != n {
		t.Fatalf("%s: size %d, want %d", label, g.Size(), n)
	}
	for k := 1; k <= n; k++ {
		if g.NodeName(k) != fmt.Sprintf("node-%d", k) {
			t.Fatalf("%s: NodeName(%d) = %q", label, k, g.NodeName(k))
		}
	}
	// Chain present.
	for i := 1; i < n; i++ {
		a, b := NodeName(i), NodeName(i+1)
		if !contains(g.Neighbors(a), b) || !contains(g.Neighbors(b), a) {
			t.Fatalf("%s: chain edge %s-%s missing", label, a, b)
		}
	}
	// Symmetry, self-loops, duplicates, degree.
	for k := 1; k <= n; k++ {
		id := NodeName(k)
		nb := g.Neighbors(id)
		if len(nb) > max {
			t.Fatalf("%s: %s has degree %d > %d", label, id, len(nb), max)
		}
		seen := map[string]bool{}
		for i, p := range nb {
			if p == id {
				t.Fatalf("%s: self-loop on %s", label, id)
			}
			if seen[p] {
				t.Fatalf("%s: duplicate neighbor %s on %s", label, p, id)
			}
			seen[p] = true
			if i > 0 && nb[i-1] >= p {
				t.Fatalf("%s: neighbors of %s not sorted: %v", label, id, nb)
			}
			if !contains(g.Neighbors(p), id) {
				t.Fatalf("%s: asymmetric %s -> %s", label, id, p)
			}
		}
	}
	// Edge list: sorted, a<b, unique, consistent with Neighbors, exactly `extra` non-chain.
	edges := g.Edges()
	nonChain := 0
	seen := map[[2]string]bool{}
	degree := map[string]int{}
	for i, e := range edges {
		if e[0] >= e[1] {
			t.Fatalf("%s: edge %v not ordered", label, e)
		}
		if seen[e] {
			t.Fatalf("%s: duplicate edge %v", label, e)
		}
		seen[e] = true
		if i > 0 && !(edges[i-1][0] < e[0] || (edges[i-1][0] == e[0] && edges[i-1][1] < e[1])) {
			t.Fatalf("%s: edges not sorted at %d: %v", label, i, edges)
		}
		if !isChainEdge(e[0], e[1]) {
			nonChain++
		}
		degree[e[0]]++
		degree[e[1]]++
	}
	if nonChain != extra {
		t.Fatalf("%s: %d non-chain edges, want %d", label, nonChain, extra)
	}
	for k := 1; k <= n; k++ {
		if degree[NodeName(k)] != len(g.Neighbors(NodeName(k))) {
			t.Fatalf("%s: Edges and Neighbors disagree on %s", label, NodeName(k))
		}
	}
	// Connectivity by BFS.
	visited := map[string]bool{NodeName(1): true}
	queue := []string{NodeName(1)}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, p := range g.Neighbors(cur) {
			if !visited[p] {
				visited[p] = true
				queue = append(queue, p)
			}
		}
	}
	if len(visited) != n {
		t.Fatalf("%s: graph not connected (%d of %d reached)", label, len(visited), n)
	}
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func TestGenerateProperties(t *testing.T) {
	seeds := []uint64{1, 2, 3, 42, 1337}
	for n := 3; n <= 30; n++ {
		maxes := map[int]bool{2: true, 3: true, 4: true, n / 2: true, n - 1: true}
		for max := range maxes {
			if max < 2 || max > n-1 {
				continue
			}
			for _, seed := range seeds {
				// Walk extra = 0,1,2,4,8,... until the first unreachable value.
				// Reachability is monotonic: a successful run for k edges
				// contains the run for k-1 as a prefix.
				failed := false
				for extra := 0; ; extra = nextExtra(extra) {
					label := fmt.Sprintf("n=%d max=%d extra=%d seed=%d", n, max, extra, seed)
					g, err := Generate(n, max, extra, seed)
					if err != nil {
						if extra == 0 {
							t.Fatalf("%s: unexpected error %v", label, err)
						}
						failed = true
						break
					}
					checkGraph(t, g, n, max, extra, label)
					if extra > n*n {
						break
					}
				}
				if !failed {
					t.Fatalf("n=%d max=%d seed=%d: no extra value was ever unreachable", n, max, seed)
				}
			}
		}
		// The complete graph is reachable when max = n-1.
		full := n*(n-1)/2 - (n - 1)
		g, err := Generate(n, n-1, full, 42)
		if err != nil {
			t.Fatalf("n=%d complete graph: %v", n, err)
		}
		checkGraph(t, g, n, n-1, full, fmt.Sprintf("n=%d complete", n))
		if _, err := Generate(n, n-1, full+1, 42); err == nil {
			t.Fatalf("n=%d: extra beyond the complete graph accepted", n)
		}
	}
}

func nextExtra(e int) int {
	if e == 0 {
		return 1
	}
	return e * 2
}

func TestGenerateDeterministic(t *testing.T) {
	for _, p := range [][4]int{{4, 3, 1, 42}, {10, 4, 5, 7}, {30, 5, 20, 1337}} {
		a, err := Generate(p[0], p[1], p[2], uint64(p[3]))
		if err != nil {
			t.Fatal(err)
		}
		b, err := Generate(p[0], p[1], p[2], uint64(p[3]))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(a, b) || !reflect.DeepEqual(a.Edges(), b.Edges()) {
			t.Fatalf("%v: graphs differ", p)
		}
	}
}

func TestGenerateErrors(t *testing.T) {
	cases := []struct {
		name          string
		n, max, extra int
	}{
		{"n<3", 2, 2, 0},
		{"max<2", 5, 1, 0},
		{"max>n-1", 4, 4, 0},
		{"extra<0", 4, 3, -1},
		{"unreachable extra", 4, 2, 2},
	}
	for _, c := range cases {
		if _, err := Generate(c.n, c.max, c.extra, 42); err == nil {
			t.Errorf("%s: expected an error", c.name)
		}
	}
	// The single edge that fits for n=4, max=2 is node-1 — node-4.
	g, err := Generate(4, 2, 1, 42)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(g.Neighbors("node-1"), "node-4") {
		t.Fatalf("expected node-1 — node-4, got %v", g.Edges())
	}
}

func TestNodeNames(t *testing.T) {
	if NodeName(3) != "node-3" {
		t.Fatalf("NodeName(3) = %q", NodeName(3))
	}
	for _, ok := range []string{"node-1", "node-12"} {
		if _, err := ParseNodeName(ok); err != nil {
			t.Errorf("ParseNodeName(%q): %v", ok, err)
		}
	}
	for _, bad := range []string{"", "node-", "node-0", "node-01", "node--1", "foo", "node-x", "node-1@2"} {
		if _, err := ParseNodeName(bad); err == nil {
			t.Errorf("ParseNodeName(%q) accepted", bad)
		}
	}
}
