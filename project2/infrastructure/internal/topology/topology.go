// Package topology builds the static, seeded neighbor graph shared by all nodes.
package topology

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
)

// Graph is an undirected neighbor graph over node-1 … node-N.
type Graph struct {
	n     int
	adj   map[string][]string // sorted neighbor lists
	edges [][2]string         // sorted, each with a < b
}

// NodeName returns "node-k".
func NodeName(k int) string { return "node-" + strconv.Itoa(k) }

// ParseNodeName returns k for a canonical "node-k" with k ≥ 1.
func ParseNodeName(s string) (int, error) {
	rest, ok := strings.CutPrefix(s, "node-")
	if !ok || rest == "" {
		return 0, fmt.Errorf("%q is not of the form node-k", s)
	}
	k, err := strconv.Atoi(rest)
	if err != nil || k < 1 || strconv.Itoa(k) != rest {
		return 0, fmt.Errorf("%q is not of the form node-k with k ≥ 1", s)
	}
	return k, nil
}

// Generate builds the chain node-1 — … — node-n plus exactly extraEdges
// seeded random edges, with no node above maxNeighbors.
func Generate(n, maxNeighbors, extraEdges int, seed uint64) (*Graph, error) {
	if n < 3 {
		return nil, fmt.Errorf("node count %d: must be at least 3", n)
	}
	if maxNeighbors < 2 || maxNeighbors > n-1 {
		return nil, fmt.Errorf("max neighbors %d: must be between 2 and %d", maxNeighbors, n-1)
	}
	if extraEdges < 0 {
		return nil, fmt.Errorf("extra edges %d: must be at least 0", extraEdges)
	}

	degree := make([]int, n+1)
	var pairs [][2]int
	for i := 1; i < n; i++ {
		pairs = append(pairs, [2]int{i, i + 1})
		degree[i]++
		degree[i+1]++
	}

	var candidates [][2]int
	for i := 1; i <= n; i++ {
		for j := i + 2; j <= n; j++ {
			candidates = append(candidates, [2]int{i, j})
		}
	}
	rng := rand.New(rand.NewPCG(seed, 0))
	rng.Shuffle(len(candidates), func(a, b int) { candidates[a], candidates[b] = candidates[b], candidates[a] })

	added := 0
	for _, c := range candidates {
		if added == extraEdges {
			break
		}
		if degree[c[0]] < maxNeighbors && degree[c[1]] < maxNeighbors {
			pairs = append(pairs, c)
			degree[c[0]]++
			degree[c[1]]++
			added++
		}
	}
	if added < extraEdges {
		return nil, fmt.Errorf("extra edges %d: only %d fit with n=%d and max neighbors %d", extraEdges, added, n, maxNeighbors)
	}

	g := &Graph{n: n, adj: make(map[string][]string, n)}
	for k := 1; k <= n; k++ {
		g.adj[NodeName(k)] = []string{}
	}
	for _, p := range pairs {
		a, b := NodeName(p[0]), NodeName(p[1])
		g.adj[a] = append(g.adj[a], b)
		g.adj[b] = append(g.adj[b], a)
		if a > b {
			a, b = b, a
		}
		g.edges = append(g.edges, [2]string{a, b})
	}
	for _, nb := range g.adj {
		sort.Strings(nb)
	}
	sort.Slice(g.edges, func(i, j int) bool {
		if g.edges[i][0] != g.edges[j][0] {
			return g.edges[i][0] < g.edges[j][0]
		}
		return g.edges[i][1] < g.edges[j][1]
	})
	if !g.connected() {
		return nil, errors.New("topology is not connected")
	}
	return g, nil
}

func (g *Graph) connected() bool {
	start := NodeName(1)
	seen := map[string]bool{start: true}
	queue := []string{start}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, p := range g.adj[cur] {
			if !seen[p] {
				seen[p] = true
				queue = append(queue, p)
			}
		}
	}
	return len(seen) == g.n
}

// Neighbors returns id's neighbors, sorted. The slice is a copy.
func (g *Graph) Neighbors(id string) []string {
	return append([]string(nil), g.adj[id]...)
}

// Edges returns every edge, sorted, each with a < b. The slice is a copy.
func (g *Graph) Edges() [][2]string {
	return append([][2]string(nil), g.edges...)
}

// NodeName returns "node-k".
func (g *Graph) NodeName(k int) string { return NodeName(k) }

// Size returns the node count.
func (g *Graph) Size() int { return g.n }
