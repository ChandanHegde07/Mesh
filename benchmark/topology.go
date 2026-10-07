package benchmark

import (
	"fmt"
	"math/rand"
)

type Edge struct{ A, B int }

// Topology always returns a connected graph. Random starts with a spanning tree
// and then adds deterministic pseudo-random edges.
func Topology(kind string, nodes int, seed int64) ([]Edge, error) {
	if nodes < 2 {
		return nil, fmt.Errorf("at least two nodes required")
	}
	edges := make([]Edge, 0)
	switch kind {
	case "linear":
		for i := 0; i < nodes-1; i++ {
			edges = append(edges, Edge{i, i + 1})
		}
	case "ring":
		for i := 0; i < nodes; i++ {
			edges = append(edges, Edge{i, (i + 1) % nodes})
		}
	case "random":
		r := rand.New(rand.NewSource(seed))
		present := map[[2]int]bool{}
		add := func(a, b int) {
			if a == b {
				return
			}
			if a > b {
				a, b = b, a
			}
			k := [2]int{a, b}
			if !present[k] {
				present[k] = true
				edges = append(edges, Edge{a, b})
			}
		}
		for i := 1; i < nodes; i++ {
			add(i, r.Intn(i))
		}
		// A sparse graph exposes routing behaviour without overwhelming the local TCP test.
		for i := 0; i < nodes/2; i++ {
			add(r.Intn(nodes), r.Intn(nodes))
		}
	default:
		return nil, fmt.Errorf("unknown topology %q", kind)
	}
	return edges, nil
}
