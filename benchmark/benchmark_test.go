package benchmark

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestTopologyReproducibleAndConnected(t *testing.T) {
	a, err := Topology("random", 12, 42)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Topology("random", 12, 42)
	if len(a) != len(b) {
		t.Fatal("non-reproducible length")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatal("topology is not reproducible")
		}
	}
	seen := map[int]bool{0: true}
	for changed := true; changed; {
		changed = false
		for _, e := range a {
			if seen[e.A] && !seen[e.B] {
				seen[e.B] = true
				changed = true
			}
			if seen[e.B] && !seen[e.A] {
				seen[e.A] = true
				changed = true
			}
		}
	}
	if len(seen) != 12 {
		t.Fatal("random topology is disconnected")
	}
}
func TestTopologyHasNoSelfLoops(t *testing.T) {
	for seed := int64(0); seed < 100; seed++ {
		edges, err := Topology("random", 8, seed)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range edges {
			if e.A == e.B {
				t.Fatalf("self-loop %v for seed %d", e, seed)
			}
		}
	}
}
func TestStatistics(t *testing.T) {
	d := DistributionOf([]time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond, 5 * time.Millisecond})
	if d.Count != 5 || d.Median != 3 || d.P95 != 5 || d.P99 != 5 {
		t.Fatalf("unexpected distribution %+v", d)
	}
}
func TestConfigValidation(t *testing.T) {
	c := Config{Nodes: 1}
	if c.Normalize() == nil {
		t.Fatal("accepted invalid nodes")
	}
	c = Config{Nodes: 2, Messages: 1, PayloadSize: 1, Runs: 1, Topology: "bad"}
	if c.Normalize() == nil {
		t.Fatal("accepted topology")
	}
}
func TestResultSerialization(t *testing.T) {
	dir := t.TempDir()
	j, c, err := Write(Result{Config: Config{OutputDir: dir}, StartedAt: time.Now(), Runs: []RunResult{{Run: 1, Sent: 1, Delivered: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(j); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(c); err != nil {
		t.Fatal(err)
	}
}
func TestSmallRunnerCleansUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := Run(ctx, Config{Nodes: 3, Messages: 5, PayloadSize: 8, Topology: "linear", Seed: 1, Runs: 1, Duration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if r.Runs[0].Sent != 5 || r.Runs[0].Delivered != 5 {
		t.Fatalf("unexpected result %+v", r.Runs[0])
	}
}
