package benchmark

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"mesh/internal/node"
)

type RunResult struct {
	Run                   int          `json:"run"`
	Sent                  int          `json:"sent"`
	Delivered             int          `json:"delivered"`
	Dropped               int          `json:"dropped"`
	NoRoute               int          `json:"no_route"`
	NextHopUnavailable    int          `json:"next_hop_unavailable"`
	PeerCongested         int          `json:"peer_congested"`
	TimedOut              int          `json:"timed_out"`
	AverageHops           float64      `json:"average_hops"`
	Bytes                 int          `json:"bytes"`
	DeliveryRate          float64      `json:"delivery_rate"`
	MessagesPerSecond     float64      `json:"messages_per_second"`
	BytesPerSecond        float64      `json:"bytes_per_second"`
	Latency               Distribution `json:"latency"`
	TopologyConvergenceMS float64      `json:"topology_convergence_ms"`
	FailureDetectionMS    float64      `json:"failure_detection_ms"`
	RouteRecoveryMS       float64      `json:"route_recovery_ms"`
	DurationMS            float64      `json:"duration_ms"`
	samples               []time.Duration
}
type Result struct {
	Config    Config      `json:"config"`
	StartedAt time.Time   `json:"started_at"`
	Runs      []RunResult `json:"runs"`
	Aggregate RunResult   `json:"aggregate"`
}

// Run uses genuine loopback TCP listeners. Nodes are owned by this runner and
// always closed before it returns, making it safe for repeatable local runs.
func Run(ctx context.Context, config Config) (Result, error) {
	if err := config.Normalize(); err != nil {
		return Result{}, err
	}
	result := Result{Config: config, StartedAt: time.Now().UTC()}
	for i := 0; i < config.Runs; i++ {
		r, err := runOnce(ctx, config, i)
		if err != nil {
			return result, err
		}
		result.Runs = append(result.Runs, r)
	}
	result.Aggregate = aggregate(result.Runs)
	return result, nil
}
func runOnce(ctx context.Context, c Config, run int) (RunResult, error) {
	ns := make([]*node.Node, c.Nodes)
	for i := range ns {
		n, e := node.New(node.Config{ID: fmt.Sprintf("N%d", i+1), Address: "127.0.0.1:0", HeartbeatInterval: 30 * time.Millisecond, PeerTimeout: 180 * time.Millisecond, Logger: log.New(io.Discard, "", 0)})
		if e != nil {
			return RunResult{}, e
		}
		if e = n.Start(); e != nil {
			return RunResult{}, e
		}
		ns[i] = n
	}
	defer func() {
		for _, n := range ns {
			n.Close()
		}
	}()
	edges, e := Topology(c.Topology, c.Nodes, c.Seed+int64(run))
	if e != nil {
		return RunResult{}, e
	}
	// Convergence is measured from the moment connections start forming until
	// every node can route to every other node.
	convergeStart := time.Now()
	for _, edge := range edges {
		if e = ns[edge.A].Connect(ctx, ns[edge.B].Address()); e != nil {
			return RunResult{}, e
		}
	}
	convergence := waitFor(3*time.Second, func() bool {
		for _, n := range ns {
			if len(n.Routes()) < c.Nodes-1 {
				return false
			}
		}
		return true
	})
	if convergence == 0 {
		return RunResult{}, fmt.Errorf("topology did not converge")
	}
	convergeMS := float64(time.Since(convergeStart)) / float64(time.Millisecond)
	type sent struct {
		at time.Time
	}
	pending := map[string]sent{}
	arrivals := map[string]time.Time{}
	var mu sync.Mutex
	received := make(chan time.Duration, c.Messages)
	for _, n := range ns {
		n := n
		go func() {
			for m := range n.Messages() {
				mu.Lock()
				s, ok := pending[m.ID]
				if !ok {
					// On loopback a receiver can beat Send's returned ID. Keep the
					// timestamp briefly instead of treating that valid packet as lost.
					arrivals[m.ID] = time.Now()
				}
				mu.Unlock()
				if ok {
					received <- time.Since(s.at)
				}
			}
		}()
	}
	r := rand.New(rand.NewSource(c.Seed + int64(run)))
	payload := strings.Repeat("x", c.PayloadSize)
	// trackSend sends one message and registers it for delivery accounting,
	// resolving the loopback race where a receiver observes a message before
	// Send returns its generated ID.
	trackSend := func(src *node.Node, dstID string) (int, error) {
		route := src.Route(dstID)
		if len(route) == 0 {
			return 0, node.ErrNoRoute
		}
		id, err := src.Send(dstID, payload)
		if err != nil {
			return 0, err
		}
		s := sent{time.Now()}
		mu.Lock()
		pending[id] = s
		arrived, early := arrivals[id]
		if early {
			delete(arrivals, id)
		}
		mu.Unlock()
		if early {
			received <- arrived.Sub(s.at)
		}
		return len(route) - 1, nil
	}
	trafficStart := time.Now()
	sentCount, noRoute, unavailable, congested, totalHops := 0, 0, 0, 0, 0
	for i := 0; i < c.Messages; i++ {
		src := r.Intn(c.Nodes)
		dst := r.Intn(c.Nodes - 1)
		if dst >= src {
			dst++
		}
		hops, err := trackSend(ns[src], ns[dst].ID())
		switch {
		case err == nil:
			sentCount++
			totalHops += hops
		case errors.Is(err, node.ErrNoRoute):
			noRoute++
		case errors.Is(err, node.ErrNextHopUnavailable):
			unavailable++
		case errors.Is(err, node.ErrPeerCongested):
			congested++
		}
	}
	deadline := time.NewTimer(c.Duration)
	var ds []time.Duration
	for len(ds) < sentCount {
		select {
		case d := <-received:
			ds = append(ds, d)
		case <-deadline.C:
			goto done
		case <-ctx.Done():
			return RunResult{}, ctx.Err()
		}
	}
done:
	if !deadline.Stop() {
		select {
		case <-deadline.C:
		default:
		}
	}
	trafficElapsed := time.Since(trafficStart)
	// Optional failure experiment: stop one interior node and measure how long
	// peers take to detect it and for the surviving network to re-converge.
	var detect, recoverMS float64
	if c.FailureRate > 0 && c.Nodes >= 4 {
		victim := pickVictim(edges, c.Nodes)
		vID := fmt.Sprintf("N%d", victim+1)
		ns[victim].Close()
		if d := waitFor(2*time.Second, func() bool {
			for i, n := range ns {
				if i == victim {
					continue
				}
				for _, p := range n.Peers() {
					if p.ID == vID {
						return false
					}
				}
			}
			return true
		}); d > 0 {
			detect = float64(d) / float64(time.Millisecond)
		}
		// Recovery is reached when every survivor has dropped the victim and can
		// route to every other survivor. A partitioned topology never satisfies
		// this, so recovery honestly reports as 0.
		recoverMS = float64(waitFor(2*time.Second, func() bool {
			for i, n := range ns {
				if i == victim {
					continue
				}
				if len(n.Routes()) < c.Nodes-2 {
					return false
				}
			}
			return true
		})) / float64(time.Millisecond)
	}
	elapsed := time.Since(convergeStart)
	out := RunResult{Run: run + 1, Sent: c.Messages, Delivered: len(ds), NoRoute: noRoute, NextHopUnavailable: unavailable, PeerCongested: congested, Bytes: len(ds) * c.PayloadSize, Latency: DistributionOf(ds), TopologyConvergenceMS: convergeMS, FailureDetectionMS: detect, RouteRecoveryMS: recoverMS, DurationMS: float64(elapsed) / float64(time.Millisecond)}
	out.samples = ds
	out.TimedOut = sentCount - len(ds)
	out.Dropped = out.Sent - out.Delivered
	if sentCount > 0 {
		out.AverageHops = float64(totalHops) / float64(sentCount)
	}
	out.DeliveryRate = 100 * float64(out.Delivered) / float64(max(1, out.Sent))
	if trafficElapsed > 0 {
		out.MessagesPerSecond = float64(out.Delivered) / trafficElapsed.Seconds()
		out.BytesPerSecond = float64(out.Bytes) / trafficElapsed.Seconds()
	}
	return out, nil
}

// pickVictim returns a high-degree interior node so that failing it actually
// interrupts traffic and requires rerouting.
func pickVictim(edges []Edge, nodes int) int {
	degree := make([]int, nodes)
	for _, e := range edges {
		if e.A != e.B {
			degree[e.A]++
			degree[e.B]++
		}
	}
	best := 0
	for i, d := range degree {
		if d > degree[best] {
			best = i
		}
	}
	return best
}
func waitFor(limit time.Duration, ok func() bool) time.Duration {
	start := time.Now()
	end := start.Add(limit)
	for time.Now().Before(end) {
		if ok() {
			return time.Since(start)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return 0
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func aggregate(rs []RunResult) RunResult {
	if len(rs) == 0 {
		return RunResult{}
	}
	var o RunResult
	var ds []time.Duration
	for _, r := range rs {
		o.Sent += r.Sent
		o.Delivered += r.Delivered
		o.Dropped += r.Dropped
		o.NoRoute += r.NoRoute
		o.NextHopUnavailable += r.NextHopUnavailable
		o.PeerCongested += r.PeerCongested
		o.TimedOut += r.TimedOut
		o.AverageHops += r.AverageHops
		o.Bytes += r.Bytes
		o.MessagesPerSecond += r.MessagesPerSecond
		o.BytesPerSecond += r.BytesPerSecond
		o.TopologyConvergenceMS += r.TopologyConvergenceMS
		o.FailureDetectionMS += r.FailureDetectionMS
		o.RouteRecoveryMS += r.RouteRecoveryMS
		ds = append(ds, r.samples...)
	}
	n := float64(len(rs))
	o.DeliveryRate = 100 * float64(o.Delivered) / float64(max(1, o.Sent))
	o.MessagesPerSecond /= n
	o.BytesPerSecond /= n
	o.TopologyConvergenceMS /= n
	o.FailureDetectionMS /= n
	o.RouteRecoveryMS /= n
	o.AverageHops /= n
	o.Latency = DistributionOf(ds)
	return o
}
