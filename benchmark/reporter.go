package benchmark

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

func Write(result Result) (string, string, error) {
	if err := os.MkdirAll(result.Config.OutputDir, 0755); err != nil {
		return "", "", err
	}
	tag := result.StartedAt.Format("20060102T150405Z")
	j := filepath.Join(result.Config.OutputDir, "mesh-benchmark-"+tag+".json")
	b, e := json.MarshalIndent(result, "", "  ")
	if e != nil {
		return "", "", e
	}
	if e = os.WriteFile(j, b, 0644); e != nil {
		return "", "", e
	}
	c := filepath.Join(result.Config.OutputDir, "mesh-benchmark-"+tag+".csv")
	f, e := os.Create(c)
	if e != nil {
		return "", "", e
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"run", "sent", "delivered", "dropped", "no_route", "next_hop_unavailable", "peer_congested", "timed_out", "delivery_rate", "mean_latency_ms", "p95_latency_ms", "p99_latency_ms", "messages_per_second", "average_hops", "topology_convergence_ms", "failure_detection_ms", "route_recovery_ms"})
	for _, r := range result.Runs {
		_ = w.Write([]string{strconv.Itoa(r.Run), strconv.Itoa(r.Sent), strconv.Itoa(r.Delivered), strconv.Itoa(r.Dropped), strconv.Itoa(r.NoRoute), strconv.Itoa(r.NextHopUnavailable), strconv.Itoa(r.PeerCongested), strconv.Itoa(r.TimedOut), fmt.Sprint(r.DeliveryRate), fmt.Sprint(r.Latency.Mean), fmt.Sprint(r.Latency.P95), fmt.Sprint(r.Latency.P99), fmt.Sprint(r.MessagesPerSecond), fmt.Sprint(r.AverageHops), fmt.Sprint(r.TopologyConvergenceMS), fmt.Sprint(r.FailureDetectionMS), fmt.Sprint(r.RouteRecoveryMS)})
	}
	return j, c, w.Error()
}
