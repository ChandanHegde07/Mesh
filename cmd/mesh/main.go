package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"mesh/benchmark"
	"mesh/internal/node"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		return
	}
	switch os.Args[1] {
	case "node":
		runNode(os.Args[2:])
	case "benchmark":
		runBenchmark(os.Args[2:])
	default:
		usage()
	}
}

func runBenchmark(args []string) {
	fs := flag.NewFlagSet("benchmark", flag.ExitOnError)
	c := benchmark.Config{}
	fs.IntVar(&c.Nodes, "nodes", 5, "number of nodes")
	fs.IntVar(&c.Messages, "messages", 100, "messages per run")
	fs.IntVar(&c.PayloadSize, "payload-size", 64, "payload bytes")
	fs.StringVar(&c.Topology, "topology", "random", "linear, ring, or random")
	fs.Int64Var(&c.Seed, "seed", 42, "random seed")
	fs.DurationVar(&c.Duration, "duration", 5e9, "maximum delivery wait per run")
	fs.Float64Var(&c.FailureRate, "failure-rate", 0, "enable a failure experiment when above zero")
	fs.IntVar(&c.Runs, "runs", 5, "repetitions")
	fs.StringVar(&c.OutputDir, "output", "results", "result directory")
	fs.Parse(args)
	r, err := benchmark.Run(context.Background(), c)
	if err != nil {
		log.Fatal(err)
	}
	j, csv, err := benchmark.Write(r)
	if err != nil {
		log.Fatal(err)
	}
	a := r.Aggregate
	fmt.Printf("MESH BENCHMARK\n==============\nNodes: %d  Messages: %d  Payload: %d bytes  Topology: %s  Seed: %d  Runs: %d\n\nLatency min/mean/median/stddev/p95/p99/max: %.3f / %.3f / %.3f / %.3f / %.3f / %.3f / %.3f ms\nReliability: %d/%d delivered (%.2f%%), %d failed\nFailure reasons: no-route=%d unavailable=%d congested=%d timeout=%d\nThroughput: %.2f msg/s, %.2f bytes/s\nRouting: %.2f average hops\nTopology convergence: %.3f ms\nFailure detection: %.3f ms\nRoute recovery: %.3f ms\nResults: %s, %s\n", c.Nodes, c.Messages, c.PayloadSize, c.Topology, c.Seed, c.Runs, a.Latency.Min, a.Latency.Mean, a.Latency.Median, a.Latency.StdDev, a.Latency.P95, a.Latency.P99, a.Latency.Max, a.Delivered, a.Sent, a.DeliveryRate, a.Dropped, a.NoRoute, a.NextHopUnavailable, a.PeerCongested, a.TimedOut, a.MessagesPerSecond, a.BytesPerSecond, a.AverageHops, a.TopologyConvergenceMS, a.FailureDetectionMS, a.RouteRecoveryMS, j, csv)
}
func runNode(args []string) {
	fs := flag.NewFlagSet("node", flag.ExitOnError)
	id := fs.String("id", "", "unique node ID")
	port := fs.Int("port", 0, "TCP port")
	peers := fs.String("peers", "", "comma-separated seed addresses")
	fs.Parse(args)
	n, err := node.New(node.Config{ID: *id, Address: fmt.Sprintf("127.0.0.1:%d", *port), Logger: log.New(os.Stderr, "mesh ", log.LstdFlags)})
	if err != nil {
		log.Fatal(err)
	}
	if err = n.Start(); err != nil {
		log.Fatal(err)
	}
	defer n.Close()
	for _, a := range strings.Split(*peers, ",") {
		if a != "" {
			if err := n.Connect(context.Background(), a); err != nil {
				log.Printf("connect %s: %v", a, err)
			}
		}
	}
	fmt.Printf("Mesh node %s listening on %s. Commands: peers, status, routes, send --to ID MESSAGE, quit\n", n.ID(), n.Address())
	go func() {
		for m := range n.Messages() {
			fmt.Printf("received from %s: %s\n", m.Source, m.Payload)
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	lines := make(chan string)
	go func() {
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			lines <- s.Text()
		}
		close(lines)
	}()
	for {
		select {
		case <-sig:
			return
		case line, ok := <-lines:
			if !ok {
				return
			}
			handle(n, line)
		}
	}
}
func handle(n *node.Node, line string) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return
	}
	switch parts[0] {
	case "quit", "exit":
		os.Exit(0)
	case "peers":
		for _, p := range n.Peers() {
			fmt.Printf("%s %s\n", p.ID, p.Address)
		}
	case "routes":
		for id, r := range n.Routes() {
			fmt.Printf("%s: %s\n", id, strings.Join(r, " -> "))
		}
	case "status":
		fmt.Printf("id=%s address=%s peers=%d\n", n.ID(), n.Address(), len(n.Peers()))
	case "send":
		if len(parts) < 4 || parts[1] != "--to" {
			fmt.Println("usage: send --to ID MESSAGE")
			return
		}
		if _, e := n.Send(parts[2], strings.Join(parts[3:], " ")); e != nil {
			fmt.Println("send:", e)
		}
	default:
		fmt.Println("commands: peers, status, routes, send --to ID MESSAGE, quit")
	}
}
func usage() {
	fmt.Println("usage: mesh node --id A --port 8001 [--peers host:port,...] | mesh benchmark [options]")
}
