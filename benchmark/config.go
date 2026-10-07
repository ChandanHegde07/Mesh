// Package benchmark runs reproducible, local Mesh overlay experiments.
package benchmark

import (
	"fmt"
	"time"
)

type Config struct {
	Nodes, Messages, PayloadSize, Runs int
	Topology                           string
	Seed                               int64
	Duration                           time.Duration
	FailureRate                        float64
	OutputDir                          string
}

func (c *Config) Normalize() error {
	if c.Nodes == 0 {
		c.Nodes = 5
	}
	if c.Messages == 0 {
		c.Messages = 100
	}
	if c.PayloadSize == 0 {
		c.PayloadSize = 64
	}
	if c.Runs == 0 {
		c.Runs = 5
	}
	if c.Topology == "" {
		c.Topology = "random"
	}
	if c.Duration == 0 {
		c.Duration = 5 * time.Second
	}
	if c.OutputDir == "" {
		c.OutputDir = "results"
	}
	if c.Nodes < 2 || c.Messages < 1 || c.PayloadSize < 0 || c.Runs < 1 {
		return fmt.Errorf("nodes >= 2, messages >= 1, payload-size >= 0, and runs >= 1 are required")
	}
	if c.Topology != "linear" && c.Topology != "ring" && c.Topology != "random" {
		return fmt.Errorf("unknown topology %q", c.Topology)
	}
	if c.FailureRate < 0 || c.FailureRate > 1 {
		return fmt.Errorf("failure-rate must be between 0 and 1")
	}
	return nil
}
