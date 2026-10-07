// Package protocol defines the small wire format shared by Mesh nodes.
package protocol

import "encoding/json"

const (
	Hello     = "hello"
	Heartbeat = "heartbeat"
	Topology  = "topology"
	Data      = "data"
)

// Frame is newline-delimited JSON on a TCP connection.
type Frame struct {
	Type      string   `json:"type"`
	NodeID    string   `json:"node_id,omitempty"`
	Address   string   `json:"address,omitempty"`
	Neighbors []string `json:"neighbors,omitempty"`
	Origin    string   `json:"origin,omitempty"`
	Seq       uint64   `json:"seq,omitempty"`
	Source    string   `json:"source,omitempty"`
	Dest      string   `json:"dest,omitempty"`
	MessageID string   `json:"message_id,omitempty"`
	Payload   string   `json:"payload,omitempty"`
	TTL       int      `json:"ttl,omitempty"`
}

func Encode(f Frame) ([]byte, error) { return json.Marshal(f) }
