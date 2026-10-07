package protocol

import (
	"encoding/json"
	"testing"
)

// FuzzFrameDecoding verifies untrusted wire bytes cannot panic the decoder.
func FuzzFrameDecoding(f *testing.F) {
	for _, seed := range [][]byte{
		[]byte("{"), []byte("not json"), []byte(`{"type":"data","ttl":-1}`),
		[]byte(`{"type":"unknown","node_id":"","payload":"x"}`),
		[]byte(`{"type":"data","payload":"` + string(make([]byte, 1024)) + `"}`),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, wire []byte) {
		var frame Frame
		_ = json.Unmarshal(wire, &frame)
		if encoded, err := Encode(frame); err == nil {
			var roundTrip Frame
			_ = json.Unmarshal(encoded, &roundTrip)
		}
	})
}
