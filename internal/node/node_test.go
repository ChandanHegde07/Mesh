package node

import (
	"context"
	"io"
	"log"
	"net"
	"testing"
	"time"
)

func testNode(t *testing.T, id string) *Node {
	t.Helper()
	n, err := New(Config{ID: id, Address: "127.0.0.1:0", HeartbeatInterval: 20 * time.Millisecond, PeerTimeout: 100 * time.Millisecond, Logger: log.New(io.Discard, "", 0)})
	if err != nil {
		t.Fatal(err)
	}
	if err = n.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}
func join(t *testing.T, a, b *Node) {
	t.Helper()
	if err := a.Connect(context.Background(), b.Address()); err != nil {
		t.Fatal(err)
	}
}
func eventually(t *testing.T, what string, fn func() bool) {
	t.Helper()
	d := time.Now().Add(2 * time.Second)
	for time.Now().Before(d) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestNodeConnections(t *testing.T) {
	a, b := testNode(t, "A"), testNode(t, "B")
	join(t, a, b)
	eventually(t, "peers", func() bool { return len(a.Peers()) == 1 && len(b.Peers()) == 1 })
}
func TestMultiHopDelivery(t *testing.T) {
	a, b, c := testNode(t, "A"), testNode(t, "B"), testNode(t, "C")
	join(t, a, b)
	join(t, b, c)
	eventually(t, "A route to C", func() bool { return len(a.Route("C")) == 3 })
	if _, e := a.Send("C", "hello"); e != nil {
		t.Fatal(e)
	}
	select {
	case m := <-c.Messages():
		if m.Payload != "hello" || m.Source != "A" {
			t.Fatalf("unexpected %+v", m)
		}
	case <-time.After(time.Second):
		t.Fatal("message not delivered")
	}
}
func TestPeerFailure(t *testing.T) {
	a, b := testNode(t, "A"), testNode(t, "B")
	join(t, a, b)
	eventually(t, "connection", func() bool { return len(a.Peers()) == 1 })
	b.Close()
	eventually(t, "peer removal", func() bool { return len(a.Peers()) == 0 })
}
func TestRouteRecovery(t *testing.T) {
	a, b, c, d := testNode(t, "A"), testNode(t, "B"), testNode(t, "C"), testNode(t, "D")
	join(t, a, b)
	join(t, b, d)
	join(t, a, c)
	join(t, c, d)
	eventually(t, "first route", func() bool { return len(a.Route("D")) == 3 })
	b.Close()
	eventually(t, "B removal at forwarders", func() bool { return !hasPeer(a, "B") && !hasPeer(c, "B") })
	eventually(t, "recovered route through C", func() bool { r := a.Route("D"); return len(r) == 3 && r[1] == "C" })
	// A's route is only an ingress readiness signal. C must also remove B and
	// select its direct D link before traffic can be safely sent over A -> C -> D.
	// This exercises the complete asynchronous recovery sequence rather than
	// racing a packet against C's heartbeat-based failure detection.
	eventually(t, "forwarder convergence after B failure", func() bool {
		r := c.Route("D")
		return len(r) == 2 && r[0] == "C" && r[1] == "D"
	})
	if _, e := a.Send("D", "recovered"); e != nil {
		t.Fatal(e)
	}
	select {
	case m := <-d.Messages():
		if m.Payload != "recovered" {
			t.Fatal(m)
		}
	case <-time.After(time.Second):
		t.Fatal("message not delivered after recovery")
	}
}

func TestConnectRejectsSelfDial(t *testing.T) {
	a := testNode(t, "A")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.Connect(ctx, a.Address()); err == nil {
		t.Fatal("dialling itself should not become a peer")
	}
	if len(a.Peers()) != 0 {
		t.Fatalf("unexpected peers after self dial: %v", a.Peers())
	}
}

func TestConnectReturnsWhenPeerNeverGreets(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		if c, e := ln.Accept(); e == nil {
			_ = c.Close()
		}
	}()
	a := testNode(t, "A")
	start := time.Now()
	if err := a.Connect(context.Background(), ln.Addr().String()); err == nil {
		t.Fatal("expected handshake failure for a non-Mesh peer")
	}
	if time.Since(start) > time.Second {
		t.Fatal("Connect did not return promptly when the peer closed")
	}
}

func hasPeer(n *Node, id string) bool {
	for _, p := range n.Peers() {
		if p.ID == id {
			return true
		}
	}
	return false
}
