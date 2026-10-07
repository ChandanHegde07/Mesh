// Package node contains Mesh's TCP transport, peer manager and link-state router.
package node

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"mesh/internal/protocol"
)

const (
	defaultHeartbeat = 300 * time.Millisecond
	defaultTimeout   = 1200 * time.Millisecond
	// defaultTTL bounds forwarding hops. It must exceed the diameter of any
	// network Mesh is expected to serve (a 50-node line has diameter 49), while
	// duplicate suppression still prevents loops.
	defaultTTL = 64
	// peerSendBuffer is the per-connection outbound queue depth. It absorbs
	// bursts so a transit node does not drop forwarding frames under load;
	// sends are still non-blocking and drop only if this queue is exhausted.
	peerSendBuffer = 1024
	// handshakeTimeout bounds how long an accepted connection may stay open
	// before it completes the hello handshake and is registered as a peer.
	handshakeTimeout = 5 * time.Second
)

var (
	ErrNoRoute            = errors.New("no route")
	ErrNextHopUnavailable = errors.New("next hop unavailable")
	ErrPeerCongested      = errors.New("peer send queue congested")
)

// Config controls a node. Address may use port 0 in tests.
type Config struct {
	ID                string
	Address           string
	HeartbeatInterval time.Duration
	PeerTimeout       time.Duration
	Logger            *log.Logger
}

// Message is delivered locally when its destination matches this node.
type Message struct{ Source, Destination, ID, Payload string }

type PeerInfo struct {
	ID, Address string
	LastSeen    time.Time
}
type peer struct {
	id, address string
	conn        net.Conn
	send        chan protocol.Frame
	lastSeen    atomic.Int64 // Unix nanoseconds
	ready       chan struct{}
	closed      chan struct{}
	readyOnce   sync.Once
	closeOnce   sync.Once
}

// Node is a process-local participant in a Mesh overlay.
type Node struct {
	cfg   Config
	ln    net.Listener
	mu    sync.RWMutex
	peers map[string]*peer
	// graph maps an advertised node to its direct neighbours. seq records the
	// latest advertisement version accepted for each origin; monotonic versions
	// let receivers reject stale advertisements and re-flood dropped ones.
	graph     map[string]map[string]bool
	seq       map[string]uint64
	seen      map[string]time.Time
	received  chan Message
	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func New(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("node ID is required")
	}
	if cfg.Address == "" {
		cfg.Address = "127.0.0.1:0"
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = defaultHeartbeat
	}
	if cfg.PeerTimeout == 0 {
		cfg.PeerTimeout = defaultTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &Node{cfg: cfg, peers: make(map[string]*peer), graph: map[string]map[string]bool{cfg.ID: {}}, seq: map[string]uint64{cfg.ID: 0}, seen: make(map[string]time.Time), received: make(chan Message, 64), done: make(chan struct{})}, nil
}

func (n *Node) ID() string { return n.cfg.ID }
func (n *Node) Address() string {
	if n.ln != nil {
		return n.ln.Addr().String()
	}
	return n.cfg.Address
}
func (n *Node) Messages() <-chan Message { return n.received }

func (n *Node) Start() error {
	ln, err := net.Listen("tcp", n.cfg.Address)
	if err != nil {
		return err
	}
	n.ln = ln
	n.logf("node started id=%s address=%s", n.ID(), n.Address())
	n.wg.Add(3)
	go n.acceptLoop()
	go n.maintenanceLoop()
	go n.topologyLoop()
	return nil
}

func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		close(n.done)
		if n.ln != nil {
			_ = n.ln.Close()
		}
		n.mu.Lock()
		for _, p := range n.peers {
			_ = p.conn.Close()
		}
		n.peers = map[string]*peer{}
		n.mu.Unlock()
		n.wg.Wait()
		close(n.received)
	})
	return nil
}

func (n *Node) Connect(ctx context.Context, address string) error {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}
	p := n.attach(conn)
	// A successful TCP dial is not yet an overlay peer: wait for its hello so
	// callers can safely build deterministic topologies one edge at a time.
	// The wait is bounded by ctx and by the connection closing, so a peer that
	// never completes the handshake (for example this node dialling itself)
	// cannot block the caller forever.
	select {
	case <-p.ready:
		return nil
	case <-p.closed:
		return fmt.Errorf("connection to %s closed before handshake", address)
	case <-ctx.Done():
		_ = conn.Close()
		return ctx.Err()
	case <-n.done:
		return errors.New("node closed")
	}
}

func (n *Node) acceptLoop() {
	defer n.wg.Done()
	for {
		c, err := n.ln.Accept()
		if err != nil {
			select {
			case <-n.done:
				return
			default:
				n.logf("accept error: %v", err)
				continue
			}
		}
		n.attach(c)
	}
}
func (n *Node) attach(conn net.Conn) *peer {
	p := &peer{conn: conn, send: make(chan protocol.Frame, 64), ready: make(chan struct{}), closed: make(chan struct{})}
	p.lastSeen.Store(time.Now().UnixNano())
	// An accepted connection must greet within handshakeTimeout; otherwise it is
	// closed instead of leaking a goroutine and socket until shutdown.
	_ = conn.SetReadDeadline(time.Now().Add(handshakeTimeout))
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		defer p.markClosed()
		defer conn.Close()
		enc := json.NewEncoder(conn)
		for {
			select {
			case f := <-p.send:
				if err := enc.Encode(f); err != nil {
					n.removePeer(p)
					// A packet already selected for a peer can race that peer's TCP
					// failure. Re-route it once from the sender after pruning the edge.
					if f.Type == protocol.Data {
						_ = n.forward(f, p.id)
					}
					return
				}
			case <-n.done:
				return
			}
		}
	}()
	n.wg.Add(1)
	go n.readLoop(p)
	p.send <- protocol.Frame{Type: protocol.Hello, NodeID: n.ID(), Address: n.Address()}
	return p
}
func (p *peer) markClosed() { p.closeOnce.Do(func() { close(p.closed) }) }
func (n *Node) readLoop(p *peer) {
	defer n.wg.Done()
	defer p.markClosed()
	defer n.removePeer(p)
	scanner := bufio.NewScanner(p.conn)
	scanner.Buffer(make([]byte, 1024), 1<<20)
	for scanner.Scan() {
		var f protocol.Frame
		if json.Unmarshal(scanner.Bytes(), &f) != nil {
			continue
		}
		p.lastSeen.Store(time.Now().UnixNano())
		n.handle(p, f)
	}
}

func (n *Node) handle(p *peer, f protocol.Frame) {
	switch f.Type {
	case protocol.Hello:
		if f.NodeID == "" {
			return
		}
		if f.NodeID == n.ID() {
			// This connection is a loop to ourselves (for example a self-dial).
			// Close it so the dialing caller's handshake wait can finish.
			_ = p.conn.Close()
			return
		}
		p.id, p.address = f.NodeID, f.Address
		// The handshake is complete; remove the temporary read deadline.
		_ = p.conn.SetReadDeadline(time.Time{})
		n.mu.Lock()
		old := n.peers[p.id]
		n.peers[p.id] = p
		n.refreshLocalLocked()
		n.mu.Unlock()
		// Signal handshake completion only after the peer is registered so a
		// successful Connect means the peer is visible to Peers and routing.
		p.readyOnce.Do(func() { close(p.ready) })
		if old != nil && old != p {
			_ = old.conn.Close()
		}
		n.logf("peer connected id=%s address=%s", p.id, p.address)
		// A newly connected peer is sent the complete topology we already know so
		// it can route beyond its direct neighbours immediately.
		n.sendKnownTopology(p)
		n.broadcastTopology()
	case protocol.Heartbeat: // lastSeen is updated above
	case protocol.Topology:
		// Local neighbour state is authoritative. A relayed older advertisement
		// from this node can return over a topology cycle; accepting it would
		// overwrite newer local connections with stale neighbours.
		if f.Origin == "" || f.Origin == n.ID() {
			return
		}
		n.mu.Lock()
		known, ok := n.seq[f.Origin]
		if ok && f.Seq <= known {
			// Already have this version (or a newer one): reject stale/duplicate
			// advertisements so relays cannot clobber newer topology.
			n.mu.Unlock()
			return
		}
		ns := map[string]bool{}
		for _, id := range f.Neighbors {
			ns[id] = true
		}
		n.graph[f.Origin] = ns
		n.seq[f.Origin] = f.Seq
		n.mu.Unlock()
		// Flood the newer advertisement so nodes learn beyond direct peers. The
		// origin re-advertises every maintenance tick with an incremented
		// version, so any frame dropped under load is re-flooded shortly after.
		n.relayTopology(f, p.id)
	case protocol.Data:
		if f.Dest == n.ID() {
			if n.markSeen(f.MessageID) {
				n.logf("message received id=%s source=%s", f.MessageID, f.Source)
				select {
				case n.received <- Message{f.Source, f.Dest, f.MessageID, f.Payload}:
				case <-n.done:
				}
			}
			return
		}
		if f.TTL <= 0 || !n.markSeen(f.MessageID) {
			return
		}
		f.TTL--
		if err := n.forward(f, ""); err != nil {
			n.logf("route failed message=%s: %v", f.MessageID, err)
		}
	}
}

func (n *Node) Send(destination, payload string) (string, error) {
	id := randomID()
	f := protocol.Frame{Type: protocol.Data, Source: n.ID(), Dest: destination, MessageID: id, Payload: payload, TTL: defaultTTL}
	n.markSeen(id)
	if err := n.forward(f, ""); err != nil {
		return id, err
	}
	return id, nil
}
func (n *Node) forward(f protocol.Frame, avoid string) error {
	for attempt := 0; attempt < 2; attempt++ {
		path := n.Route(f.Dest)
		if len(path) < 2 {
			return fmt.Errorf("%w to %s", ErrNoRoute, f.Dest)
		}
		next := path[1]
		n.mu.RLock()
		p := n.peers[next]
		n.mu.RUnlock()
		if p == nil || next == avoid {
			// The connection may have disappeared just before its read goroutine
			// removed the graph edge. Prune it, advertise the change, and select
			// once more immediately.
			n.mu.Lock()
			delete(n.peers, next)
			n.refreshLocalLocked()
			n.mu.Unlock()
			n.broadcastTopology()
			continue
		}
		select {
		case p.send <- f:
			n.logf("route selected message=%s next=%s path=%v", f.MessageID, next, path)
			return nil
		default:
			return fmt.Errorf("%w: %s", ErrPeerCongested, next)
		}
	}
	return fmt.Errorf("%w after route refresh", ErrNextHopUnavailable)
}

// Route returns a shortest path from this node to destination using current topology.
func (n *Node) Route(destination string) []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	start := n.ID()
	// Prefer a confirmed direct TCP neighbour over equally short information
	// learned through advertisements, which may briefly be stale after failure.
	if n.graph[start][destination] && n.peers[destination] != nil {
		return []string{start, destination}
	}
	q := []string{start}
	prev := map[string]string{start: ""}
	for len(q) > 0 {
		cur := q[0]
		q = q[1:]
		if cur == destination {
			break
		}
		// Visit neighbours in sorted order so equal-cost shortest paths are
		// chosen deterministically instead of depending on Go map iteration.
		next := make([]string, 0, len(n.graph[cur]))
		for id := range n.graph[cur] {
			next = append(next, id)
		}
		sort.Strings(next)
		for _, id := range next {
			if _, ok := prev[id]; !ok {
				prev[id] = cur
				q = append(q, id)
			}
		}
	}
	if _, ok := prev[destination]; !ok {
		return nil
	}
	var out []string
	for x := destination; x != ""; x = prev[x] {
		out = append(out, x)
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}
func (n *Node) Peers() []PeerInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]PeerInfo, 0, len(n.peers))
	for _, p := range n.peers {
		out = append(out, PeerInfo{p.id, p.address, time.Unix(0, p.lastSeen.Load())})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (n *Node) Routes() map[string][]string {
	n.mu.RLock()
	ids := make([]string, 0, len(n.graph))
	for id := range n.graph {
		ids = append(ids, id)
	}
	n.mu.RUnlock()
	sort.Strings(ids)
	out := map[string][]string{}
	for _, id := range ids {
		if id != n.ID() {
			if r := n.Route(id); r != nil {
				out[id] = r
			}
		}
	}
	return out
}
func (n *Node) maintenanceLoop() {
	defer n.wg.Done()
	tick := time.NewTicker(n.cfg.HeartbeatInterval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			now := time.Now()
			n.mu.RLock()
			ps := make([]*peer, 0, len(n.peers))
			for _, p := range n.peers {
				ps = append(ps, p)
			}
			n.mu.RUnlock()
			for _, p := range ps {
				if now.Sub(time.Unix(0, p.lastSeen.Load())) > n.cfg.PeerTimeout {
					n.logf("peer timeout id=%s", p.id)
					_ = p.conn.Close()
					continue
				}
				select {
				case p.send <- protocol.Frame{Type: protocol.Heartbeat}:
				default:
				}
			}
		case <-n.done:
			return
		}
	}
}
func (n *Node) topologyLoop() {
	defer n.wg.Done()
	tick := time.NewTicker(n.cfg.HeartbeatInterval * 2)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			// Advertise every tick with a fresh version. Unchanged neighbour
			// sets still carry a new version, which forces one re-flood per tick
			// and therefore repairs any advertisement dropped under load.
			n.mu.Lock()
			n.seq[n.ID()]++
			n.mu.Unlock()
			n.broadcastTopology()
		case <-n.done:
			return
		}
	}
}
func (n *Node) broadcastTopology() {
	n.mu.RLock()
	ns := make([]string, 0, len(n.peers))
	ps := make([]*peer, 0, len(n.peers))
	for id, p := range n.peers {
		ns = append(ns, id)
		ps = append(ps, p)
	}
	seq := n.seq[n.ID()]
	n.mu.RUnlock()
	sort.Strings(ns)
	f := protocol.Frame{Type: protocol.Topology, Origin: n.ID(), Neighbors: ns, Seq: seq}
	for _, p := range ps {
		select {
		case p.send <- f:
		default:
		}
	}
}

func (n *Node) relayTopology(f protocol.Frame, except string) {
	n.mu.RLock()
	ps := make([]*peer, 0, len(n.peers))
	for id, p := range n.peers {
		if id != except {
			ps = append(ps, p)
		}
	}
	n.mu.RUnlock()
	for _, p := range ps {
		select {
		case p.send <- f:
		default:
		}
	}
}
func (n *Node) sendKnownTopology(p *peer) {
	n.mu.RLock()
	frames := make([]protocol.Frame, 0, len(n.graph))
	for origin, neighbors := range n.graph {
		ns := make([]string, 0, len(neighbors))
		for id := range neighbors {
			ns = append(ns, id)
		}
		sort.Strings(ns)
		frames = append(frames, protocol.Frame{Type: protocol.Topology, Origin: origin, Neighbors: ns, Seq: n.seq[origin]})
	}
	n.mu.RUnlock()
	for _, f := range frames {
		select {
		case p.send <- f:
		default:
		}
	}
}
func (n *Node) refreshLocalLocked() {
	ns := map[string]bool{}
	for id := range n.peers {
		ns[id] = true
	}
	// Only a genuine change in the local neighbour set advances the version, so
	// redundant refreshes do not create spurious topology updates.
	if !sameNeighbors(n.graph[n.ID()], ns) {
		n.graph[n.ID()] = ns
		n.seq[n.ID()]++
	}
}
func sameNeighbors(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for id := range a {
		if !b[id] {
			return false
		}
	}
	return true
}
func (n *Node) removePeer(p *peer) {
	n.mu.Lock()
	if p.id != "" && n.peers[p.id] == p {
		delete(n.peers, p.id)
		n.refreshLocalLocked()
		n.mu.Unlock()
		n.logf("peer disconnected id=%s", p.id)
		n.broadcastTopology()
		return
	}
	n.mu.Unlock()
}
func (n *Node) markSeen(id string) bool {
	if id == "" {
		return true
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.seen[id]; ok {
		return false
	}
	n.seen[id] = time.Now()
	for k, t := range n.seen {
		if time.Since(t) > time.Minute {
			delete(n.seen, k)
		}
	}
	return true
}
func (n *Node) logf(format string, args ...any) { n.cfg.Logger.Printf(format, args...) }
func randomID() string                          { b := make([]byte, 12); _, _ = rand.Read(b); return hex.EncodeToString(b) }
