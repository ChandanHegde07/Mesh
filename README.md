# Mesh

Mesh is a small, software-only Go implementation of a decentralized TCP overlay. Each process owns an
ID, keeps TCP connections to neighbours, exchanges versioned link-state advertisements, and forwards
messages one hop at a time over a deterministic shortest-path route.

It is a networking and distributed-systems engineering project. Messages are the mechanism used to
exercise routing, failure detection, and recovery; it is not a chat application or a production-ready
overlay network.

## At a glance

| Attribute | Value |
|---|---|
| Status | Feature-complete and frozen for v1.0.0 |
| Language | Go (module targets 1.22) |
| Transport | TCP, newline-delimited JSON frames |
| Routing | Deterministic BFS shortest path |
| Topology | Versioned link-state flooding (`seq` per origin) |
| Delivery | Best effort, no acknowledgements |
| Validation | Unit, integration, race, fuzz, reproducible loopback benchmark |

## Repository layout

```
cmd/mesh/           CLI entry point (node and benchmark subcommands)
internal/node/       TCP transport, peer manager, link-state graph, routing
internal/protocol/   wire frame types and JSON encoding
benchmark/           loopback benchmark harness
```

## Architecture

```
CLI / application
       |
     Node ---- TCP peers
       |          |
 peer manager   protocol (newline-delimited JSON)
       |
link-state graph -> deterministic BFS route selection -> message forwarding
```

Nodes exchange `hello`, `heartbeat`, `topology`, and `data` frames. Each node tracks the current
neighbour set of every node it has heard about in a link-state graph. A periodic heartbeat removes
dead direct peers, the graph is updated, and the next send re-selects a route with BFS.

## Node model

- A node is identified by a caller-supplied string ID.
- `Start` opens a TCP listener; `Connect` dials a seed peer and waits for the peer's `hello` before
  returning, so topologies can be built deterministically one edge at a time.
- A successful dial is not yet an overlay peer. A peer enters the active set only after a valid
  `hello`; an idle connection that never greets is closed after a short handshake deadline.
- `Close` stops all loops, closes listeners and connections, waits for goroutines, and closes the
  delivery channel. Shutdown is idempotent.

## Wire protocol

Frames are newline-delimited JSON, bounded to 1 MiB per frame by the scanner. Invalid JSON is
ignored, and unknown frame types are ignored.

| Type        | Fields                                             | Purpose                              |
|-------------|----------------------------------------------------|--------------------------------------|
| `hello`     | `node_id`, `address`                               | Identity handshake on connect        |
| `heartbeat` | -                                                  | Liveness for failure detection       |
| `topology`  | `origin`, `neighbors`, `seq`                       | Versioned link-state advertisement   |
| `data`      | `source`, `dest`, `message_id`, `payload`, `ttl`   | Routed payload                       |

## Topology propagation

Every node periodically advertises its own neighbour list with a monotonically increasing `seq`.
Receivers accept an advertisement only when its `seq` is newer than the last version stored for that
`origin`, then re-flood it to all other peers. Versioning gives two properties:

- **Stale advertisements cannot overwrite newer state**, including this node's authoritative local
  view (`origin == self` is always ignored).
- **Dropped advertisements self-heal.** The origin re-advertises with a fresh version every
  maintenance tick, so a frame lost under load is re-flooded shortly after instead of being
  permanently suppressed.

A newly connected peer is immediately sent the node's full known topology, so late joiners converge.

## Routing algorithm

`Route` runs breadth-first search from the local node and returns a shortest path. Direct TCP
neighbours are preferred over equally short learned routes, because learned state can be briefly
stale after a failure. Neighbours are visited in sorted order, so equal-cost shortest paths are chosen
deterministically instead of depending on Go map iteration order.

## Failure detection

A background maintenance loop sends heartbeats and closes any direct peer whose last-seen time exceeds
the configured peer timeout. When a direct peer is removed, the local neighbour set changes, the
version advances, and the change is advertised immediately.

## Route recovery

Routing is stateless per send: each `Send` recomputes a shortest path from the current graph. After a
failed peer is removed and the surviving topology re-converges, later sends select an alternate route
automatically. In-flight packets are best effort and are not retransmitted. A topology in which the
failed node is a cut vertex (for example an interior node of a line) is partitioned and has no
recovery path.

## Actual benchmark results

Loopback, in-process nodes, `--messages 2000 --payload-size 256 --runs 3 --seed 42`. All twelve
configurations delivered **100.00%** (6000/6000) with zero dropped messages.

| Nodes | Topology | Mean lat (ms) | P95 (ms) | P99 (ms) | msg/s | Avg hops | Convergence (ms) |
|------:|----------|--------------:|---------:|---------:|------:|---------:|-----------------:|
| 5     | ring     | 0.121 | 0.327  | 0.956  | 64451 | 1.51  | 1.08  |
| 10    | ring     | 0.198 | 0.471  | 0.848  | 62707 | 2.77  | 6.88  |
| 25    | ring     | 1.035 | 2.522  | 3.433  | 37465 | 6.47  | 10.69 |
| 50    | ring     | 5.702 | 12.654 | 15.950 | 18991 | 12.92 | 26.83 |
| 5     | random   | 0.109 | 0.267  | 0.402  | 63191 | 1.62  | 0.56  |
| 10    | random   | 0.174 | 0.421  | 0.703  | 63848 | 2.18  | 1.74  |
| 25    | random   | 0.233 | 0.593  | 0.802  | 52202 | 2.90  | 9.72  |
| 50    | random   | 0.749 | 5.324  | 8.978  | 34995 | 3.63  | 40.50 |
| 5     | linear   | 0.153 | 0.369  | 0.589  | 50209 | 2.00  | 6.21  |
| 10    | linear   | 0.331 | 0.884  | 1.311  | 45096 | 3.66  | 6.68  |
| 25    | linear   | 1.740 | 4.498  | 5.764  | 25656 | 8.64  | 10.44 |
| 50    | linear   | 8.940 | 22.511 | 28.140 | 12429 | 17.15 | 26.01 |

Failure/recovery experiment, `--messages 1500 --runs 3 --seed 7 --failure-rate 1` (victim is the
highest-degree node; delivery above is baseline traffic, which is unaffected because the failure is
injected afterwards):

| Nodes | Topology | Detection (ms) | Recovery (ms) | Note                         |
|------:|----------|---------------:|--------------:|------------------------------|
| 10    | ring     | 3.54  | 0.07 | alternate path present        |
| 50    | ring     | 5.50  | 9.73 | alternate path present        |
| 10    | random   | 5.62  | 0.00 | partitioned (hub had leaves)  |
| 50    | random   | 5.32  | 8.23 | alternate path present        |
| 10    | linear   | 1.82  | 0.00 | partitioned (cut vertex)      |
| 50    | linear   | 5.57  | 0.00 | partitioned (cut vertex)      |

A recovery time of `0` means the survivors did not re-converge within the deadline, i.e. removing the
victim partitioned the graph. It is not a claim of instantaneous recovery.

## Reliability validation

Reproduced on Go 1.26 (module targets Go 1.22), macOS arm64:

| Command                                   | Result |
|-------------------------------------------|--------|
| `go build ./...`                          | pass   |
| `go vet ./...`                            | pass   |
| `go test ./... -count=100`                | pass   |
| `go test -race ./... -count=50`           | pass   |
| `go test -race ./internal/node -run TestRouteRecovery -count=200` | pass |
| `go test -fuzz=FuzzFrameDecoding ./internal/protocol` | pass (millions of execs, no crash) |

The route-recovery integration test waits on explicit conditions (peer removal, forwarder
convergence) rather than fixed sleeps before sending recovery traffic.

## Known limitations

- **Transport is loopback only in the benchmarks.** Results reflect local TCP and CPU scheduling, not
  a real network. Compare repeated runs rather than treating one run as definitive.
- **Nodes run in one process** for the benchmark, using real TCP connections. Process-per-node
  execution and active partition/reconnection experiments are not implemented.
- **Delivery is best effort.** There is no end-to-end acknowledgement or retransmission. Under an
  unpaced burst on a high-diameter topology, in-flight packets can still be dropped at a saturated
  transit queue.
- **`ttl` bounds path length** (`defaultTTL = 64`). Destinations farther than the TTL are dropped even
  on a connected graph. Duplicate suppression, not the TTL, is the primary loop guard.
- **Topology advertisements are unauthenticated** and have no persistence. A malicious node could
  forge topology; versions only order advertisements from the same origin.
- **Topology convergence is asynchronous.** Recovery traffic should only be considered ready once the
  surviving nodes have dropped the failed peer and re-converged.
- **Benchmark convergence timing** is measured from the start of connection setup until every node can
  route to every other node; it includes handshake and initial flooding.

## Implemented vs future work

Implemented: TCP transport; node identity; seed-peer connection and discovery; peer management;
heartbeats and failure detection; versioned link-state topology propagation; BFS shortest-path
routing; multi-hop message forwarding; TTL handling; duplicate-message suppression; route recovery
after peer failure; CLI (`peers`, `status`, `routes`, `send`, `quit`); graceful shutdown and cleanup;
a reproducible benchmark harness; and a protocol fuzz target.

Future work (not implemented): gossip-based discovery and topology versions across restarts; TLS or
authenticated encryption; UDP/QUIC transport; deterministic network simulation and richer fault
injection; malicious-node detection and rate limiting; and end-to-end delivery guarantees.
