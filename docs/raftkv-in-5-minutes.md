# RaftKV in 5 Minutes

A key-value database that runs on **5 machines**, keeps working when **any 2 fail**, and never loses an acknowledged write or returns a stale read.
Status: **design phase, no code yet.** All numbers here are targets, not results. Details live in the other files in [docs/](.).

**Contents:** [Features](#1-features) · [Architecture](#2-architecture) · [System Design](#3-system-design) · [Database](#4-database-design) · [Sequences](#5-sequence-design) · [Observer UI](#6-observer-frontend) · [E2E Testing](#7-simple-e2e-testing) · [WBS](#8-wbs-checklist)

---

## 1. Features

- `Put`, `Get`, `Delete`, `CompareAndSwap`, `Range` over gRPC.
- Linearizable reads by default (ReadIndex); stale reads opt-in.
- Exactly-once retries via a `(client_id, seq)` session table.
- Automatic leader election (PreVote + CheckQuorum).
- Survives any 2 of 5 nodes failing, no acked writes lost.
- Snapshots for fast restarts and catching up slow nodes.
- Online membership changes: add learner, promote, remove, transfer leader.
- Prometheus metrics, OpenTelemetry traces, structured logs.
- Kubernetes StatefulSet, one disk per pod, spread across zones.
- Later (not v1): sharding, transactions, `Watch`, auth.

## 2. Architecture

```mermaid
flowchart TB
    C["Clients"] --> API["gRPC API<br/>KV + Cluster services"]
    API --> DRV["Node driver<br/>(Ready loop, does all I/O)"]
    DRV <--> CORE["Raft core<br/>(pure logic, no I/O)"]
    DRV -- "append + fsync" --> WAL["WAL<br/>(Raft log)"]
    DRV -- "committed entries" --> SM["Pebble state machine<br/>(keys + sessions + appliedIndex)"]
    DRV <--> TR["Peer transport<br/>(gRPC streams)"]
    TR <--> PEERS["4 other nodes"]
    SNAP["Snapshotter"] -- "checkpoint" --> SM
    SNAP -- "trim old log" --> WAL
    SNAP <--> TR
    MET["Metrics + tracing"] -.-> DRV
```

Requests enter through the gRPC API and reach the node driver. The Raft core only decides what should happen; the driver does the work: fsync to the WAL, send to peers, apply committed entries to Pebble. The snapshotter checkpoints Pebble so the old log can be trimmed.

**Topology:** 5 pods across 3 zones (2/2/1), so losing a whole zone still leaves a quorum of 3.

## 3. System Design

| Concern | How |
|---|---|
| Leader election | Randomized 1–2 s timeout, majority vote |
| Replication / commit | Committed once 3 of 5 nodes fsync it |
| Reads | Leader confirms it still leads, waits for apply, reads locally |
| Writes to followers | Forwarded to leader, or redirect hint |
| Snapshots | Pebble checkpoint every 100k entries or 512 MiB of WAL, streamed in chunks with resume |
| Crash recovery | Load snapshot, replay WAL after Pebble's `appliedIndex` |
| Membership | Join as learner, catch up, promote; one change at a time |
| Failure tolerance | 2 of 5 down: still serving. 3 down: stops, loses nothing |

**Key rules**

- Never trim the WAL past Pebble's flushed `appliedIndex`.
- The Raft core has no I/O, clocks, or goroutines, so tests are deterministic.
- Pause rather than return wrong answers: no quorum means `UNAVAILABLE`; fsync errors crash the node.
- An empty-disk node never rejoins under its old ID.


---

## 4. Database Design

Each node keeps two stores on one disk.

| Store | What it is | What it holds |
|---|---|---|
| **Raft WAL** | Append-only segment files, 64 MiB each, fsynced | The Raft log entries, votes and term (hard state), snapshot markers. This is the source of truth. |
| **Pebble** | The state machine, with Pebble's own WAL turned off | The current key-value data, plus `appliedIndex`, membership and client sessions. Can always be rebuilt by replaying the WAL. |

Pebble keys start with a one-byte prefix, so user keys can never clash with internal ones:

| Prefix | Holds | Example |
|---|---|---|
| `u` (0x75) | User keys and values | `u` + `user:42` |
| `m` (0x6D) | Metadata | `m/appliedIndex`, `m/appliedTerm`, `m/membership` |
| `s` (0x73) | Client sessions (for exactly-once writes) | `s` + client_id |
| `t` (0x74) | Session expiry index | `t` + last_active_ms + client_id |

```text
data/
├── META               # cluster_id, member_id
├── wal/
│   ├── wal-<seq>-<firstIndex>.seg
│   └── wal-next.tmp   # pre-created next segment
├── sm/
│   ├── CURRENT        # points at the live Pebble dir
│   └── db-<index>-<rand>/
└── snap/              # last 2 snapshots (Pebble checkpoints)
```

**Key rules:**
- `appliedIndex` is written in the **same Pebble batch** as the data it covers, so a crash never leaves one without the other.
- The WAL is trimmed only up to `min(snapshotIndex, durable appliedIndex)`. Anything Pebble hasn't flushed yet can still be replayed.
- A torn write at the very end of the WAL is cut off, which is safe because it was never acked. Corruption anywhere else means the node **refuses to start**.

## 5. Sequence Design

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant F as Followers
    participant P as Pebble
    C->>L: Put(key, value)
    par Leader disk
        L->>L: Append to WAL and fsync
    and Replicate
        L->>F: AppendEntries
        F->>F: Append to WAL and fsync
        F-->>L: Ack
    end
    L->>L: Quorum of 3 reached, entry committed
    L->>P: Apply batch with appliedIndex
    L-->>C: OK with revision
```
*A write is acked only after 3 of 5 nodes have it on disk.*

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant F as Followers
    participant P as Pebble
    C->>L: Get(key)
    L->>L: readIndex = commitIndex
    L->>F: Heartbeat
    F-->>L: Ack from majority
    L->>L: Wait until applied is at least readIndex
    L->>P: Read key
    L-->>C: Value
```
*ReadIndex proves the leader is still the leader before reading, without writing to the log.*

```mermaid
sequenceDiagram
    participant C as Client
    participant OL as Old Leader
    participant F as Follower
    participant O as Other Nodes
    OL--xF: Heartbeats stop
    F->>O: PreVote, would you vote for me
    O-->>F: Yes from majority
    F->>O: RequestVote for new term
    O-->>F: Votes from majority
    F->>O: Heartbeats as new leader
    C->>OL: Put fails or times out
    C->>O: Retry on any node
    O-->>C: NOT_LEADER with leader hint
    C->>F: Retry on new leader
```
*PreVote stops a cut-off node from disrupting the cluster, and the client follows the leader hint.*


---

## 6. Observer Frontend

A read-only live view of Raft state: who leads, terms, elections, per-node commit/applied, lagging followers, snapshots, and nodes going up or down. Built for watching e2e and chaos runs as faults land (proposed **D-48**).

```mermaid
flowchart LR
    B["Browser<br/>static HTML + tiny JS"] -- "GET /" --> O["cmd/raftkv-observer"]
    O -- "SSE /events" --> B
    O -- "Cluster.Status every 500 ms" --> N1["raftkv-0"]
    O --> N2["raftkv-1"]
    O --> N3["raftkv-2"]
    O --> N4["raftkv-3"]
    O --> N5["raftkv-4"]
```

```text
RaftKV Observer   cluster 7f3a   term 7   leader raftkv-2   ● live
+-----------+-----------+-----------+-----------+-----------+
| raftkv-0  | raftkv-1  | raftkv-2  | raftkv-3  | raftkv-4  |
| FOLLOWER  | FOLLOWER  | LEADER ★  | FOLLOWER  |   DOWN    |
| term 7    | term 7    | term 7    | term 7    | term 6    |
| commit    | commit    | commit    | commit    | last seen |
|  10482    |  10482    |  10482    |  10479    |  12s ago  |
| applied   | applied   | applied   | applied   |           |
|  10482    |  10481    |  10482    |  10471    |           |
| lag 0     | lag 1     | -         | lag 11 ⚠  | lag 1830  |
| snap 10000| snap 10000| snap 10000| snap 9000 | snap 8000 |
+-----------+-----------+-----------+-----------+-----------+
Writes/s  ▁▂▃▅▇▇▆▇▅▁▁▃▆▇   412/s (applied-index delta)
Events
 12:04:31  raftkv-4 unreachable
 12:04:33  term 6 -> 7, leader raftkv-0 -> raftkv-2
 12:04:40  raftkv-3 snapshot installed @ 9000
```

| Panel | Shows | Source |
|-------|-------|--------|
| Header | Cluster id, term, leader | `Cluster.Status` header, majority view |
| Node cards | Role, term, commit, applied, lag, snapshot index, alarms | Each node's `Status`; lag from the leader's `followers[].match_index` |
| Writes/s sparkline | Write throughput, last ~60 s | Delta of the leader's `applied_index` between polls |
| Event timeline | Elections, leader changes, snapshots, node up/down | The observer diffs successive polls (term, `leader_id`, `snapshot_index`, failed RPC) |

- **Read-only, polling only.** The observer calls nothing except `Cluster.Status`, polling at a fixed 500 ms (flag-configurable), and needs no server changes. Events are inferred by diffing polls, so very short blips (< 500 ms) can be missed. Grafana keeps metrics history and alerting; the observer gives a live, per-node picture of Raft state that dashboards built on scraped metrics show poorly.
- **One Go binary, no build step.** It embeds the page with `embed` and pushes snapshots over SSE. It runs as a 1-replica k8s Deployment and as a docker-compose service in the e2e stack.
- **No auth in v1.** It binds to localhost; reach it in k8s with `kubectl port-forward`. It uses the cluster's client mTLS cert (D-15) to call `Status`.


---

## 7. Simple E2E Testing

**Setup:** docker-compose starts 5 nodes. A test client records every op; the Porcupine checker confirms the history is linearizable.
It runs in GitHub Actions on every PR. The observer UI (section 6) runs in the same compose stack, so you can watch faults land.

| Scenario | Fault | Pass condition |
|---|---|---|
| Happy path | None | History linearizable, nodes match |
| Kill leader | `kill -9` on the leader | New leader, no acked write lost |
| Freeze a follower | `SIGSTOP`, then `SIGCONT` | Cluster serves; follower catches up |
| Network partition | Split 2 \| 3 | Majority serves, minority can't write, heals cleanly |
| Restart and catch up | Stop, then restart a node | Node catches up to leader |
| Add/remove node | Membership change under load | No lost writes, correct config |

**Commands:**
- `make e2e`: 5-node docker-compose e2e suite.
- `make sim SEED=…`: replay one deterministic simulation.
- `make chaos-up && make chaos`: Chaos Mesh fault campaign on kind.

Nightly CI runs a longer kind + Chaos Mesh campaign toward the target of 40M ops, 1,500 faults, 0 violations (targets, not results).

## 8. WBS (Checklist)

- **M0 — Repo, tooling, proto skeleton (1.5 wk)**
  - [ ] Repo, Makefile, lint, CI
  - [ ] gRPC protos
- **M1 — Raft core + simulator (5 wk)**
  - [ ] Leader election and log replication
  - [ ] Deterministic simulator with seed replay
- **M2 — WAL + gRPC transport + multi-process cluster (4 wk)**
  - [ ] Durable WAL
  - [ ] gRPC transport between nodes
  - [ ] docker-compose cluster + e2e harness
- **M3 — Pebble state machine + basic KV (3 wk)**
  - [ ] Apply committed entries to Pebble
  - [ ] Put/Get/Delete + baseline benchmark
- **M4 — Linearizable reads, sessions, full KV API (4 wk)**
  - [ ] ReadIndex / lease reads
  - [ ] Client sessions (exactly-once)
  - [ ] Full KV API (CAS, Range)
- **M5 — Snapshots + follower recovery (3.5 wk)**
  - [ ] Snapshots + log compaction
  - [ ] Catch up lagging followers
- **M6 — Membership changes + leadership transfer (3 wk)**
  - [ ] Add/remove node
  - [ ] Leadership transfer
- **M7 — Performance optimizations + benchmarks (3.5 wk)**
  - [ ] D-13 optimizations (batching, group-commit fsync)
  - [ ] Benchmarks vs. M3 baseline
- **M8 — Kubernetes, mTLS, observability, chaos, v1.0 (5 wk)**
  - [ ] Kubernetes deployment + mTLS
  - [ ] Grafana dashboards + observer UI (D-48)
  - [ ] Chaos Mesh campaign, v1.0 release

Full 391-task checklist: [WBS.md](../WBS.md)

