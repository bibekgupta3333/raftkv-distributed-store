# Design Decisions

RaftKV is a **learning project**: build Raft and a replicated key-value store from scratch, see it survive failures,
and prove it correct. Speed is **not** a goal, and there are no performance targets.

Every other document builds on this one. Each decision has an ID (`D-xx`) that the other docs cite.
**Decided** means settled. **Proposed** means it's the default unless someone objects. **Open** means it must be
answered before the listed milestone.

## Milestones

| # | Milestone |
|---|-----------|
| M0 | Repo setup, tooling, protos |
| M1 | Raft core + deterministic simulator |
| M2 | WAL + gRPC transport + real 5-process cluster |
| M3 | Pebble state machine + basic KV |
| M4 | Linearizable reads, client sessions, full KV API |
| M5 | Snapshots + follower catch-up |
| M6 | Membership changes + leadership transfer |
| M7 | Kubernetes, observer UI, chaos tests, v1.0 |

## Decisions

| ID | Topic | Status | Decision | Milestone |
|----|-------|--------|----------|-----------|
| D-01 | Raft implementation | Decided | Write our own Raft core, which is the point of the project. etcd/raft-style: a pure state machine plus a `Ready` loop, with no I/O inside. | M1 |
| D-02 | Storage engine | Decided | Pebble (pure Go, no cgo). | M3 |
| D-03 | Raft log storage | Decided | Our own segmented WAL with a CRC32C on every record. Pebble's own WAL is **off**, because the Raft log is the source of truth. | M2 |
| D-04 | Apply durability | Decided | `appliedIndex` is written in the **same Pebble batch** as the data it covers. | M3 |
| D-05 | Linearizable reads | Decided | Default **ReadIndex**. Followers serve reads by asking the leader for a ReadIndex. Stale reads are opt-in per request. Lease reads are an optional extra (D-25). | M4 |
| D-06 | Membership changes | Decided | New nodes join as **learners**, catch up, then get promoted. Changes are made **one server at a time**. Joint consensus is out of scope. Leadership transfer is supported. | M6 |
| D-07 | Sharding | Decided | One Raft group, no sharding. | — |
| D-08 | Election | Decided | **PreVote** and **CheckQuorum** on. Election timeout randomized between 1000 and 2000 ms, heartbeat every 100 ms. | M1 |
| D-09 | Exactly-once writes | Decided | A `(client_id, sequence)` dedup table replicated in the state machine. Sessions expire after 10 minutes. | M4 |
| D-10 | Client API | Decided | `Put`, `Get`, `Delete`, `CompareAndSwap`, `Range`. `Watch` and transactions are out of scope. | M4 |
| D-11 | Limits | Decided | Key ≤ 4 KiB, value ≤ 1 MiB, Range page ≤ 1,000 keys and ≤ 4 MiB. | M4 |
| D-12 | Transport | Decided | gRPC for both the client API and peer RPCs, with one long-lived stream per follower. | M2 |
| D-14 | Snapshots | Decided | A Pebble checkpoint plus metadata (`index`, `term`, `membership`), taken every 100k entries and streamed in 1 MiB chunks. An interrupted transfer starts over; there is no resume. | M5 |
| D-16 | Deployment | Decided | Kubernetes StatefulSet with 5 replicas, a headless Service for peer DNS, and one PVC per pod. Runs locally on **kind**. | M7 |
| D-17 | Observability | Decided | Prometheus metrics, OpenTelemetry traces, `log/slog` logs. | M2+ |
| D-18 | Correctness testing | Decided | A deterministic simulator for the Raft core, the Porcupine linearizability checker over recorded histories, and fault injection (kill, pause, partition). | M1+ |
| D-19 | CI | Decided | GitHub Actions. Every PR: lint, unit and race tests, simulator, and a short 5-node docker-compose e2e with faults. Nightly: a longer simulator run plus kind + Chaos Mesh. | M2+ |
| D-21 | Go & module path | Decided | Latest stable Go. Module `github.com/bibekgupta3333/raftkv-distributed-store`. | M0 |
| D-22 | License | Decided | Apache-2.0. | M0 |
| D-23 | Admin API | Decided | A `Cluster` gRPC service: `AddLearner`, `PromoteLearner`, `RemoveMember`, `TransferLeadership`, `ListMembers`, `Status`. | M6 |
| D-24 | Time | Decided | The Raft core runs on a logical `Tick()`, so tests are deterministic. Wall clock is used only for lease reads, session timestamps (D-27), and metrics. | M1 |
| D-25 | Lease reads (optional) | Decided | Off by default (`lease_reads`). When on, the lease is `(ElectionTick − 1) × tick × (1 − 0.1) − 100 ms` = 710 ms (assumed clock drift 0.1, 100 ms margin; one tick less than the election timeout because a follower may tick right after hearing from the leader). Unsafe beyond the drift bound or if a voter restarts inside the lease. | M4 |
| D-26 | Session setup | Decided | Clients call `RegisterSession` (a replicated entry), and the server assigns the ID. | M4 |
| D-27 | Session expiry time | Decided | The leader stamps the time into each command, and every replica expires sessions using that stamped time. | M4 |
| D-28 | Writes sent to a follower | Decided | The follower forwards the write to the leader. A client can opt in to a redirect (leader hint) instead. | M4 |
| D-29 | Flow control | Decided | At most 256 in-flight messages per follower and at most 64 MiB uncommitted. Beyond that, new proposals get `RESOURCE_EXHAUSTED`. | M2 |
| D-30 | **WAL truncation rule** | **Decided** (safety) | Flush Pebble before trimming the WAL, and trim only up to `min(snapshotIndex, durableAppliedIndex)`. Breaking this rule silently loses data after a crash. | M3/M5 |
| D-31 | Membership details | Decided | A config change takes effect when it is appended. Messages from a removed node are ignored only once its removal is **committed** (not merely appended), so a leader can still commit its own removal; its final `TimeoutNow` is exempt. | M6 |
| D-32 | Node with an empty disk | Decided | A node that could vote twice under its old ID is refused; it rejoins under a new ID as a learner (`--join`). As built: a node that kept its `IDENTITY` file but lost its WAL refuses to start (`ErrStateLost`). A fully wiped node is refused by its peers: each data dir has a random incarnation, sent on every message, and peers refuse a member whose incarnation changed, votes included. Limitation: peers keep incarnations in memory only, so if every peer that knew the old incarnation restarts first, the wiped node is accepted again. | M6 |
| D-33 | Disk errors | Decided | An fsync or write error crashes the node. A torn record at the WAL tail is cut off. Corruption anywhere else means the node refuses to start. | M2/M3 |
| D-34 | Range consistency | Decided | Each page is linearizable. A multi-page scan is not one snapshot. | M4 |
| D-35 | Packages | Decided | `internal/node` holds the driver loop and applier. `internal/server` holds only the gRPC handlers. | M1 |
| D-36 | Peer RPCs | Decided | `Replicate` (stream), `RequestVote` (with a pre-vote flag), `InstallSnapshot` (stream), `TimeoutNow`, `ReadIndex`. | M2–M6 |
| D-38 | Member IDs | Decided | Random 64-bit IDs, never reused. | M6 |
| D-43 | Tooling | Decided | Generated protobuf code is committed (CI checks it is current). Property tests are seeded randomized tests plus native Go fuzzing; `pgregory.net/rapid` was planned but not adopted (2026-11-24). | M0 |
| D-46 | Kubernetes probes | Decided | Ready = knows the leader and has caught up. Alive = the driver loop responds. | M7 |
| D-47 | Chaos tests | Decided | Chaos Mesh `PodChaos` (kill) and `NetworkChaos` (partition, delay, loss) on kind. | M7 |
| D-48 | Observer UI | Decided | A read-only `cmd/raftkv-observer` that polls `Cluster.Status` every 500 ms and serves one HTML page over SSE. No build step, no auth (localhost only). | M7 |

**Removed (learning-project scope, 2026-11-21):** D-13 write-path optimizations, D-15 mTLS, D-20 benchmark hardware,
D-37 rolling upgrades, D-39 counting rules for the validation campaign, D-40 and D-41 benchmark targets and gate,
D-42 optimization flags, D-44 CI budget, D-45 release signing. The IDs are not reused.

## Repository layout

```
cmd/raftkvd/          server
cmd/raftkvctl/        CLI
cmd/raftkv-observer/  live observer UI
api/raftkv/v1/        .proto files
internal/raft/        consensus core (pure, deterministic)
internal/node/        driver loop + applier
internal/wal/         write-ahead log
internal/walstore/    WAL-backed raft.Storage
internal/storage/     Pebble state machine
internal/snapshot/    snapshots
internal/transport/   gRPC peer transport
internal/server/      gRPC handlers
internal/command/     command envelope stored in log entries
internal/membership/  core config <-> proto ClusterConfig
internal/lease/       lease arithmetic (D-25)
internal/observability/ logs, metrics, traces (D-17)
pkg/client/           Go client library (sessions, leader discovery)
test/sim/             deterministic simulator
test/e2e/             docker-compose cluster tests
test/linearizability/ history recorder + Porcupine checker
test/chaos/           Chaos Mesh manifests + chaos driver
deploy/compose/       5-node compose cluster, Prometheus, Grafana, observer
deploy/k8s/           Kubernetes manifests
```

