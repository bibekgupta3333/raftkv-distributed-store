# Design Decisions & Open Questions

This is the **baseline every other document builds on**. Each decision has an ID (`D-xx`) that other docs cite.
Status values: **Decided** (baseline, change only via a new entry), **Proposed** (recommended default, needs sign-off),
**Open** (blocks a milestone until resolved).

> Rule: no implementation code is written for a milestone until every `Open` decision it depends on is resolved.

## Summary table

| ID | Topic | Status | Baseline / recommendation | Blocks |
|----|-------|--------|---------------------------|--------|
| D-01 | Raft implementation | Proposed | Write our own Raft core (etcd/raft-style: pure state machine + `Ready` loop, no I/O inside). Keep an interface so `go.etcd.io/raft/v3` could replace it. | M1 |
| D-02 | State-machine storage engine | Decided | Pebble (pure Go, no cgo). RocksDB not used. | M3 |
| D-03 | Raft log storage | Proposed | Dedicated segmented WAL (64 MiB segments, CRC32C per record, group-commit fsync). Pebble's own WAL is **disabled**; Raft log is the source of truth. | M2 |
| D-04 | Apply durability | Decided | `appliedIndex` is written in the **same Pebble batch** as the applied entries, so replay after a crash is exact. | M3 |
| D-05 | Linearizable reads | Proposed | Default **ReadIndex**. **Lease reads** optional behind a flag, only with a configured max clock drift. Followers serve reads via ReadIndex forwarding. Stale reads opt-in per request (`consistency=STALE`). | M4 |
| D-06 | Membership changes | Proposed | **Learner (non-voting) catch-up + single-server changes** (Raft dissertation §4.1). Joint consensus deferred. Leadership transfer (`TimeoutNow`) supported. | M6 |
| D-07 | Sharding | Proposed | **Single Raft group** in v1. Multi-Raft/range sharding out of scope; keep the key space and API shard-agnostic. | — |
| D-08 | Election safety extras | Decided | **PreVote** and **CheckQuorum** enabled. Election timeout randomized 1000–2000 ms, heartbeat 100 ms (tunable). | M1 |
| D-09 | Client write semantics | Proposed | Client sessions: `(client_id, sequence)` dedup table replicated in the state machine, so retried writes apply exactly once. Session TTL 10 min. | M4 |
| D-10 | Client API surface | Proposed | `Put`, `Get`, `Delete`, `CompareAndSwap`, `Range` (prefix/range scan with limit). `Watch` and multi-key transactions deferred. | M4 |
| D-11 | Limits | Proposed | Key ≤ 4 KiB, value ≤ 1 MiB, Raft entry batch ≤ 4 MiB, Range page ≤ 1,000 keys. | M4 |
| D-12 | Transport | Decided | gRPC for both client API and peer RPC. Peer replication uses a long-lived bidirectional stream per follower with pipelining (bounded in-flight window). | M2 |
| D-13 | Write path optimizations | Proposed | Proposal batching, group-commit fsync, leader writes its WAL **in parallel** with sending AppendEntries (dissertation §10.2.1), pipelined replication. Each is a separate change so its benchmark gain can be measured on its own. | M7 |
| D-14 | Snapshots | Proposed | Pebble checkpoint + metadata (`index`, `term`, `membership`). Triggered every 100k entries or 512 MiB of WAL. Sent as a chunked gRPC stream (1 MiB chunks) with resume support. | M5 |
| D-15 | Security | Open | mTLS between peers and clients: required, optional, or off in v1? Recommendation: optional mTLS in v1, AuthN/AuthZ deferred. | M8 |
| D-16 | Deployment | Proposed | Kubernetes StatefulSet (5 replicas), headless Service for peer DNS, client Service, one PVC per pod, PodDisruptionBudget `maxUnavailable: 1`, anti-affinity across nodes/zones. | M8 |
| D-17 | Observability | Decided | Prometheus metrics, OpenTelemetry tracing (OTLP), structured logs with `log/slog`. | M2+ |
| D-18 | Correctness testing | Proposed | Deterministic simulation of the Raft core, plus a Porcupine linearizability checker over recorded histories, plus fault injection (kill, SIGSTOP pause, partition, packet loss/latency, disk-full). | M1+ |
| D-19 | E2E CI | Proposed | GitHub Actions: unit + race on every PR; docker-compose 5-node e2e with fault injection on every PR (short); kind + Chaos Mesh nightly (long). | M2+ |
| D-20 | Benchmark reference hardware | Open | Pick the reference environment (e.g. 5× cloud VMs with local NVMe, same AZ, 8 vCPU/32 GiB). Targets mean nothing without it. | M7 |
| D-21 | Go version & module path | Open | Go ≥ 1.23. Module path `github.com/<owner>/raftkv`: owner not decided. | M0 |
| D-22 | License | Open | Apache-2.0 recommended. | M0 |
| D-23 | Raft-level config changes via API | Proposed | A separate `Cluster` admin gRPC service (`AddLearner`, `PromoteLearner`, `RemoveMember`, `TransferLeadership`, `ListMembers`). | M6 |
| D-24 | Time source | Decided | Election/heartbeat logic driven by a logical `Tick()` so tests stay deterministic. Wall clock is used only for lease reads (D-05) and metrics. | M1 |

## Decisions added from design review (2026-11-21)

Collected from the "New open questions" sections of the topic docs. Duplicates were merged; the source question
IDs are listed so the full discussion can be found. *arch* = architecture.md, *sys* = system-design.md,
*api* = api-design.md, *stor* = storage-design.md, *test* = testing-benchmarks.md, *ci* = ci-e2e.md,
*road* = implementation-roadmap.md.

| ID | Topic | Status | Baseline / recommendation | Blocks | Source |
|----|-------|--------|---------------------------|--------|--------|
| D-25 | Lease-read clock parameters | Proposed | Assumed max drift rate 0.1 and a safety margin of 100 ms, which gives an effective lease of about 800 ms. Requires NTP/chrony on the hosts. Lease reads stay off by default (D-05). | M4 (lease) | sys Q-S1, road 1 |
| D-26 | Client session model | Proposed | Explicit `RegisterSession` (a replicated entry; the server assigns the ID). Window of 64 outstanding requests, cap of 100k live sessions. This closes the implicit-creation hole where a first-write retry arriving after TTL eviction is applied twice. | M4 | sys Q-S2, api Q-A1, road 2 |
| D-27 | Deterministic time for session TTL | Proposed | The leader stamps wall-clock time into each command entry, and every replica evicts sessions using that replicated timestamp. **Amends D-24:** wall clock is also allowed for this replicated timestamp. | M4 | api Q-A2, stor Q-S7 |
| D-28 | Write routing | Proposed | Followers forward writes to the leader by default (one hop, same session and deadline). `write_routing=REDIRECT` is opt-in and returns UNAVAILABLE plus a leader hint. **Amends D-12.** | M4 | arch Q-A2, api Q-A7 |
| D-29 | Flow control & back-pressure | Proposed | Per follower: 256 in-flight messages of ≤ 1 MiB each. Uncommitted tail capped at 64 MiB; new proposals are rejected with RESOURCE_EXHAUSTED once the cap is hit. All internal queues are bounded. These are starting values to tune in M7. | M2 | sys Q-S3, arch Q-A5 |
| D-30 | **WAL truncation invariant (INV-TRUNC)** | **Decided** (safety) | Call `Flush()` on Pebble before any WAL truncation, and truncate only up to `min(snapshotIndex, durableAppliedIndex)`. The truncate call fails-stop if that bound is violated, and recovery also checks for a gap. Keep 50k entries after each snapshot so lagging followers can catch up from the log. | M3/M5 | sys Q-S4, road R-xx, stor §2.4, stor Q-S1 |
| D-31 | Membership mechanics | Proposed | A configuration takes effect **when appended** (dissertation; consistent with D-06). Peer messages from removed member IDs are rejected using a persisted tombstone set. | M6 | sys Q-S5, arch Q-A4 |
| D-32 | Empty-disk / lost-PVC node | Proposed | A node **must refuse to start** with an existing member ID and an empty log, because rejoining that way risks a double vote. It rejoins with a new ID as a learner. v1 does this through a manual runbook; automating it (e.g. an init container) is deferred. | M6/M8 | sys Q-S6, stor Q-S2, road 5 |
| D-33 | Storage failure policy | Proposed | fsync/write error → crash (fail-stop, because of fsyncgate). `ENOSPC` → degraded read-only mode with a ballast file. Corruption in the middle of the log → exit 78 (no quarantine mode in v1). An fsync watchdog transfers leadership if the disk stalls. | M2/M3 | sys Q-S7/S8, stor Q-S6 |
| D-34 | Range limits & consistency | Proposed | **Amends D-11:** add a cap of 4 MiB per Range page. A multi-page Range is *not* one consistent snapshot in v1; each page is linearizable on its own. | M4 | api Q-A4, sys Q-S9 |
| D-35 | Package layout | Proposed | Add `internal/node` for the Raft driver loop and the applier. `internal/server` keeps only the gRPC handlers. | M1 | arch Q-A1 |
| D-36 | Extra RPCs | Proposed | Add `Cluster.Status`, plus peer RPCs `SnapshotProgress` (resume support) and `ReadIndex` (follower reads). **Amends D-23.** | M4–M6 | api Q-A3, api Q-A6 |
| D-37 | Rolling upgrades | Open | Store a cluster version in the replicated state that gates new entry or command types. Pin Pebble's on-disk format per release so checkpoints stay readable in mixed-version clusters. Snapshot compatibility rules are still to be written. | M8 | api Q-A5, arch Q-A3, stor Q-S5 |
| D-38 | Member ID assignment | Proposed | Random 64-bit IDs generated at join time and never reused (this also supports D-32). | M6 | api Q-A8 |
| D-39 | Validation campaign counting | Proposed | Only real-process runs count toward the 40M; simulation ops and Range ops don't. Changing the Raft, WAL, storage, or read path resets the counters entirely; other changes don't. Each checked history comes from one client host so all ops share one clock. | M8 | test Q-1/2/6/7, road 3 |
| D-40 | Meaning of "sustained" and the 30% target | Open | Proposed: throughput counts as sustained only while p99 ≤ 50 ms for writes and ≤ 20 ms for reads. The 30% p99 improvement is judged at 70% of the baseline's maximum sustainable throughput. Still to settle: does the 92K reads/s target include follower reads, and is the headline run done with mTLS on or off (D-15)? | M7 | test Q-11/13/15/16 |
| D-41 | Bench regression gate | Proposed | Alert (don't block merges) on a ≥ 10% throughput drop or a ≥ 15% p99 rise against the last reference run. | M7 | road 8 |
| D-42 | Optimization & fsync flags | Proposed | D-13 optimizations stay behind flags and default to on after M7. An unsafe `--no-fsync` dev flag exists, needs `--i-know-this-is-unsafe`, and the bench harness refuses to run with it. | M2/M7 | test Q-12, road 10 |
| D-43 | Tooling | Proposed | Pin the Go toolchain to the latest stable at M0 (this tightens D-21). Commit generated protobuf code and have CI check it is up to date. Use `pgregory.net/rapid` for property tests. Test-only fault build tags are allowed. | M0 | road 4/7, ci Q-C9, test Q-3/8 |
| D-44 | CI infrastructure | Open | Budget for larger runners, repo visibility, splitting chaos runs longer than GitHub's 6 h job limit into chained jobs, whether an open P0 correctness issue blocks merges, where histories, ledgers, and benchmark results are stored, and who owns and pays for the reference cluster. | M2/M7/M8 | ci Q-C1–C7, test Q-10 |
| D-45 | Release & packaging | Proposed | Kustomize for v1 (Helm deferred), cosign keyless signing, images on GHCR under the D-21 owner, signed release tags. | M8 | road 6/9, ci Q-C5 |
| D-46 | Kubernetes probes & placement | Proposed | Readiness = knows the leader and is within N entries of commit. Liveness = the driver loop is responding (not "has a leader"). A separate WAL volume is optional. A disk-pressure alarm is node-local and also exported as a metric. | M8 | sys Q-S10, stor Q-S3/S4 |
| D-47 | Chaos tooling gaps | Proposed | Implement SIGSTOP-style pause in Kubernetes through a privileged ephemeral container with a shared process namespace. A spike in M8 must confirm IOChaos works on kind before the nightly job relies on it. | M8 | test Q-9, ci Q-14 |
| D-48 | Observer frontend | Proposed | Read-only `cmd/raftkv-observer`: polls `Cluster.Status` (D-36) every 500 ms and serves an embedded HTML page over SSE, with no JS build step. Events are inferred by diffing polls. Bound to localhost / port-forward, no auth in v1. Runs in docker-compose e2e and as a k8s Deployment. Grafana stays for metrics history. | M8 | raftkv-in-5-minutes.md §6 |

D-15 (mTLS) now also covers per-listener TLS policy (api Q-A9) and whether e2e also runs under mTLS (ci Q-C8).

## Performance targets (not results)

These are **goals for the reference hardware (D-20)**. Nothing has been measured yet. They come from the project brief.

| Metric | Target |
|--------|--------|
| Sustained writes (5 nodes, 3-of-5 quorum, 256 B values, fsync on) | ≥ 38,000 ops/s |
| Sustained linearizable reads (ReadIndex, 256 B values) | ≥ 92,000 ops/s |
| Correctness validation | ≥ 40M operations checked, ≥ 1,500 injected faults (leader kill, process kill/pause, partition), **0** linearizability violations |
| p99 write latency improvement from D-13 optimizations | ≥ 30% relative to the M3 unoptimized baseline, same hardware and workload |

## Planned repository layout

```
cmd/raftkvd/          server binary
cmd/raftkvctl/        admin/client CLI
cmd/raftkv-bench/     load generator
api/raftkv/v1/        .proto files (KV, Cluster, Raft services)
internal/raft/        consensus core (pure, deterministic)
internal/wal/         segmented write-ahead log
internal/storage/     Pebble state machine
internal/snapshot/    snapshot create/send/install
internal/transport/   gRPC peer transport
internal/server/      KV + Cluster service handlers, read/write paths
internal/metrics/     Prometheus + OTel wiring
test/sim/             deterministic simulation
test/e2e/             docker-compose cluster tests
test/linearizability/ history recorder + Porcupine checker
test/chaos/           Chaos Mesh manifests
deploy/k8s/           StatefulSet, Services, PDB, ServiceMonitor
```

## Open questions to resolve before M0/M1

1. **D-01**: Own Raft vs `etcd/raft`? Owning it is the point of the project, but it costs roughly 3–4 extra weeks and adds correctness risk.
2. **D-20**: Reference benchmark hardware and budget.
3. **D-15**: Is mTLS in scope for v1?
4. **D-21/D-22**: Module path owner and license.
5. **D-06**: Is single-server membership change acceptable, or is joint consensus a hard requirement?
6. **D-07**: Confirm a single Raft group is enough (no sharding) for v1.
7. **D-40**: What counts as "sustained" throughput, and whether follower reads count toward the 92K reads/s target.
8. **D-44**: CI budget and who owns the reference cluster.
9. **D-37**: Rolling-upgrade compatibility policy. Can wait until M8.

All `Proposed` entries (D-25 to D-47) need a quick yes/no review. Accepting them as a batch is reasonable.
