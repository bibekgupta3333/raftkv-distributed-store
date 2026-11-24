# Changelog

All notable changes to this project are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versioning: [SemVer](https://semver.org/).

## [Unreleased]

### Docs

- README: a "Demo" section with real screenshots from a 5-pod kind cluster: start, replication, killing the leader,
  killing a second node, restore and catch-up, plus the Grafana dashboard (`docs/images/demo/`).

## [1.0.0] - 2026-11-24

The first complete release: Raft and a replicated key-value store written from scratch in Go, a 5-node cluster on
docker-compose or kind, and the evidence that it stays linearizable under crashes, pauses, partitions, lossy links,
membership changes and rolling restarts. A learning project: no performance targets. This section consolidates every
change since the project started; the `0.x` sections below say what each milestone tag contained (new since `0.5.0`:
#25, #26, #27). Exit evidence per milestone is in `docs/implementation-roadmap.md`.

### Added
- **M0 — Repository and tooling** (#2, #3): Go module, Apache-2.0 license (D-22), directory layout, Makefile, pinned
  `go tool` directives (golangci-lint, buf), a lint rule banning `time.Now` in `internal/raft` (D-24),
  version-stamped binaries, the protobuf API (`api/raftkv/v1`: KV, Cluster, Raft) with committed generated code and a
  CI drift check (D-43), and the `pr.yml` workflow.
- **M1 — Raft core** (#5): `internal/raft`, a pure state machine behind `RawNode` (`Tick`/`Step`/`Propose`/`Ready`/
  `Advance`, D-01): elections with PreVote and CheckQuorum (D-08), log replication with fast backtracking, the
  current-term commit rule, proposal limits (D-29), Figure 7/8 scenario tests.
- **M1 — Deterministic simulator** (#7): `test/sim` with a seeded scheduler, virtual network (delay, drop, duplicate,
  reorder, partitions, one-way blocks) and crash model; INV-1..INV-6 and liveness checks with planted-bug tests; fault
  profiles; JSONL traces with a pretty-printer; regression seeds; `make sim`; 500 seeds per profile on every PR and
  100,000 per profile nightly.
- **M2 — WAL** (#4, #6): segmented WAL with CRC32C records, fsync (`F_FULLFSYNC` on macOS), torn-tail repair,
  refuse-to-start on any other corruption, fatal-on-fsync-error (D-03, D-33), a fault-injecting file shim and a
  crash matrix; `internal/walstore` implements `raft.Storage` on it.
- **M2 — Node, transport and cluster** (#6, #8): gRPC peer transport with one stream per follower, flow control and
  reconnect (D-12); the node driver loop (D-35); YAML config and flags; node identity file; `slog` logs, Prometheus
  metrics and OTel spans (D-17); Dockerfile and a 5-node docker-compose cluster; the `test/e2e` harness with kill,
  SIGSTOP, partition and netem faults on a seeded schedule; `e2e-short` (PR) and `e2e-long` (nightly) jobs.
- **M3 — State machine and KV** (#9): Pebble with its WAL off (D-02), `appliedIndex` in the apply batch (D-04), the
  WAL truncation rule (D-30), recovery by WAL replay, a 500-round `kill -9` crash suite; the command envelope,
  `Put`/`Get`/`Delete` with proposal tracking and leader hints; `pkg/client`; `raftkvctl put/get/del` and
  `debug state-hash`/`wal-dump`; the history recorder and per-key Porcupine checker.
- **M4 — Linearizable reads** (#14): ReadIndex in the core, batched per heartbeat round, served by any member through
  the `ReadIndex` peer RPC (D-05, D-36); opt-in stale reads; optional lease reads, off by default (D-25); simulator
  read checks and a `lease` profile.
- **M4 — Sessions and the full API** (#15, #18, #19): exactly-once writes through `RegisterSession` and a replicated
  `(client_id, sequence)` table with leader-stamped expiry (D-09, D-26, D-27); `CompareAndSwap` and paged `Range`
  (D-10, D-34); follower write forwarding with opt-in redirect (D-28); D-11 limits checked before propose;
  `raftkvctl cas/range`; Porcupine models for CAS and Range pages; `make mutation`, a deliberately broken read path
  the checker must catch; docker e2e under Porcupine (`TestLinearizableKVUnderFaults`, `TestOldLeaderServesNoStaleRead`).
- **M5 — Snapshots** (#17): Pebble checkpoints every 100k entries, the 2 newest kept; WAL compaction under D-30 with a
  retention tail; `InstallSnapshot` in 1 MiB CRC-checked chunks with restart on interruption and an atomic
  `sm/CURRENT` swap (D-14); snapshot metrics; a `snapshot` simulator profile; a follower-wipe e2e fault.
- **M6 — Membership and leadership transfer** (#22): single-server `AddLearner` / `PromoteLearner` / `RemoveMember`
  with learners, effective-on-append configs and one change at a time (D-06, D-31); random never-reused member IDs
  (D-38) and data-directory incarnations (D-32); `TransferLeadership` with `TimeoutNow`, also on SIGTERM;
  `raftkvctl member ...` and `raftkvd --join`; a `membership` simulator profile with ConfigOverlap/ConfigEarly
  invariants; replace-a-node and scale 3 → 5 → 3 e2e tests under Porcupine.
- **M7 — Observability** (#12): `Cluster.Status`; `cmd/raftkv-observer`, a read-only live UI over SSE with an event
  timeline (D-48); Grafana with a provisioned dashboard in compose; gRPC request metrics.
- **M7 — Kubernetes** (#13, #26): Kustomize base and kind overlay (StatefulSet, headless peer Service, PVC per pod,
  D-16), `/livez` and `/readyz` probes (D-46), `--node-id-from-ordinal`, `make kind-up/kind-down`; preStop sleep and a
  30 s grace period sized for the shutdown handover.
- **M7 — Chaos tests** (#13, #23, #26): Chaos Mesh PodChaos/NetworkChaos manifests (D-47) and `make chaos-up`; a chaos
  driver (`make chaos`) whose in-cluster workload records a mixed KV history under a seeded schedule of pod kills,
  partitions, delay, loss and rolling restarts (each leader must hand over by `TimeoutNow` in under 800 ms), checked
  by Porcupine; a nightly `chaos` job.
- **M7 — Hardening** (#21): native fuzz targets for every decoder of bytes from disk or the network, `make fuzz` and a
  nightly `fuzz-long` job; on-disk format version checks (WAL, Pebble schema, snapshot) that refuse to start with
  exit 78; `docs/configuration.md` checked against `--help` (`make docs-check`).
- **Evidence** (#25, #27): exit evidence for M0–M7 in `docs/implementation-roadmap.md`, a nightly `sim-race` job
  (10,000 seeds with `-race`, session-table determinism at 10,000 seeds), the v1.0 Definition of Done walk, and a
  README quick start for compose and kind with known limitations.

### Changed
- Re-scoped as a learning project (2026-11-21): no performance targets, benchmarks or production-only work (mTLS,
  release signing, rolling upgrades); milestones M0–M7; decisions D-13, D-15, D-20, D-37, D-39–D-42, D-44, D-45
  removed and their IDs not reused.
- D-25 lease is `(ElectionTick-1)*tick*(1-drift)-margin` = 710 ms (#14). D-31: a removed member's messages are ignored
  once its removal is committed, so a leader can commit its own removal (#22). D-32 as built: `IDENTITY` without a WAL
  refuses to start; a fully wiped node is refused by peers through its incarnation, which peers keep in memory only
  (#22). D-43: property tests are seeded randomized tests plus fuzzing; `pgregory.net/rapid` was not adopted (#27).
- **Breaking:** the state machine lives in `sm/db-*` behind `sm/CURRENT`; pre-M5 data directories are refused (#17).
  A wiped follower must rejoin as a new learner instead of via snapshot under its old ID (#22).
- `pkg/client` uses sessions and write forwarding by default and retries indeterminate writes with the same sequence
  (#15). Linearizable `Get` goes through ReadIndex, not the log (#14).
- The design docs describe the code as built: one gRPC listener per node (`:7000`), the applier inline in the Ready
  loop, `IDENTITY` instead of `META` + `flock`, the real CI jobs, simulator and test layout (#25, #27).

### Fixed
- A leader stopped by SIGTERM under load fell back to an election timeout: the transport was started with the signal
  context, so the catch-up appends of the shutdown handover were dropped. Peer senders now stay up until the
  handover ends (#26).
- docker-compose e2e, broken since M3: the harness proposes encoded commands, and the debug `Propose` RPC rejects
  payloads the state machine cannot apply instead of committing a poison pill (#11).
- A snapshot with an unknown format version was deleted at startup instead of refusing to start; a YAML config with a
  second document (`---`) had it silently ignored (#21).
- Flaky `TestRetryBoundedByDeadline` (#11); `TestLimitBoundaries` timing out under `-race` (#19).

### Removed
- Performance targets, benchmark methodology, the `bench.yml` and `release.yml` workflows, and the 40M-op /
  1,500-fault campaign rules (2026-11-21).

## [0.5.0] - 2026-11-24

M6: membership changes, leadership transfer, data-directory incarnations (#22). Also the M4 checker coverage (#18),
fuzzing and format version checks (#21) and the kind + Chaos Mesh driver (#23), which merged between the M5 and M6
merges. Not yet in it: the rolling-restart test and the SIGTERM handover fix under load (#26).

## [0.4.0] - 2026-11-23

M5: snapshots and log compaction (#17).

## [0.3.0] - 2026-11-23

M4: ReadIndex reads, lease reads (off), sessions, `CompareAndSwap`, paged `Range`, write forwarding, limits (#14, #15,
#19). Also the first M7 pieces: `Cluster.Status`, the observer and Grafana (#12), and the kind / Chaos Mesh
scaffolding (#13).

## [0.2.0] - 2026-11-23

M3: Pebble state machine, basic KV API, client library, Porcupine harness (#9), and the compose e2e fix (#11).

## [0.1.0] - 2026-11-23

M2: WAL, gRPC transport, node loop, observability, 5-node docker-compose cluster and e2e harness (#4, #6, #8).

## [0.1.0-alpha] - 2026-11-23

M1: Raft core and the deterministic simulator (#5, #7).

## [0.0.1] - 2026-11-23

M0: repository skeleton, tooling, protobuf API, PR CI (#2, #3).

[Unreleased]: https://github.com/bibekgupta3333/raftkv-distributed-store/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/bibekgupta3333/raftkv-distributed-store/compare/v0.5.0...v1.0.0
[0.5.0]: https://github.com/bibekgupta3333/raftkv-distributed-store/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/bibekgupta3333/raftkv-distributed-store/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/bibekgupta3333/raftkv-distributed-store/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/bibekgupta3333/raftkv-distributed-store/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/bibekgupta3333/raftkv-distributed-store/compare/v0.1.0-alpha...v0.1.0
[0.1.0-alpha]: https://github.com/bibekgupta3333/raftkv-distributed-store/compare/v0.0.1...v0.1.0-alpha
[0.0.1]: https://github.com/bibekgupta3333/raftkv-distributed-store/releases/tag/v0.0.1
