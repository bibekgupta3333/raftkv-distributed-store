# Changelog

All notable changes to this project are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versioning: [SemVer](https://semver.org/).

## [Unreleased]

### Added
- 2026-11-23: ReadIndex in the Raft core: waits for the leader's no-op, batches concurrent reads into one heartbeat
  round, and lets followers ask the leader through the new `ReadIndex` peer RPC (D-05, D-36) (#14).
- 2026-11-23: STALE `Get` reports the applied index it read at. Optional lease reads (`--lease-reads`) are off by
  default (D-25). New error reason `READ_INDEX_TIMEOUT`. The simulator gained a read workload, READ-1/READ-2 checks
  and a `lease` profile (#14).
- 2026-11-23: `Cluster.Status`, the `raftkv-observer` live UI (D-48), Grafana with a provisioned dashboard in
  compose, and `raftkv_grpc_requests_total` / `raftkv_grpc_request_duration_seconds` metrics (#12).
- 2026-11-23: Kubernetes on kind: Kustomize base and overlay, `/livez` and `/readyz` probes (D-46),
  `-node-id-from-ordinal`, `make kind-up/kind-down`, `make chaos-up/chaos-apply/chaos-clear/chaos-down`, and Chaos
  Mesh PodChaos/NetworkChaos manifests (D-47) (#13).

### Changed
- 2026-11-23: Linearizable `Get` is served by any member through ReadIndex instead of through the log. Followers no
  longer answer `Get` with NOT_LEADER. D-25 lease is now `(ElectionTick-1)*tick*(1-drift)-margin` = 710 ms (#14).
- 2026-11-23: Brought `WBS.md` (100/201 leaves ticked with PR links), `README.md`, `Agent.md` and `memory.md` up
  to date with the merged M0–M3 code.
- 2026-11-22: `raft.proto`: `AppendEntries` request/response gained a `heartbeat` flag (#6); `Command` gained
  `request_id` and a `GetOp` (#9).
- 2026-11-22: Rewrote `README.md` with standard sections: overview, features, tech stack, getting started,
  documentation, contributing, license.
- 2026-11-21: **Re-scoped as a learning project.** Speed is no longer a goal. Milestones are now M0–M7: the old M7
  (performance) is gone and the old M8 is now M7. Most decisions are now Decided; only D-22 (license) is open.
  All docs are shorter. `WBS.md` went from 391 to 201 tasks and the estimate is about 25 weeks.
- 2026-11-21: Renamed `docs/testing-benchmarks.md` to `docs/testing.md`.
- 2026-11-21: Simplified the design: snapshots restart instead of resuming (no `SnapshotProgress`), disk-full
  crashes the node like any other write error, sessions use an explicit `RegisterSession`, and the leader fsyncs
  before replicating.

### Removed
- 2026-11-21: Performance targets, benchmark methodology, the `bench.yml` and `release.yml` workflows, and the
  40M-op / 1,500-fault campaign rules.
- 2026-11-21: Production-only scope: mTLS, release signing, rolling upgrades, multi-zone placement, CI budget.
  Decisions D-13, D-15, D-20, D-37, D-39–D-42, D-44, D-45 removed; their IDs are not reused.

### Added
- 2026-11-22: M3 Pebble state machine (`internal/storage`): Pebble WAL off, `appliedIndex` in the apply batch,
  durable applied index, D-30 truncation bound, recovery by WAL replay, 500-round `kill -9` crash suite (#9).
- 2026-11-22: M3 command envelope (`internal/command`) and KV service (`internal/server`): `Put`, `Delete`, `Get`
  through the log, proposal tracker, `NOT_LEADER` with leader hint (#9).
- 2026-11-22: M3 client library (`pkg/client`) with leader cache, hint following and deadline-bounded retries;
  `raftkvctl put/get/del` and `raftkvctl debug state-hash` (#9).
- 2026-11-22: M3 linearizability harness (`test/linearizability`): history recorder, per-key Porcupine model, fault
  run on an in-process 5-node cluster (#9).
- 2026-11-22: M2 multi-stage Dockerfile, 5-node docker-compose cluster with Prometheus, `make cluster-up/down/logs`,
  and the `test/e2e` harness with a seeded fault driver; `e2e-short` (PR) and `e2e-long` (nightly) CI jobs (#8).
- 2026-11-22: M1 deterministic simulator (`test/sim`) with fault profiles, invariant checker, JSONL traces,
  regression seeds, `make sim`, a 500-seed PR job and a nightly 100,000-seed job (#7).
- 2026-11-22: M2 WAL-backed `raft.Storage` (`internal/walstore`), gRPC peer transport, node driver loop, YAML config,
  node identity file, `raftkvd` wired up, `raftkvctl debug wal-dump`, slog logs, Prometheus metrics, OTel spans (#6).
- 2026-11-22: M1 Raft core (`internal/raft`): `RawNode`, elections with PreVote and CheckQuorum, log replication
  with fast backtracking, proposal limits, Figure 7/8 scenario tests (#5).
- 2026-11-22: M2 segmented WAL (`internal/wal`): CRC32C records, segment rollover, replay with torn-tail repair,
  fault-injecting FS shim, crash matrix, fatal-on-fsync-error, `ReleaseTo` (#4).
- 2026-11-22: M0 protobuf API (`api/raftkv/v1`: kv, cluster, raft) with committed generated code (#3).
- 2026-11-22: M0 repo skeleton, Apache-2.0 license, Makefile, pinned `go tool` directives, golangci-lint with the
  D-24 `time.Now` ban in `internal/raft`, buf config, version-stamped `raftkvd`/`raftkvctl`, PR CI workflow (#2).
- 2026-11-21: Initial design-phase documentation: `README.md`, `docs/design-decisions.md` (decisions D-01–D-24,
  performance targets, open questions), `docs/architecture.md`, `docs/system-design.md`, `docs/api-design.md`,
  `docs/storage-design.md`, `docs/testing-benchmarks.md`, `docs/ci-e2e.md`, `docs/implementation-roadmap.md`.
- 2026-11-21: Project management files: `WBS.md` (implementation checklist), `CHANGELOG.md`, `memory.md`,
  `Agent.md` (AI agent instructions), `CLAUDE.md`.
- 2026-11-21: `docs/simple-guide.md`, a plain-English overview for non-specialists.
- 2026-11-21: `docs/raftkv-in-5-minutes.md`, a one-page summary of the whole design. Added D-48 (read-only observer frontend) and WBS tasks 8.3.11–8.3.13.
- 2026-11-21: Decisions D-25 to D-47, merged from the open questions raised in the topic docs. D-30 (WAL truncation
  invariant) is Decided. D-25 to D-47 amend D-11, D-12, D-23, and D-24.

### Fixed
- 2026-11-23: docker-compose e2e, broken since M3. The harness now proposes encoded Put commands, and the debug
  Propose RPC rejects payloads the state machine cannot apply instead of committing a poison pill (#11).
- 2026-11-23: Flaky `TestRetryBoundedByDeadline` (#11).
- 2026-11-21: Aligned CI job and make-target names across `implementation-roadmap.md`, `WBS.md`, and `ci-e2e.md`.
