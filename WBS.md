# RaftKV Work Breakdown Structure (WBS)

This is the task-level plan for [docs/implementation-roadmap.md](docs/implementation-roadmap.md). Decisions are cited by
ID from [docs/design-decisions.md](docs/design-decisions.md).

## How to use this file

- **Numbering follows the milestones.** Section `N` is milestone `MN` (section `0` = M0 … section `8` = M8). Leaves are
  numbered `N.x.y`. Never renumber an item once it has been ticked or referenced in a PR. Add new items at the end of a
  group (e.g. `2.3.9`).
- **Each leaf is ≤ ~1 engineer-day and independently verifiable.** The text after `→` says how to verify it. If a leaf
  turns out bigger than a day, split it in the PR that starts it.
- **Tick the box in the same PR that does the work**, and append the PR number: `- [x] 2.1.3 ... (#42)`. A box ticked
  without a PR link (or a commit SHA for doc-only work) doesn't count.
- **Decision tasks** (`N.0.x`) must be ticked before any other leaf in that milestone is started (the rule at the top of
  design-decisions.md). Ticking one means design-decisions.md was updated to `Decided` in that PR.
- **Each milestone ends with an exit-review group.** It is ticked only when every exit criterion in the roadmap has a
  linked CI run or artifact.
- **Update the progress table** below in the same PR (count ticked leaves per milestone). The `Leaves` column counts
  only leaf checkboxes (3-level items).
- Deferred items move to the `post-1.0` section at the bottom with a one-line reason. They are never deleted.

## Progress summary

| Milestone | Theme | Done / Leaves | Est. weeks | Status |
|-----------|-------|---------------|------------|--------|
| M0 | Repo, tooling, proto skeleton | 0/30 | 1.5 | Not started |
| M1 | Raft core + deterministic sim | 0/53 | 5.0 | Not started |
| M2 | WAL + gRPC transport + multi-process cluster | 0/59 | 4.0 | Not started |
| M3 | Pebble state machine + basic KV + baseline | 0/41 | 3.0 | Not started |
| M4 | Linearizable reads, sessions, full KV API | 0/42 | 4.0 | Not started |
| M5 | Snapshots + follower recovery | 0/33 | 3.5 | Not started |
| M6 | Membership changes + leadership transfer | 0/32 | 3.0 | Not started |
| M7 | D-13 optimizations + benchmarks | 0/35 | 3.5 | Not started |
| M8 | K8s, mTLS, observability, chaos, v1.0 | 0/66 | 5.0 | Not started |
| **Total** | | **0/391** | **32.5** | |

> Leaf counts are greater than the day estimates because many leaves (decision sign-offs, doc updates, config files)
> take well under a day.

---

## 0. M0 — Repository, tooling, proto skeleton

- [ ] **0.0 Decision resolution**
  - [ ] 0.0.1 Resolve D-21: pick module owner and Go toolchain version, update design-decisions.md → entry shows `Decided` with module path
  - [ ] 0.0.2 Resolve D-22: confirm license (Apache-2.0 recommended) → entry `Decided`
  - [ ] 0.0.3 Sign off D-07 (single Raft group) → entry `Decided`
  - [ ] 0.0.4 Sign off D-01 (own Raft core, swappable interface) → entry `Decided`
  - [ ] 0.0.5 Decide whether generated proto code is committed or generated in CI (roadmap §9 Q4) → recorded as new D-xx
- [ ] **0.1 Repository skeleton**
  - [ ] 0.1.1 `go mod init` with D-21 path, `go` and `toolchain` directives → `go build ./...` passes
  - [ ] 0.1.2 Create directory layout from design-decisions.md (with `doc.go` per package) → `go vet ./...` passes, tree matches layout
  - [ ] 0.1.3 Add LICENSE and SPDX header convention → license check script passes on all `.go` files
  - [ ] 0.1.4 README with project summary, status badge, and links to docs → renders on GitHub
  - [ ] 0.1.5 CONTRIBUTING.md (branching, commit style, WBS ticking rule, DCO/CLA if any) → reviewed
  - [ ] 0.1.6 Create `CHANGELOG.md` (Keep a Changelog, `Unreleased` section) and `memory.md` (project memory template: decisions, gotchas, how-tos) → files exist with headings
  - [ ] 0.1.7 `.gitignore`, `.editorconfig`, CODEOWNERS, PR template with "WBS items ticked" checklist → PR template shows on a test PR
- [ ] **0.2 Tooling**
  - [ ] 0.2.1 Pin tool versions (golangci-lint, buf, protoc-gen-go, protoc-gen-go-grpc) via `go tool` directives → `make tools` installs exact versions
  - [ ] 0.2.2 Makefile targets: `all fmt fmt-check lint build test race proto proto-lint proto-breaking clean help` → `make help` lists each, `make all` passes
  - [ ] 0.2.3 `.golangci.yml` (govet, staticcheck, errcheck, gosec, revive, gocritic, errorlint, exhaustive, forbidigo for `time.Now` in `internal/raft`) → lint passes, a planted `time.Now()` in raft fails
  - [ ] 0.2.4 `.pre-commit-config.yaml` (gofumpt, goimports, golangci-lint fast, buf lint, trailing whitespace, markdownlint) → `pre-commit run --all-files` passes
  - [ ] 0.2.5 Version stamping via `-ldflags` (`version`, `gitSHA`, `buildDate`) → `raftkvd --version` prints all three
  - [ ] 0.2.6 `cmd/raftkvd`, `cmd/raftkvctl`, `cmd/raftkv-bench` stub mains with flag parsing scaffold → each binary builds and prints help
- [ ] **0.3 Protobuf skeleton**
  - [ ] 0.3.1 `buf.yaml` (lint STANDARD, breaking FILE) and `buf.gen.yaml` → `buf lint` passes
  - [ ] 0.3.2 `api/raftkv/v1/kv.proto`: KV service and messages for Put/Get/Delete/CompareAndSwap/Range, `Consistency` enum, session fields (D-09, D-10) per api-design.md → generates and compiles
  - [ ] 0.3.3 `api/raftkv/v1/cluster.proto`: AddLearner, PromoteLearner, RemoveMember, TransferLeadership, ListMembers (D-23) → generates and compiles
  - [ ] 0.3.4 `api/raftkv/v1/raft.proto`: Append bidi stream, Vote, PreVote, InstallSnapshot client stream, TimeoutNow, `Entry`, `HardState`, `SnapshotMeta` (D-12, D-14) → generates and compiles
  - [ ] 0.3.5 `make proto` wired with a drift check (`git diff --exit-code` after generate if committed) → CI fails on stale generated code
- [ ] **0.4 CI (per docs/ci-e2e.md)**
  - [ ] 0.4.1 `.github/workflows/pr.yml`: checkout, Go cache, `make fmt-check lint build test race` → green on default branch
  - [ ] 0.4.2 Add `buf lint` and `buf breaking --against` default branch to pr.yml → throwaway PR renaming a field fails
  - [ ] 0.4.3 Add `govulncheck` job to pr.yml → job runs and passes
  - [ ] 0.4.4 Enable branch protection with required checks, Dependabot/Renovate config for Go modules and Actions → settings screenshot/link in PR
- [ ] **0.9 M0 exit review & docs**
  - [ ] 0.9.1 Verify each M0 exit criterion (roadmap §4 M0) and link the evidence in the milestone PR → all 5 criteria linked
  - [ ] 0.9.2 Update CHANGELOG.md (`Unreleased`: tooling, protos) and memory.md (decisions D-21/D-22, tool versions) → diff reviewed
  - [ ] 0.9.3 Tag `v0.0.1` → tag exists

---

## 1. M1 — Raft core + deterministic simulation

- [ ] **1.0 Decision resolution**
  - [ ] 1.0.1 Confirm D-08 parameters (1000–2000 ms election, 100 ms heartbeat, expressed in ticks) → entry `Decided` with tick mapping
  - [ ] 1.0.2 Sign off D-18 simulation scope (fault types, invariant list) → entry `Decided`
  - [ ] 1.0.3 Confirm D-24 (logical tick) and document tick length → entry `Decided`
- [ ] **1.1 Core types & interfaces**
  - [ ] 1.1.1 Define `Entry`, `HardState`, `Message` (or reuse proto types), `StateType`, `Config` with validation → unit tests for `Config.Validate`
  - [ ] 1.1.2 Define `Storage` interface (Entries, Term, FirstIndex, LastIndex, InitialState, Snapshot stub) and `MemoryStorage` → table tests
  - [ ] 1.1.3 Define `Ready`, `Advance()` contract and `RawNode` API (`Tick`, `Step`, `Propose`, `Ready`, `HasReady`, `Advance`) → godoc plus compile-only interface test
  - [ ] 1.1.4 Define the `Raft` interface wrapper so `etcd/raft` could be swapped in (D-01) → interface doc plus adapter stub compiles
  - [ ] 1.1.5 `raftLog` structure (unstable + stable, committed, applied) with invariants checked in debug builds → unit tests
- [ ] **1.2 Leader election**
  - [ ] 1.2.1 Follower → candidate on randomized election timeout (logical ticks, seeded RNG) → test: timeout distribution within bounds
  - [ ] 1.2.2 RequestVote handling: term check, log up-to-date check, persisted `votedFor` → table tests for every branch
  - [ ] 1.2.3 Candidate wins on majority, steps down on higher term or on AppendEntries from a valid leader → unit tests
  - [ ] 1.2.4 Leader appends a no-op entry on election → test: first entry of a new term is a no-op
  - [ ] 1.2.5 PreVote phase (D-08): pre-candidate does not bump term, votes granted without persisting → test: rejoining partitioned node doesn't raise term
  - [ ] 1.2.6 CheckQuorum (D-08): leader steps down if no majority contact within an election timeout. Followers ignore votes while they have a live leader (leader lease for votes) → unit tests
  - [ ] 1.2.7 Heartbeats every heartbeat tick → test: follower doesn't time out under heartbeats
- [ ] **1.3 Log replication**
  - [ ] 1.3.1 AppendEntries send with prevIndex/prevTerm and entries from `Progress.next` → unit test
  - [ ] 1.3.2 Follower consistency check, conflict truncation, append → tests incl. Figure 7 logs
  - [ ] 1.3.3 Reject with hint (conflict term + first index of that term) and leader fast backtracking → test: backtracks a 1,000-entry divergence in ≤ 3 round trips
  - [ ] 1.3.4 `Progress` tracking (match, next, Probe/Replicate states, paused) → unit tests
  - [ ] 1.3.5 Commit rule: advance commit only for current-term entries by quorum match → Figure 8 test passes
  - [ ] 1.3.6 Follower commit index update from leader commit (min with last new index) → unit test
  - [ ] 1.3.7 Committed entries exposed in `Ready`, applied index advanced on `Advance` → unit test
- [ ] **1.4 Proposals & client-facing core behavior**
  - [ ] 1.4.1 `Propose` on leader appends to unstable log, `ErrProposalDropped` when no leader → unit tests
  - [ ] 1.4.2 Follower proposal: return `NotLeader{leaderID}` hint (no forwarding in v1, document the choice) → unit test plus doc note
  - [ ] 1.4.3 Max uncommitted entries size limit (backpressure) → test: proposals rejected past limit
  - [ ] 1.4.4 Entry batch size limit per message (D-11 4 MiB) → unit test
- [ ] **1.5 Deterministic simulator (test/sim)**
  - [ ] 1.5.1 Seeded PRNG, virtual clock, and event loop driving N `RawNode`s → same seed gives identical trace (hash compare)
  - [ ] 1.5.2 Virtual network: per-link delay distribution, drop, duplicate, reorder → unit tests on network model
  - [ ] 1.5.3 Partition scheduler (random bipartitions, isolate leader, heal) → scenario test
  - [ ] 1.5.4 Crash/restart model: lose unpersisted state, keep persisted HardState and stable log → scenario test
  - [ ] 1.5.5 Client workload generator (proposals at random times to random nodes) → proposals visible in trace
  - [ ] 1.5.6 Reference applied-sequence model for State Machine Safety → test detects a planted divergence
- [ ] **1.6 Invariant checker**
  - [ ] 1.6.1 Election Safety (≤ 1 leader per term) → planted bug detected
  - [ ] 1.6.2 Log Matching → planted bug detected
  - [ ] 1.6.3 Leader Completeness and Leader Append-Only → planted bug detected
  - [ ] 1.6.4 State Machine Safety and commit-index monotonicity → planted bug detected
  - [ ] 1.6.5 Liveness checker (after heal: leader within 10 election timeouts, all post-heal proposals commit) → reports per-seed result
- [ ] **1.7 Simulator CLI & CI**
  - [ ] 1.7.1 `go test ./test/sim -seeds=N -start=S -faults=profile` flags, failing seed printed with repro command → manual check
  - [ ] 1.7.2 Trace dump on failure (JSONL) and a trace pretty-printer → failing seed produces readable trace
  - [ ] 1.7.3 Regression corpus `test/sim/regressions/` replayed on every PR → planted regression seed runs in CI
  - [ ] 1.7.4 pr.yml: run 500 seeds (≤ 3 min) → CI timing recorded
  - [ ] 1.7.5 Create `.github/workflows/nightly.yml` with sim job ≥ 100,000 seeds, sharded across matrix → first nightly green
  - [ ] 1.7.6 Branch-coverage report for rare paths (conflict truncation, PreVote rejection, CheckQuorum step-down) → each hit in ≥ 1% of seeds
- [ ] **1.8 Scenario tests**
  - [ ] 1.8.1 Raft paper Figure 7 divergent logs converge → test passes
  - [ ] 1.8.2 Figure 8 commit hazard → test passes
  - [ ] 1.8.3 Disruptive rejoin with PreVote on vs off (off reproduces term inflation) → both behaviors asserted
  - [ ] 1.8.4 Leader isolation with CheckQuorum → step-down asserted
  - [ ] 1.8.5 TLA+ (`raft.tla`) checklist review: map each spec action to code and tests → checklist committed to docs
- [ ] **1.9 M1 exit review & docs**
  - [ ] 1.9.1 Run 10,000 consecutive seeds with full fault profile, attach result → 0 violations
  - [ ] 1.9.2 Coverage ≥ 85% for `internal/raft`, race clean → CI artifact linked
  - [ ] 1.9.3 Update architecture.md / system-design.md Raft core sections to match the implementation → diff reviewed
  - [ ] 1.9.4 Update CHANGELOG.md and memory.md (core API, sim usage, gotchas found) → diff reviewed
  - [ ] 1.9.5 Tag `v0.1.0-alpha` → tag exists

---

## 2. M2 — WAL + gRPC transport + real multi-process cluster

- [ ] **2.0 Decision resolution**
  - [ ] 2.0.1 Sign off D-03 (segmented WAL, Pebble WAL off) → `Decided`
  - [ ] 2.0.2 Sign off D-12 details (window size by count and bytes) and D-19 (e2e CI) → `Decided`
  - [ ] 2.0.3 Decide `--no-fsync` dev flag policy (roadmap §9 Q10) → new D-xx recorded
  - [ ] 2.0.4 Kick off D-20 (reference hardware) proposal with cost estimate, must be resolved before M3 exit → proposal PR open
- [ ] **2.1 WAL (internal/wal)**
  - [ ] 2.1.1 Record format `[len u32|type u8|crc32c u32|payload]` encoder/decoder → round-trip and fuzz seed tests
  - [ ] 2.1.2 Segment file naming (`%016x-%016x.wal` seq-firstIndex), create with preallocation, directory fsync → unit test
  - [ ] 2.1.3 Append entries and HardState records, segment rollover at 64 MiB (configurable) → test: rollover at boundary
  - [ ] 2.1.4 Group-commit `Sync()` covering all appended-but-unsynced records, linux `fdatasync`, darwin `F_FULLFSYNC` → build-tagged tests
  - [ ] 2.1.5 Open/replay: read all segments, rebuild entries plus last HardState, handle overwritten (truncated-by-conflict) entries → replay tests
  - [ ] 2.1.6 Tail repair: truncate a partial or CRC-failing **final** record, hard error on mid-log corruption → tests
  - [ ] 2.1.7 Fault-injecting filesystem shim (truncate/corrupt at offset, fail fsync) → shim unit tests
  - [ ] 2.1.8 Crash matrix: every byte offset of last 2 records × 1,000 random logs → 0 silent corruptions
  - [ ] 2.1.9 Fsync failure is fatal (panic/exit, never retried) → test asserts process exit path
  - [ ] 2.1.10 Truncate-front API (`ReleaseTo(index)`) deleting whole segments, used in M5 → unit test
  - [ ] 2.1.11 WAL implements Raft `Storage` (with in-memory index of entry offsets) → Storage conformance test suite passes
  - [ ] 2.1.12 Microbenchmarks: append+sync throughput by record size → `go test -bench` output committed
- [ ] **2.2 Transport (internal/transport)**
  - [ ] 2.2.1 gRPC server for `Raft` service with message-size limits from D-11 → server starts, rejects oversize
  - [ ] 2.2.2 Outbound per-peer bidi `Append` stream with send queue → test with in-process peers
  - [ ] 2.2.3 Bounded in-flight window (count and bytes) with backpressure to the node loop → unit test
  - [ ] 2.2.4 Reconnect with exponential backoff and jitter, report `Unreachable` to Raft core → test: peer restart reconnects
  - [ ] 2.2.5 Unary Vote/PreVote RPCs → test
  - [ ] 2.2.6 Static peer address book from config (ID → addr) with DNS re-resolution on reconnect → test with changing address
  - [ ] 2.2.7 `credentials.TransportCredentials` injection point (insecure default, ready for D-15) → compile plus test with insecure creds
  - [ ] 2.2.8 Transport fault hooks for tests (drop/delay per peer) → used by an integration test
- [ ] **2.3 Node loop & server**
  - [ ] 2.3.1 Node goroutine: tick timer → `Tick`, inbound → `Step`, `Ready` → persist, send, apply, `Advance` (unoptimized order) → integration test with 3 in-process nodes
  - [ ] 2.3.2 Temporary in-memory applier plus debug `Propose` RPC behind `debug` build tag → 5 nodes commit 10k entries
  - [ ] 2.3.3 Config: file (YAML) plus flags plus env with precedence, validation errors → table tests
  - [ ] 2.3.4 Bootstrap: initial cluster from config, node ID persisted in data dir, refuse ID mismatch → tests
  - [ ] 2.3.5 Graceful shutdown on SIGTERM/SIGINT (stop accepting, flush WAL, close streams) → test
  - [ ] 2.3.6 Health endpoints `/healthz` (loop alive) and `/readyz` (has leader and caught up) → HTTP tests
  - [ ] 2.3.7 Log-dump debug tool (`raftkvctl debug wal-dump`) for convergence checks → prints entries from a data dir
- [ ] **2.4 Observability (D-17)**
  - [ ] 2.4.1 `log/slog` JSON handler, level flag, standard fields (node_id, term, component) → log sample in test
  - [ ] 2.4.2 `internal/metrics` registry and HTTP `/metrics` server → curl returns metrics
  - [ ] 2.4.3 Raft metrics from roadmap §6 M2 row (term, state, leader changes, commit/applied index, proposals, elections) → each asserted in a test
  - [ ] 2.4.4 WAL metrics (fsync histogram, bytes, segments) → asserted
  - [ ] 2.4.5 Transport metrics (sent, failures, reconnects, inflight, follower match index) → asserted
  - [ ] 2.4.6 gRPC server metrics interceptor plus Go runtime/process collectors → present on `/metrics`
  - [ ] 2.4.7 OTel tracer provider, OTLP exporter config, sampling ratio flag → spans exported to an in-memory exporter in test
  - [ ] 2.4.8 Spans: `raft.propose`, `wal.append`, `wal.fsync`, `raft.commit`, `apply` → test asserts span tree
- [ ] **2.5 Packaging & local cluster**
  - [ ] 2.5.1 Multi-stage Dockerfile (distroless, non-root, static binary) → image builds, runs `--version`
  - [ ] 2.5.2 `deploy/compose/docker-compose.yml` 5 nodes plus Prometheus plus Jaeger/Tempo → `make cluster-up` gets a leader
  - [ ] 2.5.3 Make targets `cluster-up`, `cluster-down`, `cluster-logs` → documented in README
- [ ] **2.6 E2E harness (test/e2e)**
  - [ ] 2.6.1 Go e2e framework: start/stop compose, discover leader, wait-for helpers → smoke test passes
  - [ ] 2.6.2 Fault driver: `kill -9`, restart, SIGSTOP/SIGCONT → each fault unit-tested on one container
  - [ ] 2.6.3 Fault driver: iptables partition (symmetric, asymmetric) and heal → partition test passes
  - [ ] 2.6.4 Fault driver: `tc netem` delay/loss → latency visible in metrics
  - [ ] 2.6.5 Seeded fault schedule plus fault timeline log aligned with history → timeline artifact uploaded
  - [ ] 2.6.6 Test: leader election within 3 s of start (100 starts, p99) → passes
  - [ ] 2.6.7 Test: leader kill → new leader accepts proposals within 3 s (200 kills, p99) → passes
  - [ ] 2.6.8 Test: minority partition stops committing, logs converge after heal (wal-dump compare) → passes
  - [ ] 2.6.9 Test: 1,000 kill -9 iterations, no committed entry lost → passes
  - [ ] 2.6.10 pr.yml `e2e-short` job (≤ 10 min) → green
  - [ ] 2.6.11 nightly.yml `e2e-long` job (≥ 60 min mixed faults) → 5 consecutive green nights
  - [ ] 2.6.12 Flaky-test quarantine label and process documented in ci-e2e.md → doc diff
- [ ] **2.9 M2 exit review & docs**
  - [ ] 2.9.1 Re-run sim 10,000 seeds on M2 code → 0 violations
  - [ ] 2.9.2 Link evidence for all 8 M2 exit criteria → linked
  - [ ] 2.9.3 Update storage-design.md (WAL format as built), system-design.md (node loop, metrics), ci-e2e.md (jobs) → diffs reviewed
  - [ ] 2.9.4 Update CHANGELOG.md and memory.md (WAL format, fsync gotchas, e2e tips) → diff reviewed
  - [ ] 2.9.5 Tag `v0.1.0` → tag exists

---

## 3. M3 — Pebble state machine + basic KV (unoptimized baseline)

- [ ] **3.0 Decision resolution**
  - [ ] 3.0.1 Confirm D-02 and D-04 implementation notes (batch layout, key prefixes) in storage-design.md → reviewed
  - [ ] 3.0.2 **Resolve D-20** reference hardware, provision environment, document fingerprint script → entry `Decided`, env reachable
- [ ] **3.1 Pebble state machine (internal/storage)**
  - [ ] 3.1.1 Open Pebble with WAL disabled, tuned options documented → opens and closes cleanly
  - [ ] 3.1.2 Key encoding: prefixes for user data, session table (reserved), raft meta, with versioned schema key → encoding tests
  - [ ] 3.1.3 Apply batch: decode entries, apply Put/Delete, write `appliedIndex` in the same batch (D-04) → unit test
  - [ ] 3.1.4 Flush event listener maintaining `DurableAppliedIndex()` → test: index advances only after flush
  - [ ] 3.1.5 Gate WAL `ReleaseTo` on `DurableAppliedIndex` → test: truncation never passes it
  - [ ] 3.1.6 Recovery: read `appliedIndex`, replay `(applied, commit]` from WAL → crash test
  - [ ] 3.1.7 Crash-recovery suite: 1,000 kill -9 during apply, full-state hash vs reference model → 0 mismatches
  - [ ] 3.1.8 Truncation-safety crash test with forced unflushed memtables → exact recovery
  - [ ] 3.1.9 State hash utility (`raftkvctl debug state-hash`) → same hash across replicas at equal applied index
- [ ] **3.2 Command encoding & apply results**
  - [ ] 3.2.1 Command envelope proto (type, payload, request ID) in log entries → round-trip test
  - [ ] 3.2.2 Proposal tracking: map request ID → waiter, resolved on apply, cancelled on leader loss → unit tests
  - [ ] 3.2.3 Leader loss / term change fails pending proposals with retryable error → test
- [ ] **3.3 KV service (Put/Get/Delete)**
  - [ ] 3.3.1 Put handler (validate, propose, await apply) → integration test
  - [ ] 3.3.2 Delete handler → integration test
  - [ ] 3.3.3 Get through the log (linearizable, slow path) → integration test
  - [ ] 3.3.4 `NOT_LEADER` error with leader hint in status details (api-design.md) → test
  - [ ] 3.3.5 KV metrics and server spans (roadmap §6 M3 row) → asserted
- [ ] **3.4 Client library (pkg/client)**
  - [ ] 3.4.1 Connection pool to all endpoints, leader cache, redirect on `NOT_LEADER` → unit test with fake servers
  - [ ] 3.4.2 Retry policy (retryable codes, backoff, deadline-bound) → unit tests
  - [ ] 3.4.3 OTel client interceptor for trace propagation → span linking test
  - [ ] 3.4.4 `raftkvctl put/get/del` using the client library → manual plus e2e smoke
- [ ] **3.5 Linearizability harness (test/linearizability)**
  - [ ] 3.5.1 History recorder (invoke/return timestamps, client ID, op, result, unknown outcome on timeout) → unit test
  - [ ] 3.5.2 Porcupine KV model (Put/Get/Delete), per-key partition → model unit tests incl. known-bad history detected
  - [ ] 3.5.3 Checker runner with per-history timeout (timeout = fail), HTML visualization artifact on failure → CI artifact
  - [ ] 3.5.4 Wire recorder into e2e workloads with faults (leader kill, follower kill, partition) → e2e run produces checked histories
  - [ ] 3.5.5 Start the correctness ledger `test/chaos/ledger.csv` (ops, faults by type, violations, SHA, run link) → first rows from nightly
  - [ ] 3.5.6 Run ≥ 1M ops across ≥ 100 histories with faults → 0 violations
- [ ] **3.6 Load generator (cmd/raftkv-bench)**
  - [ ] 3.6.1 Closed-loop and open-loop (fixed rate) modes, concurrency flag → runs against compose cluster
  - [ ] 3.6.2 Workloads: write-only, read-only, mixed ratio, value size, key count, uniform/zipf → flags tested
  - [ ] 3.6.3 HDR histogram, JSON output (throughput, p50/p99/p999, errors), warmup exclusion → schema documented in testing-benchmarks.md
  - [ ] 3.6.4 Hardware/config fingerprint embedded in results (CPU, disk, kernel, git SHA, flags) → fields present
- [ ] **3.7 Unoptimized baseline**
  - [ ] 3.7.1 `.github/workflows/bench.yml` (manual dispatch plus weekly, self-hosted reference runner) per ci-e2e.md → first run succeeds
  - [ ] 3.7.2 Run baseline on D-20 hardware, commit `bench/results/m3-baseline.json`, tag `m3-baseline` → file committed
  - [ ] 3.7.3 Noise check: 3 runs within ±10%, record variance → result in testing-benchmarks.md
  - [ ] 3.7.4 Smoke compare vs targets. If < 20% of target, raise risk R-04 at exit review → note in exit review
- [ ] **3.9 M3 exit review & docs**
  - [ ] 3.9.1 Link evidence for all 6 M3 exit criteria → linked
  - [ ] 3.9.2 Update storage-design.md (key schema, durability contract), api-design.md (implemented RPCs), testing-benchmarks.md (baseline method) → diffs reviewed
  - [ ] 3.9.3 Update CHANGELOG.md and memory.md → diff reviewed
  - [ ] 3.9.4 Tag `v0.2.0` → tag exists

---

## 4. M4 — Linearizable reads, client sessions, full KV API

- [ ] **4.0 Decision resolution**
  - [ ] 4.0.1 Sign off D-05 (ReadIndex default, lease flag, follower reads, STALE) → `Decided`
  - [ ] 4.0.2 Decide default `max_clock_drift` and time-sync requirement (roadmap §9 Q1) → new D-xx
  - [ ] 4.0.3 Sign off D-09 (sessions, TTL 10 min) and decide session cap (roadmap §9 Q2) → `Decided` plus new D-xx
  - [ ] 4.0.4 Sign off D-10 and D-11 → `Decided`
- [ ] **4.1 ReadIndex (Raft core)**
  - [ ] 4.1.1 `ReadIndex(ctx)` request in core: record commit index, require current-term commit → unit test
  - [ ] 4.1.2 Leadership confirmation via heartbeat quorum, batching concurrent requests per round → test: ≤ 1 round per heartbeat interval plus 1 under 64 readers
  - [ ] 4.1.3 `ReadStates` in `Ready` → unit test
  - [ ] 4.1.4 Follower ReadIndex: forward to leader, receive index → sim test
  - [ ] 4.1.5 Sim scenario: stale leader (partitioned) never completes a ReadIndex after a new leader is elected → 10,000 seeds pass
  - [ ] 4.1.6 Sim scenario: read immediately after election waits for the no-op commit → passes
- [ ] **4.2 Read path (internal/server)**
  - [ ] 4.2.1 Wait-for-apply (`appliedIndex ≥ readIndex`) with notification, deadline-aware → unit test
  - [ ] 4.2.2 Serve `Get` via ReadIndex on leader → e2e test
  - [ ] 4.2.3 Serve `Get` via ReadIndex on followers → e2e test
  - [ ] 4.2.4 `consistency=STALE` local reads returning applied index → test
  - [ ] 4.2.5 Lease reads behind flag: lease = election timeout − drift, monotonic clock, refuse to start without drift config → unit tests
  - [ ] 4.2.6 Lease sim: violation found when drift exceeds bound, none within bound → both asserted
  - [ ] 4.2.7 Read metrics and spans (roadmap §6 M4 row) → asserted
- [ ] **4.3 Client sessions (D-09)**
  - [ ] 4.3.1 `RegisterClient` through the log, session record in Pebble session prefix → integration test
  - [ ] 4.3.2 Dedup on apply: `(client_id, seq)` check, cached response, update in same batch → unit test
  - [ ] 4.3.3 Leader-assigned timestamp in each entry envelope, used for TTL → test: replicas agree
  - [ ] 4.3.4 TTL expiry on apply (deterministic sweep), `SESSION_EXPIRED` error → test
  - [ ] 4.3.5 Session cap enforcement → test
  - [ ] 4.3.6 Client library: auto-register, monotonic sequence, retry with same seq → unit test
  - [ ] 4.3.7 Exactly-once e2e: 1M writes with 1–3 retries after random drops, counter model → 0 duplicates
  - [ ] 4.3.8 Session determinism sim: 10,000 seeds with TTL crossings, identical session table hash → passes
  - [ ] 4.3.9 Session metrics (`sessions_active`, duplicates, expired) → asserted
- [ ] **4.4 Full KV API (D-10)**
  - [ ] 4.4.1 `CompareAndSwap` (expected value/version, absent-key semantics per api-design.md) → unit plus e2e tests
  - [ ] 4.4.2 `Range` prefix and [start, end) scan with limit and continuation token → tests incl. pagination across writes
  - [ ] 4.4.3 Range consistency modes (linearizable via ReadIndex, STALE) → tests
  - [ ] 4.4.4 Per-key version/revision in responses if specified in api-design.md → tests
  - [ ] 4.4.5 `raftkvctl cas/range` → e2e smoke
- [ ] **4.5 Limits & errors (D-11)**
  - [ ] 4.5.1 Key ≤ 4 KiB, value ≤ 1 MiB validation before propose → boundary tests (limit ok, limit+1 rejected)
  - [ ] 4.5.2 Entry batch ≤ 4 MiB, Range page ≤ 1,000 → boundary tests
  - [ ] 4.5.3 gRPC status codes and error details per api-design.md, deadline propagation → table tests
  - [ ] 4.5.4 `requests_rejected_total{reason}` metric → asserted
- [ ] **4.6 Linearizability coverage**
  - [ ] 4.6.1 Porcupine models for CAS and Range (Range checked as multi-key read, or per-key projections) → known-bad histories detected
  - [ ] 4.6.2 e2e workloads mixing Put/Get/Delete/CAS/Range with leader/follower ReadIndex → runs in nightly
  - [ ] 4.6.3 Run ≥ 5M ops with ≥ 300 faults (leader kill, SIGSTOP, partition), update ledger → 0 violations
- [ ] **4.9 M4 exit review & docs**
  - [ ] 4.9.1 Link evidence for all 8 M4 exit criteria → linked
  - [ ] 4.9.2 Freeze API: tag `v0.3.0`, make `buf breaking` against it a required check → check required
  - [ ] 4.9.3 Update api-design.md (final v1 surface), system-design.md (read path, sessions) → diffs reviewed
  - [ ] 4.9.4 Update CHANGELOG.md and memory.md → diff reviewed

---

## 5. M5 — Snapshots + follower recovery

- [ ] **5.0 Decision resolution**
  - [ ] 5.0.1 Sign off D-14 (checkpoint, triggers, 1 MiB chunks, resume) → `Decided`
  - [ ] 5.0.2 Sign off D-06 (needed before M6 starts, roadmap §1) → `Decided` or re-estimate M6
- [ ] **5.1 Raft core snapshot support**
  - [ ] 5.1.1 `Storage.Snapshot()` / `ApplySnapshot`, log compaction index in `raftLog` → unit tests
  - [ ] 5.1.2 Leader sends snapshot when `next < firstIndex`, Progress Snapshot state, pause and resume replication → unit tests
  - [ ] 5.1.3 Follower handles snapshot message (reject stale, reset log, update commit) → unit tests
  - [ ] 5.1.4 Sim: snapshot support with low threshold plus crash points during install → 10,000 seeds pass
- [ ] **5.2 Snapshot creation (internal/snapshot)**
  - [ ] 5.2.1 Metadata format `{index, term, membership, format_version, checksum}` → round-trip test
  - [ ] 5.2.2 Pebble checkpoint taken on the apply goroutine between batches so metadata index equals `appliedIndex` → test asserts equality
  - [ ] 5.2.3 Triggers: every 100k entries or 512 MiB WAL (configurable) → test with small thresholds
  - [ ] 5.2.4 Free-space precheck (≥ 2× estimated size), clean failure plus metric → test with fake fs
  - [ ] 5.2.5 Retention (keep last 2), orphan temp dir cleanup on start → test
- [ ] **5.3 WAL compaction**
  - [ ] 5.3.1 Release segments below `min(snapshotIndex, DurableAppliedIndex) − retentionTail` → unit test
  - [ ] 5.3.2 Retention tail config for slow followers → test: follower within tail catches up from log, not snapshot
  - [ ] 5.3.3 2-hour sustained-write run, WAL disk bounded (target ≤ 2× (512 MiB + tail)) → result recorded
- [ ] **5.4 Snapshot transfer**
  - [ ] 5.4.1 Sender: `InstallSnapshot` client stream, 1 MiB chunks, per-chunk CRC → integration test
  - [ ] 5.4.2 Receiver: stage chunks to temp dir, track acked offset → integration test
  - [ ] 5.4.3 Resume from last acked offset when the snapshot ID is unchanged → test with mid-transfer kill (100 iterations)
  - [ ] 5.4.4 Bandwidth throttle and ≤ 1 concurrent send per follower → test
  - [ ] 5.4.5 Whole-snapshot checksum verification before install → corrupted chunk rejected
- [ ] **5.5 Snapshot install**
  - [ ] 5.5.1 Close Pebble, atomic directory swap (rename plus dir fsync), reopen → test
  - [ ] 5.5.2 Reset WAL to snapshot index, persist marker → test
  - [ ] 5.5.3 Install step journal for crash safety → kill -9 at each step, always old-or-new state
  - [ ] 5.5.4 Session table restored from snapshot, exactly-once test across snapshot boundary → passes
- [ ] **5.6 Recovery scenarios (e2e)**
  - [ ] 5.6.1 Wiped follower data dir rejoins via snapshot, state hash matches (50 runs) → 100% pass
  - [ ] 5.6.2 Long-partitioned follower catches up past compaction → passes
  - [ ] 5.6.3 Add follower-wipe fault to e2e fault driver and nightly → nightly includes it
  - [ ] 5.6.4 e2e linearizability ≥ 2M ops with wipe faults, update ledger → 0 violations
- [ ] **5.7 Observability**
  - [ ] 5.7.1 Snapshot and WAL-compaction metrics (roadmap §6 M5 row) → asserted
  - [ ] 5.7.2 Spans `snapshot.create/send/install` → asserted
- [ ] **5.9 M5 exit review & docs**
  - [ ] 5.9.1 Link evidence for all 7 M5 exit criteria → linked
  - [ ] 5.9.2 Update storage-design.md (snapshot format, compaction), runbook "follower wiped disk" → diff reviewed
  - [ ] 5.9.3 Update CHANGELOG.md and memory.md → diff reviewed
  - [ ] 5.9.4 Tag `v0.4.0` → tag exists

---

## 6. M6 — Membership changes + leadership transfer

- [ ] **6.0 Decision resolution**
  - [ ] 6.0.1 Confirm D-06 outcome is reflected in the plan (single-server) → plan unchanged or re-estimated
  - [ ] 6.0.2 Sign off D-23 (Cluster admin service) → `Decided`
  - [ ] 6.0.3 Confirm D-15 resolution status (must be Decided by M6 exit, risk R-12) → status noted
- [ ] **6.1 Raft core membership**
  - [ ] 6.1.1 `ConfChange` entry type (add learner, promote, remove) with versioned encoding → round-trip test
  - [ ] 6.1.2 Config takes effect on append, rollback on truncation → unit tests
  - [ ] 6.1.3 One uncommitted config change at a time, first change only after a current-term commit (dissertation bug fix) → unit test plus sim reproducing the bug without the fix
  - [ ] 6.1.4 Learner role: replicated, non-voting, excluded from quorum and elections → unit tests
  - [ ] 6.1.5 Leader removal: step down after committing own removal → unit test
  - [ ] 6.1.6 Removed node stops campaigning and shuts down on seeing its removal → sim test
  - [ ] 6.1.7 Membership persisted in HardState/WAL and snapshot metadata → restart test
- [ ] **6.2 Leadership transfer**
  - [ ] 6.2.1 `TransferLeadership(target)`: stop proposals, catch up target, send `TimeoutNow` → unit test
  - [ ] 6.2.2 Abort after one election timeout, resume proposals → unit test
  - [ ] 6.2.3 Lease invalidated before transfer (if lease reads enabled) → test
  - [ ] 6.2.4 SIGTERM on leader triggers transfer before shutdown → e2e test
- [ ] **6.3 Sim coverage**
  - [ ] 6.3.1 Random membership ops interleaved with crashes and partitions → 10,000 seeds pass
  - [ ] 6.3.2 Invariant: no two disjoint majorities across overlapping configurations → planted bug detected
- [ ] **6.4 Cluster admin service (D-23)**
  - [ ] 6.4.1 `AddLearner` handler (dynamic peer registration in transport) → integration test
  - [ ] 6.4.2 `PromoteLearner` with catch-up guard (configurable threshold) → refused when lagging, accepted when caught up
  - [ ] 6.4.3 `RemoveMember` handler → integration test
  - [ ] 6.4.4 `TransferLeadership` and `ListMembers` handlers → integration tests
  - [ ] 6.4.5 Concurrent change rejected with `FAILED_PRECONDITION` → test
  - [ ] 6.4.6 `raftkvctl member add-learner|promote|remove|transfer|list` → e2e smoke
  - [ ] 6.4.7 Membership metrics (roadmap §6 M6 row) → asserted
- [ ] **6.5 E2E scenarios**
  - [ ] 6.5.1 Replace-a-node under load (learner → snapshot → promote → remove), linearizability checked → 0 violations
  - [ ] 6.5.2 Scale 3 → 5 → 3 under load, 20 iterations → 0 violations
  - [ ] 6.5.3 Leadership transfer success rate (500 attempts, ≥ 99% within one election timeout) → recorded
  - [ ] 6.5.4 Rolling graceful restart of all 5 nodes under load, unavailability ≤ 1 s p99 target → recorded
  - [ ] 6.5.5 Add membership ops to nightly fault mix, update ledger → nightly green
- [ ] **6.9 M6 exit review & docs**
  - [ ] 6.9.1 Link evidence for all 6 M6 exit criteria → linked
  - [ ] 6.9.2 Runbooks "replace a node", "scale 3→5", "transfer leadership" in system-design.md / ops docs → diff reviewed
  - [ ] 6.9.3 Update CHANGELOG.md and memory.md → diff reviewed
  - [ ] 6.9.4 Tag `v0.5.0` → tag exists

---

## 7. M7 — Performance optimizations (D-13) + benchmarks

- [ ] **7.0 Decision resolution**
  - [ ] 7.0.1 Confirm D-20 environment provisioned and M3 baseline exists on it (re-run if provisional) → baseline file has the final fingerprint
  - [ ] 7.0.2 Sign off D-13 → `Decided`
  - [ ] 7.0.3 Agree on bench regression gate thresholds and blocking policy (roadmap §9 Q8) → new D-xx
- [ ] **7.1 Benchmark infrastructure**
  - [ ] 7.1.1 Multi-client-host runner for `raftkv-bench` with result merge → 2 hosts produce merged histogram
  - [ ] 7.1.2 Client-saturation guard (fail run if client CPU > 70%) → guard trips in a test run
  - [ ] 7.1.3 Host prep script (CPU governor, disable THP, fs mount opts recorded) → script output in fingerprint
  - [ ] 7.1.4 pprof capture (CPU, alloc, mutex, block) during bench runs, uploaded as artifacts → artifacts present
  - [ ] 7.1.5 bench.yml regression gate vs last tagged result → gate fails on a synthetic regression
- [ ] **7.2 Proposal batching (separate PR)**
  - [ ] 7.2.1 Coalesce concurrent proposals into one batch (≤ 4 MiB, max-wait bound) → unit test
  - [ ] 7.2.2 `proposal_batch_size` and `proposal_batch_wait_seconds` metrics → asserted
  - [ ] 7.2.3 Sim plus e2e linearizability re-run → 0 violations
  - [ ] 7.2.4 Benchmark delta vs baseline committed (`bench/results/m7-batching.json`) → file plus write-up
- [ ] **7.3 Group-commit fsync across Ready iterations (separate PR)**
  - [ ] 7.3.1 Node loop batches WAL syncs across consecutive `Ready`s without violating persist-before-send for followers → unit test
  - [ ] 7.3.2 `wal_group_commit_records` metric → asserted
  - [ ] 7.3.3 Crash durability suite (2.6.9, 3.1.7) re-run → 0 losses
  - [ ] 7.3.4 Benchmark delta committed → file plus write-up
- [ ] **7.4 Parallel leader WAL write (separate PR, dissertation §10.2.1)**
  - [ ] 7.4.1 Leader sends AppendEntries before/while its own fsync. Self-ack counted only after fsync → unit test
  - [ ] 7.4.2 Sim invariant: no commit counting the leader before its persist → planted bug detected
  - [ ] 7.4.3 Sim 10,000 seeds plus e2e linearizability → 0 violations
  - [ ] 7.4.4 Benchmark delta committed → file plus write-up
- [ ] **7.5 Pipelined replication (separate PR)**
  - [ ] 7.5.1 Multiple in-flight appends in Replicate state, window by count and bytes → unit test
  - [ ] 7.5.2 Correct fallback to Probe on reject / stream reset → sim scenario
  - [ ] 7.5.3 `transport_pipeline_depth` metric → asserted
  - [ ] 7.5.4 Sim plus e2e linearizability → 0 violations
  - [ ] 7.5.5 Benchmark delta committed → file plus write-up
- [ ] **7.6 Read path and hotspot tuning**
  - [ ] 7.6.1 ReadIndex batching tuning plus Pebble block cache sizing → read benchmark delta committed
  - [ ] 7.6.2 Fix top 3 pprof hotspots (one leaf per hotspot if large) → before/after profiles committed
- [ ] **7.7 Target runs (D-20 hardware)**
  - [ ] 7.7.1 Sustained write run ≥ 10 min (target ≥ 38,000 ops/s), 3 runs → results committed
  - [ ] 7.7.2 Sustained linearizable read run ≥ 10 min (target ≥ 92,000 ops/s), 3 runs → results committed
  - [ ] 7.7.3 p99 write latency vs M3 baseline at same offered load (target ≥ 30% lower) → results committed
  - [ ] 7.7.4 `bench/results/m7-report.md` with every delta, variance, and any missed target with analysis → report reviewed
- [ ] **7.9 M7 exit review & docs**
  - [ ] 7.9.1 Link evidence for all 6 M7 exit criteria. If a target is missed, file a waiver entry in design-decisions.md → linked
  - [ ] 7.9.2 Update testing-benchmarks.md (method, results tables) and system-design.md (optimized write path) → diffs reviewed
  - [ ] 7.9.3 Update CHANGELOG.md and memory.md → diff reviewed
  - [ ] 7.9.4 Tag `v0.6.0` → tag exists

---

## 8. M8 — Kubernetes, mTLS, observability, chaos campaign, v1.0 hardening

- [ ] **8.0 Decision resolution**
  - [ ] 8.0.1 **Resolve D-15** (mTLS off/optional/required in v1) → `Decided`
  - [ ] 8.0.2 Sign off D-16 → `Decided`
  - [ ] 8.0.3 Decide lost-PVC rejoin approach and Helm vs Kustomize (roadmap §9 Q5, Q6) → new D-xx entries
  - [ ] 8.0.4 Agree chaos ledger counting rules (roadmap §9 Q3) and artifact signing/registry (Q9) → new D-xx entries
- [ ] **8.1 Kubernetes manifests (deploy/k8s)**
  - [ ] 8.1.1 Kustomize base: StatefulSet (5 replicas, PVC template, resources, securityContext non-root/read-only rootfs) → `kubectl apply --dry-run=server` passes
  - [ ] 8.1.2 Headless Service (peer DNS) and client Service → DNS resolves in kind
  - [ ] 8.1.3 PodDisruptionBudget `maxUnavailable: 1`, pod anti-affinity and topology spread across nodes/zones → kubeconform plus kind check
  - [ ] 8.1.4 Probes: readiness (caught up within lag threshold), liveness (loop alive, not leadership) → probe tests in kind
  - [ ] 8.1.5 `preStop` hook calling leadership transfer, `terminationGracePeriodSeconds` sized to it → rolling restart test
  - [ ] 8.1.6 ConfigMap for config, node ID derived from pod ordinal, stable across restarts → restart keeps ID
  - [ ] 8.1.7 ServiceMonitor plus PrometheusRule resources → applied in kind with kube-prometheus-stack
  - [ ] 8.1.8 Overlays `kind` and `prod` → both render with `kustomize build`
  - [ ] 8.1.9 Lost-PVC rejoin (automated init container or runbook per 8.0.3) → tested in kind
  - [ ] 8.1.10 kubeconform/kube-linter in pr.yml → job green
- [ ] **8.2 Security (D-15, scope per decision)**
  - [ ] 8.2.1 TLS 1.3 server and client creds for client API and peer transport from files → integration test
  - [ ] 8.2.2 Peer identity verification (SAN = node ID) → untrusted/mismatched cert rejected
  - [ ] 8.2.3 Cert hot reload without restart → rotation test
  - [ ] 8.2.4 Modes off/optional/required per D-15, client without cert rejected in `required` → tests
  - [ ] 8.2.5 cert-manager `Issuer`/`Certificate` manifests in overlays → certs issued in kind
  - [ ] 8.2.6 TLS metrics (cert expiry, handshake failures) → asserted
- [ ] **8.3 Observability dashboards & alerts**
  - [ ] 8.3.1 Finalize metrics catalogue in system-design.md (names, labels, units, cardinality notes) → reviewed
  - [ ] 8.3.2 Grafana dashboard: cluster overview (leader, term, members, throughput, errors) → JSON committed, imports cleanly
  - [ ] 8.3.3 Grafana dashboard: Raft internals (commit/applied lag, elections, follower match, proposals) → JSON committed
  - [ ] 8.3.4 Grafana dashboard: storage/WAL/snapshots (fsync p99, disk, Pebble compactions, snapshot status) → JSON committed
  - [ ] 8.3.5 Grafana dashboard: client API (RPC rate/latency/codes, read consistency mix, sessions) → JSON committed
  - [ ] 8.3.6 Alert rules: no leader, leader flapping, follower lag, WAL fsync p99, disk > 80%, snapshot failures, cert expiry → rules committed
  - [ ] 8.3.7 `promtool test rules` unit tests for each alert, run in pr.yml → green
  - [ ] 8.3.8 Grafana provisioning test in CI (dashboards load without errors) → green
  - [ ] 8.3.9 Trace sampling defaults and log field standard documented → doc diff
  - [ ] 8.3.10 `build_info` gauge → asserted
  - [ ] 8.3.11 `cmd/raftkv-observer`: polls `Cluster.Status` on all nodes every 500 ms and serves an embedded HTML page over SSE (D-48) → page shows 5 node cards against the compose cluster
  - [ ] 8.3.12 Observer event timeline (term, leader, snapshot changes, node down) inferred by diffing polls → leader kill appears in the timeline within 1 s
  - [ ] 8.3.13 Observer added to the docker-compose e2e stack and as a 1-replica k8s Deployment (localhost / port-forward, no auth) → reachable in both
- [ ] **8.4 Chaos (test/chaos)**
  - [ ] 8.4.1 kind cluster setup script with Chaos Mesh installed → `make chaos-up` works
  - [ ] 8.4.2 Chaos Mesh manifests: PodKill, PodFailure → applied in kind
  - [ ] 8.4.3 NetworkChaos: partition, loss, delay → applied in kind
  - [ ] 8.4.4 IOChaos (fsync latency, fault) and StressChaos → applied in kind
  - [ ] 8.4.5 TimeChaos (clock skew) for lease-read testing → applied in kind
  - [ ] 8.4.6 Disk-full test (size-limited volume in compose or kind): clean failure, recovery, state hash matches → passes
  - [ ] 8.4.7 Chaos workflow driver: schedule, linearizability workload, ledger append → one automated campaign run
  - [ ] 8.4.8 nightly.yml kind plus Chaos Mesh job per ci-e2e.md → 7 consecutive green nights
  - [ ] 8.4.9 Long campaign on D-20 hardware or a real cluster until ledger shows ≥ 40M ops and ≥ 1,500 faults → ledger committed, 0 violations
- [ ] **8.5 Hardening**
  - [ ] 8.5.1 Fuzz test: WAL record decoder → ≥ 1 CPU-hour, 0 crashers
  - [ ] 8.5.2 Fuzz test: snapshot chunk/metadata decoder → ≥ 1 CPU-hour, 0 crashers
  - [ ] 8.5.3 Fuzz test: command envelope and KV request validation → ≥ 1 CPU-hour, 0 crashers
  - [ ] 8.5.4 Nightly fuzz job → green
  - [ ] 8.5.5 On-disk format version checks (WAL, snapshot, Pebble schema) with refusal on unknown version → tests
  - [ ] 8.5.6 Upgrade test v0.6 → v1.0 rolling on kind → passes
  - [ ] 8.5.7 Container image scan (e.g. Trivy) in pr.yml and release → 0 critical/high with fix
  - [ ] 8.5.8 Resource-limit soak: 24 h mixed load, no memory growth beyond target, no goroutine leak → soak report
- [ ] **8.6 Release pipeline**
  - [ ] 8.6.1 `.github/workflows/release.yml`: on tag, build multi-arch binaries plus checksums → dry-run on rc tag
  - [ ] 8.6.2 Multi-arch image build and push to registry → image pulled in kind
  - [ ] 8.6.3 SBOM generation and signing (images and binaries) per 8.0.4 → signature verifies
  - [ ] 8.6.4 GitHub Release notes from CHANGELOG section → rc release created
  - [ ] 8.6.5 Release checklist in CONTRIBUTING.md (or RELEASING.md) → reviewed
- [ ] **8.7 Documentation**
  - [ ] 8.7.1 Operations runbook: bootstrap, replace node, scale, lost PVC, disk full, cert rotation, alert responses → each alert links to a section
  - [ ] 8.7.2 Configuration reference generated from config struct → doc matches `--help`
  - [ ] 8.7.3 Upgrade guide and compatibility promise (API, on-disk format) → reviewed
  - [ ] 8.7.4 Final pass over architecture.md, system-design.md, api-design.md, storage-design.md, testing-benchmarks.md, ci-e2e.md → each marked "matches v1.0"
  - [ ] 8.7.5 README quick start verified on a clean machine (compose and kind) → verified by a second person
  - [ ] 8.7.6 Known limitations section (D-07 single group, D-10 no Watch/txn, D-15 no AuthZ) → in README
- [ ] **8.9 M8 exit review & v1.0**
  - [ ] 8.9.1 Link evidence for all 10 M8 exit criteria → linked
  - [ ] 8.9.2 Walk the v1.0 Definition of Done (roadmap §8), every box linked → all checked
  - [ ] 8.9.3 Release candidate `v1.0.0-rc.1`, 7 green nightlies on rc SHA → recorded
  - [ ] 8.9.4 Update CHANGELOG.md (`1.0.0` section) and memory.md (final state, lessons learned) → diff reviewed
  - [ ] 8.9.5 Tag `v1.0.0` and publish release → release page live

---

## post-1.0 (deferred)

Items moved here keep their original number and get a one-line reason.

- (none yet) Candidates already deferred by decisions: joint consensus (D-06), multi-Raft sharding (D-07), `Watch` and multi-key transactions (D-10), AuthN/AuthZ (D-15).
