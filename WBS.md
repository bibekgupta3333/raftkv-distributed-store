# RaftKV Work Breakdown Structure (WBS)

This is the task-level plan for [docs/implementation-roadmap.md](docs/implementation-roadmap.md). Decisions are cited by
ID from [docs/design-decisions.md](docs/design-decisions.md). RaftKV is a learning project: there are no performance
tasks, and every task aims at a working, tested feature.

## How to use this file

- **Numbering follows the milestones.** Section `N` is milestone `MN` (section `0` = M0 … section `7` = M7). Leaves are
  numbered `N.x.y`. Never renumber an item once it has been ticked or referenced in a PR. Add new items at the end of a
  group (e.g. `2.3.9`).
- **Each leaf is a small, checkable piece of work** (roughly a day or less). The text after `→` says how to verify it.
  If a leaf turns out bigger, split it in the PR that starts it.
- **Tick the box in the same PR that does the work**, and append the PR number: `- [x] 2.1.3 ... (#42)`. A box ticked
  without a PR link (or a commit SHA for doc-only work) doesn't count.
- **Each milestone ends with an exit-review group.** It is ticked only when every exit criterion in the roadmap has a
  linked CI run or artifact, and `CHANGELOG.md` and `memory.md` are updated.
- **Update the progress table** below in the same PR (count ticked leaves per milestone). The `Leaves` column counts
  only leaf checkboxes (3-level items).
- Deferred items move to the `post-1.0` section at the bottom with a one-line reason. They are never deleted.

## Progress summary

| Milestone | Theme | Done / Leaves | Est. weeks | Status |
|-----------|-------|---------------|------------|--------|
| M0 | Repo setup, tooling, protos | 6/15 | 1 | Not started |
| M1 | Raft core + deterministic simulator | 0/39 | 5 | Not started |
| M2 | WAL + gRPC transport + real 5-process cluster | 0/38 | 4 | Not started |
| M3 | Pebble state machine + basic KV | 0/20 | 2.5 | Not started |
| M4 | Linearizable reads, client sessions, full KV API | 0/25 | 3.5 | Not started |
| M5 | Snapshots + follower catch-up | 0/18 | 2.5 | Not started |
| M6 | Membership changes + leadership transfer | 0/21 | 3 | Not started |
| M7 | Kubernetes, observer UI, chaos tests, v1.0 | 0/25 | 3.5 | Not started |
| **Total** | | **0/201** | **25** | |

---

## 0. M0 — Repo setup, tooling, protos

- [ ] **0.1 Repository skeleton**
  - [x] 0.1.1 Resolve D-22 (license, Apache-2.0 unless changed) and add LICENSE → design-decisions.md shows `Decided`
  - [x] 0.1.2 `go mod init` with the D-21 module path and current Go toolchain → `go build ./...` passes
  - [x] 0.1.3 Create the directory layout from design-decisions.md (a `doc.go` per package) → `go vet ./...` passes, tree matches layout
  - [x] 0.1.4 README (summary, links to docs) and CONTRIBUTING.md (branching, commit style, WBS ticking rule) → render on GitHub
  - [x] 0.1.5 Create `CHANGELOG.md` (Keep a Changelog, `Unreleased` section) and `memory.md` (decisions, gotchas, how-tos) → files exist with headings
  - [x] 0.1.6 `.gitignore`, `.editorconfig`, PR template with a "WBS items ticked" checklist → template shows on a test PR
- [ ] **0.2 Tooling**
  - [ ] 0.2.1 Pin tool versions (golangci-lint, buf, protoc-gen-go, protoc-gen-go-grpc) via `go tool` directives → `make tools` installs exact versions
  - [ ] 0.2.2 Makefile targets `all fmt fmt-check lint build test race proto clean help` → `make help` lists each, `make all` passes
  - [ ] 0.2.3 `.golangci.yml` with a `forbidigo` rule banning `time.Now` in `internal/raft` (D-24) → lint passes, a planted `time.Now()` fails
  - [ ] 0.2.4 `cmd/raftkvd` and `cmd/raftkvctl` stub mains with version stamped via `-ldflags` → `raftkvd --version` prints version and git SHA
- [ ] **0.3 Protobuf skeleton**
  - [ ] 0.3.1 `buf.yaml` (lint STANDARD) and `buf.gen.yaml` → `buf lint` passes
  - [ ] 0.3.2 `api/raftkv/v1/kv.proto`, `cluster.proto` (D-23), `raft.proto` (D-36) with messages per api-design.md → generates and compiles
  - [ ] 0.3.3 Commit generated code and add a drift check (`make proto && git diff --exit-code`) (D-43) → CI fails on stale generated code
- [ ] **0.4 CI and exit review**
  - [ ] 0.4.1 `.github/workflows/pr.yml`: Go cache, `make fmt-check lint build test race`, buf lint, proto drift check → green on `main`
  - [ ] 0.4.2 M0 exit review: link evidence for each roadmap M0 exit criterion, update CHANGELOG.md and memory.md, tag `v0.0.1` → all criteria linked, tag exists

---

## 1. M1 — Raft core + deterministic simulator

- [ ] **1.1 Core types and interfaces**
  - [ ] 1.1.1 `Entry`, `HardState`, `Message`, `StateType`, `Config` with validation → unit tests for `Config.Validate`
  - [ ] 1.1.2 `Storage` interface and `MemoryStorage` → table tests
  - [ ] 1.1.3 `RawNode` API (`Tick`, `Step`, `Propose`, `Ready`, `HasReady`, `Advance`) and the `Ready` contract (D-01) → godoc plus compile test
  - [ ] 1.1.4 `raftLog` (unstable + stable, committed, applied) with invariant checks in tests → unit tests
- [ ] **1.2 Leader election**
  - [ ] 1.2.1 Follower → candidate on randomized election timeout in ticks (D-08, D-24) → test: timeouts stay within bounds
  - [ ] 1.2.2 RequestVote handling: term check, log up-to-date check, persisted `votedFor` → table tests for every branch
  - [ ] 1.2.3 Candidate wins on majority, steps down on higher term or a valid leader's message → unit tests
  - [ ] 1.2.4 Leader appends a no-op entry on election and sends heartbeats each heartbeat tick → tests: first entry of a term is a no-op, followers don't time out
  - [ ] 1.2.5 PreVote (D-08): pre-candidate doesn't bump term → test: rejoining partitioned node doesn't raise term
  - [ ] 1.2.6 CheckQuorum (D-08): leader steps down without majority contact, followers ignore votes while they have a live leader → unit tests
- [ ] **1.3 Log replication**
  - [ ] 1.3.1 AppendEntries send with prevIndex/prevTerm from `Progress.next` → unit test
  - [ ] 1.3.2 Follower consistency check, conflict truncation, append → tests incl. Figure 7 logs
  - [ ] 1.3.3 Reject with conflict hint and fast leader backtracking → test: large divergence resolves in a few round trips
  - [ ] 1.3.4 `Progress` tracking (match, next, Probe/Replicate) → unit tests
  - [ ] 1.3.5 Commit rule: only current-term entries are committed by counting replicas; followers take commit from leader → Figure 8 test passes
  - [ ] 1.3.6 Committed entries exposed in `Ready`, applied index advanced on `Advance` → unit test
- [ ] **1.4 Proposals**
  - [ ] 1.4.1 `Propose` on leader appends to log, `ErrProposalDropped` when there is no leader → unit tests
  - [ ] 1.4.2 Follower proposal returns a not-leader error with a leader hint (forwarding is added in M4, D-28) → unit test
  - [ ] 1.4.3 Uncommitted-size limit (D-29) and per-message size limit → tests: proposals rejected past the limit
- [ ] **1.5 Deterministic simulator (test/sim)**
  - [ ] 1.5.1 Seeded PRNG, virtual clock, event loop driving N `RawNode`s → same seed gives identical trace (hash compare)
  - [ ] 1.5.2 Virtual network: delay, drop, duplicate, reorder per link → unit tests on the network model
  - [ ] 1.5.3 Partition scheduler (random splits, isolate leader, heal) → scenario test
  - [ ] 1.5.4 Crash/restart model: lose unpersisted state, keep HardState and stable log → scenario test
  - [ ] 1.5.5 Client workload generator plus a reference model of the applied sequence → planted divergence detected
- [ ] **1.6 Invariant checker**
  - [ ] 1.6.1 Election Safety and Leader Append-Only → planted bugs detected
  - [ ] 1.6.2 Log Matching and Leader Completeness → planted bugs detected
  - [ ] 1.6.3 State Machine Safety and commit-index never decreases → planted bugs detected
  - [ ] 1.6.4 Liveness check: after heal, a leader is elected and new proposals commit → per-seed result reported
- [ ] **1.7 Simulator tooling and CI**
  - [ ] 1.7.1 Flags `-seeds=N -start=S -faults=profile`; a failing seed prints a repro command → manual check
  - [ ] 1.7.2 Trace dump on failure (JSONL) and a pretty-printer → failing seed gives a readable trace
  - [ ] 1.7.3 Regression seeds in `test/sim/regressions/`, replayed on every PR → planted regression seed runs in CI
  - [ ] 1.7.4 Add a 500-seed sim job to `pr.yml` → green
  - [ ] 1.7.5 Create `.github/workflows/nightly.yml` with a long sim job (100,000+ seeds) → first nightly green
- [ ] **1.8 Scenario tests**
  - [ ] 1.8.1 Raft paper Figure 7 divergent logs converge, Figure 8 commit hazard handled → tests pass
  - [ ] 1.8.2 Disruptive rejoin with PreVote on vs off (off shows term inflation) → both behaviors asserted
  - [ ] 1.8.3 Leader isolation with CheckQuorum → step-down asserted
- [ ] **1.9 M1 exit review**
  - [ ] 1.9.1 Run 10,000 seeds with the full fault profile, race detector clean → 0 violations, CI run linked
  - [ ] 1.9.2 Update architecture.md / system-design.md Raft core sections to match the code → diff reviewed
  - [ ] 1.9.3 Update CHANGELOG.md and memory.md (core API, sim usage, bugs the sim found), tag `v0.1.0-alpha` → diff reviewed, tag exists

---

## 2. M2 — WAL + gRPC transport + real 5-process cluster

- [ ] **2.1 WAL (internal/wal)**
  - [ ] 2.1.1 Record format `[len|type|crc32c|payload]` encoder/decoder → round-trip test plus fuzz seeds
  - [ ] 2.1.2 Segment files, rollover at a size limit, directory fsync on create → test: rollover at boundary
  - [ ] 2.1.3 Append entries and HardState records, `Sync()` with fsync (`F_FULLFSYNC` on macOS) → tests
  - [ ] 2.1.4 Open/replay: rebuild entries and last HardState, handle entries overwritten by a conflict → replay tests
  - [ ] 2.1.5 Tail repair: cut a torn final record, refuse to start on corruption elsewhere (D-33) → tests
  - [ ] 2.1.6 Fault-injecting file shim (truncate/corrupt at offset, fail fsync) → shim unit tests
  - [ ] 2.1.7 Crash matrix: every byte offset of the last 2 records across many random logs → 0 silent corruptions
  - [ ] 2.1.8 fsync or write error crashes the node, never retried (D-33) → test asserts the exit path
  - [ ] 2.1.9 `ReleaseTo(index)` deletes whole old segments (used in M5) → unit test
  - [ ] 2.1.10 WAL implements Raft `Storage` → same Storage test suite as `MemoryStorage` passes
- [ ] **2.2 Transport (internal/transport)**
  - [ ] 2.2.1 gRPC server for the Raft service with message-size limits → server starts, rejects oversize messages
  - [ ] 2.2.2 One outbound `Replicate` stream per follower with a send queue (D-12) → test with in-process peers
  - [ ] 2.2.3 Flow control: max 256 in-flight messages per follower (D-29) → unit test
  - [ ] 2.2.4 Reconnect with backoff and jitter, report unreachable peers to the core → test: peer restart reconnects
  - [ ] 2.2.5 `RequestVote` RPC with pre-vote flag (D-36) → test
  - [ ] 2.2.6 Static peer address book from config, DNS re-resolved on reconnect → test
- [ ] **2.3 Node loop and server (internal/node)**
  - [ ] 2.3.1 Driver loop: tick → `Tick`, inbound → `Step`, `Ready` → persist, send, apply, `Advance` (D-35) → 3 in-process nodes commit entries
  - [ ] 2.3.2 Temporary in-memory applier plus a debug `Propose` RPC behind a build tag → 5 nodes commit 10k entries
  - [ ] 2.3.3 Config from YAML file plus flags with validation → table tests
  - [ ] 2.3.4 Bootstrap from config, node ID saved in the data dir, refuse ID mismatch → tests
  - [ ] 2.3.5 Graceful shutdown on SIGTERM/SIGINT → test
  - [ ] 2.3.6 `raftkvctl debug wal-dump` to compare logs across nodes → prints entries from a data dir
- [ ] **2.4 Observability (D-17)**
  - [ ] 2.4.1 `log/slog` JSON logs with `node_id`, `term`, `component` fields → log sample in test
  - [ ] 2.4.2 `/metrics` with Raft metrics (term, state, leader changes, commit/applied index) → asserted in a test
  - [ ] 2.4.3 WAL fsync timing and transport metrics (sent, failures, reconnects) → asserted
  - [ ] 2.4.4 OTel tracing with spans `raft.propose`, `wal.fsync`, `apply` → test with an in-memory exporter
- [ ] **2.5 Packaging and local cluster**
  - [ ] 2.5.1 Multi-stage Dockerfile (non-root, static binary) → image builds, runs `--version`
  - [ ] 2.5.2 `deploy/compose/docker-compose.yml` with 5 nodes plus Prometheus, and `make cluster-up/down/logs` → cluster elects a leader
- [ ] **2.6 E2E harness (test/e2e)**
  - [ ] 2.6.1 Go e2e framework: start/stop compose, find leader, wait helpers → smoke test passes
  - [ ] 2.6.2 Fault driver: `kill -9`, restart, SIGSTOP/SIGCONT → each fault works on one container
  - [ ] 2.6.3 Fault driver: iptables partition and heal, `tc netem` delay/loss → partition test passes
  - [ ] 2.6.4 Seeded fault schedule with a timestamped fault log → fault log uploaded as a CI artifact
  - [ ] 2.6.5 Tests: leader elected on start, new leader after leader kill, minority stops committing and logs match after heal → pass
  - [ ] 2.6.6 Test: 200 rounds of kill -9 and restart, no committed entry lost → passes
  - [ ] 2.6.7 Add the short e2e job to `pr.yml` and a long e2e job to `nightly.yml` → both green
- [ ] **2.9 M2 exit review**
  - [ ] 2.9.1 Re-run sim 10,000 seeds and link evidence for every M2 exit criterion → all linked
  - [ ] 2.9.2 Update storage-design.md (WAL as built), system-design.md (node loop), ci-e2e.md (jobs) → diffs reviewed
  - [ ] 2.9.3 Update CHANGELOG.md and memory.md (WAL format, fsync gotchas, e2e tips), tag `v0.1.0` → diff reviewed, tag exists

---

## 3. M3 — Pebble state machine + basic KV

- [ ] **3.1 Pebble state machine (internal/storage)**
  - [ ] 3.1.1 Open Pebble with its WAL off (D-02, D-03) → opens and closes cleanly
  - [ ] 3.1.2 Key prefixes for user data, sessions, and metadata, plus a schema version key → encoding tests
  - [ ] 3.1.3 Apply batch: Put/Delete plus `appliedIndex` in the same batch (D-04) → unit test
  - [ ] 3.1.4 Track the durable applied index with a flush listener → test: it advances only after a flush
  - [ ] 3.1.5 WAL truncation rule (D-30): flush first, never `ReleaseTo` past the durable applied index → test: truncation never passes it
  - [ ] 3.1.6 Recovery: read `appliedIndex`, replay the rest from the WAL → crash test
  - [ ] 3.1.7 Crash suite: 500 kill -9 during apply, state hash vs reference model; plus truncation-safety test with unflushed memtables → 0 mismatches
  - [ ] 3.1.8 `raftkvctl debug state-hash` → same hash on all replicas at the same applied index
- [ ] **3.2 Commands and KV service**
  - [ ] 3.2.1 Command envelope proto (type, payload, request ID) in log entries → round-trip test
  - [ ] 3.2.2 Proposal tracking: request ID → waiter, resolved on apply, failed with a retryable error on leader change → unit tests
  - [ ] 3.2.3 `Put` and `Delete` handlers (validate, propose, wait for apply) → integration tests
  - [ ] 3.2.4 `Get` through the log, and `NOT_LEADER` errors with a leader hint → integration tests
- [ ] **3.3 Client library (pkg/client)**
  - [ ] 3.3.1 Connect to all endpoints, cache the leader, follow `NOT_LEADER` hints → unit test with fake servers
  - [ ] 3.3.2 Retry on retryable codes with backoff, bounded by the deadline → unit tests
  - [ ] 3.3.3 `raftkvctl put/get/del` using the client library → e2e smoke
- [ ] **3.4 Linearizability harness (test/linearizability)**
  - [ ] 3.4.1 History recorder (invoke/return times, client, op, result, unknown on timeout) → unit test
  - [ ] 3.4.2 Porcupine KV model split per key, checker with a timeout (timeout = fail), HTML view on failure → known-bad history detected
  - [ ] 3.4.3 Wire the recorder into e2e with leader kill, follower kill, and partition → checked histories, 0 violations
- [ ] **3.9 M3 exit review**
  - [ ] 3.9.1 Link evidence for every M3 exit criterion; update storage-design.md (key schema, durability) and api-design.md (implemented RPCs) → linked, diffs reviewed
  - [ ] 3.9.2 Update CHANGELOG.md and memory.md, tag `v0.2.0` → diff reviewed, tag exists

---

## 4. M4 — Linearizable reads, client sessions, full KV API

- [ ] **4.1 ReadIndex (Raft core)**
  - [ ] 4.1.1 `ReadIndex` request: record commit index, require a current-term commit first → unit test
  - [ ] 4.1.2 Confirm leadership with a heartbeat quorum, batching concurrent requests → test: many readers share one round
  - [ ] 4.1.3 `ReadStates` in `Ready`, and follower ReadIndex via the leader (D-36) → unit and sim tests
  - [ ] 4.1.4 Sim: a partitioned old leader never completes a ReadIndex after a new leader exists; a read right after election waits for the no-op → 10,000 seeds pass
- [ ] **4.2 Read path**
  - [ ] 4.2.1 Wait until `appliedIndex ≥ readIndex`, deadline-aware → unit test
  - [ ] 4.2.2 `Get` via ReadIndex on the leader and on followers (D-05) → e2e tests
  - [ ] 4.2.3 Opt-in stale reads that return the applied index → test
  - [ ] 4.2.4 Optional lease reads behind a flag, off by default (D-25) → unit tests
  - [ ] 4.2.5 Lease sim: violation found when drift is beyond the bound, none within it → both asserted
- [ ] **4.3 Client sessions (D-09, D-26, D-27)**
  - [ ] 4.3.1 `RegisterSession` through the log, server assigns the ID, stored under the session prefix → integration test
  - [ ] 4.3.2 Dedup on apply: `(client_id, seq)` check, cached response, update in the same batch → unit test
  - [ ] 4.3.3 Leader stamps time into each command; replicas expire sessions after 10 minutes using it → test: replicas agree
  - [ ] 4.3.4 Client library: auto-register, increasing sequence, retry with the same sequence → unit test
  - [ ] 4.3.5 Exactly-once e2e: every write retried 1–3 times after random drops, counter model → 0 duplicates
  - [ ] 4.3.6 Session sim: 10,000 seeds crossing the expiry time, identical session table hash → passes
- [ ] **4.4 Full KV API (D-10, D-28, D-34)**
  - [ ] 4.4.1 Follower forwards writes to the leader; client can opt in to a redirect instead (D-28) → e2e test
  - [ ] 4.4.2 `CompareAndSwap` per api-design.md → unit and e2e tests
  - [ ] 4.4.3 `Range` with prefix or [start, end), page limit, continuation token, linearizable or stale per page → tests incl. paging across writes
  - [ ] 4.4.4 `raftkvctl cas/range` → e2e smoke
- [ ] **4.5 Limits and errors (D-11)**
  - [ ] 4.5.1 Key ≤ 4 KiB, value ≤ 1 MiB, Range page ≤ 1,000 keys / 4 MiB, checked before propose → boundary tests (limit ok, limit + 1 rejected)
  - [ ] 4.5.2 gRPC status codes and error details per api-design.md, deadlines passed through → table tests
- [ ] **4.6 Linearizability coverage**
  - [ ] 4.6.1 Porcupine models for CAS and Range → known-bad histories detected
  - [ ] 4.6.2 e2e mixing Put/Get/Delete/CAS/Range with leader and follower reads under leader kill, pause, partition → 0 violations, runs nightly
- [ ] **4.9 M4 exit review**
  - [ ] 4.9.1 Link evidence for every M4 exit criterion; update api-design.md (final API) and system-design.md (read path, sessions) → linked, diffs reviewed
  - [ ] 4.9.2 Update CHANGELOG.md and memory.md, tag `v0.3.0` → diff reviewed, tag exists

---

## 5. M5 — Snapshots + follower catch-up

- [ ] **5.1 Raft core snapshot support**
  - [ ] 5.1.1 `Storage.Snapshot()` / `ApplySnapshot`, compaction index in `raftLog` → unit tests
  - [ ] 5.1.2 Leader sends a snapshot when the follower needs compacted entries (Progress Snapshot state) → unit tests
  - [ ] 5.1.3 Follower handles a snapshot (reject stale, reset log, update commit) → unit tests
  - [ ] 5.1.4 Sim: low snapshot threshold plus crashes during install → 10,000 seeds pass
- [ ] **5.2 Snapshot creation (internal/snapshot)**
  - [ ] 5.2.1 Metadata `{index, term, membership, format_version, checksum}` (D-14) → round-trip test
  - [ ] 5.2.2 Pebble checkpoint taken between apply batches so its index equals `appliedIndex` → test asserts equality
  - [ ] 5.2.3 Trigger every 100k entries (configurable), keep the last 2, clean up temp dirs on start → tests with small thresholds
- [ ] **5.3 WAL compaction**
  - [ ] 5.3.1 Release WAL segments under the D-30 rule, with a small retention tail for slow followers → unit test
  - [ ] 5.3.2 Steady-write run: WAL disk use stays bounded → result recorded
- [ ] **5.4 Transfer and install**
  - [ ] 5.4.1 Sender: `InstallSnapshot` stream in 1 MiB chunks → integration test
  - [ ] 5.4.2 Receiver: stage chunks in a temp dir, verify checksum; an interrupted transfer starts over (D-14) → corrupt chunk rejected, killed transfer restarts
  - [ ] 5.4.3 Install: close Pebble, atomic dir swap (rename plus dir fsync), reset WAL to the snapshot index, reopen → test
  - [ ] 5.4.4 Crash safety: kill -9 at each install step → always old or new state, never a mix
  - [ ] 5.4.5 Session table restored from the snapshot; exactly-once test across a snapshot boundary → passes
- [ ] **5.5 Recovery scenarios (e2e)**
  - [ ] 5.5.1 Wiped follower rejoins via snapshot and its state hash matches (20 runs); long-partitioned follower catches up past compaction → all pass
  - [ ] 5.5.2 Add follower-wipe to the e2e fault driver and nightly; linearizability still passes → 0 violations
- [ ] **5.9 M5 exit review**
  - [ ] 5.9.1 Link evidence for every M5 exit criterion; add snapshot metrics (size, duration, installs) and update storage-design.md → linked, diff reviewed
  - [ ] 5.9.2 Update CHANGELOG.md and memory.md, tag `v0.4.0` → diff reviewed, tag exists

---

## 6. M6 — Membership changes + leadership transfer

- [ ] **6.1 Raft core membership**
  - [ ] 6.1.1 Config-change entry (add learner, promote, remove) with versioned encoding → round-trip test
  - [ ] 6.1.2 Config takes effect when appended, rolled back if the entry is truncated (D-31) → unit tests
  - [ ] 6.1.3 One pending change at a time, first change only after a current-term commit → unit test plus sim that reproduces the bug without the fix
  - [ ] 6.1.4 Learners replicate but don't vote or count toward quorum → unit tests
  - [ ] 6.1.5 Leader removal: step down after committing its own removal; messages from removed nodes ignored (D-31) → unit and sim tests
  - [ ] 6.1.6 Membership saved in WAL and snapshot metadata → restart test
- [ ] **6.2 Leadership transfer**
  - [ ] 6.2.1 `TransferLeadership(target)`: pause proposals, catch up target, send `TimeoutNow` → unit test
  - [ ] 6.2.2 Abort after one election timeout and resume; drop the lease first if lease reads are on → unit tests
  - [ ] 6.2.3 SIGTERM on the leader transfers leadership before shutdown → e2e test
- [ ] **6.3 Sim coverage**
  - [ ] 6.3.1 Random membership changes mixed with crashes and partitions, with a "no two disjoint majorities" invariant → 10,000 seeds pass, planted bug detected
- [ ] **6.4 Cluster admin service (D-23)**
  - [ ] 6.4.1 Random 64-bit member IDs, never reused (D-38) → unit test
  - [ ] 6.4.2 Node with an empty disk refuses its old ID and must rejoin as a new learner (D-32) → test
  - [ ] 6.4.3 `AddLearner` with dynamic peer registration in the transport → integration test
  - [ ] 6.4.4 `PromoteLearner` with a catch-up check → refused when lagging, accepted when caught up
  - [ ] 6.4.5 `RemoveMember`, `TransferLeadership`, `ListMembers`, `Status` handlers → integration tests
  - [ ] 6.4.6 Second change while one is pending → rejected with `FAILED_PRECONDITION`
  - [ ] 6.4.7 `raftkvctl member add-learner|promote|remove|transfer|list` → e2e smoke
- [ ] **6.5 E2E scenarios**
  - [ ] 6.5.1 Replace a node under load (learner → catch up → promote → remove old) → 0 linearizability violations
  - [ ] 6.5.2 Scale 3 → 5 → 3 under load, plus membership changes in the nightly fault mix → 0 violations, nightly green
- [ ] **6.9 M6 exit review**
  - [ ] 6.9.1 Link evidence for every M6 exit criterion; write how-tos "replace a node", "scale 3 → 5", "transfer leadership" in system-design.md → linked, diff reviewed
  - [ ] 6.9.2 Update CHANGELOG.md and memory.md, tag `v0.5.0` → diff reviewed, tag exists

---

## 7. M7 — Kubernetes, observer UI, chaos tests, v1.0

- [ ] **7.1 Kubernetes on kind (deploy/k8s, D-16)**
  - [ ] 7.1.1 Kustomize base: StatefulSet (5 replicas, PVC template, non-root) → `kubectl apply --dry-run=server` passes
  - [ ] 7.1.2 Headless Service for peer DNS and a client Service → DNS resolves in kind
  - [ ] 7.1.3 Node ID from the pod ordinal, stable across restarts; config from a ConfigMap → restart keeps the ID
  - [ ] 7.1.4 Readiness (knows leader, caught up) and liveness (driver loop responds) probes (D-46) → probe behavior checked in kind
  - [ ] 7.1.5 `preStop` hook that transfers leadership, grace period sized for it → rolling restart test
  - [ ] 7.1.6 `kind` overlay and `make kind-up` script → 5-node cluster passes the e2e smoke test
- [ ] **7.2 Observer UI (D-48)**
  - [ ] 7.2.1 `cmd/raftkv-observer`: polls `Cluster.Status` on all nodes every 500 ms and serves an embedded HTML page over SSE → page shows 5 node cards against the compose cluster
  - [ ] 7.2.2 Event timeline (term, leader, snapshot changes, node down) inferred by diffing polls → leader kill appears in the timeline within 1 s
  - [ ] 7.2.3 Observer added to the docker-compose stack and as a 1-replica k8s Deployment (localhost / port-forward, no auth) → reachable in both
- [ ] **7.3 Metrics dashboard**
  - [ ] 7.3.1 One Grafana dashboard (leader, term, commit/applied index, request rate and errors, fsync timing) in compose → JSON committed, imports cleanly
- [ ] **7.4 Chaos tests (test/chaos, D-47)**
  - [ ] 7.4.1 Install Chaos Mesh on kind (`make chaos-up`) → Chaos Mesh pods running
  - [ ] 7.4.2 `PodChaos` pod-kill manifests → applied in kind
  - [ ] 7.4.3 `NetworkChaos` partition, delay, and loss manifests → applied in kind
  - [ ] 7.4.4 Chaos driver: run a fault schedule while recording histories, then run Porcupine → one automated run, 0 violations
  - [ ] 7.4.5 Rolling restart of the StatefulSet under load → 0 violations, each pod hands off leadership
  - [ ] 7.4.6 Add the kind + Chaos Mesh job to `nightly.yml` → 3 green nights in a row
- [ ] **7.5 Hardening**
  - [ ] 7.5.1 Fuzz tests for the WAL record decoder, snapshot metadata, and command envelope, run in nightly → 0 crashers
  - [ ] 7.5.2 On-disk format version check (WAL, snapshot, Pebble schema), refuse unknown versions → tests
- [ ] **7.6 Documentation**
  - [ ] 7.6.1 Configuration reference matching `--help` → doc matches flags
  - [ ] 7.6.2 Final pass over architecture.md, system-design.md, api-design.md, storage-design.md, testing.md, ci-e2e.md → each matches v1.0
  - [ ] 7.6.3 README quick start (compose and kind) tried on a clean machine; known limitations listed (one Raft group, no Watch/transactions, no TLS/auth) → verified
- [ ] **7.9 M7 exit review and v1.0**
  - [ ] 7.9.1 Link evidence for every M7 exit criterion → linked
  - [ ] 7.9.2 Walk the v1.0 Definition of Done (roadmap §5), every box linked → all checked
  - [ ] 7.9.3 Update CHANGELOG.md (`1.0.0` section) and memory.md (final state, lessons learned) → diff reviewed
  - [ ] 7.9.4 Tag `v1.0.0` and write GitHub release notes from the CHANGELOG → release page live

---

## post-1.0 (deferred)

Items moved here keep their original number and get a one-line reason.

- (none yet) Out of scope by decision: joint consensus (D-06), sharding (D-07), `Watch` and transactions (D-10). Removed
  with the learning-project re-scope: performance work, TLS/auth, release signing, rolling upgrades.
