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
| M0 | Repo setup, tooling, protos | 15/15 | 1 | Done (`v0.0.1`) |
| M1 | Raft core + deterministic simulator | 39/39 | 5 | Done (`v0.1.0-alpha`) |
| M2 | WAL + gRPC transport + real 5-process cluster | 38/38 | 4 | Done (`v0.1.0`) |
| M3 | Pebble state machine + basic KV | 20/20 | 2.5 | Done (`v0.2.0`) |
| M4 | Linearizable reads, client sessions, full KV API | 25/25 | 3.5 | Done (`v0.3.0`) |
| M5 | Snapshots + follower catch-up | 18/18 | 2.5 | Done (`v0.4.0`) |
| M6 | Membership changes + leadership transfer | 21/21 | 3 | Done (`v0.5.0`) |
| M7 | Kubernetes, observer UI, chaos tests, v1.0 | 20/25 | 3.5 | In progress (v1.0 DoD walk: 2 items open) |
| **Total** | | **196/201** | **25** | |

M0–M6 are done and tagged: every exit criterion is linked in docs/implementation-roadmap.md ("Exit evidence").
M7's evidence and the v1.0 Definition of Done walk are in the same file.

---

## 0. M0 — Repo setup, tooling, protos

- [x] **0.1 Repository skeleton**
  - [x] 0.1.1 Resolve D-22 (license, Apache-2.0 unless changed) and add LICENSE → design-decisions.md shows `Decided`
  - [x] 0.1.2 `go mod init` with the D-21 module path and current Go toolchain → `go build ./...` passes
  - [x] 0.1.3 Create the directory layout from design-decisions.md (a `doc.go` per package) → `go vet ./...` passes, tree matches layout
  - [x] 0.1.4 README (summary, links to docs) and CONTRIBUTING.md (branching, commit style, WBS ticking rule) → render on GitHub
  - [x] 0.1.5 Create `CHANGELOG.md` (Keep a Changelog, `Unreleased` section) and `memory.md` (decisions, gotchas, how-tos) → files exist with headings
  - [x] 0.1.6 `.gitignore`, `.editorconfig`, PR template with a "WBS items ticked" checklist → template shows on a test PR
- [x] **0.2 Tooling**
  - [x] 0.2.1 Pin tool versions (golangci-lint, buf, protoc-gen-go, protoc-gen-go-grpc) via `go tool` directives → `make tools` installs exact versions (#2)
  - [x] 0.2.2 Makefile targets `all fmt fmt-check lint build test race proto clean help` → `make help` lists each, `make all` passes (#2)
  - [x] 0.2.3 `.golangci.yml` with a `forbidigo` rule banning `time.Now` in `internal/raft` (D-24) → lint passes, a planted `time.Now()` fails (#2)
  - [x] 0.2.4 `cmd/raftkvd` and `cmd/raftkvctl` stub mains with version stamped via `-ldflags` → `raftkvd --version` prints version and git SHA (#2)
- [x] **0.3 Protobuf skeleton**
  - [x] 0.3.1 `buf.yaml` (lint STANDARD) and `buf.gen.yaml` → `buf lint` passes (#2)
  - [x] 0.3.2 `api/raftkv/v1/kv.proto`, `cluster.proto` (D-23), `raft.proto` (D-36) with messages per api-design.md → generates and compiles (#3)
  - [x] 0.3.3 Commit generated code and add a drift check (`make proto && git diff --exit-code`) (D-43) → CI fails on stale generated code (#2, #3)
- [x] **0.4 CI and exit review**
  - [x] 0.4.1 `.github/workflows/pr.yml`: Go cache, `make fmt-check lint build test race`, buf lint, proto drift check → green on `main` (#2)
  - [x] 0.4.2 M0 exit review: link evidence for each roadmap M0 exit criterion, update CHANGELOG.md and memory.md, tag `v0.0.1` → all criteria linked, tag exists (#24 drift proof, #25; evidence in implementation-roadmap.md M0) (tag `v0.0.1`)

---

## 1. M1 — Raft core + deterministic simulator

- [x] **1.1 Core types and interfaces**
  - [x] 1.1.1 `Entry`, `HardState`, `Message`, `StateType`, `Config` with validation → unit tests for `Config.Validate` (#5)
  - [x] 1.1.2 `Storage` interface and `MemoryStorage` → table tests (#5)
  - [x] 1.1.3 `RawNode` API (`Tick`, `Step`, `Propose`, `Ready`, `HasReady`, `Advance`) and the `Ready` contract (D-01) → godoc plus compile test (#5)
  - [x] 1.1.4 `raftLog` (unstable + stable, committed, applied) with invariant checks in tests → unit tests (#5)
- [x] **1.2 Leader election**
  - [x] 1.2.1 Follower → candidate on randomized election timeout in ticks (D-08, D-24) → test: timeouts stay within bounds (#5)
  - [x] 1.2.2 RequestVote handling: term check, log up-to-date check, persisted `votedFor` → table tests for every branch (#5)
  - [x] 1.2.3 Candidate wins on majority, steps down on higher term or a valid leader's message → unit tests (#5)
  - [x] 1.2.4 Leader appends a no-op entry on election and sends heartbeats each heartbeat tick → tests: first entry of a term is a no-op, followers don't time out (#5)
  - [x] 1.2.5 PreVote (D-08): pre-candidate doesn't bump term → test: rejoining partitioned node doesn't raise term (#5)
  - [x] 1.2.6 CheckQuorum (D-08): leader steps down without majority contact, followers ignore votes while they have a live leader → unit tests (#5)
- [x] **1.3 Log replication**
  - [x] 1.3.1 AppendEntries send with prevIndex/prevTerm from `Progress.next` → unit test (#5)
  - [x] 1.3.2 Follower consistency check, conflict truncation, append → tests incl. Figure 7 logs (#5)
  - [x] 1.3.3 Reject with conflict hint and fast leader backtracking → test: large divergence resolves in a few round trips (#5)
  - [x] 1.3.4 `Progress` tracking (match, next, Probe/Replicate) → unit tests (#5)
  - [x] 1.3.5 Commit rule: only current-term entries are committed by counting replicas; followers take commit from leader → Figure 8 test passes (#5)
  - [x] 1.3.6 Committed entries exposed in `Ready`, applied index advanced on `Advance` → unit test (#5)
- [x] **1.4 Proposals**
  - [x] 1.4.1 `Propose` on leader appends to log, `ErrProposalDropped` when there is no leader → unit tests (#5)
  - [x] 1.4.2 Follower proposal returns a not-leader error with a leader hint (forwarding is added in M4, D-28) → unit test (#5)
  - [x] 1.4.3 Uncommitted-size limit (D-29) and per-message size limit → tests: proposals rejected past the limit (#5)
- [x] **1.5 Deterministic simulator (test/sim)**
  - [x] 1.5.1 Seeded PRNG, virtual clock, event loop driving N `RawNode`s → same seed gives identical trace (hash compare) (#7)
  - [x] 1.5.2 Virtual network: delay, drop, duplicate, reorder per link → unit tests on the network model (#7)
  - [x] 1.5.3 Partition scheduler (random splits, isolate leader, heal) → scenario test (#7)
  - [x] 1.5.4 Crash/restart model: lose unpersisted state, keep HardState and stable log → scenario test (#7)
  - [x] 1.5.5 Client workload generator plus a reference model of the applied sequence → planted divergence detected (#7)
- [x] **1.6 Invariant checker**
  - [x] 1.6.1 Election Safety and Leader Append-Only → planted bugs detected (#7)
  - [x] 1.6.2 Log Matching and Leader Completeness → planted bugs detected (#7)
  - [x] 1.6.3 State Machine Safety and commit-index never decreases → planted bugs detected (#7)
  - [x] 1.6.4 Liveness check: after heal, a leader is elected and new proposals commit → per-seed result reported (#7)
- [x] **1.7 Simulator tooling and CI**
  - [x] 1.7.1 Flags `-seeds=N -start=S -faults=profile`; a failing seed prints a repro command → manual check (#7)
  - [x] 1.7.2 Trace dump on failure (JSONL) and a pretty-printer → failing seed gives a readable trace (#7)
  - [x] 1.7.3 Regression seeds in `test/sim/regressions/`, replayed on every PR → planted regression seed runs in CI (#7)
  - [x] 1.7.4 Add a 500-seed sim job to `pr.yml` → green (#7)
  - [x] 1.7.5 Create `.github/workflows/nightly.yml` with a long sim job (100,000+ seeds) → first nightly green (#7)
- [x] **1.8 Scenario tests**
  - [x] 1.8.1 Raft paper Figure 7 divergent logs converge, Figure 8 commit hazard handled → tests pass (#5)
  - [x] 1.8.2 Disruptive rejoin with PreVote on vs off (off shows term inflation) → both behaviors asserted (#5)
  - [x] 1.8.3 Leader isolation with CheckQuorum → step-down asserted (#5)
- [x] **1.9 M1 exit review**
  - [x] 1.9.1 Run 10,000 seeds with the full fault profile, race detector clean → 0 violations, CI run linked (#25; nightly sim-race run 45302500294: 10,000 seeds, full profile, -race, 0 violations)
  - [x] 1.9.2 Update architecture.md / system-design.md Raft core sections to match the code → diff reviewed (#25)
  - [x] 1.9.3 Update CHANGELOG.md and memory.md (core API, sim usage, bugs the sim found), tag `v0.1.0-alpha` → diff reviewed, tag exists (#25) (tag `v0.1.0-alpha`)

---

## 2. M2 — WAL + gRPC transport + real 5-process cluster

- [x] **2.1 WAL (internal/wal)**
  - [x] 2.1.1 Record format `[len|type|crc32c|payload]` encoder/decoder → round-trip test plus fuzz seeds (#4)
  - [x] 2.1.2 Segment files, rollover at a size limit, directory fsync on create → test: rollover at boundary (#4)
  - [x] 2.1.3 Append entries and HardState records, `Sync()` with fsync (`F_FULLFSYNC` on macOS) → tests (#4)
  - [x] 2.1.4 Open/replay: rebuild entries and last HardState, handle entries overwritten by a conflict → replay tests (#4)
  - [x] 2.1.5 Tail repair: cut a torn final record, refuse to start on corruption elsewhere (D-33) → tests (#4)
  - [x] 2.1.6 Fault-injecting file shim (truncate/corrupt at offset, fail fsync) → shim unit tests (#4)
  - [x] 2.1.7 Crash matrix: every byte offset of the last 2 records across many random logs → 0 silent corruptions (#4)
  - [x] 2.1.8 fsync or write error crashes the node, never retried (D-33) → test asserts the exit path (#4)
  - [x] 2.1.9 `ReleaseTo(index)` deletes whole old segments (used in M5) → unit test (#4)
  - [x] 2.1.10 WAL implements Raft `Storage` → same Storage test suite as `MemoryStorage` passes (#6)
- [x] **2.2 Transport (internal/transport)**
  - [x] 2.2.1 gRPC server for the Raft service with message-size limits → server starts, rejects oversize messages (#6)
  - [x] 2.2.2 One outbound `Replicate` stream per follower with a send queue (D-12) → test with in-process peers (#6)
  - [x] 2.2.3 Flow control: max 256 in-flight messages per follower (D-29) → unit test (#6)
  - [x] 2.2.4 Reconnect with backoff and jitter, report unreachable peers to the core → test: peer restart reconnects (#6)
  - [x] 2.2.5 `RequestVote` RPC with pre-vote flag (D-36) → test (#6)
  - [x] 2.2.6 Static peer address book from config, DNS re-resolved on reconnect → test (#6)
- [x] **2.3 Node loop and server (internal/node)**
  - [x] 2.3.1 Driver loop: tick → `Tick`, inbound → `Step`, `Ready` → persist, send, apply, `Advance` (D-35) → 3 in-process nodes commit entries (#6)
  - [x] 2.3.2 Temporary in-memory applier plus a debug `Propose` RPC behind a build tag → 5 nodes commit 10k entries (#6)
  - [x] 2.3.3 Config from YAML file plus flags with validation → table tests (#6)
  - [x] 2.3.4 Bootstrap from config, node ID saved in the data dir, refuse ID mismatch → tests (#6)
  - [x] 2.3.5 Graceful shutdown on SIGTERM/SIGINT → test (#6)
  - [x] 2.3.6 `raftkvctl debug wal-dump` to compare logs across nodes → prints entries from a data dir (#6)
- [x] **2.4 Observability (D-17)**
  - [x] 2.4.1 `log/slog` JSON logs with `node_id`, `term`, `component` fields → log sample in test (#6)
  - [x] 2.4.2 `/metrics` with Raft metrics (term, state, leader changes, commit/applied index) → asserted in a test (#6)
  - [x] 2.4.3 WAL fsync timing and transport metrics (sent, failures, reconnects) → asserted (#6)
  - [x] 2.4.4 OTel tracing with spans `raft.propose`, `wal.fsync`, `apply` → test with an in-memory exporter (#6)
- [x] **2.5 Packaging and local cluster**
  - [x] 2.5.1 Multi-stage Dockerfile (non-root, static binary) → image builds, runs `--version` (#8)
  - [x] 2.5.2 `deploy/compose/docker-compose.yml` with 5 nodes plus Prometheus, and `make cluster-up/down/logs` → cluster elects a leader (#8)
- [x] **2.6 E2E harness (test/e2e)**
  - [x] 2.6.1 Go e2e framework: start/stop compose, find leader, wait helpers → smoke test passes (#8)
  - [x] 2.6.2 Fault driver: `kill -9`, restart, SIGSTOP/SIGCONT → each fault works on one container (#8)
  - [x] 2.6.3 Fault driver: iptables partition and heal, `tc netem` delay/loss → partition test passes (#8)
  - [x] 2.6.4 Seeded fault schedule with a timestamped fault log → fault log uploaded as a CI artifact (#8)
  - [x] 2.6.5 Tests: leader elected on start, new leader after leader kill, minority stops committing and logs match after heal → pass (#8)
  - [x] 2.6.6 Test: 200 rounds of kill -9 and restart, no committed entry lost → passes (#8, #11; nightly e2e-long run 45293603026: 200 rounds, 400 acked entries on all 5 nodes)
  - [x] 2.6.7 Add the short e2e job to `pr.yml` and a long e2e job to `nightly.yml` → both green (#8, #11; e2e-short green on main, nightly e2e-long green in run 45293603026)
- [x] **2.9 M2 exit review**
  - [x] 2.9.1 Re-run sim 10,000 seeds and link evidence for every M2 exit criterion → all linked (#25; evidence in implementation-roadmap.md M2)
  - [x] 2.9.2 Update storage-design.md (WAL as built), system-design.md (node loop), ci-e2e.md (jobs) → diffs reviewed (#25)
  - [x] 2.9.3 Update CHANGELOG.md and memory.md (WAL format, fsync gotchas, e2e tips), tag `v0.1.0` → diff reviewed, tag exists (#25) (tag `v0.1.0`)

---

## 3. M3 — Pebble state machine + basic KV

- [x] **3.1 Pebble state machine (internal/storage)**
  - [x] 3.1.1 Open Pebble with its WAL off (D-02, D-03) → opens and closes cleanly (#9)
  - [x] 3.1.2 Key prefixes for user data, sessions, and metadata, plus a schema version key → encoding tests (#9)
  - [x] 3.1.3 Apply batch: Put/Delete plus `appliedIndex` in the same batch (D-04) → unit test (#9)
  - [x] 3.1.4 Track the durable applied index with a flush listener → test: it advances only after a flush (#9)
  - [x] 3.1.5 WAL truncation rule (D-30): flush first, never `ReleaseTo` past the durable applied index → test: truncation never passes it (#9)
  - [x] 3.1.6 Recovery: read `appliedIndex`, replay the rest from the WAL → crash test (#9)
  - [x] 3.1.7 Crash suite: 500 kill -9 during apply, state hash vs reference model; plus truncation-safety test with unflushed memtables → 0 mismatches (#9)
  - [x] 3.1.8 `raftkvctl debug state-hash` → same hash on all replicas at the same applied index (#9)
- [x] **3.2 Commands and KV service**
  - [x] 3.2.1 Command envelope proto (type, payload, request ID) in log entries → round-trip test (#9)
  - [x] 3.2.2 Proposal tracking: request ID → waiter, resolved on apply, failed with a retryable error on leader change → unit tests (#9)
  - [x] 3.2.3 `Put` and `Delete` handlers (validate, propose, wait for apply) → integration tests (#9)
  - [x] 3.2.4 `Get` through the log, and `NOT_LEADER` errors with a leader hint → integration tests (#9)
- [x] **3.3 Client library (pkg/client)**
  - [x] 3.3.1 Connect to all endpoints, cache the leader, follow `NOT_LEADER` hints → unit test with fake servers (#9)
  - [x] 3.3.2 Retry on retryable codes with backoff, bounded by the deadline → unit tests (#9)
  - [x] 3.3.3 `raftkvctl put/get/del` using the client library → e2e smoke (#9)
- [x] **3.4 Linearizability harness (test/linearizability)**
  - [x] 3.4.1 History recorder (invoke/return times, client, op, result, unknown on timeout) → unit test (#9)
  - [x] 3.4.2 Porcupine KV model split per key, checker with a timeout (timeout = fail), HTML view on failure → known-bad history detected (#9)
  - [x] 3.4.3 Wire the recorder into e2e with leader kill, follower kill, and partition → checked histories, 0 violations (#9 in-process, #18 docker e2e, #23 kind chaos with pod kills; 0 violations)
- [x] **3.9 M3 exit review**
  - [x] 3.9.1 Link evidence for every M3 exit criterion; update storage-design.md (key schema, durability) and api-design.md (implemented RPCs) → linked, diffs reviewed (#25; evidence in implementation-roadmap.md M3)
  - [x] 3.9.2 Update CHANGELOG.md and memory.md, tag `v0.2.0` → diff reviewed, tag exists (#25) (tag `v0.2.0`)

---

## 4. M4 — Linearizable reads, client sessions, full KV API

- [x] **4.1 ReadIndex (Raft core)**
  - [x] 4.1.1 `ReadIndex` request: record commit index, require a current-term commit first → unit test (#14)
  - [x] 4.1.2 Confirm leadership with a heartbeat quorum, batching concurrent requests → test: many readers share one round (#14)
  - [x] 4.1.3 `ReadStates` in `Ready`, and follower ReadIndex via the leader (D-36) → unit and sim tests (#14)
  - [x] 4.1.4 Sim: a partitioned old leader never completes a ReadIndex after a new leader exists; a read right after election waits for the no-op → 10,000 seeds pass (#14)
- [x] **4.2 Read path**
  - [x] 4.2.1 Wait until `appliedIndex ≥ readIndex`, deadline-aware → unit test (#14)
  - [x] 4.2.2 `Get` via ReadIndex on the leader and on followers (D-05) → e2e tests (#14)
  - [x] 4.2.3 Opt-in stale reads that return the applied index → test (#14)
  - [x] 4.2.4 Optional lease reads behind a flag, off by default (D-25) → unit tests (#14)
  - [x] 4.2.5 Lease sim: violation found when drift is beyond the bound, none within it → both asserted (#14)
- [x] **4.3 Client sessions (D-09, D-26, D-27)**
  - [x] 4.3.1 `RegisterSession` through the log, server assigns the ID, stored under the session prefix → integration test (#15)
  - [x] 4.3.2 Dedup on apply: `(client_id, seq)` check, cached response, update in the same batch → unit test (#15)
  - [x] 4.3.3 Leader stamps time into each command; replicas expire sessions after 10 minutes using it → test: replicas agree (#15)
  - [x] 4.3.4 Client library: auto-register, increasing sequence, retry with the same sequence → unit test (#15)
  - [x] 4.3.5 Exactly-once e2e: every write retried 1–3 times after random drops, counter model → 0 duplicates (#15)
  - [x] 4.3.6 Session sim: 10,000 seeds crossing the expiry time, identical session table hash → passes (#15)
- [x] **4.4 Full KV API (D-10, D-28, D-34)**
  - [x] 4.4.1 Follower forwards writes to the leader; client can opt in to a redirect instead (D-28) → e2e test (#15)
  - [x] 4.4.2 `CompareAndSwap` per api-design.md → unit and e2e tests (#15)
  - [x] 4.4.3 `Range` with prefix or [start, end), page limit, continuation token, linearizable or stale per page → tests incl. paging across writes (#15)
  - [x] 4.4.4 `raftkvctl cas/range` → e2e smoke (#15)
- [x] **4.5 Limits and errors (D-11)**
  - [x] 4.5.1 Key ≤ 4 KiB, value ≤ 1 MiB, Range page ≤ 1,000 keys / 4 MiB, checked before propose → boundary tests (limit ok, limit + 1 rejected) (#15)
  - [x] 4.5.2 gRPC status codes and error details per api-design.md, deadlines passed through → table tests (#15)
- [x] **4.6 Linearizability coverage**
  - [x] 4.6.1 Porcupine models for CAS and Range → known-bad histories detected (#18)
  - [x] 4.6.2 e2e mixing Put/Get/Delete/CAS/Range with leader and follower reads under leader kill, pause, partition → 0 violations, runs nightly (#18)
- [x] **4.9 M4 exit review**
  - [x] 4.9.1 Link evidence for every M4 exit criterion; update api-design.md (final API) and system-design.md (read path, sessions) → linked, diffs reviewed (#18, #25; evidence in implementation-roadmap.md M4)
  - [x] 4.9.2 Update CHANGELOG.md and memory.md, tag `v0.3.0` → diff reviewed, tag exists (#25) (tag `v0.3.0`)

---

## 5. M5 — Snapshots + follower catch-up

- [x] **5.1 Raft core snapshot support**
  - [x] 5.1.1 `Storage.Snapshot()` / `ApplySnapshot`, compaction index in `raftLog` → unit tests (#17)
  - [x] 5.1.2 Leader sends a snapshot when the follower needs compacted entries (Progress Snapshot state) → unit tests (#17)
  - [x] 5.1.3 Follower handles a snapshot (reject stale, reset log, update commit) → unit tests (#17)
  - [x] 5.1.4 Sim: low snapshot threshold plus crashes during install → 10,000 seeds pass (#17)
- [x] **5.2 Snapshot creation (internal/snapshot)**
  - [x] 5.2.1 Metadata `{index, term, membership, format_version, checksum}` (D-14) → round-trip test (#17)
  - [x] 5.2.2 Pebble checkpoint taken between apply batches so its index equals `appliedIndex` → test asserts equality (#17)
  - [x] 5.2.3 Trigger every 100k entries (configurable), keep the last 2, clean up temp dirs on start → tests with small thresholds (#17)
- [x] **5.3 WAL compaction**
  - [x] 5.3.1 Release WAL segments under the D-30 rule, with a small retention tail for slow followers → unit test (#17)
  - [x] 5.3.2 Steady-write run: WAL disk use stays bounded → result recorded (#17)
- [x] **5.4 Transfer and install**
  - [x] 5.4.1 Sender: `InstallSnapshot` stream in 1 MiB chunks → integration test (#17)
  - [x] 5.4.2 Receiver: stage chunks in a temp dir, verify checksum; an interrupted transfer starts over (D-14) → corrupt chunk rejected, killed transfer restarts (#17)
  - [x] 5.4.3 Install: close Pebble, atomic dir swap (rename plus dir fsync), reset WAL to the snapshot index, reopen → test (#17)
  - [x] 5.4.4 Crash safety: kill -9 at each install step → always old or new state, never a mix (#17)
  - [x] 5.4.5 Session table restored from the snapshot; exactly-once test across a snapshot boundary → passes (#17)
- [x] **5.5 Recovery scenarios (e2e)**
  - [x] 5.5.1 Wiped follower rejoins via snapshot and its state hash matches (20 runs); long-partitioned follower catches up past compaction → all pass (#17)
  - [x] 5.5.2 Add follower-wipe to the e2e fault driver and nightly; linearizability still passes → 0 violations (#17)
- [x] **5.9 M5 exit review**
  - [x] 5.9.1 Link evidence for every M5 exit criterion; add snapshot metrics (size, duration, installs) and update storage-design.md → linked, diff reviewed (#17, #25; evidence in implementation-roadmap.md M5)
  - [x] 5.9.2 Update CHANGELOG.md and memory.md, tag `v0.4.0` → diff reviewed, tag exists (#25) (tag `v0.4.0`)

---

## 6. M6 — Membership changes + leadership transfer

- [x] **6.1 Raft core membership**
  - [x] 6.1.1 Config-change entry (add learner, promote, remove) with versioned encoding → round-trip test (#22)
  - [x] 6.1.2 Config takes effect when appended, rolled back if the entry is truncated (D-31) → unit tests (#22)
  - [x] 6.1.3 One pending change at a time, first change only after a current-term commit → unit test plus sim that reproduces the bug without the fix (#22)
  - [x] 6.1.4 Learners replicate but don't vote or count toward quorum → unit tests (#22)
  - [x] 6.1.5 Leader removal: step down after committing its own removal; messages from removed nodes ignored (D-31) → unit and sim tests (#22)
  - [x] 6.1.6 Membership saved in WAL and snapshot metadata → restart test (#22)
- [x] **6.2 Leadership transfer**
  - [x] 6.2.1 `TransferLeadership(target)`: pause proposals, catch up target, send `TimeoutNow` → unit test (#22)
  - [x] 6.2.2 Abort after one election timeout and resume; drop the lease first if lease reads are on → unit tests (#22)
  - [x] 6.2.3 SIGTERM on the leader transfers leadership before shutdown → e2e test (#22)
- [x] **6.3 Sim coverage**
  - [x] 6.3.1 Random membership changes mixed with crashes and partitions, with a "no two disjoint majorities" invariant → 10,000 seeds pass, planted bug detected (#22)
- [x] **6.4 Cluster admin service (D-23)**
  - [x] 6.4.1 Random 64-bit member IDs, never reused (D-38) → unit test (#22)
  - [x] 6.4.2 Node with an empty disk refuses its old ID and must rejoin as a new learner (D-32) → test (#22)
  - [x] 6.4.3 `AddLearner` with dynamic peer registration in the transport → integration test (#22)
  - [x] 6.4.4 `PromoteLearner` with a catch-up check → refused when lagging, accepted when caught up (#22)
  - [x] 6.4.5 `RemoveMember`, `TransferLeadership`, `ListMembers`, `Status` handlers → integration tests (#22)
  - [x] 6.4.6 Second change while one is pending → rejected with `FAILED_PRECONDITION` (#22)
  - [x] 6.4.7 `raftkvctl member add-learner|promote|remove|transfer|list` → e2e smoke (#22)
- [x] **6.5 E2E scenarios**
  - [x] 6.5.1 Replace a node under load (learner → catch up → promote → remove old) → 0 linearizability violations (#22)
  - [x] 6.5.2 Scale 3 → 5 → 3 under load, plus membership changes in the nightly fault mix → 0 violations, nightly green (#22)
- [x] **6.9 M6 exit review**
  - [x] 6.9.1 Link evidence for every M6 exit criterion; write how-tos "replace a node", "scale 3 → 5", "transfer leadership" in system-design.md → linked, diff reviewed (#22 how-tos in system-design §7.6–7.8, #25; evidence in implementation-roadmap.md M6)
  - [x] 6.9.2 Update CHANGELOG.md and memory.md, tag `v0.5.0` → diff reviewed, tag exists (#25) (tag `v0.5.0`)

---

## 7. M7 — Kubernetes, observer UI, chaos tests, v1.0

- [x] **7.1 Kubernetes on kind (deploy/k8s, D-16)**
  - [x] 7.1.1 Kustomize base: StatefulSet (5 replicas, PVC template, non-root) → `kubectl apply --dry-run=server` passes (#13)
  - [x] 7.1.2 Headless Service for peer DNS and a client Service → DNS resolves in kind (#13)
  - [x] 7.1.3 Node ID from the pod ordinal, stable across restarts; config from a ConfigMap → restart keeps the ID (#13)
  - [x] 7.1.4 Readiness (knows leader, caught up) and liveness (driver loop responds) probes (D-46) → probe behavior checked in kind (#13)
  - [x] 7.1.5 `preStop` hook that transfers leadership, grace period sized for it → rolling restart test (#13, #22, #26: preStop sleep 2 s, raftkvd hands over on SIGTERM, grace period 30 s for a ~9 s worst case; rolling restart test in the chaos run)
  - [x] 7.1.6 `kind` overlay and `make kind-up` script → 5-node cluster passes the e2e smoke test (#13)
- [x] **7.2 Observer UI (D-48)**
  - [x] 7.2.1 `cmd/raftkv-observer`: polls `Cluster.Status` on all nodes every 500 ms and serves an embedded HTML page over SSE → page shows 5 node cards against the compose cluster (#12)
  - [x] 7.2.2 Event timeline (term, leader, snapshot changes, node down) inferred by diffing polls → leader kill appears in the timeline within 1 s (#12)
  - [x] 7.2.3 Observer added to the docker-compose stack and as a 1-replica k8s Deployment (localhost / port-forward, no auth) → reachable in both (#12)
- [x] **7.3 Metrics dashboard**
  - [x] 7.3.1 One Grafana dashboard (leader, term, commit/applied index, request rate and errors, fsync timing) in compose → JSON committed, imports cleanly (#12)
- [ ] **7.4 Chaos tests (test/chaos, D-47)**
  - [x] 7.4.1 Install Chaos Mesh on kind (`make chaos-up`) → Chaos Mesh pods running (#13)
  - [x] 7.4.2 `PodChaos` pod-kill manifests → applied in kind (#13)
  - [x] 7.4.3 `NetworkChaos` partition, delay, and loss manifests → applied in kind (#13)
  - [x] 7.4.4 Chaos driver: run a fault schedule while recording histories, then run Porcupine → one automated run, 0 violations (#23; nightly chaos run 45299681977: 925,159 ops, 0 violations)
  - [x] 7.4.5 Rolling restart of the StatefulSet under load → 0 violations, each pod hands off leadership (#26; nightly chaos run 45309611538: 3 rolling restarts, 4 handovers in 36-68 ms, 1,104,335 ops, linearizable)
  - [ ] 7.4.6 Add the kind + Chaos Mesh job to `nightly.yml` → 3 green nights in a row
    (partial #23, #26: job added; green on dispatch, runs 45297525546, 45299681977 and 45309611538; the first scheduled
    night with the chaos job is the 03:17 UTC run after #26, so 3 green nights in a row cannot be shown yet)
- [x] **7.5 Hardening**
  - [x] 7.5.1 Fuzz tests for the WAL record decoder, snapshot metadata, and command envelope, run in nightly → 0 crashers (#21)
  - [x] 7.5.2 On-disk format version check (WAL, snapshot, Pebble schema), refuse unknown versions → tests (#21)
- [ ] **7.6 Documentation**
  - [x] 7.6.1 Configuration reference matching `--help` → doc matches flags (#21)
  - [x] 7.6.2 Final pass over architecture.md, system-design.md, api-design.md, storage-design.md, testing.md, ci-e2e.md → each matches v1.0 (#27; also configuration.md, design-decisions.md, raftkv-in-5-minutes.md, simple-guide.md)
  - [ ] 7.6.3 README quick start (compose and kind) tried on a clean machine; known limitations listed (one Raft group, no Watch/transactions, no TLS/auth) → verified
    (partial #27: quick start and known limitations written, every target and port checked against the Makefile,
    compose file and manifests; the clean-machine trial is still to do)
- [ ] **7.9 M7 exit review and v1.0**
  - [ ] 7.9.1 Link evidence for every M7 exit criterion → linked
    (partial #27: criteria 1-4 linked in implementation-roadmap.md M7; 5 (3 green nights) open with 7.4.6, 6 with 7.9.2)
  - [ ] 7.9.2 Walk the v1.0 Definition of Done (roadmap §5), every box linked → all checked
    (partial #27: walked in implementation-roadmap.md §5; open: 3 green nights (7.4.6), the clean-machine quick start
    (7.6.3) and a live look at the observer on compose and kind)
  - [x] 7.9.3 Update CHANGELOG.md (`1.0.0` section) and memory.md (final state, lessons learned) → diff reviewed (#27)
  - [ ] 7.9.4 Tag `v1.0.0` and write GitHub release notes from the CHANGELOG → release page live

---

## post-1.0 (deferred)

Items moved here keep their original number and get a one-line reason.

- (no WBS leaf deferred.) Out of scope by decision: joint consensus (D-06), sharding (D-07), `Watch` and transactions
  (D-10). Removed with the learning-project re-scope: performance work, TLS/auth, release signing, rolling upgrades.
- v1 limitation, not a WBS leaf: membership changes on Kubernetes (scaling the StatefulSet, replacing a lost PVC). Pod
  ordinals give fixed node IDs while `AddLearner` assigns random ones (D-38); it would need a per-pod ID source such as
  an ID file on the PVC written by an init step (system-design.md 7.9).
