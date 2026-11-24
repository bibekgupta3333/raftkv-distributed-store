# memory.md

Context and reasoning that isn't obvious from the docs or git history. Newest first.

## 2026-11-24 — Exit reviews M0–M6, membership, chaos, hardening (PRs #21–#24)

- **Exit reviews found doc drift, not code bugs.** The design docs still described the plan: separate client/peer
  listeners (built: one gRPC listener, `:7000`), an async applier goroutine (built: apply runs inline in the Ready
  loop), `META` + `flock` (built: `IDENTITY` JSON; Pebble's `LOCK` is the only two-process guard), protobuf WAL
  payloads (built: raw bytes), and a CI layout with `pr-gate` and `report-failures` that was never built.
- **"100 leader kills" has no dedicated test.** It was counted from the nightly e2e-long artifacts: match each
  `kill-9` in the kill-restart fault log against the latest "raft state changed ... Leader" line in the node logs
  (96 of 200 rounds), plus the Porcupine leader kills. Re-count the same way if the evidence is ever questioned.
- **The proto drift check was proven with a throwaway PR (#24)** that hand-edited `kv.pb.go`; the `check` job failed
  at `make proto && git diff --exit-code`. A dispatchable workflow would have been cheaper; `pr.yml` has no
  `workflow_dispatch`.
- **Tags lag milestones on `main`.** M3 broke the compose e2e (#9), so `v0.2.0` goes on the #11 fix. M4's checker
  coverage (#18) merged after M5 (#17), so `v0.3.0` is the last green commit before M5 and #18 ships in `v0.4.0`.
- **Tombstones use the committed config (D-31).** A leader removing itself still needs followers to accept the
  appends that commit the removal; filtering on the appended config broke exactly that. Its final `TimeoutNow` is
  exempt (#22).
- **Wiped-node detection is an explicit incarnation, not inference (D-32).** The M5 "reject at the acked index ⇒
  wiped" rule gives false positives under reordering (the sim reorders, gRPC streams don't), and making it sticky
  broke liveness in the sim. Each data dir has a random incarnation in gRPC metadata; peers remember it in memory
  only. The core keeps the harmless reset-match-and-probe (#22).
- **The bootstrap member table in Pebble holds no addresses.** Nodes may know peers by different addresses (kvtest
  proxies), which broke state-hash equality; each node fills addresses from its own peer list (#22).
- **ConfigOverlap is checked per leader**, not pairwise over all nodes: a stale node can hold a truncated config two
  steps away but cannot win an election (#22).
- **A stopping leader hands over and the new leader appends a no-op.** Tests that compare logs or hashes must stop
  followers first. Anything polling `Node.Status()` after a loop request must expect it to lag the core by one
  Ready (#22).
- **The chaos workload runs in a pod**, not behind `kubectl port-forward` (a forward dies with its pod). The image is
  FROM scratch, so the host drives it with `kubectl exec ... raftkv-chaos stop|cat|leader`, and the pod idles after
  writing the history because a completed pod cannot be exec'd into (#23).
- **Prove each chaos fault took effect.** `readyReplicas` alone can't tell "recovered" from "not killed yet"; the
  driver waits for killed pods' UIDs to change and for `AllInjected` on network faults (#23).
- **gRPC's DNS resolver re-resolves at most every 30 s**, so a client pinned to a just-killed member fails reads for up
  to 30 s. Harmless: failed reads are dropped from the history (#23).
- **Format checks:** the WAL version is checked before the header CRC (a newer layout may move the CRC).
  `snapshot.Open` must tell "never completed" (delete) from "complete but unknown format" (refuse). `yaml.v3`
  ignores anything after `---` unless you decode again and demand `io.EOF`. A new flag in any binary needs a row in
  `docs/configuration.md` or `make test` fails (#21).

## 2026-11-24 — Sessions, snapshots, linearizability models (PRs #15, #17–#19)

- **The session clock is replicated state** (`m/sessionClock`, max of leader-stamped times). Without it a new
  leader with a slow wall clock would move session time backwards. Expiry runs *before* the command it rides on.
- **Indeterminate is sticky in the client.** If any attempt of a write was indeterminate, the final error must be
  too, even if the last attempt failed definitely (e.g. SESSION_EXPIRED). Porcupine depends on this.
- **Sessions hide faults from the checker.** With sessions on, the client retries away almost every indeterminate
  write, so keep one or two `DisableSessions` workers in every checked workload.
- **A forwarded reply carries the follower's `member_id`**, not the leader's, or the client's endpoint map breaks.
- **A checkpoint must flush first.** With Pebble's WAL off, unflushed memtables aren't in the checkpoint.
- **Snapshot install discards the whole log**, so the WAL marker needs a reset flag. After finishing an install,
  recovery must raise `HardState.Term` to the snapshot's term.
- **Wiping a follower under the same ID breaks the leader's `match`.** Ignore a heartbeat commit beyond the last
  index, and treat a rejection at exactly `match` as lost state. Still unsafe if the node had voted (D-32 → M6).
- **Range pages are checked as one atomic read of `[start, lastKey]`**, not per key; per-key checks miss a page
  that mixes two points in time. Fresh keys per fault cycle keep long histories cheap to check.
- **Don't push 1,000+ writes through Raft in a unit test.** Under -race on CI runners it took over 90 s (#19). Test
  storage-level limits at the store.
- **Local disk filled up during parallel agent work**, which corrupted Docker Desktop's image store. Agents must
  keep build and fuzz caches small, and only one compose e2e can run per Docker host.

## 2026-11-23 — M4 reads, observer, Kubernetes (PRs #11–#14)

- **Never put raw bytes in the log.** Since M3 an entry that doesn't decode as a `Command` is fatal on every replica
  (D-33). Anything that bypasses the KV service (debug RPC, test harnesses) must propose an encoded `Command`. This
  is what broke e2e after M3 (#11).
- **Only one docker-compose e2e run per Docker host.** Container names, ports and volume names are hard-coded, so
  `COMPOSE_PROJECT_NAME` can't isolate parallel runs.
- **ReadIndex messages carry no term on purpose.** The leader confirmed leadership *after* the request arrived, so
  the answer stays valid even if the follower's term moved on. A read must never join a heartbeat round already in
  flight: that round's index can miss a write acked in between (`TestReadIndexLaterRequestGetsLaterIndex`).
- **Lease reads are fragile.** The lease uses `ElectionTick-1` ticks because a follower can tick right after
  resetting its timer, and a lease does not survive a voter restart (a restarted node votes immediately). That is
  why lease reads stay off by default and the `lease` sim profile has no crashes.
- **Kubernetes bootstrap.** The headless peer Service needs `publishNotReadyAddresses: true` and the StatefulSet
  `podManagementPolicy: Parallel`; otherwise readiness (which needs a leader) deadlocks. Liveness must never depend
  on leadership, because a node in a minority partition is healthy.
- **Pebble's file lock is the only guard against two processes sharing a data dir** (pod-kill can start the
  replacement before the old container dies). The WAL has no lock of its own.
- **An ordinal-derived node ID is safe only while the PVC survives** (D-32). Disk replacement needs M6.
- **Observer gotchas.** Dial with `MaxDelay: 1s` or a restarted node shows DOWN for minutes (gRPC backoff). 64-bit
  IDs go to the page as JSON strings because JS numbers lose precision above 2^53.

## 2026-11-23 — M0–M3 built (PRs #2–#9)

- **Sim false positive, seed 9940 (0e3f6b1).** The Leader Completeness check first compared the *entry's* term
  with the leader's term. An old-term entry can be committed later by a newer leader, so a leader of an
  intermediate term need not hold it. The check now uses the *commit* term (observer's term when the commit was
  first seen, made monotone). Seed 9940 is in `test/sim/regressions/seeds.txt`. The sim found no bug in
  `internal/raft` itself; when a seed fails, suspect the checker as well as the core.
- **`debugpropose` build tag.** M2 needed a way to put raw entries in the log before the KV API existed. The debug
  `Propose` RPC is hand-registered (no proto) and only compiled with `-tags debugpropose`, so a normal binary
  cannot inject entries. The e2e image (`make e2e-image`) is built with the tag; the release image is not.
  `test/e2e` repeats the method name as a string so it builds without the tag. The 5-node 10k-entry test also
  needs it: `go test -tags debugpropose ./internal/node/`.
- **WAL tail repair (D-33).** Only a bad record at the end of the *last* segment is a torn write and is cut on
  open. A CRC-valid record that breaks log invariants, corruption in an earlier segment, sequence gaps or header
  mismatches refuse to start. `raftkvctl debug wal-dump` opens the WAL like a real start, so it also repairs the
  tail: run it on stopped nodes only.
- **Durable applied index (D-30).** Pebble's flush listener cannot say which indexes a flush covered, so
  background flushes are only counted. `DurableApplied` is raised only at startup and by our own synchronous
  `Flush`, which reads the applied index *before* flushing. At start the node refuses to run if Pebble's applied
  index falls before the WAL's first entry (D-30 broken) or after its last (committed entries lost); the WAL's
  lazily written commit index may lag and is raised to the applied index.
- **No blind write retries before sessions.** Until M4 adds sessions, `pkg/client` does not retry a write whose
  outcome is unknown; the proposal tracker returns a retryable "indeterminate" error on leader change and the
  linearizability checker treats those writes as maybe-applied.
- **Heartbeat flag in `raft.proto`.** An empty `AppendEntries` cannot be told apart from a heartbeat on the wire,
  so both request and response carry `heartbeat`. On a reject, `match_index` carries the rejected prev index.
- **e2e container privileges.** Nodes run as non-root; faults (iptables, tc netem) run via `docker exec -u 0` with
  `NET_ADMIN`. Each fault run writes a timestamped log to `out/e2e/<test>/fault-log.txt`.
- **Linearizability run is in-process for now.** The Porcupine fault run uses a real 5-node gRPC cluster on
  localhost inside one test process, not the docker-compose cluster. Wiring it into `test/e2e` (WBS 3.4.3) is open.

## 2026-11-21 — Re-scoped as a learning project

- The user said speed doesn't matter; this is a learning project. Removed all performance targets, benchmarks, the
  performance milestone (old M7), and production-only work: mTLS, release signing, rolling upgrades, CI budget, and
  the 40M-op counting rules. The old M8 is now M7. The removed decision IDs are not reused.
- The correctness testing stays, because proving Raft correct *is* the learning: simulator, Porcupine checker,
  fault injection, nightly chaos.
- Everything must run on a laptop (Docker / kind).

## 2026-11-21 — Kickoff (design phase)

- Pebble's WAL is off because the Raft log is the source of truth. Two rules keep that safe: `appliedIndex` is
  written in the same batch as the data (D-04), and the WAL is never trimmed past Pebble's flushed data (D-30).
  Three design agents found the D-30 risk independently. It is the most important storage rule.
- Session expiry needs a time every replica agrees on, so the leader stamps the time into each entry (D-27).
- A node that comes back with an empty disk under its old ID could vote twice in one term. It must refuse to
  start (D-32).
- The observer UI (D-48) infers events by comparing successive status polls, so blips under 500 ms can be missed.
