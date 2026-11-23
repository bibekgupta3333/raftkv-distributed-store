# memory.md

Context and reasoning that isn't obvious from the docs or git history. Newest first.

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
