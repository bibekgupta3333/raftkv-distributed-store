# memory.md

Context and reasoning that isn't obvious from the docs or git history. Newest first.

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
