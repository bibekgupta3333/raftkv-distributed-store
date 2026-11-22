# memory.md

Running project memory: the context and reasoning that isn't obvious from the docs or git history.
Newest entries first. Dates are absolute.

## 2026-11-21 — Project kickoff (design phase)

- The project brief came as résumé-style bullets claiming ~38K writes/s, ~92K reads/s, 40M ops / 1,500 faults
  validated, and a 31% p99 improvement. **None of this has been measured.** These are recorded as *targets* in
  `docs/design-decisions.md`. The p99 goal is phrased as "≥ 30% vs the M3 baseline" so it can be checked.
- "Implementing leader election…" in the brief suggests writing our own Raft (D-01, Proposed), not wrapping
  etcd/raft. The core is designed etcd/raft-style (pure state machine + `Ready` loop) so swapping stays possible if
  the schedule slips.
- Pebble's WAL is disabled because the Raft log is already the durable source of truth. Correctness then depends
  on writing `appliedIndex` atomically with applied data (D-03, D-04). This is the most important storage invariant.
- Scope kept to a single Raft group (D-07). Multi-Raft would double the project's size without adding much to the
  goal of showing correct consensus.
- Design docs were drafted by parallel agents from the shared baseline in `design-decisions.md`. Each doc ends with
  a "New open questions" section. Merge those into `design-decisions.md` once reviewed.

- The most important finding of the design review: with Pebble's WAL off, truncating the Raft WAL past data
  Pebble has not flushed loses data silently after a crash. This is now D-30 (INV-TRUNC), the only `Decided` entry
  added during review. Three of the four doc agents found it independently.
- Session TTL needs a time that every replica agrees on, which collided with D-24's "no wall clock" rule. Resolved
  by having the leader stamp the time into each replicated entry (D-27).
- A member that comes back with an empty disk under its old ID could vote twice in the same term. It must refuse to
  start (D-32).

## Open items to remember

- Resolve the open decisions D-01, D-06, D-07, D-15, D-20, D-21, D-22 with the user before M0; D-37, D-40, D-44 later.
- Batch-review the Proposed entries D-25 to D-47.
- The Mermaid diagrams and proto blocks in the docs have not been rendered or linted yet.
