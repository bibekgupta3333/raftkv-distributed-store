# Agent.md

Instructions for AI coding agents (Claude Code, Codex, etc.) working in this repository.

RaftKV is a **learning project**: Raft and a replicated KV store built from scratch in Go. Correctness and clarity
matter; speed does not. Don't add benchmarks, performance tuning, or production-hardening features unless asked.

## Current phase

**Design only.** Write no implementation code until the user starts a milestone.

## Sources of truth

1. `docs/design-decisions.md`: binding decisions (`D-xx`) and milestones M0–M7. Never contradict it silently. To
   change a decision, edit its entry, log it in `CHANGELOG.md`, and update the docs that cite it.
2. `docs/implementation-roadmap.md`: what each milestone builds and its exit criteria.
3. `WBS.md`: the task checklist. Tick boxes in the same change that completes the work.

## Rules

- **Correctness first.** Changes to the Raft core, WAL, apply path, or read path must keep the simulator and the
  linearizability checks passing (D-18). Never weaken fsync or ordering rules.
- **The Raft core stays pure.** `internal/raft` does no I/O and reads no wall clock (D-01, D-24).
- **Respect the WAL truncation rule (D-30).**
- Cite decision IDs in code where they apply (e.g. `// D-04: appliedIndex in same batch`).

## On every meaningful change

- `CHANGELOG.md`: add an entry under `[Unreleased]`.
- `memory.md`: record non-obvious reasoning, gotchas, and failed approaches.
- `WBS.md`: tick finished items and update the progress table.

## Commands

None yet. Planned: `make test`, `make sim SEED=...`, `make e2e`, `make chaos-up`, `make chaos`, `make chaos-down`
(see `docs/ci-e2e.md`).
