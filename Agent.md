# Agent.md

Instructions for AI coding agents (Claude Code, Codex, etc.) working in this repository.

## Current phase

**Design only.** Do not write implementation code (Go, `.proto` files, workflow YAML, k8s manifests) until the
user explicitly starts a milestone *and* every `Open` decision blocking it in
[docs/design-decisions.md](docs/design-decisions.md) has been resolved.

## Sources of truth (read in this order)

1. `docs/design-decisions.md`: binding decisions `D-xx`. Never contradict it silently. To change a decision, add or
   modify its entry, note it in `CHANGELOG.md`, and update the docs that cite it.
2. `docs/implementation-roadmap.md`: milestones M0–M8 and their acceptance criteria.
3. `WBS.md`: the task checklist. Tick boxes in the same change that completes the work.
4. The topic docs: `architecture.md`, `system-design.md`, `api-design.md`, `storage-design.md`,
   `testing-benchmarks.md`, `ci-e2e.md`.

## Rules

- **Never invent results.** Benchmark and correctness numbers are targets until a recorded run on the reference
  hardware (D-20) produces them. Put real results in `docs/testing-benchmarks.md` with the run's date, commit, and
  hardware.
- **Cite decisions.** When code or docs rely on a decision, reference its ID (e.g. `// D-04: appliedIndex in same batch`).
- **Safety over speed.** Any change to the Raft core, WAL, apply path, or read path must keep the deterministic
  simulation and linearizability checks passing (D-18). Never weaken fsync or ordering rules for a benchmark win.
- **Raft core stays pure.** `internal/raft` performs no I/O and reads no wall clock (D-01, D-24).
- **One optimization per change** in M7 (D-13), each with a before/after benchmark using the methodology in
  `testing-benchmarks.md`.

## Housekeeping on every meaningful change

- `CHANGELOG.md`: add an entry under `[Unreleased]` (Keep a Changelog format).
- `memory.md`: record non-obvious context: why a decision went the way it did, gotchas, failed approaches.
  Don't duplicate what the docs or git history already say.
- `WBS.md`: tick completed items and update the progress table.

## Commands

None yet. The planned Makefile targets (`make test`, `make sim SEED=...`, `make e2e`, `make chaos`, `make bench`)
are described in `docs/ci-e2e.md`. Update this section when they exist.
