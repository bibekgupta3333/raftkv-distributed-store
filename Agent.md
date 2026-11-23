# Agent.md

Instructions for AI coding agents (Claude Code, Codex, etc.) working in this repository.

RaftKV is a **learning project**: Raft and a replicated KV store built from scratch in Go. Correctness and clarity
matter; speed does not. Don't add benchmarks, performance tuning, or production-hardening features unless asked.

## Current phase

**Implementation: M0–M3 done, M4–M7 in progress.** See the progress table in `WBS.md`.

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

Run `make help` for the full list. Variables in parentheses are optional.

- `make all`: fmt-check, lint, build, test
- `make tools`: download the pinned tools (`go tool` directives)
- `make fmt` / `make fmt-check`: format code / fail if it is not gofmt'd
- `make lint`: golangci-lint and buf lint
- `make build`: build `raftkvd` and `raftkvctl` into `bin/` with the version stamped
- `make test`: unit tests
- `make race`: unit tests with the race detector
- `make sim` (`SEEDS=500 SEED=1 FAULTS=full`): deterministic simulator
- `make proto`: regenerate protobuf code (commit the result, D-43)
- `make e2e-image`: build the e2e node image (`-tags debugpropose`, iptables, tc)
- `make cluster-up` / `make cluster-down` / `make cluster-logs`: 5-node docker-compose cluster plus Prometheus
- `make e2e` (`E2E_PROFILE=pr|nightly E2E_SEED=n E2E_ROUNDS=n`): docker-compose e2e tests
- `make clean`: remove build output
