# Contributing

- Branch from `main`: `feat/<topic>`, `fix/<topic>`, `docs/<topic>`. Open a PR; merge when CI is green.
- Commit style: Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `chore:`).
- Binding decisions live in `docs/design-decisions.md` (`D-xx`). Don't contradict them silently.
- Add a `CHANGELOG.md` entry under `[Unreleased]`; record non-obvious reasoning in `memory.md`.
- The Raft core (`internal/raft`) does no I/O and never reads the wall clock (D-01, D-24).
- Run `make all` before pushing.
