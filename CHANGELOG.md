# Changelog

All notable changes to this project are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versioning: [SemVer](https://semver.org/).

## [Unreleased]

### Added
- 2026-11-21: Initial design-phase documentation: `README.md`, `docs/design-decisions.md` (decisions D-01–D-24,
  performance targets, open questions), `docs/architecture.md`, `docs/system-design.md`, `docs/api-design.md`,
  `docs/storage-design.md`, `docs/testing-benchmarks.md`, `docs/ci-e2e.md`, `docs/implementation-roadmap.md`.
- 2026-11-21: Project management files: `WBS.md` (implementation checklist), `CHANGELOG.md`, `memory.md`,
  `Agent.md` (AI agent instructions), `CLAUDE.md`.
- 2026-11-21: `docs/simple-guide.md`, a plain-English overview for non-specialists.
- 2026-11-21: `docs/raftkv-in-5-minutes.md`, a one-page summary of the whole design. Added D-48 (read-only observer frontend) and WBS tasks 8.3.11–8.3.13.
- 2026-11-21: Decisions D-25 to D-47, merged from the open questions raised in the topic docs. D-30 (WAL truncation
  invariant) is Decided. D-25 to D-47 amend D-11, D-12, D-23, and D-24.

### Fixed
- 2026-11-21: Aligned CI job and make-target names across `implementation-roadmap.md`, `WBS.md`, and `ci-e2e.md`.
