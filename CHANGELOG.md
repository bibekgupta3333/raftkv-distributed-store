# Changelog

All notable changes to this project are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versioning: [SemVer](https://semver.org/).

## [Unreleased]

### Changed
- 2026-11-22: Rewrote `README.md` with standard sections: overview, features, tech stack, getting started,
  documentation, contributing, license.
- 2026-11-21: **Re-scoped as a learning project.** Speed is no longer a goal. Milestones are now M0–M7: the old M7
  (performance) is gone and the old M8 is now M7. Most decisions are now Decided; only D-22 (license) is open.
  All docs are shorter. `WBS.md` went from 391 to 201 tasks and the estimate is about 25 weeks.
- 2026-11-21: Renamed `docs/testing-benchmarks.md` to `docs/testing.md`.
- 2026-11-21: Simplified the design: snapshots restart instead of resuming (no `SnapshotProgress`), disk-full
  crashes the node like any other write error, sessions use an explicit `RegisterSession`, and the leader fsyncs
  before replicating.

### Removed
- 2026-11-21: Performance targets, benchmark methodology, the `bench.yml` and `release.yml` workflows, and the
  40M-op / 1,500-fault campaign rules.
- 2026-11-21: Production-only scope: mTLS, release signing, rolling upgrades, multi-zone placement, CI budget.
  Decisions D-13, D-15, D-20, D-37, D-39–D-42, D-44, D-45 removed; their IDs are not reused.

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
