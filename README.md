# RaftKV

**A linearizable, replicated key-value store, built from the ground up in Go.**

> Status: **design phase.** No implementation code exists yet. Everything below describes the system being built.
> Performance numbers in this repo are **targets**, not measured results.

## The idea, in plain words

Imagine five computers that all keep the same notebook of key → value pairs. You can write to any of them, and
even if one or two crash, lose power, or get cut off from the network, the notebook never loses a confirmed write.
It also never shows you an out-of-date value as if it were current.

RaftKV does that by having the five nodes **elect a leader** and run every write through it. The leader writes the
change to its own **write-ahead log** and copies it to the others. Once a **majority (3 of 5)** have it safely on
disk, the write is *committed* and the client gets "OK". If the leader dies, the survivors elect a new one within a
second or two. Raft's rules guarantee the new leader already has every committed write.

Reads are **linearizable**: a read always reflects every write that finished before it started, even right after
a leader change. To keep the log from growing forever, each node regularly takes a **snapshot** of its data. A node
that was down for a long time catches up by downloading a snapshot instead of replaying millions of log entries.
Nodes can be **added and removed** while the cluster keeps running.

The hard part isn't the happy path. It's proving the system stays correct when things break. So the project leans
heavily on **fault injection** (killing leaders, freezing processes, partitioning networks, filling disks) and on
an automated **linearizability checker** that inspects tens of millions of recorded operations for any violation.

## Goals (targets, on reference hardware still to be chosen)

| | Target |
|-|-|
| Sustained writes (5 nodes, fsync on) | ≥ 38K ops/s |
| Sustained linearizable reads | ≥ 92K ops/s |
| Correctness campaign | ≥ 40M ops, ≥ 1,500 injected faults, 0 violations |
| p99 write latency from batching / fsync / replication tuning | ≥ 30% lower than the unoptimized baseline |

## Tech stack

Go · gRPC · Protobuf · Raft (own implementation) · Pebble · Kubernetes · OpenTelemetry · Prometheus · GitHub Actions

## Documentation

| Doc | What's in it |
|-----|--------------|
| [docs/raftkv-in-5-minutes.md](docs/raftkv-in-5-minutes.md) | **One-page summary:** features, architecture, DB, sequences, observer UI, e2e, WBS |
| [docs/simple-guide.md](docs/simple-guide.md) | **New here? Read this first.** Plain-English overview |
| [docs/design-decisions.md](docs/design-decisions.md) | **Start here (technical).** Decided, proposed, and open decisions (`D-xx`) |
| [docs/architecture.md](docs/architecture.md) | Components, node internals, package map, tradeoffs |
| [docs/system-design.md](docs/system-design.md) | Election, replication, reads, snapshots, recovery, membership, failure scenarios |
| [docs/api-design.md](docs/api-design.md) | gRPC/Protobuf contracts, error model, retries, CLI |
| [docs/storage-design.md](docs/storage-design.md) | WAL format, Pebble state machine, snapshots, crash recovery |
| [docs/testing-benchmarks.md](docs/testing-benchmarks.md) | Test strategy, fault injection, linearizability checking, benchmark methodology |
| [docs/ci-e2e.md](docs/ci-e2e.md) | GitHub Actions workflows for unit, e2e, chaos, and bench |
| [docs/implementation-roadmap.md](docs/implementation-roadmap.md) | Milestones M0–M8, acceptance criteria, risks |
| [WBS.md](WBS.md) | Implementation checklist |
| [CHANGELOG.md](CHANGELOG.md) | Change history |
| [memory.md](memory.md) | Running project memory: context, decisions made, lessons |
| [Agent.md](Agent.md) | Instructions for AI coding agents working in this repo |

## Unresolved decisions (must be settled before coding)

See [docs/design-decisions.md](docs/design-decisions.md#open-questions-to-resolve-before-m0m1). In short: whether to
write our own Raft or use etcd/raft, the reference benchmark hardware, whether mTLS is in v1, the module path and
license, joint consensus vs single-server membership changes, and confirming a single Raft group (no sharding).
