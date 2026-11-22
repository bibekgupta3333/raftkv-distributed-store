# RaftKV

**A learning project: a replicated key-value store with Raft built from scratch in Go.**

> Status: **design phase.** No code yet.

## The idea

Five nodes keep identical copies of a key → value store. They elect a **leader**, and every write goes through it.
The leader saves each change to its **write-ahead log**, copies it to the others, and replies "OK" once **3 of 5**
have it on disk. If the leader dies, the rest elect a new one within a second or two, and no confirmed write is lost.

Reads are **linearizable**: you always see the latest confirmed value. Nodes take **snapshots** so a node that was
down can catch up quickly, and nodes can be **added or removed** while the cluster runs.

The fun part is proving it works. Tests kill leaders, freeze processes, and cut the network while a
**linearizability checker** looks for any wrong answer. A small **observer page** lets you watch all of it live.

It runs entirely on a laptop: 5 nodes in Docker, or on a local Kubernetes cluster (kind).

## Tech

Go · gRPC · Protobuf · Raft (own implementation) · Pebble · Docker · Kubernetes (kind) · Prometheus · OpenTelemetry · GitHub Actions

## Docs

| Doc | What's in it |
|-----|--------------|
| [docs/raftkv-in-5-minutes.md](docs/raftkv-in-5-minutes.md) | **Start here.** One-page summary of everything |
| [docs/simple-guide.md](docs/simple-guide.md) | Plain-English explanation, no jargon |
| [docs/design-decisions.md](docs/design-decisions.md) | All decisions (`D-xx`), milestones, repo layout |
| [docs/architecture.md](docs/architecture.md) | Components inside a node |
| [docs/system-design.md](docs/system-design.md) | Election, replication, reads, snapshots, recovery, membership, failures |
| [docs/api-design.md](docs/api-design.md) | gRPC API and errors |
| [docs/storage-design.md](docs/storage-design.md) | WAL format, Pebble layout, crash recovery |
| [docs/testing.md](docs/testing.md) | Simulator, linearizability checker, fault injection |
| [docs/ci-e2e.md](docs/ci-e2e.md) | GitHub Actions e2e and nightly chaos |
| [docs/implementation-roadmap.md](docs/implementation-roadmap.md) | Milestones M0–M7 |
| [WBS.md](WBS.md) | Task checklist |
| [CHANGELOG.md](CHANGELOG.md) · [memory.md](memory.md) · [Agent.md](Agent.md) | History, project memory, agent rules |
