# RaftKV

A replicated key-value store built on the Raft consensus algorithm, written from scratch in Go as a learning project.

> **Status:** in development. Milestones M0–M3 are done: Raft core, deterministic simulator, write-ahead log,
> gRPC transport, a 5-node cluster, and a basic KV API (`Put`, `Get`, `Delete`). M4–M7 are in progress. See the
> [roadmap](docs/implementation-roadmap.md) and [WBS](WBS.md).

## Overview

RaftKV runs as a 5-node cluster. The nodes elect a leader, replicate every write to a majority before acknowledging
it, and keep serving as long as any 3 nodes are up. Reads are linearizable.

## Features

The v1.0 feature set. The status note above says which parts are built.

- Leader election with PreVote and CheckQuorum
- Log replication with majority commit
- Linearizable reads (ReadIndex)
- Exactly-once writes through client sessions
- Write-ahead log with crash recovery
- Snapshots and follower catch-up
- Online membership changes and leadership transfer
- gRPC API: `Put`, `Get`, `Delete`, `CompareAndSwap`, `Range`
- Prometheus metrics, OpenTelemetry tracing, and a live observer UI

## Tech stack

Go · gRPC · Protocol Buffers · Pebble · Docker · Kubernetes (kind) · Prometheus · OpenTelemetry · GitHub Actions

## Getting started

Requires Go (the toolchain in `go.mod` is fetched automatically) and Docker for the cluster.

```sh
make build         # build raftkvd and raftkvctl into bin/
make test          # unit tests
make sim           # deterministic simulator (SEEDS=500 FAULTS=full)
make cluster-up    # 5-node cluster plus Prometheus (localhost:19090) in docker-compose
make e2e           # e2e tests against the docker-compose cluster with fault injection
make cluster-down  # stop the cluster and delete its data
```

With the cluster up, try the CLI:

```sh
EP=127.0.0.1:17001,127.0.0.1:17002,127.0.0.1:17003,127.0.0.1:17004,127.0.0.1:17005
bin/raftkvctl put --endpoints=$EP color blue
bin/raftkvctl get --endpoints=$EP color
```

Run `make help` for all targets.

## Documentation

- [RaftKV in 5 minutes](docs/raftkv-in-5-minutes.md): one-page overview
- [Design decisions](docs/design-decisions.md)
- [Architecture](docs/architecture.md) · [System design](docs/system-design.md)
- [API design](docs/api-design.md) · [Storage design](docs/storage-design.md)
- [Testing](docs/testing.md) · [CI and e2e](docs/ci-e2e.md)
- [Roadmap](docs/implementation-roadmap.md) · [Task checklist](WBS.md)

## Contributing

See [Agent.md](Agent.md) for project conventions. Log notable changes in [CHANGELOG.md](CHANGELOG.md).

## License

[Apache-2.0](LICENSE).
