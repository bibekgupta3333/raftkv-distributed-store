# RaftKV

A replicated key-value store built on the Raft consensus algorithm, written from scratch in Go as a learning project.

> **Status:** design phase. No code yet. See the [roadmap](docs/implementation-roadmap.md).

## Overview

RaftKV runs as a 5-node cluster. The nodes elect a leader, replicate every write to a majority before acknowledging
it, and keep serving as long as any 3 nodes are up. Reads are linearizable.

## Features

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

Not available yet. The first runnable cluster arrives in milestone M2. The planned commands are:

```sh
make test   # unit and simulator tests
make e2e    # 5-node cluster in docker-compose with fault injection
make chaos  # Chaos Mesh tests on a local kind cluster
```

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

Not chosen yet. Apache-2.0 is planned ([D-22](docs/design-decisions.md)).
