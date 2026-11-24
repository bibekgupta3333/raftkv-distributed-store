# RaftKV Configuration Reference

Every command-line flag and YAML key of the three binaries: `raftkvd` (the server), `raftkvctl` (the CLI client and
offline debug tools) and `raftkv-observer` (the read-only live view, D-48).

This page is checked against the binaries: `make docs-check` (also part of `make test`) runs each binary's `-h` and
fails if a flag is missing here or listed here but gone from the binary, and does the same for the YAML keys of
`raftkvd`'s config struct. Add a flag, add a row.

Go's `flag` package accepts both `-flag` and `--flag`, and `--flag=value` or `--flag value` (booleans only as
`--flag` or `--flag=false`). This page writes `--flag`.

## raftkvd

```
raftkvd [--config FILE] [flags]
raftkvd --version
```

Configuration is layered (WBS 2.3.3): built-in defaults, then the YAML file named by `--config`, then every flag given
explicitly on the command line. A flag that is not given never overrides the file, even if its value would be zero.
The merged result is validated before anything touches the data directory; every problem is reported at once.

Exit codes: `0` clean shutdown (SIGTERM or SIGINT); `1` other failure; `75` an fsync, write or Pebble error at run
time (fail-stop, D-33); `78` (EX_CONFIG) the node **refuses to start**: invalid configuration, a data directory that
belongs to another node or cluster (D-32), a corrupt WAL or state (D-33), or an on-disk format version this build does
not read ([storage-design.md](storage-design.md) §10).

### raftkvd flags

| Flag | YAML key | Default | Description |
|------|----------|---------|-------------|
| `--config` | | none | Path to a YAML config file (below). Unknown keys and a second YAML document are errors. |
| `--node-id` | `node_id` | none (required) | This node's ID, non-zero, and listed in the peers. Mutually exclusive with `--node-id-from-ordinal`. |
| `--node-id-from-ordinal` | | none | Derive the node ID from a StatefulSet pod name: 1 + its ordinal (`raftkv-2` → 3, D-16). Used by the Kubernetes manifests as `--node-id-from-ordinal=$(POD_NAME)`. |
| `--cluster-id` | `cluster_id` | `1` | Cluster ID, non-zero. Written into the data directory's identity file and every WAL segment header; a mismatch refuses to start (D-32). Clients may check it (`raftkvctl --cluster-id`). |
| `--data-dir` | `data_dir` | none (required) | Data directory: identity file, `wal/`, `sm/`, `snap/` ([storage-design.md](storage-design.md) §7). |
| `--listen` | `listen_addr` | `:<port of this node's peer entry>` | gRPC listen address. One server carries the peer (Raft) and client (KV, Cluster) services. |
| `--metrics-addr` | `metrics_addr` | empty (disabled) | HTTP address for `/metrics` (Prometheus), `/livez` and `/readyz` (D-46). |
| `--peers` | `peers` | none (required) | Static members including this node. Flag form `ID=host:port,ID=host:port`; YAML form below. |
| `--tick-interval` | `tick_interval` | `100ms` | Wall-clock length of one logical Raft tick (D-24). |
| `--election-tick` | `election_tick` | `10` | Election timeout base in ticks; the timeout is randomized in `[e, 2e)` (D-08). Must be greater than the heartbeat tick. |
| `--heartbeat-tick` | `heartbeat_tick` | `1` | Heartbeat interval in ticks. |
| `--log-level` | `log_level` | `info` | `debug`, `info`, `warn` or `error`. Logs are JSON lines on stderr. |
| `--lease-reads` | `lease_reads` | `false` | Serve linearizable reads from a leader lease instead of a ReadIndex round (D-25). Only safe while clock drift stays within the lease margin. |
| `--snapshot-entries` | `snapshot_entries` | `100000` | Entries applied between two snapshots (D-14). Must be positive. |
| `--snapshot-retain-entries` | `snapshot_retain_entries` | `10000` | Entries kept in the WAL below the snapshot index, so briefly lagging followers catch up from the log ([storage-design.md](storage-design.md) §1.8). |
| `--wal-segment-bytes` | `wal_segment_bytes` | `0` (= 64 MiB) | WAL segment rollover size (D-03). Trimming deletes whole segments, so this is the granularity of reclaimed disk. |
| `--version` | | | Print the version and exit. |

### raftkvd YAML config file

All keys are optional in the file; the required ones (`node_id`, `data_dir`, `peers`) may come from flags instead.
Durations use Go syntax (`100ms`, `2s`).

| Key | Type | Flag | Default |
|-----|------|------|---------|
| `node_id` | uint64 | `--node-id` | none |
| `cluster_id` | uint64 | `--cluster-id` | `1` |
| `data_dir` | string | `--data-dir` | none |
| `listen_addr` | host:port | `--listen` | derived from `peers` |
| `metrics_addr` | host:port | `--metrics-addr` | empty |
| `peers` | list of `{id, addr}` | `--peers` | none |
| `id` | uint64 (in `peers`) | | |
| `addr` | host:port (in `peers`) | | |
| `tick_interval` | duration | `--tick-interval` | `100ms` |
| `election_tick` | int | `--election-tick` | `10` |
| `heartbeat_tick` | int | `--heartbeat-tick` | `1` |
| `log_level` | string | `--log-level` | `info` |
| `max_msg_bytes` | int | none (YAML only) | `8388608` (8 MiB): largest gRPC message to or from peers |
| `lease_reads` | bool | `--lease-reads` | `false` |
| `snapshot_entries` | uint64 | `--snapshot-entries` | `100000` |
| `snapshot_retain_entries` | uint64 | `--snapshot-retain-entries` | `10000` |
| `wal_segment_bytes` | int64 | `--wal-segment-bytes` | `0` (64 MiB) |

Example (the Kubernetes ConfigMap, `deploy/k8s/base/configmap.yaml`; the node ID comes from the pod name):

```yaml
cluster_id: 1
data_dir: /data
listen_addr: ":7000"
metrics_addr: ":9100"
log_level: info
peers:
  - {id: 1, addr: "raftkv-0.raftkv-peer:7000"}
  - {id: 2, addr: "raftkv-1.raftkv-peer:7000"}
  - {id: 3, addr: "raftkv-2.raftkv-peer:7000"}
```

## raftkvctl

```
raftkvctl --version
raftkvctl put|get|del|cas|range [flags] ARGS
raftkvctl debug state-hash|wal-dump --data-dir DIR [flags]
```

Flags go after the subcommand. Exit codes ([api-design.md](api-design.md) §9): `0` success, `1` usage or
`INVALID_ARGUMENT`, `2` unavailable or deadline (also bad flags), `3` key not found (`get`), `4` compare failed (`cas`,
the current value is printed), `6` `FAILED_PRECONDITION`, `7` a write's outcome is unknown (it may have been applied).

Environment: `RAFTKV_ENDPOINTS` is the default for `--endpoints`.

### raftkvctl put, get, del

`put KEY VALUE` (or `put --value-file F KEY`), `get KEY`, `del KEY`. They share one flag set; a flag that does not
apply to the subcommand is ignored.

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoints` | `$RAFTKV_ENDPOINTS`, else `localhost:2379` | Comma-separated member addresses. The client finds the leader itself. |
| `--timeout` | `10s` | Overall deadline for the request, retries included. |
| `--cluster-id` | `0` (don't check) | Refuse servers of another cluster. |
| `--prev` | `false` | Return the previous value (`put`, `del`). |
| `--value-file` | none | Read the value from this file (`put`). |
| `--stale` | `false` | Serve from one member's local state, not linearizable (`get`). |
| `--min-revision` | `0` | With `--stale`, wait until the member has applied this index (`get`). |
| `--hex` | `false` | Print the value as hex (`get`). |

### raftkvctl cas

`cas (--expect-value V | --expect-revision N | --expect-absent) (--new-value V | --delete) KEY`. Exactly one compare
and one action.

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoints` | as for `put` | Comma-separated member addresses. |
| `--timeout` | `10s` | Overall deadline for the request. |
| `--cluster-id` | `0` | Refuse servers of another cluster. |
| `--expect-value` | none | Swap only if the key has exactly this value. |
| `--expect-revision` | none | Swap only if the key's mod revision is this (non-zero). |
| `--expect-absent` | `false` | Swap only if the key does not exist. |
| `--new-value` | none | Value to set on success. |
| `--delete` | `false` | Delete the key on success. |

### raftkvctl range

`range START [END]` or `range --prefix PREFIX`. Without `--all`, prints one page and, if more remain, a final
`more=true next=HEX` line; pass `HEX` to `--page-token` with the same bounds and flags to continue.

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoints` | as for `put` | Comma-separated member addresses. |
| `--timeout` | `10s` | Overall deadline for the request. |
| `--cluster-id` | `0` | Refuse servers of another cluster. |
| `--prefix` | `false` | Treat START as a key prefix. |
| `--limit` | `0` (= 1000) | Keys per page, 1..1000. |
| `--keys-only` | `false` | Print keys only. |
| `--reverse` | `false` | Scan in descending key order. |
| `--all` | `false` | Follow continuation tokens to the end. |
| `--stale` | `false` | Serve from one member's local state, not linearizable. |
| `--min-revision` | `0` | With `--stale`, wait until the member has applied this index. |
| `--page-token` | none | Continue from a previous page (hex). |
| `--hex` | `false` | Print values as hex. |

### raftkvctl debug state-hash

Opens a **stopped** node's state machine read-only and prints `applied_index=N state_hash=H`. Equal hashes at the same
applied index mean equal state machines (D-18).

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | none (required) | Data directory of a stopped node. |

### raftkvctl debug wal-dump

Prints a **stopped** node's HardState and log entries, ending with a digest line; equal digests on two nodes mean
equal logs.

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | none (required) | Data directory of a stopped node. |
| `--from` | `0` | First entry index to print. |
| `--to` | `0` (no limit) | Last entry index to print. |
| `--max-data` | `32` | Payload bytes to show per entry. |

## raftkv-observer

```
raftkv-observer [flags]
raftkv-observer --version
```

Polls `Cluster.Status` on every endpoint and serves one HTML page with a live event stream (D-48). It has no auth:
keep `--listen` on localhost.

### raftkv-observer flags

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoints` | the compose cluster, `node1=127.0.0.1:17001` … `node5=127.0.0.1:17005` | Comma-separated nodes to poll, each `name=host:port` or `host:port` (named after its host). |
| `--listen` | `127.0.0.1:18080` | HTTP listen address. |
| `--interval` | `500ms` | `Cluster.Status` poll interval. Must be positive. |
| `--rpc-timeout` | 80% of `--interval` | Per-node Status timeout. |
| `--version` | | Print the version and exit. |
