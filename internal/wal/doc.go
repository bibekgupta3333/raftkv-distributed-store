// Package wal is RaftKV's own segmented write-ahead log (D-03).
//
// It stores Raft log entries and the HardState as CRC32C-protected records in
// append-only segment files (docs/storage-design.md §1). It deliberately has
// its own Entry and HardState types so it does not depend on internal/raft;
// an adapter implementing raft.Storage lives elsewhere.
//
// Durability and failure rules:
//   - Append* only write; nothing is durable until Sync returns (fsync).
//   - Any write, fsync or directory-fsync error is fatal and never retried
//     (D-33): the WAL calls Options.OnFatal once and refuses all later work.
//   - Open cuts a torn final record, but refuses to start on corruption
//     anywhere else (D-33).
//   - Segments are deleted whole by ReleaseTo; the caller computes the bound
//     min(snapshotIndex, durableAppliedIndex) (D-30).
package wal
