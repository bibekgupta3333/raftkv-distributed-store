// Package storage is the Pebble-backed replicated state machine (D-02, D-04,
// D-30; docs/storage-design.md sections 2-3).
//
// [Store] owns the database: key layout, the atomic apply batch that carries
// appliedIndex (D-04), flush tracking and the WAL truncation bound (D-30).
// [Applier] is the node-facing state machine that decodes committed Raft
// entries into Store batches and reports each command's result.
package storage
