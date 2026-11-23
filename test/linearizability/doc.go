// Package linearizability records client histories and checks them for
// linearizability with Porcupine (D-18, docs/testing.md section 3).
//
// A [Recorder] captures every operation as an invoke event and one result event
// (ok, fail or info). [Recorder.Operations] turns the log into Porcupine
// operations: failed ops are dropped (they provably did not happen), indeterminate
// writes get an open-ended return time, indeterminate reads are dropped. [Check]
// runs the per-key KV model; a checker timeout counts as a failure.
package linearizability
