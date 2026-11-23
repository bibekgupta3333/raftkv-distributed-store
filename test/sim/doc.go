// Package sim is RaftKV's deterministic simulator (D-18, docs/testing.md 2.2).
//
// It drives N [raft.RawNode]s from one goroutine with a virtual clock, a
// seeded PRNG (math/rand/v2 PCG), a faulty virtual network, a crash/restart
// model and a client workload, and checks the Raft safety invariants after
// every event. No wall clock is read and no map is iterated where order
// matters, so one seed on one commit always produces the same trace; the
// trace hash proves it (D-24).
//
// Building blocks:
//
//   - [Scheduler]: event queue ordered by (virtual time, sequence).
//   - [Network]: per-link delay, drop, duplicate, reorder, partitions.
//   - [Cluster]: nodes, disks, Ready loop, crashes, workload, fault plan.
//   - [Checker]: INV-1..INV-6 over [NodeView]s; [Violation] is the result.
//   - [Trace]: hashed event log with a JSONL dump and pretty-printer.
//   - [Run]: one seed, one [Profile], one [Result].
//
// Seeds that once failed live in test/sim/regressions/ and are replayed on
// every run. Usage is described on the test flags in sim_test.go.
package sim
