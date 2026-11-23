// Package raft is RaftKV's consensus core: a pure, deterministic Raft state
// machine in the style of etcd/raft (D-01).
//
// # Purity rules (D-01, D-24)
//
// The core performs no I/O, never reads a wall clock, never sleeps and never
// starts goroutines. Its inputs are method calls on [RawNode] (Tick, Step,
// Propose, Campaign) and its only output is a [Ready] value. Randomized
// election timeouts are drawn from the injectable [Rand] in [Config], so a
// simulator run with a fixed seed replays exactly (D-18). Time is measured in
// logical ticks: one call to [RawNode.Tick] is one tick.
//
// # The Ready loop
//
// A driver (internal/node, or the simulator) owns one RawNode from a single
// goroutine and repeats:
//
//	node.Tick()                  // on a timer, or Step/Propose on input
//	if node.HasReady() {
//	    rd := node.Ready()
//	    // 0. install rd.Snapshot, if any (D-14)
//	    // 1. persist rd.Entries and rd.HardState (fsync if rd.MustSync)
//	    // 2. only then send rd.Messages
//	    // 3. apply rd.CommittedEntries in order
//	    // 4. hand rd.ReadStates to waiting reads (D-05)
//	    node.Advance(rd)
//	}
//	// for each MsgSnap sent: stream the snapshot, then
//	node.ReportSnapshot(to, failed)
//
// A node must never acknowledge state it has not made durable, so step 2 comes
// after step 1. No other mutating RawNode method may be called between Ready
// and Advance; the core panics if that contract is broken, which makes driver
// bugs loud in the simulator.
//
// # Extension points
//
// The API leaves room for later milestones without changing its shape:
// ReadIndex (M4, D-05) added [RawNode.ReadIndex] and Ready.ReadStates, with
// the round context carried by heartbeats (see read.go); snapshots (M5, D-14)
// added [MsgSnap], Ready.Snapshot, the Snapshot progress state and
// [RawNode.ReportSnapshot]; membership changes (M6) use [EntryConfChange] and
// per-node voter/learner sets next to Config.Peers.
package raft
