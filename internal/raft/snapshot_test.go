package raft

import (
	"errors"
	"fmt"
	"testing"
)

// Snapshot support in the core (WBS 5.1.1-5.1.3, D-14).

func TestMemoryStorageCreateSnapshot(t *testing.T) {
	st := storageWith(2, 1, 1, 2, 2, 2)
	snap, err := st.CreateSnapshot(4)
	if err != nil {
		t.Fatal(err)
	}
	if snap != (Snapshot{Index: 4, Term: 2}) {
		t.Fatalf("CreateSnapshot(4) = %+v", snap)
	}
	if got, _ := st.Snapshot(); got != snap {
		t.Fatalf("Snapshot() = %+v, want %+v", got, snap)
	}
	// Creating a snapshot does not compact the log (D-30 is the caller's job).
	if first, _ := st.FirstIndex(); first != 1 {
		t.Fatalf("FirstIndex = %d after CreateSnapshot, want 1", first)
	}
	if _, err := st.CreateSnapshot(3); !errors.Is(err, ErrSnapOutOfDate) {
		t.Fatalf("older snapshot: err = %v, want ErrSnapOutOfDate", err)
	}
	if _, err := st.CreateSnapshot(9); err == nil {
		t.Fatal("snapshot beyond the last index must fail")
	}
	if err := st.Compact(4); err != nil {
		t.Fatal(err)
	}
	if term, err := st.Term(4); err != nil || term != 2 {
		t.Fatalf("Term(4) after compaction = %d, %v; the compaction point keeps its term", term, err)
	}
}

func TestMemoryStorageSetCompacted(t *testing.T) {
	st := NewMemoryStorage()
	st.SetCompacted(7, 3)
	first, _ := st.FirstIndex()
	last, _ := st.LastIndex()
	if first != 8 || last != 7 {
		t.Fatalf("range = [%d,%d], want [8,7]", first, last)
	}
	st.Append([]Entry{{Index: 8, Term: 3}})
	if term, err := st.Term(7); err != nil || term != 3 {
		t.Fatalf("Term(7) = %d, %v", term, err)
	}
}

// compactLeader snapshots and compacts the leader's storage up to its applied
// index, as the driver does once a snapshot exists (D-30).
func compactLeader(t *testing.T, n *testNode, upTo uint64) Snapshot {
	t.Helper()
	snap, err := n.st.CreateSnapshot(upTo)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.st.Compact(upTo); err != nil {
		t.Fatal(err)
	}
	return snap
}

func TestLeaderSendsSnapshotForCompactedEntries(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	nw.isolate(3)
	for i := range 10 {
		if _, _, err := nw.propose(1, fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	leader := nw.node(1)
	applied := leader.rn.Status().Applied
	snap := compactLeader(t, leader, applied)

	// Hold snapshots back to observe the Snapshot progress state.
	nw.drop = func(m Message) bool { return m.Type == MsgSnap }
	nw.heal()
	nw.tick(2) // heartbeats -> the follower answers -> the leader probes

	pr := leader.rn.Status().Progress[3]
	if pr.State != ProgressSnapshot || pr.PendingSnapshot != snap.Index {
		t.Fatalf("progress of 3 = %+v, want Snapshot state pending %d", pr, snap.Index)
	}
	var sent *Message
	for i := range nw.sent {
		if m := nw.sent[i]; m.Type == MsgSnap && m.To == 3 {
			sent = &nw.sent[i]
		}
	}
	if sent == nil || sent.Snapshot == nil || *sent.Snapshot != snap {
		t.Fatalf("leader did not send MsgSnap with %+v (got %+v)", snap, sent)
	}
	// Paused: no appends to 3 while the snapshot is in flight.
	before := len(nw.sent)
	if _, _, err := nw.propose(1, "more"); err != nil {
		t.Fatal(err)
	}
	for _, m := range nw.sent[before:] {
		if m.To == 3 && m.Type == MsgApp {
			t.Fatalf("MsgApp sent to a follower in Snapshot state: %+v", m)
		}
	}

	// A failed transfer starts over (D-14): back to Probe from Match.
	leader.rn.ReportSnapshot(3, true)
	pr = leader.rn.Status().Progress[3]
	if pr.State != ProgressProbe || pr.PendingSnapshot != 0 || pr.Next != pr.Match+1 {
		t.Fatalf("after failed report: %+v, want Probe from Match+1", pr)
	}

	// Let snapshots through: the follower catches up via snapshot + log.
	f := nw.node(3)
	appliedBefore := len(f.applied)
	nw.drop = nil
	nw.tick(3)
	if len(f.snaps) == 0 {
		t.Fatal("follower never installed a snapshot")
	}
	lst := leader.rn.Status()
	fst := f.rn.Status()
	if fst.Commit != lst.Commit || fst.Applied != lst.Commit || fst.LastIndex != lst.LastIndex {
		t.Fatalf("follower %+v did not catch up with leader %+v", fst, lst)
	}
	if got := leader.rn.Status().Progress[3]; got.State != ProgressReplicate || got.Match != lst.LastIndex {
		t.Fatalf("leader progress of 3 after catch-up = %+v", got)
	}
	// Entries after the snapshot came through the log and were applied in order.
	after := f.applied[appliedBefore:]
	for i, e := range after {
		if e.Index <= f.snaps[0].Index {
			t.Fatalf("applied entry %d at or below the snapshot %d", e.Index, f.snaps[0].Index)
		}
		if i > 0 && e.Index != after[i-1].Index+1 {
			t.Fatalf("applied out of order: %d after %d", e.Index, after[i-1].Index)
		}
	}
	if len(after) == 0 || after[0].Index != f.snaps[0].Index+1 {
		t.Fatalf("entries after the snapshot were not applied from the log: %+v", after)
	}
}

func TestReportSnapshotSuccessProbesAfterSnapshot(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	nw.isolate(3)
	for i := range 5 {
		if _, _, err := nw.propose(1, fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	leader := nw.node(1)
	snap := compactLeader(t, leader, leader.rn.Status().Applied)
	nw.drop = func(m Message) bool { return m.Type == MsgSnap }
	nw.heal()
	nw.tick(2)
	leader.rn.ReportSnapshot(3, false)
	pr := leader.rn.Status().Progress[3]
	if pr.State != ProgressProbe || pr.Next != snap.Index+1 {
		t.Fatalf("after successful report: %+v, want Probe from %d", pr, snap.Index+1)
	}
	// Reports for a follower not in Snapshot state, or on a follower, are no-ops.
	leader.rn.ReportSnapshot(2, true)
	if got := leader.rn.Status().Progress[2]; got.State != ProgressReplicate {
		t.Fatalf("report for a replicating follower changed it: %+v", got)
	}
	nw.node(2).rn.ReportSnapshot(3, true)
}

// stepSnap delivers a MsgSnap from leader 1 at term to node n and returns the
// answer it sent.
func stepSnap(t *testing.T, n *testNode, term uint64, s Snapshot) (Ready, Message) {
	t.Helper()
	if err := n.rn.Step(Message{Type: MsgSnap, From: 1, To: n.id, Term: term, Snapshot: &s}); err != nil {
		t.Fatal(err)
	}
	rd := n.rn.Ready()
	var resp *Message
	for i := range rd.Messages {
		if rd.Messages[i].Type == MsgAppResp {
			resp = &rd.Messages[i]
		}
	}
	if resp == nil {
		t.Fatalf("no MsgAppResp to a MsgSnap: %+v", rd.Messages)
	}
	return rd, *resp
}

func TestFollowerRejectsStaleSnapshot(t *testing.T) {
	st := storageWith(2, 1, 1, 2, 2, 2)
	st.SetHardState(HardState{Term: 2, Commit: 4})
	rn, err := NewRawNode(baseConfig(2, []uint64{1, 2, 3}, st))
	if err != nil {
		t.Fatal(err)
	}
	n := &testNode{id: 2, rn: rn, st: st}
	rd, resp := stepSnap(t, n, 2, Snapshot{Index: 3, Term: 2})
	if rd.Snapshot != nil {
		t.Fatal("stale snapshot (index <= commit) must not be installed")
	}
	if resp.Reject || resp.Index != 4 {
		t.Fatalf("answer = %+v, want ack of commit index 4", resp)
	}
	n.rn.Advance(rd)
	if last := n.rn.Status().LastIndex; last != 5 {
		t.Fatalf("log changed by a stale snapshot: last = %d", last)
	}
}

func TestFollowerFastForwardsOnMatchingSnapshot(t *testing.T) {
	st := storageWith(2, 1, 1, 2, 2, 2)
	rn, err := NewRawNode(baseConfig(2, []uint64{1, 2, 3}, st))
	if err != nil {
		t.Fatal(err)
	}
	n := &testNode{id: 2, rn: rn, st: st}
	rd, resp := stepSnap(t, n, 2, Snapshot{Index: 4, Term: 2})
	if rd.Snapshot != nil {
		t.Fatal("a snapshot whose last entry is in the log must not replace the log")
	}
	if resp.Index != 4 {
		t.Fatalf("answer index = %d, want 4", resp.Index)
	}
	if st := n.rn.Status(); st.Commit != 4 || st.LastIndex != 5 {
		t.Fatalf("status %+v, want commit 4 and the suffix kept", st)
	}
	if len(rd.CommittedEntries) != 4 {
		t.Fatalf("committed entries = %d, want 4 (applied from the log)", len(rd.CommittedEntries))
	}
	n.rn.Advance(rd)
}

func TestFollowerInstallsSnapshot(t *testing.T) {
	// The follower's log diverges from the snapshot at index 4 (term 1 vs 3).
	st := storageWith(2, 1, 1, 1, 1, 2)
	st.SetHardState(HardState{Term: 2, Commit: 2})
	cfg := baseConfig(2, []uint64{1, 2, 3}, st)
	cfg.Applied = 2
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	n := &testNode{id: 2, rn: rn, st: st}
	snap := Snapshot{Index: 7, Term: 3}
	rd, resp := stepSnap(t, n, 3, snap)
	if rd.Snapshot == nil || *rd.Snapshot != snap {
		t.Fatalf("Ready.Snapshot = %+v, want %+v", rd.Snapshot, snap)
	}
	if !rd.MustSync {
		t.Fatal("installing a snapshot must be fsynced")
	}
	if len(rd.CommittedEntries) != 0 {
		t.Fatalf("committed entries while a snapshot is pending: %+v", rd.CommittedEntries)
	}
	if rd.HardState.Commit != 7 || rd.HardState.Term != 3 {
		t.Fatalf("HardState = %+v, want term 3 commit 7", rd.HardState)
	}
	if resp.Index != 7 || resp.Reject {
		t.Fatalf("answer = %+v, want ack of 7", resp)
	}
	if err := st.ApplySnapshot(*rd.Snapshot); err != nil {
		t.Fatal(err)
	}
	n.rn.Advance(rd)
	s := n.rn.Status()
	if s.Commit != 7 || s.Applied != 7 || s.LastIndex != 7 || s.Lead != 1 {
		t.Fatalf("status after install = %+v", s)
	}
	if err := n.rn.raft.log.checkInvariants(); err != nil {
		t.Fatal(err)
	}
	if n.rn.HasReady() {
		t.Fatalf("unexpected Ready after install: %+v", n.rn.Ready())
	}
	// The log continues after the snapshot.
	if err := n.rn.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 3, Index: 7, LogTerm: 3,
		Entries: []Entry{{Index: 8, Term: 3}}, Commit: 8}); err != nil {
		t.Fatal(err)
	}
	rd = n.rn.Ready()
	if len(rd.Entries) != 1 || len(rd.CommittedEntries) != 1 || rd.CommittedEntries[0].Index != 8 {
		t.Fatalf("after install: entries %+v committed %+v", rd.Entries, rd.CommittedEntries)
	}
}

func TestSnapshotFromStaleTermIsAnsweredNotInstalled(t *testing.T) {
	st := storageWith(5, 1, 1)
	rn, err := NewRawNode(baseConfig(2, []uint64{1, 2, 3}, st))
	if err != nil {
		t.Fatal(err)
	}
	n := &testNode{id: 2, rn: rn, st: st}
	rd, resp := stepSnap(t, n, 4, Snapshot{Index: 9, Term: 4})
	if rd.Snapshot != nil {
		t.Fatal("a snapshot from an older term must not be installed")
	}
	if resp.Term != 5 {
		t.Fatalf("answer term = %d, want 5 so the stale leader steps down", resp.Term)
	}
}

func TestCandidateStepsDownOnSnapshot(t *testing.T) {
	st := storageWith(1, 1)
	rn, err := NewRawNode(baseConfig(2, []uint64{1, 2, 3}, st, withPreVote(false)))
	if err != nil {
		t.Fatal(err)
	}
	rn.Campaign() // term 2, candidate
	rd := rn.Ready()
	rn.Advance(rd)
	n := &testNode{id: 2, rn: rn, st: st}
	rd, _ = stepSnap(t, n, 2, Snapshot{Index: 5, Term: 2})
	if rd.Snapshot == nil {
		t.Fatal("a candidate must accept a snapshot from the leader of its term")
	}
	if s := rn.Status(); s.State != StateFollower || s.Lead != 1 {
		t.Fatalf("status = %+v, want follower of 1", s)
	}
}
