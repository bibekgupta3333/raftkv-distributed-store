package raft

import (
	"errors"
	"fmt"
	"testing"
)

// ReadIndex tests (WBS 4.1, D-05).

func readCtx(i int) []byte { return fmt.Appendf(nil, "read-%d", i) }

// readRoundHeartbeats counts heartbeats sent that carry a ReadIndex round.
func readRoundHeartbeats(nw *network) int {
	n := 0
	for _, m := range nw.sent {
		if m.Type == MsgHeartbeat && len(m.Context) > 0 {
			n++
		}
	}
	return n
}

func onlyRead(t *testing.T, n *testNode) ReadState {
	t.Helper()
	if len(n.reads) != 1 {
		t.Fatalf("node %d: want exactly 1 ReadState, got %+v", n.id, n.reads)
	}
	return n.reads[0]
}

// WBS 4.1.1: the leader records its commit index as the read index once a
// heartbeat quorum confirms it.
func TestReadIndexLeaderRecordsCommitIndex(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	idx, _, err := nw.propose(1, "a")
	if err != nil {
		t.Fatal(err)
	}
	l := nw.node(1)
	if err := l.rn.ReadIndex(readCtx(1)); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	rs := onlyRead(t, l)
	if rs.Rejected || rs.Index != idx || string(rs.RequestCtx) != "read-1" {
		t.Fatalf("ReadState = %+v, want index %d ctx read-1", rs, idx)
	}
	if got := readRoundHeartbeats(nw); got != 2 {
		t.Errorf("heartbeats carrying the round: %d, want 2 (one per follower)", got)
	}
}

// WBS 4.1.1: before the leader commits an entry in its own term (its no-op,
// T6) a read is queued, not answered; it is answered with an index at or after
// the no-op once that commits.
func TestReadIndexWaitsForCurrentTermCommit(t *testing.T) {
	nw := newNetwork(t, 3, withCheckQuorum(false)) // lets node 2 campaign at once
	nw.campaign(1)
	if _, _, err := nw.propose(1, "a"); err != nil {
		t.Fatal(err)
	}
	// Node 2 wins term 2 but cannot hear append acks, so its no-op stays
	// uncommitted while its commit index (2) still points at term 1.
	nw.drop = func(m Message) bool { return m.Type == MsgAppResp && m.To == 2 }
	nw.campaign(2)
	l := nw.node(2)
	st := l.rn.Status()
	if st.State != StateLeader {
		t.Fatalf("node 2 is %v, want leader", st.State)
	}
	noop := st.LastIndex
	if st.Commit >= noop {
		t.Fatalf("no-op %d already committed (commit %d); test setup is wrong", noop, st.Commit)
	}
	if err := l.rn.ReadIndex(readCtx(1)); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	nw.tick(1)
	if len(l.reads) != 0 {
		t.Fatalf("read answered before the no-op committed: %+v", l.reads)
	}
	if got := readRoundHeartbeats(nw); got != 0 {
		t.Fatalf("a round started before the no-op committed (%d heartbeats)", got)
	}

	nw.drop = nil
	nw.tick(1) // heartbeat acks make the leader resend the no-op; it commits
	rs := onlyRead(t, l)
	if rs.Rejected || rs.Index < noop {
		t.Fatalf("ReadState = %+v, want index >= no-op %d", rs, noop)
	}
}

// WBS 4.1.2: many readers that arrive while a round is in flight share the
// next round: 100 reads cost two rounds, not 100.
func TestReadIndexBatchesConcurrentReads(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	l := nw.node(1)
	before := readRoundHeartbeats(nw)
	for i := range 100 {
		if err := l.rn.ReadIndex(readCtx(i)); err != nil {
			t.Fatal(err)
		}
	}
	nw.settle()
	if got := readRoundHeartbeats(nw) - before; got != 4 {
		t.Errorf("heartbeats carrying a round: %d, want 4 (2 rounds x 2 followers)", got)
	}
	if len(l.reads) != 100 {
		t.Fatalf("got %d ReadStates, want 100", len(l.reads))
	}
	commit := l.rn.Status().Commit
	for i, rs := range l.reads {
		if rs.Rejected || rs.Index != commit || string(rs.RequestCtx) != string(readCtx(i)) {
			t.Fatalf("ReadState %d = %+v, want index %d ctx %s, in request order", i, rs, commit, readCtx(i))
		}
	}
}

// A request that arrives during a round must not reuse that round's read
// index: a write committed after the round started could be missed.
func TestReadIndexLaterRequestGetsLaterIndex(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	l := nw.node(1)
	// Round 1 starts, but its heartbeats are held back.
	nw.blocked[[2]uint64{1, 2}], nw.blocked[[2]uint64{1, 3}] = true, true
	if err := l.rn.ReadIndex(readCtx(1)); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	nw.heal()
	idx, _, err := nw.propose(1, "w") // commits while round 1 is in flight
	if err != nil {
		t.Fatal(err)
	}
	if got := l.rn.Status().Commit; got != idx {
		t.Fatalf("write not committed: commit %d, want %d", got, idx)
	}
	if err := l.rn.ReadIndex(readCtx(2)); err != nil {
		t.Fatal(err)
	}
	nw.tick(1) // the heartbeat resends round 1; then round 2 runs
	if len(l.reads) != 2 {
		t.Fatalf("got %+v, want two ReadStates", l.reads)
	}
	if l.reads[0].Index >= idx {
		t.Errorf("read 1 index %d: want the commit index from before the write (%d)", l.reads[0].Index, idx-1)
	}
	if l.reads[1].Index < idx {
		t.Errorf("read 2 index %d misses write %d committed before it was requested", l.reads[1].Index, idx)
	}
}

// WBS 4.1.3: a follower forwards ReadIndex to the leader (term-less) and gets
// the leader's read index back as a ReadState.
func TestReadIndexFollowerViaLeader(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	idx, _, err := nw.propose(1, "a")
	if err != nil {
		t.Fatal(err)
	}
	f := nw.node(3)
	termBefore := f.rn.Status().Term
	if err := f.rn.ReadIndex(readCtx(7)); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	rs := onlyRead(t, f)
	if rs.Rejected || rs.Index != idx || string(rs.RequestCtx) != "read-7" {
		t.Fatalf("ReadState = %+v, want index %d", rs, idx)
	}
	var fwd []Message
	for _, m := range nw.sent {
		if m.Type == MsgReadIndex {
			fwd = append(fwd, m)
		}
	}
	if len(fwd) != 1 || fwd[0].To != 1 || fwd[0].From != 3 || fwd[0].Term != 0 {
		t.Errorf("forwarded requests = %+v, want one term-less MsgReadIndex 3->1", fwd)
	}
	if len(nw.node(1).reads) != 0 {
		t.Error("the leader surfaced a ReadState for a forwarded request")
	}
	if got := f.rn.Status().Term; got != termBefore {
		t.Errorf("follower term moved %d -> %d", termBefore, got)
	}
}

func TestReadIndexNoLeader(t *testing.T) {
	nw := newNetwork(t, 3)
	if err := nw.node(2).rn.ReadIndex(readCtx(1)); !errors.Is(err, ErrReadIndexDropped) {
		t.Fatalf("ReadIndex with no leader: err = %v, want ErrReadIndexDropped", err)
	}
}

// A node that is not the leader rejects a forwarded request, and the
// requester sees a rejected ReadState instead of waiting forever.
func TestReadIndexNonLeaderRejects(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	if err := nw.node(3).rn.Step(Message{Type: MsgReadIndex, From: 2, To: 3, Context: readCtx(1)}); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	rs := onlyRead(t, nw.node(2))
	if !rs.Rejected || rs.Index != 0 || string(rs.RequestCtx) != "read-1" {
		t.Fatalf("ReadState = %+v, want rejected read-1", rs)
	}
}

// ReadIndex messages never change a node's term, even when they carry a
// higher one.
func TestReadIndexMessagesAreTermless(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	f := nw.node(2)
	st := f.rn.Status()
	if err := f.rn.Step(Message{Type: MsgReadIndexResp, From: 1, To: 2, Term: st.Term + 5, Index: 1, Context: readCtx(1)}); err != nil {
		t.Fatal(err)
	}
	if got := f.rn.Status(); got.Term != st.Term || got.Lead != st.Lead {
		t.Errorf("term/lead changed by a ReadIndex response: %d/%d -> %d/%d", st.Term, st.Lead, got.Term, got.Lead)
	}
}

// WBS 4.1.4 (unit form): a partitioned old leader never completes a ReadIndex
// once a newer leader exists. Its reads (local and forwarded) are rejected
// when it steps down.
func TestReadIndexPartitionedOldLeader(t *testing.T) {
	nw := newNetwork(t, 5)
	nw.campaign(1)
	old := nw.node(1)
	nw.isolate(1)
	if err := old.rn.ReadIndex(readCtx(1)); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	// The majority elects a new leader and commits a write. The old leader
	// keeps ticking, so CheckQuorum makes it step down.
	nw.tick(4 * DefaultElectionTick)
	leaders := nw.leaders()
	if len(leaders) != 1 || leaders[0] == 1 {
		t.Fatalf("leaders = %v, want one new leader", leaders)
	}
	if _, _, err := nw.propose(leaders[0], "w"); err != nil {
		t.Fatal(err)
	}
	rs := onlyRead(t, old)
	if !rs.Rejected {
		t.Fatalf("partitioned old leader completed a read: %+v", rs)
	}
}

// The old leader learns of the new term from the heartbeat replies of its
// read round: it steps down and rejects, without CheckQuorum.
func TestReadIndexStaleLeaderStepsDownOnRound(t *testing.T) {
	nw := newNetwork(t, 3, withCheckQuorum(false))
	nw.campaign(1)
	nw.isolate(1)
	nw.campaign(2)
	if _, _, err := nw.propose(2, "w"); err != nil {
		t.Fatal(err)
	}
	old := nw.node(1)
	if old.rn.Status().State != StateLeader {
		t.Fatal("node 1 should still believe it leads")
	}
	// Forwarded request from node 3 (as if 3 still followed node 1).
	if err := old.rn.Step(Message{Type: MsgReadIndex, From: 3, To: 1, Context: readCtx(3)}); err != nil {
		t.Fatal(err)
	}
	if err := old.rn.ReadIndex(readCtx(1)); err != nil {
		t.Fatal(err)
	}
	nw.heal()
	nw.settle()
	if st := old.rn.Status(); st.State == StateLeader {
		t.Fatal("old leader did not step down")
	}
	if rs := onlyRead(t, old); !rs.Rejected {
		t.Fatalf("old leader completed its own read: %+v", rs)
	}
	for _, rs := range nw.node(3).reads {
		if string(rs.RequestCtx) == "read-3" && !rs.Rejected {
			t.Fatalf("old leader completed a forwarded read: %+v", rs)
		}
	}
}

func TestReadIndexSingleNode(t *testing.T) {
	nw := newNetwork(t, 1)
	nw.campaign(1)
	l := nw.node(1)
	if err := l.rn.ReadIndex(readCtx(1)); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	if rs := onlyRead(t, l); rs.Rejected || rs.Index != l.rn.Status().Commit {
		t.Fatalf("ReadState = %+v, want commit %d", rs, l.rn.Status().Commit)
	}
}
