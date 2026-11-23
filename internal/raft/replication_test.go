package raft

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func appsTo(msgs []Message, to uint64) []Message {
	var out []Message
	for _, m := range msgs {
		if m.Type == MsgApp && m.To == to {
			out = append(out, m)
		}
	}
	return out
}

// WBS 1.3.1: MsgApp carries prevIndex/prevTerm taken from Progress.Next.
func TestAppendSendsPrevIndexAndTermFromNext(t *testing.T) {
	st := storageWith(2, 1, 1, 2)
	st.SetHardState(HardState{Term: 2})
	peers := []uint64{1, 2, 3}
	rn := newLone(t, 1, peers, st, withPreVote(false))
	rn.Campaign()
	flush(t, rn, st)
	if err := rn.Step(Message{Type: MsgVoteResp, From: 2, To: 1, Term: 3}); err != nil {
		t.Fatal(err)
	}
	msgs := flush(t, rn, st)
	apps := appsTo(msgs, 3)
	if len(apps) != 1 {
		t.Fatalf("want one MsgApp to node 3, got %+v", msgs)
	}
	m := apps[0]
	// Leader log: 1:t1 2:t1 3:t2 and the no-op 4:t3. Next starts at last+1 = 4.
	if m.Index != 3 || m.LogTerm != 2 || len(m.Entries) != 1 || m.Entries[0].Index != 4 || m.Entries[0].Term != 3 {
		t.Fatalf("MsgApp = %+v, want prev 3/t2 with the no-op at 4", m)
	}
	// Probe state: nothing more is sent until the reply (one MsgApp per RTT).
	rn.Tick()
	if more := appsTo(flush(t, rn, st), 3); len(more) != 0 {
		t.Fatalf("probe sent a second MsgApp before a reply: %+v", more)
	}
}

// WBS 1.3.3: a large divergence resolves in a couple of round trips.
func TestFastBacktrackingResolvesLargeDivergenceQuickly(t *testing.T) {
	leaderTerms := append([]uint64{1}, repeat(2, 59)...) // 60 entries
	stores := map[uint64]*MemoryStorage{
		1: storageWith(2, leaderTerms...),
		2: storageWith(1, repeat(1, 50)...),
		3: storageWith(2, leaderTerms...),
	}
	nw := newNetworkWithStorage(t, stores, withPreVote(false))
	nw.campaign(1)
	if nw.node(1).rn.Status().State != StateLeader {
		t.Fatal("node 1 did not win")
	}
	apps := appsTo(nw.sent, 2)
	if len(apps) > 3 {
		t.Fatalf("took %d MsgApp to repair a 59-entry divergence; fast backtracking should need at most 3", len(apps))
	}
	want := nw.node(1).logOf()
	if got := nw.node(2).logOf(); !reflect.DeepEqual(termsOf(got), termsOf(want)) || len(got) != 61 {
		t.Fatalf("follower log not repaired: %d entries", len(got))
	}
	// A rejection must have carried a usable hint.
	var rejects int
	for _, m := range nw.sent {
		if m.Type == MsgAppResp && m.From == 2 && m.Reject {
			rejects++
			if m.RejectHint != 50 || m.LogTerm != 1 {
				t.Fatalf("reject hint = %d/t%d, want 50/t1", m.RejectHint, m.LogTerm)
			}
		}
	}
	if rejects != 1 {
		t.Fatalf("rejects = %d, want exactly 1", rejects)
	}
}

func repeat(v uint64, n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = v
	}
	return out
}

// WBS 1.3.4: Probe -> Replicate on first ack; rejection returns to Probe.
func TestProgressProbeReplicateTransitions(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	p := nw.node(1).rn.Status().Progress
	if p[2].State != ProgressReplicate || p[2].Next != 2 || p[2].Match != 1 {
		t.Fatalf("after first ack: %+v", p[2])
	}

	// Drop everything to node 3, propose more: leader streams optimistically.
	nw.isolate(3)
	for _, d := range []string{"a", "b"} {
		if _, _, err := nw.propose(1, d); err != nil {
			t.Fatal(err)
		}
	}
	p = nw.node(1).rn.Status().Progress
	if p[3].State != ProgressReplicate || p[3].Next != 4 || p[3].Match != 1 {
		t.Fatalf("optimistic replicate progress for the cut-off node = %+v", p[3])
	}
	if p[2].Match != 3 {
		t.Fatalf("node 2 match = %d", p[2].Match)
	}

	// Node 3 rejects the next append (it lacks 2..3): leader goes back to Probe.
	nw.heal()
	nw.tick(2)
	p = nw.node(1).rn.Status().Progress
	if p[3].Match != 3 || p[3].State != ProgressReplicate {
		t.Fatalf("after heal node 3 should catch up: %+v", p[3])
	}
	if got := termsOf(nw.node(3).logOf()); !reflect.DeepEqual(got, []uint64{1, 1, 1}) {
		t.Fatalf("node 3 log terms = %v", got)
	}
}

// D-29: at most MaxInflight unacknowledged appends per follower.
func TestInflightCapPausesAppends(t *testing.T) {
	nw := newNetwork(t, 3)
	for _, n := range nw.nodes {
		n.rn.raft.cfg.MaxInflight = 2
		for _, pr := range n.rn.raft.prs {
			pr.inflights.cap = 2
		}
	}
	nw.campaign(1)
	nw.blocked[[2]uint64{1, 2}] = true
	nw.sent = nil
	for i := 0; i < 6; i++ {
		if _, _, err := nw.propose(1, "x"); err != nil {
			t.Fatal(err)
		}
	}
	withEntries := func(to uint64) (n int) {
		for _, m := range appsTo(nw.sent, to) {
			if len(m.Entries) > 0 {
				n++
			}
		}
		return n
	}
	// Empty appends (commit propagation) are not counted against the cap.
	if got := withEntries(2); got != 2 {
		t.Fatalf("sent %d entry-bearing MsgApp to the unreachable follower, want exactly MaxInflight=2", got)
	}
	if got := withEntries(3); got != 6 {
		t.Fatalf("healthy follower got %d entry-bearing MsgApp, want 6", got)
	}
}

// WBS 1.4.3: per-message size limit.
func TestMaxMsgBytesLimitsEntriesPerMessage(t *testing.T) {
	nw := newNetwork(t, 3)
	for _, n := range nw.nodes {
		n.rn.raft.cfg.MaxMsgBytes = 10
	}
	nw.campaign(1)
	nw.isolate(3)
	for i := 0; i < 4; i++ {
		if _, _, err := nw.propose(1, "123456"); err != nil {
			t.Fatal(err)
		}
	}
	nw.heal()
	nw.sent = nil
	nw.tick(10)
	apps := appsTo(nw.sent, 3)
	if len(apps) < 4 {
		t.Fatalf("expected the backlog to be split over several messages, got %d", len(apps))
	}
	for _, m := range apps {
		var size uint64
		for _, e := range m.Entries {
			size += e.Size()
		}
		if len(m.Entries) > 1 && size > 10 {
			t.Fatalf("message carries %d bytes in %d entries, over the limit", size, len(m.Entries))
		}
	}
	if got := len(nw.node(3).logOf()); got != 5 {
		t.Fatalf("node 3 has %d entries, want 5", got)
	}
}

// WBS 1.3.5 / 1.3.6: commit propagates and committed entries reach Ready.
func TestCommitPropagationAndApply(t *testing.T) {
	nw := newNetwork(t, 5)
	nw.campaign(1)
	for _, d := range []string{"a", "b", "c"} {
		if _, _, err := nw.propose(1, d); err != nil {
			t.Fatal(err)
		}
	}
	nw.tick(2) // heartbeats carry the final commit index
	for _, id := range nw.ids {
		n := nw.node(id)
		s := n.rn.Status()
		if s.Commit != 4 || s.Applied != 4 {
			t.Fatalf("node %d commit/applied = %d/%d, want 4/4", id, s.Commit, s.Applied)
		}
		var data []string
		for _, e := range n.applied {
			data = append(data, string(e.Data))
		}
		if strings.Join(data, ",") != ",a,b,c" {
			t.Fatalf("node %d applied %q", id, data)
		}
		for i, e := range n.applied {
			if e.Index != uint64(i+1) {
				t.Fatalf("node %d applied out of order: %+v", id, n.applied)
			}
		}
	}
}

func TestMinorityCannotCommit(t *testing.T) {
	nw := newNetwork(t, 5)
	nw.campaign(1)
	nw.isolate(3)
	nw.isolate(4)
	nw.isolate(5)
	if _, _, err := nw.propose(1, "lost"); err != nil {
		t.Fatal(err)
	}
	if s := nw.node(1).rn.Status(); s.Commit != 1 {
		t.Fatalf("leader with 2 of 5 committed index %d", s.Commit)
	}
}

// Followers never apply beyond what the leader committed and what they have.
func TestFollowerCommitClampedToLastNewEntry(t *testing.T) {
	st := storageWith(1, 1, 1)
	rn := newLone(t, 2, []uint64{1, 2, 3}, st)
	err := rn.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 1, Index: 2, LogTerm: 1, Commit: 99,
		Entries: []Entry{{Term: 1, Index: 3}}})
	if err != nil {
		t.Fatal(err)
	}
	flush(t, rn, st)
	if s := rn.Status(); s.Commit != 3 {
		t.Fatalf("commit = %d, want 3 (min of leaderCommit and last new)", s.Commit)
	}
}

func TestFollowerAppendBranches(t *testing.T) {
	tests := []struct {
		name       string
		m          Message
		wantReject bool
		wantIndex  uint64
		wantHint   uint64
		wantLogTrm uint64
	}{
		{"match and extend", Message{Index: 2, LogTerm: 2, Entries: []Entry{{Term: 3, Index: 3}}}, false, 3, 0, 0},
		{"prev term mismatch rejects with hint at older term", Message{Index: 2, LogTerm: 3}, true, 2, 2, 2},
		{"prev beyond log: hint is our last index", Message{Index: 9, LogTerm: 3}, true, 9, 2, 2},
		{"prev below commit: ack our commit point", Message{Index: 0, LogTerm: 0}, false, 0, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := storageWith(3, 1, 2)
			st.SetHardState(HardState{Term: 3})
			rn := newLone(t, 2, []uint64{1, 2, 3}, st)
			m := tt.m
			m.Type, m.From, m.To, m.Term = MsgApp, 1, 2, 3
			if err := rn.Step(m); err != nil {
				t.Fatal(err)
			}
			msgs := flush(t, rn, st)
			if len(msgs) != 1 || msgs[0].Type != MsgAppResp {
				t.Fatalf("messages = %+v", msgs)
			}
			r := msgs[0]
			if r.Reject != tt.wantReject || r.Index != tt.wantIndex || r.RejectHint != tt.wantHint || r.LogTerm != tt.wantLogTrm {
				t.Fatalf("resp = %+v", r)
			}
		})
	}
}

func TestStaleLeaderLearnsHigherTerm(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	st := nw.node(2)
	// A message from a stale leader at term 0 gets a reply carrying our term.
	nw.sent = nil
	if err := st.rn.Step(Message{Type: MsgHeartbeat, From: 3, To: 2, Term: 0}); err != nil {
		t.Fatal(err)
	}
	// Term 0 is below ours; reply goes out.
	rd := st.rn.Ready()
	st.rn.Advance(rd)
	if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgAppResp || rd.Messages[0].Term != 1 {
		t.Fatalf("reply = %+v", rd.Messages)
	}
}

// WBS 1.4.1 / 1.4.2.
func TestProposeErrors(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(t *testing.T) *network
		node       uint64
		wantErr    error
		wantLeader uint64 // for NotLeaderError
	}{
		{"leader accepts", func(t *testing.T) *network {
			nw := newNetwork(t, 3)
			nw.campaign(1)
			return nw
		}, 1, nil, 0},
		{"follower reports leader hint", func(t *testing.T) *network {
			nw := newNetwork(t, 3)
			nw.campaign(1)
			return nw
		}, 2, ErrNotLeader, 1},
		{"no leader known: dropped", func(t *testing.T) *network { return newNetwork(t, 3) }, 2, ErrProposalDropped, 0},
		{"candidate: dropped", func(t *testing.T) *network {
			nw := newNetwork(t, 3, withPreVote(false))
			nw.isolate(1)
			nw.campaign(1)
			return nw
		}, 1, ErrProposalDropped, 0},
		{"pre-candidate: dropped", func(t *testing.T) *network {
			nw := newNetwork(t, 3)
			nw.isolate(1)
			nw.campaign(1)
			return nw
		}, 1, ErrProposalDropped, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nw := tt.setup(t)
			idx, term, err := nw.node(tt.node).rn.Propose([]byte("v"))
			if tt.wantErr == nil {
				if err != nil || idx == 0 || term == 0 {
					t.Fatalf("Propose = %d,%d,%v", idx, term, err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			var nl *NotLeaderError
			if tt.wantLeader != 0 {
				if !errors.As(err, &nl) || nl.Leader != tt.wantLeader {
					t.Fatalf("err = %v, want NotLeaderError{Leader: %d}", err, tt.wantLeader)
				}
			}
			if idx != 0 || term != 0 {
				t.Fatalf("failed proposal returned position %d/%d", idx, term)
			}
		})
	}
}

func TestProposeReturnsPositionOfEntry(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	idx, term, err := nw.propose(1, "first")
	if err != nil || idx != 2 || term != 1 {
		t.Fatalf("got %d,%d,%v", idx, term, err)
	}
	log := nw.node(1).logOf()
	if string(log[idx-1].Data) != "first" || log[idx-1].Term != term {
		t.Fatalf("entry at %d = %+v", idx, log[idx-1])
	}
}

// D-29 / WBS 1.4.3: uncommitted-size limit.
func TestUncommittedSizeLimit(t *testing.T) {
	type step struct {
		size    int
		wantErr error
	}
	tests := []struct {
		name  string
		limit uint64
		steps []step
	}{
		{"under then over", 10, []step{{6, nil}, {6, ErrProposalDropped}, {4, nil}, {1, ErrProposalDropped}}},
		{"empty proposals are always allowed", 10, []step{{10, nil}, {0, nil}, {0, nil}}},
		{"one oversized entry admitted when nothing is pending", 10, []step{{50, nil}, {1, ErrProposalDropped}}},
		{"unlimited", 0, []step{{1000, nil}, {1000, nil}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nw := newNetwork(t, 3)
			for _, n := range nw.nodes {
				n.rn.raft.cfg.MaxUncommittedBytes = tt.limit
			}
			nw.campaign(1)
			nw.isolate(2) // nothing can commit: both followers unreachable
			nw.isolate(3)
			for i, s := range tt.steps {
				_, _, err := nw.propose(1, strings.Repeat("x", s.size))
				if !errors.Is(err, s.wantErr) {
					t.Fatalf("step %d (%d bytes): err = %v, want %v", i, s.size, err, s.wantErr)
				}
			}
		})
	}
}

func TestUncommittedSizeReleasedOnCommit(t *testing.T) {
	nw := newNetwork(t, 3)
	for _, n := range nw.nodes {
		n.rn.raft.cfg.MaxUncommittedBytes = 10
	}
	nw.campaign(1)
	nw.isolate(2)
	nw.isolate(3)
	if _, _, err := nw.propose(1, "123456"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := nw.propose(1, "123456"); !errors.Is(err, ErrProposalDropped) {
		t.Fatalf("err = %v, want ErrProposalDropped", err)
	}
	nw.heal()
	nw.tick(3)
	if s := nw.node(1).rn.Status(); s.Commit != 2 {
		t.Fatalf("commit = %d", s.Commit)
	}
	if _, _, err := nw.propose(1, "123456"); err != nil {
		t.Fatalf("after commit the budget must be free again: %v", err)
	}
}

func TestUncommittedSizeResetOnStepDown(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	nw.isolate(2)
	nw.isolate(3)
	if _, _, err := nw.propose(1, "abc"); err != nil {
		t.Fatal(err)
	}
	r := nw.node(1).rn.raft
	if r.uncommittedSize != 3 {
		t.Fatalf("uncommittedSize = %d", r.uncommittedSize)
	}
	r.becomeFollower(r.term+1, None)
	if r.uncommittedSize != 0 {
		t.Fatal("step-down must clear the uncommitted counter")
	}
}

// D-01: the Ready contract is enforced loudly.
func TestReadyContractViolationsPanic(t *testing.T) {
	newNode := func() *RawNode {
		return newLone(t, 1, []uint64{1}, NewMemoryStorage())
	}
	tests := []struct {
		name string
		do   func(*RawNode)
	}{
		{"Advance without Ready", func(n *RawNode) { n.Advance(Ready{}) }},
		{"Tick while outstanding", func(n *RawNode) { n.Campaign(); n.Ready(); n.Tick() }},
		{"Step while outstanding", func(n *RawNode) { n.Campaign(); n.Ready(); _ = n.Step(Message{Type: MsgApp}) }},
		{"Propose while outstanding", func(n *RawNode) { n.Campaign(); n.Ready(); _, _, _ = n.Propose(nil) }},
		{"second Ready", func(n *RawNode) { n.Campaign(); n.Ready(); n.Ready() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			tt.do(newNode())
		})
	}
}

func TestStepFiltering(t *testing.T) {
	st := NewMemoryStorage()
	rn := newLone(t, 1, []uint64{1, 2, 3}, st)
	if err := rn.Step(Message{Type: MessageType(0), From: 2, To: 1}); !errors.Is(err, ErrUnknownMessage) {
		t.Fatalf("err = %v, want ErrUnknownMessage", err)
	}
	// Unknown sender and wrong destination are ignored without effect.
	for _, m := range []Message{
		{Type: MsgVote, From: 9, To: 1, Term: 5},
		{Type: MsgVote, From: 2, To: 3, Term: 5},
	} {
		if err := rn.Step(m); err != nil {
			t.Fatal(err)
		}
	}
	if rn.HasReady() || rn.Status().Term != 0 {
		t.Fatal("ignored messages must not change state")
	}
}

// sig pins a function value to an exact signature at compile time.
func sig[F any](F) {}

// The Ready/Advance API must compile against the documented signatures.
func TestRawNodeAPICompiles(*testing.T) {
	sig[func(Config) (*RawNode, error)](NewRawNode)
	sig[func(*RawNode)]((*RawNode).Tick)
	sig[func(*RawNode)]((*RawNode).Campaign)
	sig[func(*RawNode, Message) error]((*RawNode).Step)
	sig[func(*RawNode, []byte) (uint64, uint64, error)]((*RawNode).Propose)
	sig[func(*RawNode) bool]((*RawNode).HasReady)
	sig[func(*RawNode) Ready]((*RawNode).Ready)
	sig[func(*RawNode, Ready)]((*RawNode).Advance)
	sig[func(*RawNode) Status]((*RawNode).Status)
}

// ReportUnreachable drops a Replicate follower to Probe (D-12) and is a no-op
// for followers and unknown nodes.
func TestReportUnreachableDropsToProbe(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	if _, _, err := nw.propose(1, "x"); err != nil {
		t.Fatal(err)
	}
	lead := nw.node(1).rn
	if st := lead.Status().Progress[2].State; st != ProgressReplicate {
		t.Fatalf("precondition: node 2 state = %v, want Replicate", st)
	}
	lead.ReportUnreachable(2)
	lead.ReportUnreachable(1)  // self: ignored
	lead.ReportUnreachable(99) // unknown: ignored
	if st := lead.Status().Progress[2].State; st != ProgressProbe {
		t.Fatalf("node 2 state after ReportUnreachable = %v, want Probe", st)
	}
	nw.node(2).rn.ReportUnreachable(1) // follower: no-op, must not panic
}
