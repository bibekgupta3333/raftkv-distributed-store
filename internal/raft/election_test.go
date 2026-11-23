package raft

import (
	"math/rand/v2"
	"testing"
)

// flush runs one correct driver cycle for a lone node and returns its Ready
// messages. Storage is updated like a real driver would.
func flush(t *testing.T, rn *RawNode, st *MemoryStorage) []Message {
	t.Helper()
	var msgs []Message
	for rn.HasReady() {
		rd := rn.Ready()
		st.Append(rd.Entries)
		if !rd.HardState.IsEmpty() {
			st.SetHardState(rd.HardState)
		}
		msgs = append(msgs, rd.Messages...)
		rn.Advance(rd)
	}
	return msgs
}

func newLone(t *testing.T, id uint64, peers []uint64, st *MemoryStorage, opts ...netOpt) *RawNode {
	t.Helper()
	rn, err := NewRawNode(baseConfig(id, peers, st, opts...))
	if err != nil {
		t.Fatal(err)
	}
	return rn
}

// D-08 / WBS 1.2.1: the election timeout is random in [ElectionTick, 2*ElectionTick).
func TestElectionTimeoutWithinBounds(t *testing.T) {
	const e = DefaultElectionTick
	minSeen, maxSeen := 1<<30, 0
	for seed := range uint64(300) {
		st := NewMemoryStorage()
		cfg := DefaultConfig(1, []uint64{1, 2, 3}, st, rand.New(rand.NewPCG(seed, 7)))
		rn, err := NewRawNode(cfg)
		if err != nil {
			t.Fatal(err)
		}
		ticks := 0
		for ; ticks < 3*e; ticks++ {
			rn.Tick()
			if rn.Status().State != StateFollower {
				ticks++
				break
			}
		}
		if ticks < e || ticks >= 2*e {
			t.Fatalf("seed %d: election started after %d ticks, want [%d, %d)", seed, ticks, e, 2*e)
		}
		minSeen, maxSeen = min(minSeen, ticks), max(maxSeen, ticks)
	}
	if minSeen == maxSeen {
		t.Fatalf("timeout never varied (always %d): not randomized", minSeen)
	}
	if minSeen != e || maxSeen != 2*e-1 {
		t.Logf("observed range [%d, %d]", minSeen, maxSeen)
	}
}

func TestElectionTimeoutIsDeterministicPerSeed(t *testing.T) {
	run := func() int {
		st := NewMemoryStorage()
		rn := newLone(t, 1, []uint64{1, 2, 3}, st)
		n := 0
		for rn.Status().State == StateFollower {
			rn.Tick()
			n++
		}
		return n
	}
	if a, b := run(), run(); a != b {
		t.Fatalf("same seed gave %d then %d ticks (D-24)", a, b)
	}
}

func TestStartsElectionWithPreVoteThenVote(t *testing.T) {
	tests := []struct {
		name       string
		preVote    bool
		wantState  StateType
		wantMsg    MessageType
		wantTerm   uint64 // node's own term right after the first campaign
		wantMsgTrm uint64
	}{
		{"prevote on: no term bump", true, StatePreCandidate, MsgPreVote, 0, 1},
		{"prevote off: term bump", false, StateCandidate, MsgVote, 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := NewMemoryStorage()
			rn := newLone(t, 1, []uint64{1, 2, 3}, st, withPreVote(tt.preVote))
			rn.Campaign()
			s := rn.Status()
			if s.State != tt.wantState || s.Term != tt.wantTerm {
				t.Fatalf("state/term = %v/%d, want %v/%d", s.State, s.Term, tt.wantState, tt.wantTerm)
			}
			msgs := flush(t, rn, st)
			if len(msgs) != 2 {
				t.Fatalf("got %d messages, want 2: %+v", len(msgs), msgs)
			}
			for _, m := range msgs {
				if m.Type != tt.wantMsg || m.Term != tt.wantMsgTrm || m.From != 1 {
					t.Fatalf("message = %+v", m)
				}
			}
		})
	}
}

// WBS 1.2.2: every branch of vote handling (T1-T5).
func TestRequestVoteBranches(t *testing.T) {
	type setup struct {
		hardTerm, hardVote uint64
		logTerms           []uint64
		checkQuorum        bool
		leaderKnown        bool // node has heard from a leader this term
	}
	tests := []struct {
		name        string
		s           setup
		req         Message
		wantReply   bool
		wantGrant   bool
		wantTerm    uint64 // node's term afterwards
		wantVote    uint64 // node's votedFor afterwards
		wantRespTrm uint64
	}{
		{"stale term is ignored", setup{hardTerm: 5}, Message{Type: MsgVote, From: 2, Term: 4}, false, false, 5, 0, 0},
		{"fresh term, empty logs: grant", setup{hardTerm: 1}, Message{Type: MsgVote, From: 2, Term: 2}, true, true, 2, 2, 2},
		{"same term, already voted for other: reject", setup{hardTerm: 2, hardVote: 3}, Message{Type: MsgVote, From: 2, Term: 2}, true, false, 2, 3, 2},
		{"same term, already voted for candidate: grant again", setup{hardTerm: 2, hardVote: 2}, Message{Type: MsgVote, From: 2, Term: 2}, true, true, 2, 2, 2},
		{"higher term clears old vote", setup{hardTerm: 2, hardVote: 3}, Message{Type: MsgVote, From: 2, Term: 3}, true, true, 3, 2, 3},
		{"candidate log older term: reject but adopt term", setup{hardTerm: 1, logTerms: []uint64{1, 2}}, Message{Type: MsgVote, From: 2, Term: 3, Index: 5, LogTerm: 1}, true, false, 3, 0, 3},
		{"candidate log shorter same term: reject", setup{hardTerm: 1, logTerms: []uint64{1, 1, 1}}, Message{Type: MsgVote, From: 2, Term: 2, Index: 2, LogTerm: 1}, true, false, 2, 0, 2},
		{"candidate log equal: grant", setup{hardTerm: 1, logTerms: []uint64{1, 1}}, Message{Type: MsgVote, From: 2, Term: 2, Index: 2, LogTerm: 1}, true, true, 2, 2, 2},
		{"candidate log higher term, shorter: grant", setup{hardTerm: 1, logTerms: []uint64{1, 1, 1}}, Message{Type: MsgVote, From: 2, Term: 2, Index: 1, LogTerm: 2}, true, true, 2, 2, 2},
		{"T5 live leader + CheckQuorum: ignore, no term bump", setup{hardTerm: 1, checkQuorum: true, leaderKnown: true}, Message{Type: MsgVote, From: 2, Term: 2}, false, false, 1, 0, 0},
		{"T5 only with CheckQuorum", setup{hardTerm: 1, checkQuorum: false, leaderKnown: true}, Message{Type: MsgVote, From: 2, Term: 2}, true, true, 2, 2, 2},

		// PreVote (T4): never changes term or vote.
		{"prevote stale term: reject with our term", setup{hardTerm: 5}, Message{Type: MsgPreVote, From: 2, Term: 4}, true, false, 5, 0, 5},
		{"prevote granted, term and vote untouched", setup{hardTerm: 1}, Message{Type: MsgPreVote, From: 2, Term: 2}, true, true, 1, 0, 2},
		{"prevote granted even after voting for another", setup{hardTerm: 1, hardVote: 3}, Message{Type: MsgPreVote, From: 2, Term: 2}, true, true, 1, 3, 2},
		{"prevote log stale: reject", setup{hardTerm: 1, logTerms: []uint64{1, 1}}, Message{Type: MsgPreVote, From: 2, Term: 2, Index: 1, LogTerm: 1}, true, false, 1, 0, 1},
		{"prevote ignored by node with live leader", setup{hardTerm: 1, checkQuorum: true, leaderKnown: true}, Message{Type: MsgPreVote, From: 2, Term: 2}, false, false, 1, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := storageWith(tt.s.hardTerm, tt.s.logTerms...)
			st.SetHardState(HardState{Term: tt.s.hardTerm, Vote: tt.s.hardVote})
			rn := newLone(t, 1, []uint64{1, 2, 3}, st, withCheckQuorum(tt.s.checkQuorum), withPreVote(true))
			if tt.s.leaderKnown {
				hb := Message{Type: MsgHeartbeat, From: 3, To: 1, Term: tt.s.hardTerm}
				if err := rn.Step(hb); err != nil {
					t.Fatal(err)
				}
				flush(t, rn, st)
			}
			req := tt.req
			req.To = 1
			if err := rn.Step(req); err != nil {
				t.Fatal(err)
			}
			msgs := flush(t, rn, st)
			s := rn.Status()
			if s.Term != tt.wantTerm || s.Vote != tt.wantVote {
				t.Errorf("term/vote = %d/%d, want %d/%d", s.Term, s.Vote, tt.wantTerm, tt.wantVote)
			}
			var resp []Message
			for _, m := range msgs {
				if m.Type == MsgVoteResp || m.Type == MsgPreVoteResp {
					resp = append(resp, m)
				}
			}
			if !tt.wantReply {
				if len(resp) != 0 {
					t.Fatalf("expected no reply, got %+v", resp)
				}
				return
			}
			if len(resp) != 1 {
				t.Fatalf("got %d replies: %+v", len(resp), msgs)
			}
			r := resp[0]
			if r.To != tt.req.From || r.Reject == tt.wantGrant || r.Term != tt.wantRespTrm {
				t.Fatalf("reply = %+v, want grant=%v term=%d", r, tt.wantGrant, tt.wantRespTrm)
			}
			wantType := MsgVoteResp
			if tt.req.Type == MsgPreVote {
				wantType = MsgPreVoteResp
			}
			if r.Type != wantType {
				t.Fatalf("reply type = %v, want %v", r.Type, wantType)
			}
		})
	}
}

// A granted real vote must be reflected in HardState so the driver persists it
// before replying (D-01).
func TestVoteGrantIsInHardState(t *testing.T) {
	st := NewMemoryStorage()
	rn := newLone(t, 1, []uint64{1, 2, 3}, st)
	if err := rn.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 4}); err != nil {
		t.Fatal(err)
	}
	rd := rn.Ready()
	if rd.HardState.Term != 4 || rd.HardState.Vote != 2 || !rd.MustSync {
		t.Fatalf("Ready = %+v, want HardState{Term:4 Vote:2} with MustSync", rd)
	}
	rn.Advance(rd)
}

// WBS 1.2.3: candidate wins on a majority and steps down on higher term / leader.
func TestCandidateOutcomes(t *testing.T) {
	campaign := func(t *testing.T) (*RawNode, *MemoryStorage) {
		st := NewMemoryStorage()
		rn := newLone(t, 1, []uint64{1, 2, 3, 4, 5}, st, withPreVote(false))
		rn.Campaign()
		flush(t, rn, st)
		return rn, st
	}
	vote := func(from uint64, reject bool, term uint64) Message {
		return Message{Type: MsgVoteResp, From: from, To: 1, Term: term, Reject: reject}
	}
	tests := []struct {
		name      string
		msgs      []Message
		wantState StateType
		wantTerm  uint64
		wantLead  uint64
	}{
		{"one grant of five: still candidate", []Message{vote(2, false, 1)}, StateCandidate, 1, 0},
		{"majority grants: leader", []Message{vote(2, false, 1), vote(3, false, 1)}, StateLeader, 1, 1},
		{"majority rejects: follower, same term", []Message{vote(2, true, 1), vote(3, true, 1), vote(4, true, 1)}, StateFollower, 1, 0},
		{"two grants two rejects: undecided", []Message{vote(2, false, 1), vote(3, true, 1), vote(4, true, 1)}, StateCandidate, 1, 0},
		{"reject carrying higher term: follower at that term", []Message{vote(2, true, 7)}, StateFollower, 7, 0},
		{"leader heartbeat same term: follower", []Message{{Type: MsgHeartbeat, From: 4, To: 1, Term: 1}}, StateFollower, 1, 4},
		{"leader append higher term: follower", []Message{{Type: MsgApp, From: 5, To: 1, Term: 3}}, StateFollower, 3, 5},
		{"stale vote response ignored", []Message{vote(2, false, 0)}, StateCandidate, 1, 0},
		{"duplicate grants count once", []Message{vote(2, false, 1), vote(2, false, 1)}, StateCandidate, 1, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rn, st := campaign(t)
			for _, m := range tt.msgs {
				if err := rn.Step(m); err != nil {
					t.Fatal(err)
				}
				flush(t, rn, st)
			}
			s := rn.Status()
			if s.State != tt.wantState || s.Term != tt.wantTerm || s.Lead != tt.wantLead {
				t.Fatalf("status = %v term %d lead %d; want %v term %d lead %d",
					s.State, s.Term, s.Lead, tt.wantState, tt.wantTerm, tt.wantLead)
			}
		})
	}
}

// D-08 / WBS 1.2.5: a PreCandidate does not bump its term, even over repeated
// timeouts, and moves on to a real election only after a quorum of grants.
func TestPreCandidateDoesNotBumpTerm(t *testing.T) {
	st := NewMemoryStorage()
	st.SetHardState(HardState{Term: 5})
	rn := newLone(t, 1, []uint64{1, 2, 3}, st)
	for range 5 * DefaultElectionTick {
		rn.Tick()
		flush(t, rn, st)
	}
	if s := rn.Status(); s.State != StatePreCandidate || s.Term != 5 || s.Vote != 0 {
		t.Fatalf("status = %+v, want PreCandidate at unchanged term 5 with no vote", s)
	}
	// A PreVote grant carries the prospective term and must not make us follow it.
	if err := rn.Step(Message{Type: MsgPreVoteResp, From: 2, To: 1, Term: 6}); err != nil {
		t.Fatal(err)
	}
	msgs := flush(t, rn, st)
	s := rn.Status()
	if s.State != StateCandidate || s.Term != 6 || s.Vote != 1 {
		t.Fatalf("after quorum of pre-votes: %+v, want Candidate term 6", s)
	}
	for _, m := range msgs {
		if m.Type != MsgVote || m.Term != 6 {
			t.Fatalf("expected MsgVote at term 6, got %+v", m)
		}
	}
}

func TestPreVoteRejectedByHigherTermReturnsToFollower(t *testing.T) {
	st := NewMemoryStorage()
	rn := newLone(t, 1, []uint64{1, 2, 3}, st)
	rn.Campaign()
	flush(t, rn, st)
	if err := rn.Step(Message{Type: MsgPreVoteResp, From: 2, To: 1, Term: 9, Reject: true}); err != nil {
		t.Fatal(err)
	}
	if s := rn.Status(); s.State != StateFollower || s.Term != 9 {
		t.Fatalf("status = %+v", s)
	}
}

// WBS 1.2.4: first entry of a term is a no-op; heartbeats go out every tick
// and keep followers from timing out.
func TestLeaderNoOpAndHeartbeats(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	l := nw.node(1)
	if s := l.rn.Status(); s.State != StateLeader {
		t.Fatalf("node 1 = %v", s.State)
	}
	log := l.logOf()
	if len(log) != 1 || log[0].Term != 1 || log[0].Index != 1 || len(log[0].Data) != 0 || log[0].Type != EntryNormal {
		t.Fatalf("first entry of the term must be an empty no-op, got %+v", log)
	}
	for _, id := range nw.ids {
		if got := nw.node(id).logOf(); len(got) != 1 {
			t.Fatalf("node %d log = %+v", id, got)
		}
		if s := nw.node(id).rn.Status(); s.Commit != 1 {
			t.Fatalf("node %d commit = %d, want the no-op committed", id, s.Commit)
		}
	}

	nw.sent = nil
	nw.tick(1)
	beats := map[uint64]int{}
	for _, m := range nw.sent {
		if m.Type == MsgHeartbeat {
			beats[m.To]++
		}
	}
	if beats[2] != 1 || beats[3] != 1 {
		t.Fatalf("heartbeats per tick = %v, want one to each follower", beats)
	}

	nw.tick(20 * DefaultElectionTick)
	for _, id := range []uint64{2, 3} {
		if s := nw.node(id).rn.Status(); s.State != StateFollower || s.Term != 1 || s.Lead != 1 {
			t.Fatalf("node %d timed out despite heartbeats: %+v", id, s)
		}
	}
	if got := nw.leaders(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("leaders = %v", got)
	}
}

// D-08 CheckQuorum, WBS 1.2.6.
func TestCheckQuorumLeaderStepsDownWithoutQuorum(t *testing.T) {
	for _, cq := range []bool{true, false} {
		nw := newNetwork(t, 3, withCheckQuorum(cq))
		nw.campaign(1)
		nw.isolate(1)
		nw.tickOnly(1, 3*DefaultElectionTick)
		s := nw.node(1).rn.Status()
		if cq && s.State == StateLeader {
			t.Fatal("CheckQuorum on: isolated leader must step down")
		}
		if !cq && s.State != StateLeader {
			t.Fatal("CheckQuorum off: isolated leader lingers (that is the problem CheckQuorum solves)")
		}
	}
}

func TestCheckQuorumKeepsLeaderWithMajority(t *testing.T) {
	nw := newNetwork(t, 5)
	nw.campaign(1)
	nw.isolate(4)
	nw.isolate(5)
	for range 10 {
		nw.tickOnly(1, DefaultElectionTick)
		if nw.node(1).rn.Status().State != StateLeader {
			t.Fatal("leader with 3 of 5 reachable must stay leader")
		}
	}
}

// T5: followers ignore votes while they have a live leader.
func TestFollowerIgnoresVoteWhileLeaderAlive(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	node2 := nw.node(2)
	if err := node2.rn.Step(Message{Type: MsgVote, From: 3, To: 2, Term: 9, Index: 100, LogTerm: 9}); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	if s := node2.rn.Status(); s.Term != 1 || s.Lead != 1 {
		t.Fatalf("follower with live leader was disturbed: %+v", s)
	}
}

// A single node cluster elects itself and commits on its own.
func TestSingleNodeCluster(t *testing.T) {
	nw := newNetwork(t, 1)
	nw.tick(2 * DefaultElectionTick)
	if got := nw.leaders(); len(got) != 1 {
		t.Fatalf("single node never became leader: %v", got)
	}
	idx, term, err := nw.propose(1, "x")
	if err != nil || idx != 2 || term != 1 {
		t.Fatalf("propose = %d,%d,%v", idx, term, err)
	}
	if s := nw.node(1).rn.Status(); s.Commit != 2 || s.Applied != 2 {
		t.Fatalf("status = %+v", s)
	}
}

func TestStatusProgressOnlyOnLeader(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	if nw.node(2).rn.Status().Progress != nil {
		t.Fatal("follower must not expose Progress")
	}
	p := nw.node(1).rn.Status().Progress
	if len(p) != 3 || p[2].State != ProgressReplicate || p[2].Match != 1 || p[1].Match != 1 {
		t.Fatalf("progress = %+v", p)
	}
}

func TestStateStrings(t *testing.T) {
	for _, s := range []fmtStringer{StateFollower, StateCandidate, StatePreCandidate, StateLeader, StateType(9),
		MsgApp, MessageType(0), EntryNormal, EntryConfChange, EntryType(9), ProgressProbe, ProgressReplicate, ProgressState(9)} {
		if s.String() == "" {
			t.Errorf("empty String for %#v", s)
		}
	}
}

type fmtStringer interface{ String() string }
