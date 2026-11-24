package raft

import (
	"errors"
	"slices"
	"testing"
)

// ---- harness helpers for membership tests ----

// addJoiner adds a node that joins an existing cluster (D-32): empty storage
// and no bootstrap voters, so it is passive until the leader adds it.
func (nw *network) addJoiner(id uint64) *testNode {
	nw.t.Helper()
	st := NewMemoryStorage()
	rn, err := NewRawNode(baseConfig(id, nil, st))
	if err != nil {
		nw.t.Fatal(err)
	}
	n := &testNode{id: id, rn: rn, st: st}
	nw.nodes[id] = n
	nw.ids = append(nw.ids, id)
	slices.Sort(nw.ids)
	return n
}

// confChange proposes cc on node id and settles; it fails the test on error.
func (nw *network) confChange(id uint64, cc ConfChange) uint64 {
	nw.t.Helper()
	idx, _, err := nw.nodes[id].rn.ProposeConfChange(cc)
	if err != nil {
		nw.t.Fatalf("ProposeConfChange(%v %d) on %d: %v", cc.Type, cc.NodeID, id, err)
	}
	nw.settle()
	return idx
}

func voters(n *testNode) []uint64   { return n.rn.Status().Config.Voters }
func learners(n *testNode) []uint64 { return n.rn.Status().Config.Learners }

// ---- 6.1.1 encoding ----

func TestConfChangeEncodingRoundTrip(t *testing.T) {
	cases := []struct {
		cc ConfChange
		cs ConfState
	}{
		{ConfChange{Type: ConfChangeAddLearner, NodeID: 0xdeadbeefcafe, Context: []byte("ctx")},
			ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{0xdeadbeefcafe}}},
		{ConfChange{Type: ConfChangePromoteLearner, NodeID: 4},
			ConfState{Voters: []uint64{1, 2, 3, 4}}},
		{ConfChange{Type: ConfChangeRemoveNode, NodeID: 1 << 63},
			ConfState{Voters: []uint64{2}, Removed: []uint64{1 << 63}}},
	}
	for _, c := range cases {
		data := EncodeConfChange(c.cc, c.cs)
		if data[0] != confChangeVersion {
			t.Fatalf("version byte = %d", data[0])
		}
		cc, cs, err := DecodeConfChange(data)
		if err != nil {
			t.Fatal(err)
		}
		if cc.Type != c.cc.Type || cc.NodeID != c.cc.NodeID || string(cc.Context) != string(c.cc.Context) || !cs.Equal(c.cs) {
			t.Fatalf("round trip: got %+v %v, want %+v %v", cc, cs, c.cc, c.cs)
		}
	}
	good := EncodeConfChange(cases[0].cc, cases[0].cs)
	bad := map[string][]byte{
		"empty":           nil,
		"unknown version": append([]byte{2}, good[1:]...),
		"truncated":       good[:len(good)-1],
		"trailing bytes":  append(slices.Clone(good), 0),
	}
	for name, b := range bad {
		if _, _, err := DecodeConfChange(b); !errors.Is(err, ErrBadConfChange) {
			t.Errorf("%s: err = %v, want ErrBadConfChange", name, err)
		}
	}
}

func TestConfStateApply(t *testing.T) {
	base := ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}, Removed: []uint64{9}}
	bad := []ConfChange{
		{Type: ConfChangeAddLearner, NodeID: 0},
		{Type: ConfChangeAddLearner, NodeID: 2},
		{Type: ConfChangeAddLearner, NodeID: 4},
		{Type: ConfChangeAddLearner, NodeID: 9}, // D-38: never reused
		{Type: ConfChangePromoteLearner, NodeID: 1},
		{Type: ConfChangeRemoveNode, NodeID: 7},
		{Type: ConfChangeType(99), NodeID: 5},
	}
	for _, cc := range bad {
		if _, err := base.Apply(cc); !errors.Is(err, ErrInvalidConfChange) {
			t.Errorf("%v %d: err = %v, want ErrInvalidConfChange", cc.Type, cc.NodeID, err)
		}
	}
	got, err := base.Apply(ConfChange{Type: ConfChangePromoteLearner, NodeID: 4})
	if err != nil || !got.Equal(ConfState{Voters: []uint64{1, 2, 3, 4}, Removed: []uint64{9}}) {
		t.Fatalf("promote: %v, %v", got, err)
	}
	got, err = base.Apply(ConfChange{Type: ConfChangeRemoveNode, NodeID: 2})
	if err != nil || !got.Equal(ConfState{Voters: []uint64{1, 3}, Learners: []uint64{4}, Removed: []uint64{2, 9}}) {
		t.Fatalf("remove: %v, %v", got, err)
	}
	if !slices.Equal(base.Voters, []uint64{1, 2, 3}) {
		t.Fatal("Apply modified its receiver")
	}
	lone := ConfState{Voters: []uint64{1}}
	if _, err := lone.Apply(ConfChange{Type: ConfChangeRemoveNode, NodeID: 1}); !errors.Is(err, ErrInvalidConfChange) {
		t.Fatalf("removing the last voter: err = %v", err)
	}
}

func TestQuorumsOverlap(t *testing.T) {
	cases := []struct {
		a, b []uint64
		want bool
	}{
		{[]uint64{1, 2, 3}, []uint64{1, 2, 3, 4}, true},       // one added
		{[]uint64{1, 2, 3, 4}, []uint64{1, 2, 3}, true},       // one removed
		{[]uint64{1, 2, 3}, []uint64{1, 2, 3, 4, 5}, false},   // two added: {1,2} vs {3,4,5}
		{[]uint64{1, 2, 3, 4, 5}, []uint64{1, 2, 3}, false},   // two removed
		{[]uint64{1, 2, 3, 4}, []uint64{1, 2, 3, 5}, true},    // one swapped: 3-of-4 majorities share one of 1-3
		{[]uint64{1, 2, 3, 4, 5}, []uint64{1, 2, 3, 4}, true}, // 5 -> 4
		{[]uint64{1}, []uint64{1, 2}, true},
	}
	for _, c := range cases {
		if got := QuorumsOverlap(c.a, c.b); got != c.want {
			t.Errorf("QuorumsOverlap(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
		}
		if got := QuorumsOverlap(c.b, c.a); got != c.want {
			t.Errorf("QuorumsOverlap(%v, %v) = %v, want %v (not symmetric)", c.b, c.a, got, c.want)
		}
	}
}

// ---- 6.1.2 effective on append, rolled back on truncation ----

func TestConfChangeTakesEffectOnAppend(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	nw.addJoiner(4)
	// Hold back everything to node 3 so the entry is appended on 1 and 2
	// but committed nowhere yet: block 2's answers too.
	nw.blocked[[2]uint64{1, 3}] = true
	nw.blocked[[2]uint64{2, 1}] = true
	if _, _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeAddLearner, NodeID: 4}); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	if !slices.Equal(learners(nw.node(1)), []uint64{4}) || !slices.Equal(learners(nw.node(2)), []uint64{4}) {
		t.Fatalf("appended but uncommitted change not in effect: 1 %v, 2 %v",
			nw.node(1).rn.Status().Config, nw.node(2).rn.Status().Config)
	}
	if len(learners(nw.node(3))) != 0 {
		t.Fatal("node 3 never appended the entry but uses its configuration")
	}
	if st := nw.node(1).rn.Status(); st.Commit >= st.ConfIndex {
		t.Fatalf("the change committed (commit %d, conf index %d); the test wants it pending", st.Commit, st.ConfIndex)
	}
}

func TestConfChangeRolledBackWhenTruncated(t *testing.T) {
	nw := newNetwork(t, 5)
	nw.campaign(1)
	nw.addJoiner(6)
	// Only node 2 gets the change; 1 and 2 are then cut off, and 3, 4, 5
	// (which never saw it) elect a leader that overwrites it.
	for _, to := range []uint64{3, 4, 5} {
		nw.blocked[[2]uint64{1, to}] = true
	}
	nw.blocked[[2]uint64{2, 1}] = true
	if _, _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeAddLearner, NodeID: 6}); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	if !slices.Equal(learners(nw.node(2)), []uint64{6}) {
		t.Fatal("setup: node 2 did not append the change")
	}
	nw.heal()
	nw.isolate(1)
	nw.isolate(2)
	nw.tick(3 * DefaultElectionTick)
	l := nw.leaders()
	if len(l) != 1 || l[0] < 3 {
		t.Fatalf("leaders = %v, want one of 3, 4, 5", l)
	}
	if _, _, err := nw.propose(l[0], "overwrite"); err != nil {
		t.Fatal(err)
	}
	nw.heal()
	nw.tick(3)
	for _, id := range []uint64{1, 2} {
		if got := nw.node(id).rn.Status().Config; len(got.Learners) != 0 || !slices.Equal(got.Voters, []uint64{1, 2, 3, 4, 5}) {
			t.Fatalf("node %d kept a truncated configuration: %v", id, got)
		}
	}
}

// ---- 6.1.3 one pending change, current-term commit first ----

func TestOnlyOnePendingConfChange(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	nw.addJoiner(4)
	nw.addJoiner(5)
	nw.isolate(1) // nothing commits
	if _, _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeAddLearner, NodeID: 4}); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	_, _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeAddLearner, NodeID: 5})
	if !errors.Is(err, ErrConfChangePending) {
		t.Fatalf("second change: err = %v, want ErrConfChangePending", err)
	}
	nw.heal()
	nw.tick(1)
	nw.confChange(1, ConfChange{Type: ConfChangeAddLearner, NodeID: 5}) // allowed once committed
	if got := learners(nw.node(1)); !slices.Equal(got, []uint64{4, 5}) {
		t.Fatalf("learners = %v", got)
	}
}

func TestConfChangeWaitsForCurrentTermCommit(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.addJoiner(4)
	// 1 wins, but its no-op cannot commit: nobody acks appends.
	nw.drop = func(m Message) bool { return m.Type == MsgAppResp }
	nw.campaign(1)
	if nw.node(1).rn.Status().State != StateLeader {
		t.Fatal("setup: 1 is not leader")
	}
	_, _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeAddLearner, NodeID: 4})
	if !errors.Is(err, ErrConfChangeTooEarly) {
		t.Fatalf("err = %v, want ErrConfChangeTooEarly", err)
	}
	nw.drop = nil
	nw.tick(1)
	nw.confChange(1, ConfChange{Type: ConfChangeAddLearner, NodeID: 4})
}

// The unsafe switches really switch the rules off (the simulator relies on
// them to plant the bugs, WBS 6.3.1).
func TestUnsafeOptionsDisableTheRules(t *testing.T) {
	nw := newNetwork(t, 3, func(c *Config) {
		c.Unsafe = UnsafeOptions{AllowConcurrentConfChanges: true, AllowEarlyConfChange: true}
	})
	nw.addJoiner(4)
	nw.addJoiner(5)
	nw.drop = func(m Message) bool { return m.Type == MsgAppResp }
	nw.campaign(1)
	for _, id := range []uint64{4, 5} {
		if _, _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeAddLearner, NodeID: id}); err != nil {
			t.Fatalf("unsafe leader refused change for %d: %v", id, err)
		}
	}
}

// ---- 6.1.4 learners ----

func TestLearnerReplicatesButDoesNotVoteOrCount(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	l := nw.addJoiner(4)
	nw.confChange(1, ConfChange{Type: ConfChangeAddLearner, NodeID: 4})
	idx, _, err := nw.propose(1, "x")
	if err != nil {
		t.Fatal(err)
	}
	if st := l.rn.Status(); st.Commit != idx || st.LastIndex != idx {
		t.Fatalf("learner did not replicate: %+v", st)
	}
	if !slices.Equal(learners(l), []uint64{4}) {
		t.Fatalf("learner's own config = %v", l.rn.Status().Config)
	}
	// A learner never campaigns, however long it waits.
	nw.isolate(4)
	nw.tickOnly(4, 100)
	if st := l.rn.Status(); st.State != StateFollower || st.Term != nw.node(1).rn.Status().Term {
		t.Fatalf("learner campaigned: %+v", st)
	}
	l.rn.Campaign()
	nw.settle()
	if st := l.rn.Status(); st.State != StateFollower {
		t.Fatalf("learner campaigned when asked: %+v", st)
	}
	nw.heal()
	nw.tick(1)
	// With 2 and 3 cut off, the learner's ack must not commit anything.
	nw.isolate(2)
	nw.blocked[[2]uint64{1, 3}] = true
	nw.blocked[[2]uint64{3, 1}] = true
	idx, _, err = nw.propose(1, "needs a voter quorum")
	if err != nil {
		t.Fatal(err)
	}
	if l.rn.Status().LastIndex != idx {
		t.Fatal("learner did not get the entry")
	}
	if c := nw.node(1).rn.Status().Commit; c >= idx {
		t.Fatalf("entry %d committed with only the leader and a learner (commit %d)", idx, c)
	}
	// A learner's vote is not counted: 2 campaigns with only 4 reachable.
	nw.heal()
	nw.tick(1)
	if c := nw.node(1).rn.Status().Commit; c < idx {
		t.Fatalf("entry %d not committed after healing", idx)
	}
}

func TestPromotedLearnerVotesAndCounts(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	nw.addJoiner(4)
	nw.confChange(1, ConfChange{Type: ConfChangeAddLearner, NodeID: 4})
	nw.confChange(1, ConfChange{Type: ConfChangePromoteLearner, NodeID: 4})
	for _, id := range nw.ids {
		if got := voters(nw.node(id)); !slices.Equal(got, []uint64{1, 2, 3, 4}) {
			t.Fatalf("node %d voters = %v", id, got)
		}
	}
	// 4 voters: quorum 3. With 1 and 4 cut off from 2 and 3, the leader
	// cannot commit.
	nw.isolate(2)
	idx, _, err := nw.propose(1, "x")
	if err != nil {
		t.Fatal(err)
	}
	if c := nw.node(1).rn.Status().Commit; c < idx {
		t.Fatalf("1, 3, 4 should commit: commit %d < %d", c, idx)
	}
	nw.isolate(3)
	idx, _, _ = nw.propose(1, "y")
	if c := nw.node(1).rn.Status().Commit; c >= idx {
		t.Fatalf("1 and 4 alone committed %d of 4 voters", c)
	}
}

// ---- 6.1.5 removal, including the leader ----

func TestRemovedLeaderStepsDownAfterCommit(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	idx, _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: 1})
	if err != nil {
		t.Fatal(err)
	}
	// Before anything is delivered the leader still leads, without itself
	// in the configuration.
	if st := nw.node(1).rn.Status(); st.State != StateLeader || st.Config.IsVoter(1) {
		t.Fatalf("leader after appending its removal: %+v", st)
	}
	nw.settle()
	st1 := nw.node(1).rn.Status()
	if st1.Commit < idx {
		t.Fatalf("removal not committed: %+v", st1)
	}
	if st1.State == StateLeader {
		t.Fatal("removed leader did not step down after its removal committed")
	}
	// It ordered the most up-to-date voter to campaign at once.
	if l := nw.leaders(); len(l) != 1 || l[0] == 1 {
		t.Fatalf("leaders = %v, want one of 2 or 3", l)
	}
	newLeader := nw.leaders()[0]
	// The removed node never campaigns again, and its messages are ignored.
	nw.tickOnly(1, 50)
	if st := nw.node(1).rn.Status(); st.State != StateFollower {
		t.Fatalf("removed node campaigned: %+v", st)
	}
	term := nw.node(newLeader).rn.Status().Term
	for _, id := range []uint64{2, 3} {
		_ = nw.node(id).rn.Step(Message{Type: MsgVote, From: 1, To: id, Term: term + 10, Index: 100, LogTerm: term + 5})
	}
	nw.settle()
	if got := nw.node(newLeader).rn.Status(); got.State != StateLeader || got.Term != term {
		t.Fatalf("a message from the removed node disturbed the leader: %+v", got)
	}
	if _, _, err := nw.propose(newLeader, "after"); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveFollower(t *testing.T) {
	nw := newNetwork(t, 5)
	nw.campaign(1)
	nw.confChange(1, ConfChange{Type: ConfChangeRemoveNode, NodeID: 5})
	if got := voters(nw.node(1)); !slices.Equal(got, []uint64{1, 2, 3, 4}) {
		t.Fatalf("voters = %v", got)
	}
	if _, ok := nw.node(1).rn.Status().Progress[5]; ok {
		t.Fatal("leader still tracks the removed node")
	}
	// The removed ID can never come back (D-38).
	if _, _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeAddLearner, NodeID: 5}); !errors.Is(err, ErrInvalidConfChange) {
		t.Fatalf("re-adding 5: err = %v", err)
	}
}

// ---- 6.1.6 membership survives a restart ----

func TestMembershipSurvivesRestart(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	nw.addJoiner(4)
	nw.confChange(1, ConfChange{Type: ConfChangeAddLearner, NodeID: 4})
	nw.confChange(1, ConfChange{Type: ConfChangePromoteLearner, NodeID: 4})
	nw.confChange(1, ConfChange{Type: ConfChangeRemoveNode, NodeID: 3})
	want := ConfState{Voters: []uint64{1, 2, 4}, Removed: []uint64{3}}

	restart := func(n *testNode, peers []uint64) ConfState {
		rn, err := NewRawNode(baseConfig(n.id, peers, n.st))
		if err != nil {
			t.Fatal(err)
		}
		return rn.Status().Config
	}
	// From the log alone (the conf-change entries are in the WAL).
	if got := restart(nw.node(2), []uint64{1, 2, 3}); !got.Equal(want) {
		t.Fatalf("restart from log: %v, want %v", got, want)
	}
	// The joiner had no bootstrap voters; its log still rebuilds it.
	if got := restart(nw.node(4), nil); !got.Equal(want) {
		t.Fatalf("joiner restart from log: %v, want %v", got, want)
	}
	// From a snapshot after the log was compacted past every change: the
	// configuration comes from the snapshot metadata.
	leader := nw.node(1)
	compactLeader(t, leader, leader.rn.Status().Applied)
	if got := restart(leader, []uint64{1, 2, 3}); !got.Equal(want) {
		t.Fatalf("restart from snapshot: %v, want %v", got, want)
	}
	snap, _ := leader.st.Snapshot()
	if snap.ConfState == nil || !snap.ConfState.Equal(want) {
		t.Fatalf("snapshot metadata conf = %v", snap.ConfState)
	}
}

func TestLearnerGetsConfigFromSnapshot(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	nw.addJoiner(4)
	nw.confChange(1, ConfChange{Type: ConfChangeAddLearner, NodeID: 4})
	nw.isolate(4)
	nw.confChange(1, ConfChange{Type: ConfChangeRemoveNode, NodeID: 3})
	if _, _, err := nw.propose(1, "x"); err != nil {
		t.Fatal(err)
	}
	leader := nw.node(1)
	compactLeader(t, leader, leader.rn.Status().Applied)
	nw.heal()
	nw.tick(3)
	l := nw.node(4)
	if len(l.snaps) == 0 {
		t.Fatal("learner was not sent a snapshot")
	}
	want := ConfState{Voters: []uint64{1, 2}, Learners: []uint64{4}, Removed: []uint64{3}}
	if got := l.rn.Status().Config; !got.Equal(want) {
		t.Fatalf("learner config after snapshot = %v, want %v", got, want)
	}
}

// ---- 6.2 leadership transfer ----

func TestTransferLeadership(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	// Let 3 lag so the leader must catch it up before TimeoutNow.
	nw.isolate(3)
	for range 3 {
		if _, _, err := nw.propose(1, "x"); err != nil {
			t.Fatal(err)
		}
	}
	nw.heal()
	if err := nw.node(1).rn.TransferLeadership(3); err != nil {
		t.Fatal(err)
	}
	// Proposals pause while the transfer runs.
	if st := nw.node(1).rn.Status(); st.State == StateLeader && st.LeadTransferee == 3 {
		if _, _, err := nw.node(1).rn.Propose([]byte("paused")); !errors.Is(err, ErrTransferInProgress) {
			t.Fatalf("propose during transfer: %v", err)
		}
	}
	nw.settle()
	if l := nw.leaders(); !slices.Equal(l, []uint64{3}) {
		t.Fatalf("leaders = %v, want [3]", l)
	}
	if st := nw.node(3).rn.Status(); st.LastIndex != nw.node(1).rn.Status().LastIndex {
		t.Fatalf("logs differ after transfer: %+v vs %+v", st, nw.node(1).rn.Status())
	}
	// The vote carried the transfer flag, so 2 granted despite having a live
	// leader (T5); no PreVote round was used.
	var sawTransferVote bool
	for _, m := range nw.sent {
		if m.From == 3 && m.Type == MsgVote && isTransferVote(m) {
			sawTransferVote = true
		}
		if m.From == 3 && m.Type == MsgPreVote {
			t.Fatal("transfer target used PreVote")
		}
	}
	if !sawTransferVote {
		t.Fatal("no transfer vote was sent")
	}
	if _, _, err := nw.propose(3, "after"); err != nil {
		t.Fatal(err)
	}
}

func TestTransferLeadershipToMostUpToDate(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	nw.isolate(2)
	if _, _, err := nw.propose(1, "x"); err != nil {
		t.Fatal(err)
	}
	nw.heal()
	if err := nw.node(1).rn.TransferLeadership(None); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	if l := nw.leaders(); !slices.Equal(l, []uint64{3}) {
		t.Fatalf("leaders = %v, want [3] (the most up-to-date voter)", l)
	}
}

func TestTransferLeadershipAbortsAfterElectionTimeout(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	nw.isolate(3) // the target never hears
	if err := nw.node(1).rn.TransferLeadership(3); err != nil {
		t.Fatal(err)
	}
	if _, _, err := nw.node(1).rn.Propose([]byte("x")); !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("propose during transfer: %v", err)
	}
	nw.tickOnly(1, DefaultElectionTick)
	st := nw.node(1).rn.Status()
	if st.State != StateLeader || st.LeadTransferee != None {
		t.Fatalf("transfer did not abort cleanly: %+v", st)
	}
	if _, _, err := nw.propose(1, "resumed"); err != nil {
		t.Fatalf("proposals did not resume: %v", err)
	}
}

func TestTransferLeadershipTargets(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	nw.addJoiner(4)
	nw.confChange(1, ConfChange{Type: ConfChangeAddLearner, NodeID: 4})
	if err := nw.node(1).rn.TransferLeadership(4); !errors.Is(err, ErrTransferTarget) {
		t.Fatalf("to a learner: %v", err)
	}
	if err := nw.node(1).rn.TransferLeadership(9); !errors.Is(err, ErrTransferTarget) {
		t.Fatalf("to a stranger: %v", err)
	}
	if err := nw.node(1).rn.TransferLeadership(1); err != nil {
		t.Fatalf("to itself: %v", err)
	}
	var nl *NotLeaderError
	if err := nw.node(2).rn.TransferLeadership(3); !errors.As(err, &nl) || nl.Leader != 1 {
		t.Fatalf("on a follower: %v", err)
	}
	// A conf change during a transfer waits too.
	nw.isolate(3)
	if err := nw.node(1).rn.TransferLeadership(3); err != nil {
		t.Fatal(err)
	}
	if _, _, err := nw.node(1).rn.ProposeConfChange(ConfChange{Type: ConfChangeRemoveNode, NodeID: 4}); !errors.Is(err, ErrTransferInProgress) {
		t.Fatalf("conf change during transfer: %v", err)
	}
}

func TestTimeoutNowIgnoredByLearner(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	l := nw.addJoiner(4)
	nw.confChange(1, ConfChange{Type: ConfChangeAddLearner, NodeID: 4})
	term := nw.node(1).rn.Status().Term
	if err := l.rn.Step(Message{Type: MsgTimeoutNow, From: 1, To: 4, Term: term}); err != nil {
		t.Fatal(err)
	}
	nw.settle()
	if st := l.rn.Status(); st.State != StateFollower || st.Term != term {
		t.Fatalf("learner acted on TimeoutNow: %+v", st)
	}
}
