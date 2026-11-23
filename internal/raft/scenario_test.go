package raft

import (
	"reflect"
	"testing"
)

// WBS 1.8.1: Raft paper Figure 7. A leader elected at term 8 faces followers
// whose logs are missing entries (a, b), carry uncommitted extras (c, d, e) or
// diverged long ago (f). All must converge to the leader's log.
func TestFigure7DivergentLogsConverge(t *testing.T) {
	leaderLog := []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6, 6}
	followers := []struct {
		name     string
		id       uint64
		hardTerm uint64
		log      []uint64
	}{
		{"a: missing one entry", 2, 6, []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6}},
		{"b: missing many entries", 3, 4, []uint64{1, 1, 1, 4}},
		{"c: extra uncommitted entry of the leader's term", 4, 6, []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6}},
		{"d: extra uncommitted entries from term 7", 5, 7, []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7}},
		{"e: diverged at term 4", 6, 4, []uint64{1, 1, 1, 4, 4, 4, 4}},
		{"f: diverged at terms 2 and 3", 7, 3, []uint64{1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3}},
	}
	stores := map[uint64]*MemoryStorage{1: storageWith(7, leaderLog...)}
	for _, f := range followers {
		stores[f.id] = storageWith(f.hardTerm, f.log...)
	}
	nw := newNetworkWithStorage(t, stores)
	nw.campaign(1)
	if s := nw.node(1).rn.Status(); s.State != StateLeader || s.Term != 8 {
		t.Fatalf("node 1 = %+v, want leader at term 8 (it wins despite d and c rejecting)", s)
	}
	nw.tick(2)                                             // let heartbeats spread the commit index
	want := append(append([]uint64(nil), leaderLog...), 8) // plus the term-8 no-op
	for _, f := range followers {
		t.Run(f.name, func(t *testing.T) {
			n := nw.node(f.id)
			if got := termsOf(n.logOf()); !reflect.DeepEqual(got, want) {
				t.Fatalf("log terms = %v\n want %v", got, want)
			}
			s := n.rn.Status()
			if s.Commit != 11 || s.Applied != 11 {
				t.Fatalf("commit/applied = %d/%d, want 11/11", s.Commit, s.Applied)
			}
			if got := len(appsTo(nw.sent, f.id)); got > 8 {
				t.Fatalf("%d MsgApp needed; fast backtracking should keep this small", got)
			}
		})
	}
	// Log Matching: identical (index, term, data) everywhere.
	ref := nw.node(1).logOf()
	for _, f := range followers {
		if got := nw.node(f.id).logOf(); !reflect.DeepEqual(got, ref) {
			t.Fatalf("node %d log differs from the leader's", f.id)
		}
	}
}

// WBS 1.8.1: Raft paper Figure 8, part 1: a leader must not commit an
// old-term entry just because a majority holds it.
func TestFigure8OldTermEntryNotCommittedByCounting(t *testing.T) {
	// S1's log: 1:t1, 2:t2. After a restart it is elected at term 4 and
	// appends its no-op at index 3.
	st := storageWith(3, 1, 2)
	st.SetHardState(HardState{Term: 3, Commit: 1})
	rn := newLone(t, 1, []uint64{1, 2, 3, 4, 5}, st, withPreVote(false))
	rn.Campaign()
	flush(t, rn, st)
	for _, from := range []uint64{2, 3} {
		if err := rn.Step(Message{Type: MsgVoteResp, From: from, To: 1, Term: 4}); err != nil {
			t.Fatal(err)
		}
	}
	flush(t, rn, st)
	if s := rn.Status(); s.State != StateLeader || s.Term != 4 || s.LastIndex != 3 {
		t.Fatalf("status = %+v", s)
	}

	ack := func(from, index uint64) {
		t.Helper()
		if err := rn.Step(Message{Type: MsgAppResp, From: from, To: 1, Term: 4, Index: index}); err != nil {
			t.Fatal(err)
		}
		flush(t, rn, st)
	}
	tests := []struct {
		name       string
		from       uint64
		index      uint64
		wantCommit uint64
	}{
		// After this, entry 2 (term 2) is on S1, S2, S3: a majority of five.
		{"S2 has index 2 (term 2)", 2, 2, 1},
		{"S3 has index 2 (term 2): majority holds it, still not committed", 3, 2, 1},
		{"S2 has index 3 (term 4): quorum index still 2", 2, 3, 1},
		{"S3 has index 3 (term 4): current-term entry commits, 2 along with it", 3, 3, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ack(tt.from, tt.index)
			if got := rn.Status().Commit; got != tt.wantCommit {
				t.Fatalf("commit = %d, want %d", got, tt.wantCommit)
			}
		})
	}
}

// WBS 1.8.1: Figure 8, part 2: since entry 2 (term 2) was never committed, S5
// can win with a newer last term and overwrite it everywhere without breaking
// safety.
func TestFigure8UncommittedOldEntryMayBeOverwritten(t *testing.T) {
	stores := map[uint64]*MemoryStorage{
		1: storageWith(2, 1, 2),
		2: storageWith(2, 1, 2),
		3: storageWith(2, 1),
		4: storageWith(2, 1),
		5: storageWith(3, 1, 3),
	}
	nw := newNetworkWithStorage(t, stores)
	for _, id := range nw.ids {
		if c := nw.node(id).rn.Status().Commit; c > 1 {
			t.Fatalf("node %d starts with commit %d", id, c)
		}
	}
	nw.campaign(5)
	if s := nw.node(5).rn.Status(); s.State != StateLeader || s.Term != 4 {
		t.Fatalf("S5 = %+v", s)
	}
	nw.tick(2)
	for _, id := range nw.ids {
		n := nw.node(id)
		if got := termsOf(n.logOf()); !reflect.DeepEqual(got, []uint64{1, 3, 4}) {
			t.Fatalf("node %d log terms = %v, want [1 3 4]: the term-2 entry must be gone", id, got)
		}
		if s := n.rn.Status(); s.Commit != 3 {
			t.Fatalf("node %d commit = %d", id, s.Commit)
		}
		for _, e := range n.applied {
			if e.Term == 2 {
				t.Fatalf("node %d applied the overwritten term-2 entry", id)
			}
		}
	}
}

// WBS 1.8.2: a node that was partitioned away rejoins. Without PreVote its
// inflated term can force the healthy leader to step down; with PreVote (and
// CheckQuorum) the cluster is undisturbed.
func TestDisruptiveRejoinPreVoteOnVsOff(t *testing.T) {
	tests := []struct {
		name         string
		preVote      bool
		checkQuorum  bool
		wantInflated bool
		wantDisturb  bool
	}{
		{"PreVote off, CheckQuorum off", false, false, true, true},
		{"PreVote off, CheckQuorum on", false, true, true, true},
		{"PreVote on, CheckQuorum on (D-08)", true, true, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nw := newNetwork(t, 3, withPreVote(tt.preVote), withCheckQuorum(tt.checkQuorum))
			nw.campaign(1)
			if _, _, err := nw.propose(1, "x"); err != nil {
				t.Fatal(err)
			}
			baseTerm := nw.node(1).rn.Status().Term

			nw.isolate(3)
			nw.tickOnly(3, 15*DefaultElectionTick)
			inflated := nw.node(3).rn.Status().Term > baseTerm
			if inflated != tt.wantInflated {
				t.Fatalf("isolated node term = %d (base %d), inflated=%v want %v",
					nw.node(3).rn.Status().Term, baseTerm, inflated, tt.wantInflated)
			}

			// Leader keeps working with its majority meanwhile.
			if _, _, err := nw.propose(1, "y"); err != nil {
				t.Fatal(err)
			}

			nw.heal()
			nw.tick(6 * DefaultElectionTick)
			s1 := nw.node(1).rn.Status()
			disturbed := s1.State != StateLeader || s1.Term != baseTerm
			if disturbed != tt.wantDisturb {
				t.Fatalf("leader after rejoin = %+v (base term %d), disturbed=%v want %v",
					s1, baseTerm, disturbed, tt.wantDisturb)
			}
			// Either way the cluster must end with exactly one leader and one log.
			if got := nw.leaders(); len(got) != 1 {
				t.Fatalf("leaders = %v", got)
			}
			ref := termsOf(nw.node(nw.leaders()[0]).logOf())
			for _, id := range nw.ids {
				if got := termsOf(nw.node(id).logOf())[:3]; !reflect.DeepEqual(got, ref[:3]) {
					t.Fatalf("node %d log prefix %v != %v", id, got, ref[:3])
				}
			}
		})
	}
}

// WBS 1.8.3: a leader cut off from the majority steps down (D-08), its
// uncommitted proposal is discarded, and the majority elects a replacement.
func TestCheckQuorumLeaderIsolation(t *testing.T) {
	nw := newNetwork(t, 5)
	nw.campaign(1)
	baseTerm := nw.node(1).rn.Status().Term

	nw.isolate(1)
	stale, _, err := nw.propose(1, "stale")
	if err != nil {
		t.Fatal(err)
	}
	nw.tick(4 * DefaultElectionTick)

	old := nw.node(1).rn.Status()
	if old.State == StateLeader {
		t.Fatal("isolated leader did not step down")
	}
	if old.Term != baseTerm {
		t.Fatalf("stepping down must not inflate the term: %d vs %d", old.Term, baseTerm)
	}
	if old.Commit >= stale {
		t.Fatalf("isolated leader committed %d without a quorum", old.Commit)
	}
	var newLeader uint64
	for _, id := range []uint64{2, 3, 4, 5} {
		if s := nw.node(id).rn.Status(); s.State == StateLeader {
			if newLeader != 0 {
				t.Fatalf("two leaders among the majority: %d and %d", newLeader, id)
			}
			newLeader = id
			if s.Term <= baseTerm {
				t.Fatalf("new leader term %d not above %d", s.Term, baseTerm)
			}
		}
	}
	if newLeader == 0 {
		t.Fatal("the majority side elected no leader")
	}
	if _, _, err := nw.propose(newLeader, "fresh"); err != nil {
		t.Fatal(err)
	}

	nw.heal()
	nw.tick(3 * DefaultElectionTick)
	if got := nw.leaders(); len(got) != 1 || got[0] != newLeader {
		t.Fatalf("leaders after heal = %v, want [%d]", got, newLeader)
	}
	for _, e := range nw.node(1).logOf() {
		if string(e.Data) == "stale" {
			t.Fatal("the isolated leader's uncommitted entry survived")
		}
	}
	for _, e := range nw.node(1).applied {
		if string(e.Data) == "stale" {
			t.Fatal("the isolated leader's uncommitted entry was applied")
		}
	}
	found := false
	for _, e := range nw.node(1).applied {
		found = found || string(e.Data) == "fresh"
	}
	if !found {
		t.Fatal("old leader did not catch up to the new leader's commits")
	}
}
