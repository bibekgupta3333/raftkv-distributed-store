package raft

import (
	"fmt"
	"testing"
)

// A follower whose disk was wiped comes back empty under its old ID. The
// leader still believes its old match index: heartbeats must not make it
// commit entries it does not have, and its rejection at the acknowledged
// index makes the leader probe it from scratch (here: send a snapshot).
func TestWipedFollowerIsProbedFromScratch(t *testing.T) {
	nw := newNetwork(t, 3)
	nw.campaign(1)
	for i := range 5 {
		if _, _, err := nw.propose(1, fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	leader := nw.node(1)
	compactLeader(t, leader, leader.rn.Status().Applied)
	if m := leader.rn.Status().Progress[3].Match; m != 6 {
		t.Fatalf("match of 3 = %d, want 6", m)
	}
	// Wipe node 3: fresh storage, same ID.
	st := NewMemoryStorage()
	rn, err := NewRawNode(baseConfig(3, nw.ids, st))
	if err != nil {
		t.Fatal(err)
	}
	nw.nodes[3] = &testNode{id: 3, rn: rn, st: st}
	nw.tick(3) // heartbeats carry commit 6, beyond its last index 0
	f := nw.node(3)
	if c := f.rn.Status().Commit; c != 0 {
		t.Fatalf("wiped follower committed %d entries it does not have", c)
	}
	// The next append is rejected at the acknowledged index 6.
	if _, _, err := nw.propose(1, "after-wipe"); err != nil {
		t.Fatal(err)
	}
	nw.tick(3)
	if len(f.snaps) == 0 {
		t.Fatal("the wiped follower was not sent a snapshot")
	}
	if s := f.rn.Status(); s.Commit != leader.rn.Status().Commit || s.LastIndex != leader.rn.Status().LastIndex {
		t.Fatalf("wiped follower %+v did not catch up with %+v", s, leader.rn.Status())
	}
}
