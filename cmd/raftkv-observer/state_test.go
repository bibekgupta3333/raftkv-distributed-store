package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

var eps = []Endpoint{{"n1", "a:1"}, {"n2", "a:2"}, {"n3", "a:3"}}

// st builds a Status reply for member id.
func st(id uint64, role raftkvv1.RaftState, term, leader, applied uint64) *raftkvv1.StatusResponse {
	return &raftkvv1.StatusResponse{
		Header: &raftkvv1.ResponseHeader{ClusterId: 7, MemberId: id},
		State:  role, Term: term, LeaderId: leader,
		CommitIndex: applied, AppliedIndex: applied, LastLogIndex: applied,
	}
}

func up(i int, r *raftkvv1.StatusResponse) Poll { return Poll{Endpoint: eps[i], Resp: r} }
func down(i int) Poll                           { return Poll{Endpoint: eps[i], Err: errors.New("connection refused")} }

const (
	F = raftkvv1.RaftState_FOLLOWER
	L = raftkvv1.RaftState_LEADER
)

func healthy(term, leader, applied uint64) []Poll {
	ps := make([]Poll, 3)
	for i := range 3 {
		role := F
		if uint64(i+1) == leader {
			role = L
		}
		ps[i] = up(i, st(uint64(i+1), role, term, leader, applied))
	}
	return ps
}

func texts(evs []Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Kind + ": " + e.Text
	}
	return out
}

func wantEvents(t *testing.T, got []Event, want ...string) {
	t.Helper()
	g := texts(got)
	if strings.Join(g, "\n") != strings.Join(want, "\n") {
		t.Fatalf("events:\n  got  %q\n  want %q", g, want)
	}
}

// WBS 7.2.2: events are inferred by diffing successive polls.
func TestDiffEvents(t *testing.T) {
	t0 := time.Unix(1000, 0)
	tick := func(i int) time.Time { return t0.Add(time.Duration(i) * 500 * time.Millisecond) }
	o := NewObserver(500 * time.Millisecond)

	// First poll: the initial leader and term appear; n3 is already down.
	s := o.Update(tick(0), []Poll{up(0, st(1, L, 3, 1, 10)), up(1, st(2, F, 3, 1, 10)), down(2)})
	wantEvents(t, Diff(nil, &s), "unreachable: n3 unreachable", "leader: term 0 -> 3, leader none -> n1")
	if s.ClusterID != 7 || s.Term != 3 || s.LeaderID != 1 || s.LeaderName != "n1" {
		t.Fatalf("header = %d %d %d %q", s.ClusterID, s.Term, s.LeaderID, s.LeaderName)
	}

	// Steady state: no events.
	prev := s
	s = o.Update(tick(1), []Poll{up(0, st(1, L, 3, 1, 12)), up(1, st(2, F, 3, 1, 12)), down(2)})
	wantEvents(t, Diff(&prev, &s))

	// n3 comes back.
	prev = s
	s = o.Update(tick(2), healthy(3, 1, 14))
	wantEvents(t, Diff(&prev, &s), "reachable: n3 back (term 3, applied 14)")

	// Leader n1 is killed: unreachable on the very next poll, before any election.
	prev = s
	s = o.Update(tick(3), []Poll{down(0), up(1, st(2, F, 3, 1, 14)), up(2, st(3, F, 3, 1, 14))})
	wantEvents(t, Diff(&prev, &s), "unreachable: n1 unreachable")
	if !s.Nodes[0].Seen || s.Nodes[0].Up || s.Nodes[0].Term != 3 || s.Nodes[0].Applied != 14 {
		t.Fatalf("down node should keep last-known values: %+v", s.Nodes[0])
	}

	// Election: term and leader change in one line.
	prev = s
	s = o.Update(tick(4), []Poll{down(0), up(1, st(2, L, 4, 2, 15)), up(2, st(3, F, 4, 2, 15))})
	wantEvents(t, Diff(&prev, &s), "leader: term 3 -> 4, leader n1 -> n2")

	// Snapshot index moves on n3.
	prev = s
	p := []Poll{down(0), up(1, st(2, L, 4, 2, 16)), up(2, st(3, F, 4, 2, 16))}
	p[2].Resp.SnapshotIndex = 16
	s = o.Update(tick(5), p)
	wantEvents(t, Diff(&prev, &s), "snapshot: n3 snapshot @ 16")

	// The observer accumulated all of the above.
	if len(s.Events) != 6 {
		t.Fatalf("accumulated events = %q", texts(s.Events))
	}
}

// A deposed leader cut off in a minority keeps its old term and leader; the
// header follows the majority at the highest term.
func TestClusterViewIgnoresStaleLeader(t *testing.T) {
	o := NewObserver(500 * time.Millisecond)
	s := o.Update(time.Unix(0, 0), []Poll{
		up(0, st(1, L, 3, 1, 10)), // stale
		up(1, st(2, L, 4, 2, 12)),
		up(2, st(3, F, 4, 2, 12)),
	})
	if s.Term != 4 || s.LeaderID != 2 || s.LeaderName != "n2" {
		t.Fatalf("header = term %d leader %d %q", s.Term, s.LeaderID, s.LeaderName)
	}
	// All down: the term does not go backwards and the leader is unknown.
	s = o.Update(time.Unix(1, 0), []Poll{down(0), down(1), down(2)})
	if s.Term != 4 || s.LeaderID != 0 {
		t.Fatalf("all down: term %d leader %d", s.Term, s.LeaderID)
	}
}

func TestLagAndWriteRate(t *testing.T) {
	o := NewObserver(500 * time.Millisecond)
	mk := func(applied uint64) []Poll {
		ps := healthy(2, 1, applied)
		ps[0].Resp.Followers = []*raftkvv1.FollowerProgress{
			{MemberId: 2, MatchIndex: applied}, {MemberId: 3, MatchIndex: applied - 11},
		}
		return ps
	}
	o.Update(time.Unix(10, 0), mk(100))
	s := o.Update(time.Unix(11, 0), mk(300)) // 200 entries in 1 s
	if got := s.Nodes[0].Lag; got != -1 {
		t.Errorf("leader lag = %d, want -1", got)
	}
	if s.Nodes[1].Lag != 0 || s.Nodes[2].Lag != 11 {
		t.Errorf("follower lag = %d, %d; want 0, 11", s.Nodes[1].Lag, s.Nodes[2].Lag)
	}
	if n := len(s.WritesPerSec); n != 2 || s.WritesPerSec[0] != 0 || s.WritesPerSec[1] != 200 {
		t.Errorf("writes/s = %v, want [0 200]", s.WritesPerSec)
	}
	// The sparkline keeps about 60 s of samples.
	for i := range 200 {
		s = o.Update(time.Unix(12+int64(i), 0), mk(400))
	}
	if len(s.WritesPerSec) != 120 {
		t.Errorf("kept %d samples, want 120", len(s.WritesPerSec))
	}
}

func TestParseEndpoints(t *testing.T) {
	got, err := parseEndpoints("node1=node1:7000, 10.0.0.2:7000")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != (Endpoint{"node1", "node1:7000"}) || got[1] != (Endpoint{"10.0.0.2", "10.0.0.2:7000"}) {
		t.Fatalf("got %+v", got)
	}
	for _, bad := range []string{"", "nohostport", "a=x:1,a=y:2"} {
		if _, err := parseEndpoints(bad); err == nil {
			t.Errorf("parseEndpoints(%q) accepted", bad)
		}
	}
}
