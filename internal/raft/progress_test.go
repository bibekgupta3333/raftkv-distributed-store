package raft

import (
	"reflect"
	"testing"
)

func TestQuorumIndex(t *testing.T) {
	tests := []struct {
		name    string
		matches []uint64
		want    uint64
	}{
		{"empty", nil, 0},
		{"single", []uint64{7}, 7},
		{"three, majority at second highest", []uint64{5, 3, 4}, 4},
		{"three, one lagging", []uint64{9, 9, 0}, 9},
		{"five, third highest", []uint64{10, 8, 6, 2, 0}, 6},
		{"five, only two ahead", []uint64{10, 10, 0, 0, 0}, 0},
		{"four needs three", []uint64{4, 4, 4, 1}, 4},
		{"four with two ahead", []uint64{4, 4, 1, 1}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := append([]uint64(nil), tt.matches...)
			if got := quorumIndex(tt.matches); got != tt.want {
				t.Fatalf("quorumIndex(%v) = %d, want %d", tt.matches, got, tt.want)
			}
			if !reflect.DeepEqual(in, append([]uint64(nil), tt.matches...)) {
				t.Fatal("quorumIndex modified its input")
			}
		})
	}
}

func TestQuorumSize(t *testing.T) {
	for n, want := range map[int]int{1: 1, 2: 2, 3: 2, 4: 3, 5: 3, 7: 4} {
		if got := quorumSize(n); got != want {
			t.Errorf("quorumSize(%d) = %d, want %d", n, got, want)
		}
	}
}

func TestTallyVotes(t *testing.T) {
	voters := []uint64{1, 2, 3, 4, 5}
	tests := []struct {
		name         string
		votes        map[uint64]bool
		wantG, wantR int
	}{
		{"none", map[uint64]bool{}, 0, 0},
		{"mixed", map[uint64]bool{1: true, 2: false, 3: true}, 2, 1},
		{"non-voters ignored", map[uint64]bool{9: true, 1: true}, 1, 0},
	}
	for _, tt := range tests {
		g, r := tallyVotes(voters, tt.votes)
		if g != tt.wantG || r != tt.wantR {
			t.Errorf("%s: got %d/%d want %d/%d", tt.name, g, r, tt.wantG, tt.wantR)
		}
	}
}

func TestProgressMaybeUpdate(t *testing.T) {
	tests := []struct {
		name        string
		match, next uint64
		update      uint64
		wantUpdated bool
		wantMatch   uint64
		wantNext    uint64
	}{
		{"advance", 3, 4, 5, true, 5, 6},
		{"stale", 3, 8, 2, false, 3, 8},
		{"equal", 3, 4, 3, false, 3, 4},
		{"next never regresses", 3, 10, 5, true, 5, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newProgress(tt.next, 4)
			p.Match = tt.match
			p.probeSent = true
			if got := p.maybeUpdate(tt.update); got != tt.wantUpdated {
				t.Fatalf("updated = %v", got)
			}
			if p.Match != tt.wantMatch || p.Next != tt.wantNext {
				t.Fatalf("match/next = %d/%d, want %d/%d", p.Match, p.Next, tt.wantMatch, tt.wantNext)
			}
			if tt.wantUpdated && p.probeSent {
				t.Fatal("an update must unpause a probe")
			}
		})
	}
}

func TestProgressMaybeDecrTo(t *testing.T) {
	tests := []struct {
		name         string
		state        ProgressState
		match, next  uint64
		rejected     uint64
		hint         uint64
		wantOK       bool
		wantNext     uint64
		wantProbeCan bool
	}{
		{"probe uses hint", ProgressProbe, 0, 11, 10, 3, true, 4, true},
		{"probe hint above rejected is clamped", ProgressProbe, 0, 6, 5, 9, true, 5, true},
		{"probe stale rejection", ProgressProbe, 0, 11, 7, 3, false, 11, false},
		{"probe never below one", ProgressProbe, 0, 2, 1, 0, true, 1, true},
		{"replicate resets to match+1", ProgressReplicate, 5, 20, 12, 3, true, 6, false},
		{"replicate stale (<= match)", ProgressReplicate, 5, 20, 5, 3, false, 20, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newProgress(tt.next, 4)
			p.State, p.Match, p.probeSent = tt.state, tt.match, true
			if got := p.maybeDecrTo(tt.rejected, tt.hint); got != tt.wantOK {
				t.Fatalf("ok = %v, want %v", got, tt.wantOK)
			}
			if p.Next != tt.wantNext {
				t.Fatalf("next = %d, want %d", p.Next, tt.wantNext)
			}
			if tt.wantProbeCan && p.probeSent {
				t.Fatal("probe should be unpaused after a current rejection")
			}
		})
	}
}

func TestProgressStateTransitions(t *testing.T) {
	p := newProgress(1, 2)
	p.Match = 4
	p.becomeReplicate()
	if p.State != ProgressReplicate || p.Next != 5 {
		t.Fatalf("replicate: %+v", p)
	}
	p.Next = 9
	p.inflights.add(8)
	p.becomeProbe()
	if p.State != ProgressProbe || p.Next != 5 || len(p.inflights.items) != 0 {
		t.Fatalf("probe after replicate must rewind Next to Match+1: %+v", p)
	}
}

func TestProgressIsPaused(t *testing.T) {
	p := newProgress(1, 2)
	if p.isPaused() {
		t.Fatal("fresh probe is not paused")
	}
	p.probeSent = true
	if !p.isPaused() {
		t.Fatal("probe with a message in flight is paused")
	}
	p.becomeReplicate()
	p.inflights.add(1)
	if p.isPaused() {
		t.Fatal("replicate below the cap is not paused")
	}
	p.inflights.add(2)
	if !p.isPaused() {
		t.Fatal("D-29: replicate at the inflight cap is paused")
	}
	p.inflights.freeLE(1)
	if p.isPaused() {
		t.Fatal("freeing an ack must unpause")
	}
}

func TestInflights(t *testing.T) {
	var in inflights
	in.cap = 3
	for _, i := range []uint64{2, 4, 6} {
		in.add(i)
	}
	if !in.full() {
		t.Fatal("expected full")
	}
	in.freeLE(4)
	if !reflect.DeepEqual(in.items, []uint64{6}) {
		t.Fatalf("after freeLE(4): %v", in.items)
	}
	in.freeLE(1) // no-op
	in.freeFirstOne()
	in.freeFirstOne() // empty is fine
	if len(in.items) != 0 {
		t.Fatalf("items = %v", in.items)
	}
}

func TestLimitSize(t *testing.T) {
	mk := func(sizes ...int) []Entry {
		var out []Entry
		for i, s := range sizes {
			out = append(out, Entry{Index: uint64(i + 1), Data: make([]byte, s)})
		}
		return out
	}
	tests := []struct {
		name  string
		in    []Entry
		max   uint64
		wantN int
	}{
		{"empty", nil, 10, 0},
		{"always one", mk(50, 50), 1, 1},
		{"exact fit", mk(5, 5, 5), 10, 2},
		{"all fit", mk(1, 1, 1), 100, 3},
		{"no limit", mk(9, 9, 9), noLimit, 3},
	}
	for _, tt := range tests {
		if got := len(limitSize(tt.in, tt.max)); got != tt.wantN {
			t.Errorf("%s: len = %d, want %d", tt.name, got, tt.wantN)
		}
	}
}
