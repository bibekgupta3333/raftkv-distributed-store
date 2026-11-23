package raft

import (
	"errors"
	"reflect"
	"testing"
)

func newTestLog(t *testing.T, stableTerms []uint64, unstableTerms ...uint64) *raftLog {
	t.Helper()
	l, err := newLog(storageWith(0, stableTerms...))
	if err != nil {
		t.Fatal(err)
	}
	next := uint64(len(stableTerms)) + 1
	for i, term := range unstableTerms {
		l.append(Entry{Term: term, Index: next + uint64(i)})
	}
	return l
}

func mustInvariants(t *testing.T, l *raftLog) {
	t.Helper()
	if err := l.checkInvariants(); err != nil {
		t.Fatalf("invariant violated: %v", err)
	}
}

func TestLogTermAndBounds(t *testing.T) {
	l := newTestLog(t, []uint64{1, 1, 2}, 3, 3)
	tests := []struct {
		i       uint64
		want    uint64
		wantErr error
	}{
		{0, 0, nil}, // dummy entry before the log
		{1, 1, nil},
		{3, 2, nil},
		{4, 3, nil}, // unstable
		{5, 3, nil},
		{6, 0, ErrUnavailable},
	}
	for _, tt := range tests {
		got, err := l.term(tt.i)
		if !errors.Is(err, tt.wantErr) || got != tt.want {
			t.Errorf("term(%d) = %d, %v; want %d, %v", tt.i, got, err, tt.want, tt.wantErr)
		}
	}
	if l.lastIndex() != 5 || l.lastTerm() != 3 || l.stableIndex() != 3 || l.firstIndex() != 1 {
		t.Fatalf("last=%d lastTerm=%d stable=%d first=%d", l.lastIndex(), l.lastTerm(), l.stableIndex(), l.firstIndex())
	}
	mustInvariants(t, l)
}

func TestLogFindConflict(t *testing.T) {
	l := newTestLog(t, []uint64{1, 2, 3})
	mk := func(spec ...[2]uint64) []Entry { return ents(spec...) }
	tests := []struct {
		name string
		in   []Entry
		want uint64
	}{
		{"none", nil, 0},
		{"all match", mk([2]uint64{1, 1}, [2]uint64{2, 2}), 0},
		{"new entries only", mk([2]uint64{4, 4}, [2]uint64{5, 4}), 4},
		{"partial overlap then new", mk([2]uint64{2, 2}, [2]uint64{3, 3}, [2]uint64{4, 4}), 4},
		{"term mismatch", mk([2]uint64{2, 2}, [2]uint64{3, 9}), 3},
		{"mismatch at first", mk([2]uint64{1, 5}), 1},
	}
	for _, tt := range tests {
		if got := l.findConflict(tt.in); got != tt.want {
			t.Errorf("%s: findConflict = %d, want %d", tt.name, got, tt.want)
		}
	}
}

func TestLogFindConflictByTerm(t *testing.T) {
	// indexes 1..8 terms: 1 1 1 2 2 4 4 4
	l := newTestLog(t, []uint64{1, 1, 1, 2, 2, 4, 4, 4})
	tests := []struct {
		name          string
		index, term   uint64
		wantI, wantTm uint64
	}{
		{"exact term, skip nothing", 8, 4, 8, 4},
		{"skip a whole term run", 8, 3, 5, 2},
		{"skip to term 1", 8, 1, 3, 1},
		{"index inside run", 4, 1, 3, 1},
		{"nothing qualifies", 8, 0, 0, 0},
		{"index beyond log is returned unchanged", 20, 4, 20, 0},
	}
	for _, tt := range tests {
		i, term := l.findConflictByTerm(tt.index, tt.term)
		if i != tt.wantI || term != tt.wantTm {
			t.Errorf("%s: got (%d,%d), want (%d,%d)", tt.name, i, term, tt.wantI, tt.wantTm)
		}
	}
}

func TestLogMaybeAppend(t *testing.T) {
	base := func() *raftLog {
		l := newTestLog(t, []uint64{1, 2, 3}) // terms at 1,2,3
		l.committed = 1
		return l
	}
	tests := []struct {
		name         string
		index, term  uint64
		commit       uint64
		in           []Entry
		wantOK       bool
		wantLastNew  uint64
		wantTerms    []uint64
		wantCommit   uint64
		wantPanicMsg bool
	}{
		{"mismatch", 3, 9, 3, ents([2]uint64{4, 9}), false, 0, []uint64{1, 2, 3}, 1, false},
		{"missing prev", 7, 1, 3, ents([2]uint64{8, 1}), false, 0, []uint64{1, 2, 3}, 1, false},
		{"append at tail", 3, 3, 4, ents([2]uint64{4, 3}, [2]uint64{5, 3}), true, 5, []uint64{1, 2, 3, 3, 3}, 4, false},
		{"commit clamped to last new", 3, 3, 99, ents([2]uint64{4, 3}), true, 4, []uint64{1, 2, 3, 3}, 4, false},
		{"heartbeat-like empty", 3, 3, 2, nil, true, 3, []uint64{1, 2, 3}, 2, false},
		{"overwrite conflicting suffix", 1, 1, 1, ents([2]uint64{2, 5}), true, 2, []uint64{1, 5}, 1, false},
		{"duplicate is idempotent", 1, 1, 1, ents([2]uint64{2, 2}, [2]uint64{3, 3}), true, 3, []uint64{1, 2, 3}, 1, false},
		{"truncating committed entry panics", 0, 0, 0, ents([2]uint64{1, 7}), false, 0, nil, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := base()
			if tt.wantPanicMsg {
				defer func() {
					if recover() == nil {
						t.Fatal("expected panic (system-design 2.2: truncating at/below commit is a bug)")
					}
				}()
			}
			lastNew, ok := l.maybeAppend(tt.index, tt.term, tt.commit, tt.in...)
			if ok != tt.wantOK || lastNew != tt.wantLastNew {
				t.Fatalf("ok/lastNew = %v/%d, want %v/%d", ok, lastNew, tt.wantOK, tt.wantLastNew)
			}
			if got := termsOf(l.allEntries()); !reflect.DeepEqual(got, tt.wantTerms) {
				t.Fatalf("terms = %v, want %v", got, tt.wantTerms)
			}
			if l.committed != tt.wantCommit {
				t.Fatalf("committed = %d, want %d", l.committed, tt.wantCommit)
			}
			mustInvariants(t, l)
		})
	}
}

func TestLogMaybeCommitOnlyCurrentTerm(t *testing.T) {
	l := newTestLog(t, []uint64{1, 2, 2, 4})
	tests := []struct {
		name       string
		idx, term  uint64
		wantCommit bool
		wantAt     uint64
	}{
		{"older-term entry is not committed by counting", 3, 4, false, 0},
		{"current-term entry commits (and implies earlier ones)", 4, 4, true, 4},
		{"not above committed", 2, 4, false, 4},
	}
	for _, tt := range tests {
		if got := l.maybeCommit(tt.idx, tt.term); got != tt.wantCommit {
			t.Errorf("%s: got %v", tt.name, got)
		}
		if tt.wantAt != 0 && l.committed != tt.wantAt {
			t.Errorf("%s: committed = %d, want %d", tt.name, l.committed, tt.wantAt)
		}
	}
	if l.committed != 4 {
		t.Fatalf("committed = %d", l.committed)
	}
}

func TestLogCommitBeyondLastPanics(t *testing.T) {
	l := newTestLog(t, []uint64{1})
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	l.commitTo(5)
}

func TestLogStableToAndSlice(t *testing.T) {
	l := newTestLog(t, []uint64{1, 1}, 2, 2, 3)
	if got := len(l.unstableEntries()); got != 3 {
		t.Fatalf("unstable = %d", got)
	}
	// stableTo with the wrong term is ignored.
	l.stableTo(4, 9)
	if l.stableIndex() != 2 {
		t.Fatalf("stable moved on a wrong-term confirmation: %d", l.stableIndex())
	}
	// Persist (as a driver would), then confirm.
	l.storage.(*MemoryStorage).Append(l.unstableEntries())
	l.stableTo(4, 2)
	if l.stableIndex() != 4 || len(l.unstableEntries()) != 1 {
		t.Fatalf("stable=%d unstable=%d", l.stableIndex(), len(l.unstableEntries()))
	}
	mustInvariants(t, l)

	tests := []struct {
		name    string
		lo, hi  uint64
		max     uint64
		want    []uint64
		wantErr error
	}{
		{"spans stable and unstable", 2, 6, noLimit, []uint64{1, 2, 2, 3}, nil},
		{"stable only", 1, 3, noLimit, []uint64{1, 1}, nil},
		{"unstable only", 5, 6, noLimit, []uint64{3}, nil},
		{"beyond", 4, 8, noLimit, nil, ErrUnavailable},
		{"empty", 3, 3, noLimit, nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := l.slice(tt.lo, tt.hi, tt.max)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v", err)
			}
			if tt.wantErr == nil && !reflect.DeepEqual(termsOf(got), termsOf(entsOrNil(tt.want))) {
				t.Fatalf("terms = %v, want %v", termsOf(got), tt.want)
			}
		})
	}
}

func entsOrNil(terms []uint64) []Entry {
	var out []Entry
	for _, tm := range terms {
		out = append(out, Entry{Term: tm})
	}
	return out
}

func TestLogTruncationKeepsOffsetConsistent(t *testing.T) {
	l := newTestLog(t, []uint64{1, 1, 1}, 2, 2)
	// A new leader overwrites index 3 onward (stored index 3 is stale).
	l.append(Entry{Term: 3, Index: 3})
	if l.lastIndex() != 3 || l.lastTerm() != 3 {
		t.Fatalf("last = %d/%d", l.lastIndex(), l.lastTerm())
	}
	mustInvariants(t, l)
}

func TestLogNextCommittedEnts(t *testing.T) {
	l := newTestLog(t, []uint64{1, 1, 1, 1})
	l.commitTo(3)
	l.appliedTo(1)
	got := l.nextCommittedEnts()
	if len(got) != 2 || got[0].Index != 2 || got[1].Index != 3 {
		t.Fatalf("next committed = %+v", got)
	}
	if !l.hasNextCommittedEnts() {
		t.Fatal("expected pending committed entries")
	}
	l.appliedTo(3)
	if l.hasNextCommittedEnts() || l.nextCommittedEnts() != nil {
		t.Fatal("nothing should be pending")
	}
	mustInvariants(t, l)

	defer func() {
		if recover() == nil {
			t.Fatal("applying beyond committed must panic")
		}
	}()
	l.appliedTo(4)
}

func TestIsLogUpToDate(t *testing.T) {
	tests := []struct {
		name                           string
		candIdx, candTerm, ourIdx, our uint64
		want                           bool
	}{
		{"higher term wins despite shorter log", 1, 3, 9, 2, true},
		{"lower term loses despite longer log", 9, 2, 1, 3, false},
		{"same term longer wins", 5, 2, 4, 2, true},
		{"same term equal wins", 4, 2, 4, 2, true},
		{"same term shorter loses", 3, 2, 4, 2, false},
		{"both empty", 0, 0, 0, 0, true},
	}
	for _, tt := range tests {
		if got := isLogUpToDate(tt.candIdx, tt.candTerm, tt.ourIdx, tt.our); got != tt.want {
			t.Errorf("%s: got %v want %v", tt.name, got, tt.want)
		}
	}
}
