package raft

import (
	"errors"
	"reflect"
	"testing"
)

func ents(spec ...[2]uint64) []Entry { // {index, term}
	out := make([]Entry, len(spec))
	for i, s := range spec {
		out[i] = Entry{Index: s[0], Term: s[1], Data: make([]byte, 1)}
	}
	return out
}

func TestMemoryStorageTerm(t *testing.T) {
	st := NewMemoryStorage()
	st.Append(ents([2]uint64{1, 1}, [2]uint64{2, 2}, [2]uint64{3, 3}))
	if err := st.Compact(1); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		i       uint64
		want    uint64
		wantErr error
	}{
		{0, 0, ErrCompacted},
		{1, 1, nil}, // compaction point keeps its term
		{2, 2, nil},
		{3, 3, nil},
		{4, 0, ErrUnavailable},
	}
	for _, tt := range tests {
		got, err := st.Term(tt.i)
		if !errors.Is(err, tt.wantErr) || got != tt.want {
			t.Errorf("Term(%d) = %d, %v; want %d, %v", tt.i, got, err, tt.want, tt.wantErr)
		}
	}
}

func TestMemoryStorageEntries(t *testing.T) {
	st := NewMemoryStorage()
	st.Append(ents([2]uint64{1, 1}, [2]uint64{2, 2}, [2]uint64{3, 3}, [2]uint64{4, 4}))
	if err := st.Compact(1); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		lo, hi   uint64
		max      uint64
		wantIdx  []uint64
		wantErr  error
		wantNone bool
	}{
		{"compacted", 1, 3, noLimit, nil, ErrCompacted, false},
		{"all", 2, 5, noLimit, []uint64{2, 3, 4}, nil, false},
		{"size limit keeps at least one", 2, 5, 0, []uint64{2}, nil, false},
		{"size limit two", 2, 5, 2, []uint64{2, 3}, nil, false},
		{"beyond last", 2, 6, noLimit, nil, ErrUnavailable, false},
		{"empty range", 3, 3, noLimit, []uint64{}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := st.Entries(tt.lo, tt.hi, tt.max)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			idx := make([]uint64, 0, len(got))
			for _, e := range got {
				idx = append(idx, e.Index)
			}
			if !reflect.DeepEqual(idx, tt.wantIdx) {
				t.Fatalf("indexes = %v, want %v", idx, tt.wantIdx)
			}
		})
	}
}

func TestMemoryStorageAppend(t *testing.T) {
	tests := []struct {
		name      string
		initial   []Entry
		compactTo uint64
		append    []Entry
		wantTerms []uint64 // terms of indexes first..last
		wantFirst uint64
		wantLast  uint64
	}{
		{"extend", ents([2]uint64{1, 1}, [2]uint64{2, 2}), 0, ents([2]uint64{3, 3}),
			[]uint64{1, 2, 3}, 1, 3},
		{"overwrite suffix", ents([2]uint64{1, 1}, [2]uint64{2, 2}, [2]uint64{3, 3}), 0,
			ents([2]uint64{2, 4}), []uint64{1, 4}, 1, 2},
		{"idempotent re-append", ents([2]uint64{1, 1}, [2]uint64{2, 2}), 0,
			ents([2]uint64{1, 1}, [2]uint64{2, 2}), []uint64{1, 2}, 1, 2},
		{"skips compacted prefix", ents([2]uint64{1, 1}, [2]uint64{2, 2}, [2]uint64{3, 3}), 2,
			ents([2]uint64{2, 2}, [2]uint64{3, 9}), []uint64{9}, 3, 3},
		{"entirely compacted is a no-op", ents([2]uint64{1, 1}, [2]uint64{2, 2}), 2,
			ents([2]uint64{1, 7}), nil, 3, 2},
		{"empty append", ents([2]uint64{1, 1}), 0, nil, []uint64{1}, 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := NewMemoryStorage()
			st.Append(tt.initial)
			if tt.compactTo > 0 {
				if err := st.Compact(tt.compactTo); err != nil {
					t.Fatal(err)
				}
			}
			st.Append(tt.append)
			first, _ := st.FirstIndex()
			last, _ := st.LastIndex()
			if first != tt.wantFirst || last != tt.wantLast {
				t.Fatalf("range = [%d,%d], want [%d,%d]", first, last, tt.wantFirst, tt.wantLast)
			}
			var terms []uint64
			for i := first; i <= last; i++ {
				term, err := st.Term(i)
				if err != nil {
					t.Fatal(err)
				}
				terms = append(terms, term)
			}
			if !reflect.DeepEqual(terms, tt.wantTerms) {
				t.Fatalf("terms = %v, want %v", terms, tt.wantTerms)
			}
		})
	}
}

func TestMemoryStorageAppendGapPanics(t *testing.T) {
	st := NewMemoryStorage()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic for a gap")
		}
	}()
	st.Append(ents([2]uint64{3, 1}))
}

func TestMemoryStorageCompactErrors(t *testing.T) {
	st := NewMemoryStorage()
	st.Append(ents([2]uint64{1, 1}, [2]uint64{2, 1}))
	if err := st.Compact(2); err != nil {
		t.Fatal(err)
	}
	if err := st.Compact(2); !errors.Is(err, ErrCompacted) {
		t.Errorf("repeat compact err = %v, want ErrCompacted", err)
	}
	if err := st.Compact(9); err == nil {
		t.Error("compact beyond last must fail")
	}
}

func TestMemoryStorageHardStateAndSnapshot(t *testing.T) {
	st := NewMemoryStorage()
	st.SetHardState(HardState{Term: 2, Vote: 1, Commit: 0})
	if hs, _ := st.InitialState(); hs != (HardState{Term: 2, Vote: 1}) {
		t.Fatalf("hard state = %+v", hs)
	}
	st.ApplySnapshot(Snapshot{Index: 10, Term: 3})
	first, _ := st.FirstIndex()
	last, _ := st.LastIndex()
	if first != 11 || last != 10 {
		t.Fatalf("after snapshot range = [%d,%d]", first, last)
	}
	if s, _ := st.Snapshot(); s != (Snapshot{Index: 10, Term: 3}) {
		t.Fatalf("snapshot = %+v", s)
	}
	// The Storage interface must be satisfied (compile-time contract).
	var _ Storage = st
}
