// Package storagetest is the reusable conformance suite for [raft.Storage]
// implementations (WBS 2.1.10). MemoryStorage and the WAL-backed store both run
// it, so the two cannot drift apart.
package storagetest

import (
	"errors"
	"reflect"
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Fixture is one fresh store under test plus the write operations the
// raft.Storage read interface does not have. The driver performs these writes
// from Ready (D-35).
type Fixture struct {
	Storage raft.Storage
	// Append persists entries, replacing any conflicting stored suffix.
	Append func([]raft.Entry) error
	// SetHardState persists hs.
	SetHardState func(raft.HardState) error
	// Compact discards entries up to and including index, keeping its term.
	// May be nil if the store has no compaction yet; those cases are skipped.
	Compact func(index uint64) error
}

const noLimit = ^uint64(0)

func entry(index, term uint64) raft.Entry {
	return raft.Entry{Index: index, Term: term, Data: []byte{byte(index)}}
}

func seq(from, to, term uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, entry(i, term))
	}
	return out
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// Run executes every case against a fresh Fixture from newFixture.
func Run(t *testing.T, newFixture func(t *testing.T) Fixture) {
	t.Helper()
	cases := []struct {
		name string
		fn   func(t *testing.T, f Fixture)
	}{
		{"EmptyStore", testEmpty},
		{"AppendAndIndexes", testAppendAndIndexes},
		{"Term", testTerm},
		{"Entries", testEntries},
		{"EntriesSizeLimit", testEntriesSizeLimit},
		{"ConflictTruncation", testConflictTruncation},
		{"HardState", testHardState},
		{"Compact", testCompact},
		{"EntryRoundTrip", testDataIntegrity},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.fn(t, newFixture(t)) })
	}
}

func testEmpty(t *testing.T, f Fixture) {
	s := f.Storage
	hs, err := s.InitialState()
	must(t, err)
	if !hs.IsEmpty() {
		t.Fatalf("InitialState = %+v, want zero", hs)
	}
	first, err := s.FirstIndex()
	must(t, err)
	last, err := s.LastIndex()
	must(t, err)
	if first != 1 || last != 0 {
		t.Fatalf("first/last = %d/%d, want 1/0", first, last)
	}
	if _, err := s.Entries(1, 2, noLimit); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("Entries on empty store err = %v, want ErrUnavailable", err)
	}
	if term, err := s.Term(0); err != nil || term != 0 {
		t.Fatalf("Term(0) = %d, %v; want 0, nil", term, err)
	}
	snap, err := s.Snapshot()
	must(t, err)
	if snap != (raft.Snapshot{}) {
		t.Fatalf("Snapshot = %+v, want zero", snap)
	}
}

func testAppendAndIndexes(t *testing.T, f Fixture) {
	must(t, f.Append(seq(1, 3, 1)))
	must(t, f.Append(seq(4, 5, 2)))
	must(t, f.Append(nil)) // empty append is a no-op
	first, _ := f.Storage.FirstIndex()
	last, _ := f.Storage.LastIndex()
	if first != 1 || last != 5 {
		t.Fatalf("first/last = %d/%d, want 1/5", first, last)
	}
}

func testTerm(t *testing.T, f Fixture) {
	must(t, f.Append([]raft.Entry{entry(1, 1), entry(2, 1), entry(3, 3)}))
	for _, tt := range []struct {
		i       uint64
		want    uint64
		wantErr error
	}{{0, 0, nil}, {1, 1, nil}, {3, 3, nil}, {4, 0, raft.ErrUnavailable}} {
		got, err := f.Storage.Term(tt.i)
		if !errors.Is(err, tt.wantErr) || got != tt.want {
			t.Errorf("Term(%d) = %d, %v; want %d, %v", tt.i, got, err, tt.want, tt.wantErr)
		}
	}
}

func indexes(ents []raft.Entry) []uint64 {
	out := make([]uint64, 0, len(ents))
	for _, e := range ents {
		out = append(out, e.Index)
	}
	return out
}

func testEntries(t *testing.T, f Fixture) {
	must(t, f.Append(seq(1, 4, 1)))
	for _, tt := range []struct {
		name    string
		lo, hi  uint64
		want    []uint64
		wantErr error
	}{
		{"all", 1, 5, []uint64{1, 2, 3, 4}, nil},
		{"middle", 2, 4, []uint64{2, 3}, nil},
		{"empty range", 3, 3, []uint64{}, nil},
		{"beyond last", 2, 6, nil, raft.ErrUnavailable},
	} {
		got, err := f.Storage.Entries(tt.lo, tt.hi, noLimit)
		if !errors.Is(err, tt.wantErr) {
			t.Fatalf("%s: err = %v, want %v", tt.name, err, tt.wantErr)
		}
		if tt.wantErr == nil && !reflect.DeepEqual(indexes(got), tt.want) {
			t.Fatalf("%s: indexes = %v, want %v", tt.name, indexes(got), tt.want)
		}
	}
}

func testEntriesSizeLimit(t *testing.T, f Fixture) {
	must(t, f.Append(seq(1, 4, 1))) // each entry has a 1-byte payload
	for _, tt := range []struct {
		max  uint64
		want []uint64
	}{{0, []uint64{1}}, {1, []uint64{1}}, {2, []uint64{1, 2}}, {noLimit, []uint64{1, 2, 3, 4}}} {
		got, err := f.Storage.Entries(1, 5, tt.max)
		must(t, err)
		if !reflect.DeepEqual(indexes(got), tt.want) {
			t.Errorf("max=%d: indexes = %v, want %v (always at least one entry)", tt.max, indexes(got), tt.want)
		}
	}
}

func testConflictTruncation(t *testing.T, f Fixture) {
	must(t, f.Append(seq(1, 5, 1)))
	// A new leader overwrites from index 3 with term 2: 3..5 are replaced.
	must(t, f.Append([]raft.Entry{entry(3, 2), entry(4, 2)}))
	last, _ := f.Storage.LastIndex()
	if last != 4 {
		t.Fatalf("LastIndex = %d, want 4 (suffix 3..5 replaced by 3..4)", last)
	}
	got, err := f.Storage.Entries(1, 5, noLimit)
	must(t, err)
	terms := make([]uint64, len(got))
	for i, e := range got {
		terms[i] = e.Term
	}
	if !reflect.DeepEqual(terms, []uint64{1, 1, 2, 2}) {
		t.Fatalf("terms = %v, want [1 1 2 2]", terms)
	}
	if _, err := f.Storage.Term(5); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("Term(5) err = %v, want ErrUnavailable after truncation", err)
	}
}

func testHardState(t *testing.T, f Fixture) {
	want := raft.HardState{Term: 7, Vote: 2, Commit: 3}
	must(t, f.SetHardState(raft.HardState{Term: 5, Vote: 1}))
	must(t, f.SetHardState(want))
	got, err := f.Storage.InitialState()
	must(t, err)
	if got != want {
		t.Fatalf("InitialState = %+v, want %+v (last write wins)", got, want)
	}
}

func testCompact(t *testing.T, f Fixture) {
	if f.Compact == nil {
		t.Skip("store has no compaction")
	}
	must(t, f.Append([]raft.Entry{entry(1, 1), entry(2, 2), entry(3, 3), entry(4, 4)}))
	must(t, f.Compact(2))
	first, _ := f.Storage.FirstIndex()
	if first != 3 {
		t.Fatalf("FirstIndex = %d, want 3", first)
	}
	if term, err := f.Storage.Term(2); err != nil || term != 2 {
		t.Fatalf("Term(2) = %d, %v; the compaction point keeps its term", term, err)
	}
	if _, err := f.Storage.Term(1); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("Term(1) err = %v, want ErrCompacted", err)
	}
	if _, err := f.Storage.Entries(2, 4, noLimit); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("Entries(2,4) err = %v, want ErrCompacted", err)
	}
	got, err := f.Storage.Entries(3, 5, noLimit)
	must(t, err)
	if !reflect.DeepEqual(indexes(got), []uint64{3, 4}) {
		t.Fatalf("Entries(3,5) = %v", indexes(got))
	}
	if err := f.Compact(2); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("re-compacting err = %v, want ErrCompacted", err)
	}
	// Appending after compaction continues the log.
	must(t, f.Append([]raft.Entry{entry(5, 4)}))
	if last, _ := f.Storage.LastIndex(); last != 5 {
		t.Fatalf("LastIndex = %d, want 5", last)
	}
}

func testDataIntegrity(t *testing.T, f Fixture) {
	must(t, f.Append([]raft.Entry{{Index: 1, Term: 1, Type: raft.EntryConfChange, Data: []byte("abc")}}))
	got, err := f.Storage.Entries(1, 2, noLimit)
	must(t, err)
	if len(got) != 1 || got[0].Type != raft.EntryConfChange || string(got[0].Data) != "abc" || got[0].Term != 1 {
		t.Fatalf("entry did not round-trip: %+v", got)
	}
}
