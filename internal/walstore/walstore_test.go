package walstore_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft/storagetest"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/wal"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/walstore"
)

func open(t *testing.T, dir string) *walstore.Store {
	t.Helper()
	s, err := walstore.Open(dir, wal.Options{ClusterID: 9, MemberID: 1, OnFatal: func(err error) { t.Errorf("wal fatal: %v", err) }})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// WBS 2.1.10: the WAL-backed store passes the same suite as MemoryStorage.
func TestWALStoreConformance(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) storagetest.Fixture {
		s := open(t, t.TempDir())
		t.Cleanup(func() { _ = s.Close() })
		return storagetest.Fixture{
			Storage:      s,
			Append:       s.Append,
			SetHardState: s.SetHardState,
			Compact: func(i uint64) error {
				// D-30: a trim needs a snapshot covering it.
				term, err := s.Term(i)
				if err != nil {
					return err
				}
				if snap, _ := s.Snapshot(); snap.Index < i {
					s.SetSnapshot(raft.Snapshot{Index: i, Term: term})
				}
				return s.CompactTo(i)
			},
		}
	})
}

// A restart rebuilds exactly what was synced (D-03: the WAL is the truth).
func TestReopenRecoversLogAndHardState(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	ents := []raft.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Type: raft.EntryConfChange, Data: []byte("b")},
		{Index: 3, Term: 2},
	}
	rd := raft.Ready{Entries: ents, HardState: raft.HardState{Term: 2, Vote: 1, Commit: 2}, MustSync: true}
	if err := s.Save(rd); err != nil {
		t.Fatal(err)
	}
	// A conflicting overwrite must also survive a restart (WAL 1.7).
	if err := s.Save(raft.Ready{Entries: []raft.Entry{{Index: 3, Term: 3, Data: []byte("c")}}, MustSync: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open(t, dir)
	defer func() { _ = s.Close() }()
	hs, _ := s.InitialState()
	if want := (raft.HardState{Term: 2, Vote: 1, Commit: 2}); hs != want {
		t.Fatalf("HardState = %+v, want %+v", hs, want)
	}
	got, err := s.Entries(1, 4, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	want := []raft.Entry{ents[0], ents[1], {Index: 3, Term: 3, Data: []byte("c")}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("entries after reopen = %+v, want %+v", got, want)
	}
}

func seqEnts(from, to, term uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Index: i, Term: term, Data: []byte{byte(i)}})
	}
	return out
}

// D-30 backstop: never trim past the newest snapshot.
func TestCompactToRefusesBeyondSnapshot(t *testing.T) {
	s := open(t, t.TempDir())
	defer func() { _ = s.Close() }()
	if err := s.Append(seqEnts(1, 10, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.CompactTo(5); !errors.Is(err, walstore.ErrBeyondSnapshot) {
		t.Fatalf("trim without a snapshot: err = %v", err)
	}
	s.SetSnapshot(raft.Snapshot{Index: 6, Term: 1})
	if err := s.CompactTo(7); !errors.Is(err, walstore.ErrBeyondSnapshot) {
		t.Fatalf("trim past the snapshot: err = %v", err)
	}
	if err := s.CompactTo(6); err != nil {
		t.Fatal(err)
	}
	if snap, _ := s.Snapshot(); snap != (raft.Snapshot{Index: 6, Term: 1}) {
		t.Fatalf("Snapshot() = %+v", snap)
	}
}

// The compaction point and its term survive a restart (SNAPSHOT_MARKER).
func TestCompactionSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	if err := s.Save(raft.Ready{Entries: append(seqEnts(1, 4, 1), seqEnts(5, 9, 2)...), MustSync: true}); err != nil {
		t.Fatal(err)
	}
	s.SetSnapshot(raft.Snapshot{Index: 7, Term: 2})
	if err := s.CompactTo(6); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, dir)
	defer func() { _ = s.Close() }()
	first, _ := s.FirstIndex()
	last, _ := s.LastIndex()
	if first != 7 || last != 9 {
		t.Fatalf("range after reopen = [%d,%d], want [7,9]", first, last)
	}
	if term, err := s.Term(6); err != nil || term != 2 {
		t.Fatalf("Term(6) = %d, %v; the compaction point keeps its term", term, err)
	}
}

// An installed snapshot replaces the log, durably (storage-design 4.4 step 6).
func TestApplySnapshotSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir)
	if err := s.Save(raft.Ready{Entries: seqEnts(1, 8, 1), HardState: raft.HardState{Term: 1, Commit: 2}, MustSync: true}); err != nil {
		t.Fatal(err)
	}
	snap := raft.Snapshot{Index: 20, Term: 3}
	rd := raft.Ready{Snapshot: &snap, Entries: seqEnts(21, 22, 3), HardState: raft.HardState{Term: 3, Commit: 20}, MustSync: true}
	if err := s.Save(rd); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, dir)
	defer func() { _ = s.Close() }()
	first, _ := s.FirstIndex()
	last, _ := s.LastIndex()
	if first != 21 || last != 22 {
		t.Fatalf("range after reopen = [%d,%d], want [21,22]", first, last)
	}
	if term, _ := s.Term(20); term != 3 {
		t.Fatalf("Term(20) = %d, want 3", term)
	}
	if hs, _ := s.InitialState(); hs.Commit != 20 {
		t.Fatalf("commit = %d, want 20", hs.Commit)
	}
}
