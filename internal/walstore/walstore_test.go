package walstore_test

import (
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
			Compact:      s.CompactTo,
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
