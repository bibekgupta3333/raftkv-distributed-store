// Package walstore adapts the write-ahead log (internal/wal, D-03) to the Raft
// core's [raft.Storage] interface (WBS 2.1.10).
//
// The WAL only appends and replays; it has no random-access reads. Store
// therefore keeps the recovered log in a [raft.MemoryStorage] that mirrors every
// write, and serves all reads from it. The WAL stays the source of truth: every
// mutation is written to the WAL first, and the in-memory view is rebuilt from
// the WAL on Open. Holding the whole log in memory is a deliberate
// learning-project simplification; snapshots and trimming (M3, M5, D-30) bound
// it later.
//
// The package is the one place that knows about both the pure core and the WAL,
// which keeps internal/raft free of I/O (D-01) and internal/wal free of raft
// types.
package walstore

import (
	"fmt"
	"sync"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/wal"
)

// Store is a WAL-backed raft.Storage. Reads and writes are serialized by an
// internal mutex, so diagnostics may read while the driver writes.
type Store struct {
	mu  sync.RWMutex
	w   *wal.WAL
	mem *raft.MemoryStorage

	// TruncatedBytes is how many torn tail bytes Open cut off (D-33).
	TruncatedBytes int64
}

var _ raft.Storage = (*Store)(nil)

// Open opens the WAL in dir (repairing a torn tail, refusing other corruption,
// D-33) and rebuilds the in-memory view of the log and HardState.
func Open(dir string, opts wal.Options) (*Store, error) {
	w, rec, err := wal.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("walstore: open wal: %w", err)
	}
	s := &Store{w: w, mem: raft.NewMemoryStorage()}
	s.TruncatedBytes = rec.TruncatedBytes
	if n := len(rec.Entries); n > 0 {
		if first := rec.Entries[0].Index; first > 1 {
			// Segments below first were released (D-30). Their terms are gone
			// until M5 adds SNAPSHOT_MARKER records, so the compaction point
			// carries term 0.
			s.mem.ApplySnapshot(raft.Snapshot{Index: first - 1})
		}
		s.mem.Append(fromWAL(rec.Entries))
	}
	s.mem.SetHardState(raft.HardState(rec.HardState))
	return s, nil
}

// InitialState implements [raft.Storage].
func (s *Store) InitialState() (raft.HardState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.InitialState()
}

// Entries implements [raft.Storage].
func (s *Store) Entries(lo, hi, maxSize uint64) ([]raft.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.Entries(lo, hi, maxSize)
}

// Term implements [raft.Storage].
func (s *Store) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.Term(i)
}

// FirstIndex implements [raft.Storage].
func (s *Store) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.FirstIndex()
}

// LastIndex implements [raft.Storage].
func (s *Store) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.LastIndex()
}

// Snapshot implements [raft.Storage]. Always the zero value until M5 (D-14).
func (s *Store) Snapshot() (raft.Snapshot, error) { return raft.Snapshot{}, nil }

// Append writes entries to the WAL (durable only after [Store.Sync]) and then
// to the in-memory view, replacing any conflicting suffix (WAL §1.7).
func (s *Store) Append(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.w.AppendEntries(toWAL(entries)); err != nil {
		return err
	}
	s.mem.Append(entries)
	return nil
}

// SetHardState writes hs to the WAL (durable only after [Store.Sync]).
func (s *Store) SetHardState(hs raft.HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.w.SetHardState(wal.HardState(hs)); err != nil {
		return err
	}
	s.mem.SetHardState(hs)
	return nil
}

// Sync fsyncs the WAL. An error is fatal (D-33); see wal.Options.OnFatal.
func (s *Store) Sync() error { return s.w.Sync() }

// Save persists one Ready in the order the driver contract requires (D-35,
// docs/architecture.md 4.2): entries, then HardState, then one fsync when
// rd.MustSync. The caller sends rd.Messages only after Save returns.
func (s *Store) Save(rd raft.Ready) error {
	if err := s.Append(rd.Entries); err != nil {
		return err
	}
	if !rd.HardState.IsEmpty() {
		if err := s.SetHardState(rd.HardState); err != nil {
			return err
		}
	}
	if rd.MustSync {
		return s.Sync()
	}
	return nil
}

// CompactTo drops entries up to and including index from the in-memory view and
// releases whole WAL segments below it. D-30: the caller must pass a bound no
// higher than min(snapshotIndex, durableAppliedIndex); nothing calls this
// before M3/M5. After a restart the first index may be lower than the
// compaction point, because the WAL deletes only whole segments.
func (s *Store) CompactTo(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.mem.Compact(index); err != nil {
		return err
	}
	return s.w.ReleaseTo(index)
}

// Close syncs and closes the WAL.
func (s *Store) Close() error { return s.w.Close() }

func toWAL(ents []raft.Entry) []wal.Entry {
	out := make([]wal.Entry, len(ents))
	for i, e := range ents {
		out[i] = wal.Entry{Index: e.Index, Term: e.Term, Type: uint32(e.Type), Data: e.Data}
	}
	return out
}

func fromWAL(ents []wal.Entry) []raft.Entry {
	out := make([]raft.Entry, len(ents))
	for i, e := range ents {
		out[i] = raft.Entry{Index: e.Index, Term: e.Term, Type: raft.EntryType(e.Type), Data: e.Data}
	}
	return out
}
