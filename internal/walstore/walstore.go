// Package walstore adapts the write-ahead log (internal/wal, D-03) to the Raft
// core's [raft.Storage] interface (WBS 2.1.10).
//
// The WAL only appends and replays; it has no random-access reads. Store
// therefore keeps the recovered log in a [raft.MemoryStorage] that mirrors every
// write, and serves all reads from it. The WAL stays the source of truth: every
// mutation is written to the WAL first, and the in-memory view is rebuilt from
// the WAL on Open. Holding the whole log in memory is a deliberate
// learning-project simplification; snapshots and trimming (D-14, D-30) bound
// it.
//
// The package is the one place that knows about both the pure core and the WAL,
// which keeps internal/raft free of I/O (D-01) and internal/wal free of raft
// types.
package walstore

import (
	"errors"
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
	switch {
	case rec.Start.Index > 0:
		// The log starts after the last SNAPSHOT_MARKER (D-14, D-30).
		s.mem.SetCompacted(rec.Start.Index, rec.Start.Term)
	case len(rec.Entries) > 0 && rec.Entries[0].Index > 1:
		// Segments released before markers existed: the compaction
		// point's term is unknown.
		s.mem.SetCompacted(rec.Entries[0].Index-1, 0)
	}
	if len(rec.Entries) > 0 {
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

// Snapshot implements [raft.Storage]: the newest local snapshot recorded with
// [Store.SetSnapshot] or installed with [Store.ApplySnapshot] (D-14).
func (s *Store) Snapshot() (raft.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mem.Snapshot()
}

// SetSnapshot records that a complete local snapshot exists at snap (created by
// the node, or found on disk at start-up). The leader sends it to followers
// that need compacted entries, and it bounds trimming (D-30).
func (s *Store) SetSnapshot(snap raft.Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mem.SetSnapshot(snap)
}

// ApplySnapshot records an installed snapshot (Ready.Snapshot, storage-design
// 4.4 step 6): a reset SNAPSHOT_MARKER in the WAL, after which the log is
// empty and starts at snap.Index+1. It is durable after [Store.Sync]; the
// driver persists the Ready's HardState (commit >= snap.Index) in the same
// fsync. The state machine must already hold the snapshot.
func (s *Store) ApplySnapshot(snap raft.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.w.ResetTo(wal.Start{Index: snap.Index, Term: snap.Term}); err != nil {
		return err
	}
	return s.mem.ApplySnapshot(snap)
}

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
// docs/architecture.md 4.2): the snapshot marker (the state machine must
// already hold rd.Snapshot), entries, then HardState, then one fsync when
// rd.MustSync. The caller sends rd.Messages only after Save returns.
func (s *Store) Save(rd raft.Ready) error {
	if rd.Snapshot != nil {
		if err := s.ApplySnapshot(*rd.Snapshot); err != nil {
			return err
		}
	}
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

// ErrBeyondSnapshot means a trim would drop entries no local snapshot covers
// (D-30).
var ErrBeyondSnapshot = errors.New("walstore: compaction beyond the newest snapshot")

// CompactTo drops entries up to and including index from the in-memory view,
// writes and fsyncs a SNAPSHOT_MARKER, and then releases whole WAL segments at
// or below it (storage-design 1.8). D-30: the caller must pass a bound no higher
// than min(snapshotIndex, durableAppliedIndex) (see storage.TruncateWAL); a
// bound above the newest snapshot is refused here as a backstop. After a
// restart the first index is the compaction point, whatever segments remain.
func (s *Store) CompactTo(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap, _ := s.mem.Snapshot(); index > snap.Index {
		return fmt.Errorf("%w: %d > snapshot %d", ErrBeyondSnapshot, index, snap.Index)
	}
	term, err := s.mem.Term(index)
	if err != nil {
		return err
	}
	if err := s.mem.Compact(index); err != nil {
		return err
	}
	if err := s.w.MarkCompacted(wal.Start{Index: index, Term: term}); err != nil {
		return err
	}
	if err := s.w.Sync(); err != nil {
		return err
	}
	return s.w.ReleaseTo(index)
}

// SegmentCount is the number of WAL segment files currently held.
func (s *Store) SegmentCount() int { return s.w.SegmentCount() }

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
