package raft

import "fmt"

// Storage is the core's read-only view of stable (persisted) state. The
// driver writes to the real store using the contents of [Ready]; the core only
// reads. Implementations are called from the driver goroutine only.
//
// The WAL (M2) implements this interface and must pass the same test suite as
// [MemoryStorage] (WBS 2.1.10).
type Storage interface {
	// InitialState returns the persisted HardState at start-up. Membership
	// (a ConfState) is added here by M6.
	InitialState() (HardState, error)
	// Entries returns log entries in [lo, hi), stopping early once their
	// total [Entry.Size] exceeds maxSize but always returning at least one
	// entry when the range is non-empty. It returns [ErrCompacted] when lo is
	// below FirstIndex and [ErrUnavailable] when hi is beyond LastIndex+1.
	Entries(lo, hi, maxSize uint64) ([]Entry, error)
	// Term returns the term of entry i. Valid for FirstIndex-1 through
	// LastIndex (the term of the compaction point is retained). It returns
	// [ErrCompacted] or [ErrUnavailable] outside that range.
	Term(i uint64) (uint64, error)
	// FirstIndex is the index of the first entry still available.
	FirstIndex() (uint64, error)
	// LastIndex is the index of the last stored entry.
	LastIndex() (uint64, error)
	// Snapshot returns the metadata of the newest snapshot the node can send
	// (D-14); a storage with no snapshot returns the zero value. A leader
	// sends it to a follower whose next index was compacted away, so once
	// the log is compacted it must cover the compaction point: Index >=
	// FirstIndex-1 (D-30 guarantees this: the log is never trimmed past the
	// snapshot index). It may return [ErrSnapshotTemporarilyUnavailable].
	Snapshot() (Snapshot, error)
}

// MemoryStorage is an in-memory [Storage] for tests and the simulator. It is
// not safe for concurrent use. Index 0 of ents is a dummy entry carrying the
// index and term of the compaction point.
type MemoryStorage struct {
	hardState HardState
	snapshot  Snapshot
	ents      []Entry
}

// NewMemoryStorage returns an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{ents: make([]Entry, 1)}
}

// InitialState implements [Storage].
func (m *MemoryStorage) InitialState() (HardState, error) { return m.hardState, nil }

// SetHardState persists hs. Callers do this for every non-empty Ready.HardState.
func (m *MemoryStorage) SetHardState(hs HardState) { m.hardState = hs }

// Entries implements [Storage].
func (m *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]Entry, error) {
	offset := m.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > m.lastIndex()+1 {
		return nil, ErrUnavailable
	}
	out := append([]Entry(nil), m.ents[lo-offset:hi-offset]...)
	return limitSize(out, maxSize), nil
}

// Term implements [Storage].
func (m *MemoryStorage) Term(i uint64) (uint64, error) {
	offset := m.ents[0].Index
	if i < offset {
		return 0, ErrCompacted
	}
	if int(i-offset) >= len(m.ents) {
		return 0, ErrUnavailable
	}
	return m.ents[i-offset].Term, nil
}

// FirstIndex implements [Storage].
func (m *MemoryStorage) FirstIndex() (uint64, error) { return m.ents[0].Index + 1, nil }

// LastIndex implements [Storage].
func (m *MemoryStorage) LastIndex() (uint64, error) { return m.lastIndex(), nil }

func (m *MemoryStorage) lastIndex() uint64 { return m.ents[0].Index + uint64(len(m.ents)) - 1 }

// Snapshot implements [Storage].
func (m *MemoryStorage) Snapshot() (Snapshot, error) { return m.snapshot, nil }

// ApplySnapshot installs a snapshot received from the leader ([Ready.Snapshot]):
// the whole log is replaced by the snapshot's compaction point (D-14). It
// returns [ErrSnapOutOfDate] if s is not newer than the current snapshot.
func (m *MemoryStorage) ApplySnapshot(s Snapshot) error {
	if s.Index <= m.snapshot.Index {
		return ErrSnapOutOfDate
	}
	m.snapshot = s
	m.ents = []Entry{{Index: s.Index, Term: s.Term}}
	return nil
}

// CreateSnapshot records that the state machine has been snapshotted at index
// i (the applied index; D-14) and returns the snapshot's metadata. It does not
// compact the log; call [MemoryStorage.Compact] separately (D-30). i must be in
// the stored log and newer than the current snapshot.
//
// cs is the configuration in effect at i (WBS 6.1.6); nil records none.
func (m *MemoryStorage) CreateSnapshot(i uint64, cs *ConfState) (Snapshot, error) {
	if i <= m.snapshot.Index {
		return Snapshot{}, ErrSnapOutOfDate
	}
	offset := m.ents[0].Index
	if i < offset {
		return Snapshot{}, ErrCompacted
	}
	if i > m.lastIndex() {
		return Snapshot{}, fmt.Errorf("raft: snapshot %d is beyond last index %d", i, m.lastIndex())
	}
	m.snapshot = Snapshot{Index: i, Term: m.ents[i-offset].Term}
	if cs != nil {
		c := cs.Clone()
		m.snapshot.ConfState = &c
	}
	return m.snapshot, nil
}

// SetSnapshot records the metadata of a snapshot that already exists outside
// the log (one found on disk at start-up). It does not touch the log.
func (m *MemoryStorage) SetSnapshot(s Snapshot) { m.snapshot = s }

// SetCompacted makes an empty storage start after (index, term): the log's
// compaction point as recovered from disk (a WAL SNAPSHOT_MARKER). It panics if
// entries are already stored, which would be a driver bug.
func (m *MemoryStorage) SetCompacted(index, term uint64) {
	if len(m.ents) != 1 {
		panic("raft: MemoryStorage.SetCompacted on a non-empty log")
	}
	m.ents[0] = Entry{Index: index, Term: term}
}

// Append persists entries, truncating any stored suffix that conflicts with
// them (index at or below the stored last index). Entries already compacted
// are skipped. It panics if the entries would leave a gap, which indicates a
// driver bug.
func (m *MemoryStorage) Append(entries []Entry) {
	if len(entries) == 0 {
		return
	}
	first := m.ents[0].Index + 1
	last := entries[0].Index + uint64(len(entries)) - 1
	if last < first {
		return
	}
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}
	offset := entries[0].Index - m.ents[0].Index
	switch {
	case uint64(len(m.ents)) > offset:
		m.ents = append(append([]Entry(nil), m.ents[:offset]...), entries...)
	case uint64(len(m.ents)) == offset:
		m.ents = append(m.ents, entries...)
	default:
		panic(fmt.Sprintf("raft: MemoryStorage.Append leaves a gap: last stored %d, appending %d",
			m.lastIndex(), entries[0].Index))
	}
}

// Compact discards entries up to and including index, keeping its term.
func (m *MemoryStorage) Compact(index uint64) error {
	offset := m.ents[0].Index
	if index <= offset {
		return ErrCompacted
	}
	if index > m.lastIndex() {
		return fmt.Errorf("raft: compact %d is beyond last index %d", index, m.lastIndex())
	}
	i := index - offset
	ents := make([]Entry, 1, 1+uint64(len(m.ents))-i)
	ents[0] = Entry{Index: m.ents[i].Index, Term: m.ents[i].Term}
	m.ents = append(ents, m.ents[i+1:]...)
	return nil
}

// limitSize truncates ents so the summed payload size stays within maxSize,
// but always keeps at least one entry. Pure helper.
func limitSize(ents []Entry, maxSize uint64) []Entry {
	if len(ents) == 0 {
		return ents
	}
	size := ents[0].Size()
	n := 1
	for ; n < len(ents); n++ {
		size += ents[n].Size()
		if size > maxSize {
			break
		}
	}
	return ents[:n]
}
