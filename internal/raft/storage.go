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
	// Snapshot returns the metadata of the latest snapshot. It is a stub until
	// M5 (D-14); a storage with no snapshot returns the zero value.
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

// ApplySnapshot replaces the whole log with a snapshot marker. Stub for M5.
func (m *MemoryStorage) ApplySnapshot(s Snapshot) {
	m.snapshot = s
	m.ents = []Entry{{Index: s.Index, Term: s.Term}}
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
