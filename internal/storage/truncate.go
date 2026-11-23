package storage

import (
	"errors"
	"fmt"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// LogCompactor is the part of the WAL store that trims the log
// (walstore.Store.CompactTo satisfies it).
type LogCompactor interface {
	CompactTo(index uint64) error
}

// TruncationBound is the highest log index that may be dropped from the WAL:
// min(snapshotIndex, durableApplied) minus the entries kept for lagging
// followers (D-30, storage-design 1.8). It never exceeds the durable applied
// index, which is the point of the rule: with Pebble's WAL off, entries above
// it are the only copy of unflushed applies. A result of 0 means nothing may be
// trimmed.
func TruncationBound(snapshotIndex, durableApplied, retain uint64) uint64 {
	bound := min(snapshotIndex, durableApplied)
	if bound <= retain {
		return 0
	}
	return bound - retain
}

// TruncateWAL trims log by D-30: flush Pebble first so DurableApplied is
// current, compute the bound from it, and only then call CompactTo. It returns
// the index it trimmed to (0 if nothing was eligible). snapshotIndex is the
// newest complete local snapshot (M5; until then callers pass the bound they
// want to honour).
func (s *Store) TruncateWAL(log LogCompactor, snapshotIndex, retain uint64) (uint64, error) {
	if err := s.Flush(); err != nil {
		return 0, err
	}
	bound := TruncationBound(snapshotIndex, s.DurableApplied(), retain)
	if bound == 0 {
		return 0, nil
	}
	if err := log.CompactTo(bound); err != nil {
		if errors.Is(err, raft.ErrCompacted) {
			return 0, nil // already trimmed at least this far
		}
		return 0, fmt.Errorf("storage: trim WAL to %d: %w", bound, err)
	}
	return bound, nil
}
