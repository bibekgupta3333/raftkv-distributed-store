package raft_test

import (
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft/storagetest"
)

// WBS 2.1.10: MemoryStorage runs the shared Storage conformance suite; the WAL
// store (internal/walstore) runs the very same one.
func TestMemoryStorageConformance(t *testing.T) {
	storagetest.Run(t, func(*testing.T) storagetest.Fixture {
		m := raft.NewMemoryStorage()
		return storagetest.Fixture{
			Storage:      m,
			Append:       func(e []raft.Entry) error { m.Append(e); return nil },
			SetHardState: func(hs raft.HardState) error { m.SetHardState(hs); return nil },
			Compact:      m.Compact,
		}
	})
}
