package node

import (
	"sync"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Applier executes committed entries, strictly in index order (D-04). The real
// one (M3) writes to Pebble with appliedIndex in the same batch; until then
// [MemoryApplier] stands in. The driver calls Apply from its own goroutine
// between persisting and Advance.
type Applier interface {
	// Apply handles entries in index order. An error is fatal to the node.
	Apply(entries []raft.Entry) error
	// Applied is the highest index applied; it seeds raft.Config.Applied on
	// restart so entries are not applied twice.
	Applied() uint64
}

// ProposalChecker is optionally implemented by an [Applier] whose Apply fails
// on payloads it cannot decode (the Pebble applier, M3). Such an entry, once
// committed, is a poison pill: every replica hits the same fatal apply error
// (D-33) and the WAL can never be replayed. Paths that propose raw bytes (the
// debug Propose RPC) call CheckProposal first and reject bad payloads.
type ProposalChecker interface {
	CheckProposal(data []byte) error
}

// MemoryApplier is the temporary in-memory applier for M2 (WBS 2.3.2): it
// records every applied entry so tests can compare replicas. It is lost on
// restart, so a restarted node re-applies the whole committed log.
type MemoryApplier struct {
	mu      sync.Mutex
	applied uint64
	entries []raft.Entry
}

// NewMemoryApplier returns an empty MemoryApplier.
func NewMemoryApplier() *MemoryApplier { return &MemoryApplier{} }

// Apply implements [Applier].
func (a *MemoryApplier) Apply(entries []raft.Entry) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, e := range entries {
		a.entries = append(a.entries, e)
		a.applied = e.Index
	}
	return nil
}

// Applied implements [Applier].
func (a *MemoryApplier) Applied() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.applied
}

// Entries returns a copy of every applied entry, including the leader's empty
// no-ops.
func (a *MemoryApplier) Entries() []raft.Entry {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]raft.Entry(nil), a.entries...)
}

// Commands returns the payloads of applied entries that carry data, in order.
func (a *MemoryApplier) Commands() [][]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out [][]byte
	for _, e := range a.entries {
		if e.Type == raft.EntryNormal && len(e.Data) > 0 {
			out = append(out, e.Data)
		}
	}
	return out
}
