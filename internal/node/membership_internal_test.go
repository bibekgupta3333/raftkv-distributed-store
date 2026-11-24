package node

import (
	"testing"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// WBS 6.4.1 (D-38): member IDs are random 64-bit values, never 0, never a
// current member and never a removed one. (The core also refuses to re-add a
// removed ID: raft.ConfState.Apply.)
func TestRandomIDsAreFreshAndNeverReused(t *testing.T) {
	table := &raftkvv1.ClusterConfig{Members: []*raftkvv1.Member{{Id: 1}, {Id: 2}}, Removed: []uint64{3}}
	seen := map[uint64]bool{}
	for range 10000 {
		id := randomID(table, 1)
		if id == 0 || id <= 3 || seen[id] {
			t.Fatalf("randomID returned %d (seen %v)", id, seen[id])
		}
		seen[id] = true
	}
	// Removed IDs stay unusable at the core too.
	cs := raft.ConfState{Voters: []uint64{1, 2}, Removed: []uint64{3}}
	if _, err := cs.Apply(raft.ConfChange{Type: raft.ConfChangeAddLearner, NodeID: 3}); err == nil {
		t.Fatal("a removed ID was accepted again")
	}
}
