package node_test

import (
	"errors"
	"io"
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

type fixedApplier struct{ applied uint64 }

func (f fixedApplier) Apply([]raft.Entry) error { return nil }
func (f fixedApplier) Applied() uint64          { return f.applied }

// Storage-design 5 step 6: a state machine ahead of the WAL means committed
// entries are lost, so the node refuses to start instead of guessing.
func TestRefusesAppliedBeyondWAL(t *testing.T) {
	cfg := node.DefaultConfig()
	cfg.NodeID, cfg.DataDir = 1, t.TempDir()
	cfg.Peers = []node.PeerConfig{{ID: 1, Addr: "127.0.0.1:0"}}
	_, err := node.New(cfg, node.Options{LogWriter: io.Discard, Applier: fixedApplier{applied: 7}})
	if !errors.Is(err, node.ErrAppliedOutsideLog) {
		t.Fatalf("New = %v, want ErrAppliedOutsideLog", err)
	}
}
