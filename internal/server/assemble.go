package server

import (
	"path/filepath"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
)

// KVNode is a node with the Pebble state machine and the KV service wired in.
type KVNode struct {
	*node.Node
	Store   *storage.Store
	Tracker *Tracker
}

// OpenKVNode replaces the in-memory applier with the Pebble state machine
// (WBS 3.2.3): it opens <data_dir>/sm, builds the proposal tracker as the
// applier's result sink and the node's observer, registers the KV and Cluster
// services on the node's gRPC server, and hands the applier to the node, which closes it
// (flush first) on shutdown. opts.Applier and opts.Observer must be unset.
func OpenKVNode(cfg node.Config, opts node.Options) (*KVNode, error) {
	// The identity check must come before anything is created in the data dir.
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := node.EnsureIdentity(cfg.DataDir, node.Identity{NodeID: cfg.NodeID, ClusterID: cfg.ClusterID}); err != nil {
		return nil, err
	}
	store, err := storage.Open(storage.Options{Dir: filepath.Join(cfg.DataDir, node.StateDir)})
	if err != nil {
		return nil, err
	}
	tracker := NewTracker()
	opts.Applier = storage.NewApplier(store, tracker)
	opts.Observer = tracker
	n, err := node.New(cfg, opts)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	n.RegisterService(New(n, store, tracker).Register)
	n.RegisterService(NewCluster(n, store).Register) // Status only until M6 (D-48)
	return &KVNode{Node: n, Store: store, Tracker: tracker}, nil
}
