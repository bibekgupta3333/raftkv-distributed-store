package server

import (
	"path/filepath"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/membership"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/snapshot"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
)

// KVNode is a node with the Pebble state machine and the KV service wired in.
type KVNode struct {
	*node.Node
	Store   *storage.Store
	Tracker *Tracker
	KV      *KV
}

// nodeObserver forwards driver-loop events to the tracker and, on shutdown,
// closes the KV service's forwarding connections.
type nodeObserver struct {
	*Tracker
	kv      *KV
	cluster *Cluster
}

func (o *nodeObserver) OnStop() {
	o.Tracker.OnStop()
	if o.kv != nil {
		o.kv.Close()
	}
	if o.cluster != nil {
		o.cluster.Close()
	}
}

// OpenKVNode replaces the in-memory applier with the Pebble state machine
// (WBS 3.2.3): it opens the live database named by <data_dir>/sm/CURRENT and
// the snapshot manager over <data_dir>/snap (D-14), builds the proposal tracker as the
// applier's result sink and the node's observer, registers the KV and Cluster
// services on the node's gRPC server, and hands the applier to the node, which closes it
// (flush first) on shutdown. opts.Applier and opts.Observer must be unset.
func OpenKVNode(cfg node.Config, opts node.Options) (*KVNode, error) {
	// The identity check must come before anything is created in the data dir.
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	ident, err := node.EnsureIdentity(cfg.DataDir, node.Identity{NodeID: cfg.NodeID, ClusterID: cfg.ClusterID, Joined: cfg.Join})
	if err != nil {
		return nil, err
	}
	smDir := filepath.Join(cfg.DataDir, node.StateDir)
	live, err := storage.PrepareStateDir(smDir) // storage-design 5 step 2
	if err != nil {
		return nil, err
	}
	store, err := storage.Open(storage.Options{Dir: live})
	if err != nil {
		return nil, err
	}
	// WBS 6.1.6: a bootstrapped cluster's state machine records its initial
	// member table; a node that joined (D-32) learns it from the leader.
	var bootstrap *raftkvv1.ClusterConfig
	if !ident.Joined {
		bootstrap = membership.Bootstrap(cfg.PeerAddrs())
	}
	snaps, err := snapshot.Open(snapshot.Config{
		Dir: filepath.Join(cfg.DataDir, node.SnapDir), StateDir: smDir, Store: store,
		ClusterID: cfg.ClusterID, Members: bootstrap.GetMembers(),
	})
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	opts.Snapshots = snaps
	tracker := NewTracker()
	applier := storage.NewApplier(store, tracker)
	applier.SetBootstrap(bootstrap)
	opts.Applier = applier
	obs := &nodeObserver{Tracker: tracker}
	opts.Observer = obs
	n, err := node.New(cfg, opts)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	kv := New(n, store, tracker)
	obs.kv = kv
	n.RegisterService(kv.Register)
	cl := NewCluster(n, store) // D-23
	obs.cluster = cl
	n.RegisterService(cl.Register)
	return &KVNode{Node: n, Store: store, Tracker: tracker, KV: kv}, nil
}
