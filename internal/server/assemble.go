package server

import (
	"path/filepath"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
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
	kv *KV
}

func (o *nodeObserver) OnStop() {
	o.Tracker.OnStop()
	if o.kv != nil {
		o.kv.Close()
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
	if err := node.EnsureIdentity(cfg.DataDir, node.Identity{NodeID: cfg.NodeID, ClusterID: cfg.ClusterID}); err != nil {
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
	members := make([]*raftkvv1.Member, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		members = append(members, &raftkvv1.Member{Id: p.ID, PeerUrl: p.Addr, Role: raftkvv1.MemberRole_VOTER})
	}
	snaps, err := snapshot.Open(snapshot.Config{
		Dir: filepath.Join(cfg.DataDir, node.SnapDir), StateDir: smDir, Store: store,
		ClusterID: cfg.ClusterID, Members: members,
	})
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	opts.Snapshots = snaps
	tracker := NewTracker()
	opts.Applier = storage.NewApplier(store, tracker)
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
	n.RegisterService(NewCluster(n, store).Register) // Status only until M6 (D-48)
	return &KVNode{Node: n, Store: store, Tracker: tracker, KV: kv}, nil
}
