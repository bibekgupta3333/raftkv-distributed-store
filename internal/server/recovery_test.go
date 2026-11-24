package server_test

import (
	"io"
	"net"
	"path/filepath"
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/command"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/server"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/wal"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/walstore"
)

// WBS 5.4.4 (storage-design 4.4 crash window, 5 step 6): the node crashed after
// the sm/CURRENT swap made the snapshot's database live but before the WAL got
// its reset marker. On restart the state machine is ahead of the log and
// disagrees with it; recovery must finish the install: the log restarts after
// the snapshot, commit covers it, the term never trails the log, and a local
// snapshot is taken so the node can serve others.
func TestRecoveryFinishesInstallWithoutWALMarker(t *testing.T) {
	dir := t.TempDir()
	cfg := node.DefaultConfig()
	cfg.NodeID, cfg.ClusterID, cfg.DataDir = 2, 1, dir
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Peers = []node.PeerConfig{{ID: 1, Addr: "127.0.0.1:1"}, {ID: 2, Addr: lis.Addr().String()}, {ID: 3, Addr: "127.0.0.1:3"}}
	if _, err := node.EnsureIdentity(dir, node.Identity{NodeID: 2, ClusterID: 1}); err != nil {
		t.Fatal(err)
	}

	// The old log: 10 entries of term 1, committed up to 4, term 1.
	ws, err := walstore.Open(filepath.Join(dir, node.WALDir), wal.Options{ClusterID: 1, MemberID: 2})
	if err != nil {
		t.Fatal(err)
	}
	var ents []raft.Entry
	for i := uint64(1); i <= 10; i++ {
		ents = append(ents, raft.Entry{Index: i, Term: 1})
	}
	if err := ws.Save(raft.Ready{Entries: ents, HardState: raft.HardState{Term: 1, Vote: 1, Commit: 4}, MustSync: true}); err != nil {
		t.Fatal(err)
	}
	if err := ws.Close(); err != nil {
		t.Fatal(err)
	}
	// The live database is the installed snapshot: applied 50 at term 3.
	sm := filepath.Join(dir, node.StateDir)
	if _, err := storage.PrepareStateDir(sm); err != nil {
		t.Fatal(err)
	}
	newDB := filepath.Join(sm, storage.DBDirName(50))
	st, err := storage.Open(storage.Options{Dir: newDB})
	if err != nil {
		t.Fatal(err)
	}
	var snap []raft.Entry
	for i := uint64(1); i <= 50; i++ {
		e := raft.Entry{Index: i, Term: 1 + i/20}
		if i%5 == 0 {
			cmd, _ := command.Put(i, 0, []byte("k"), []byte{byte(i)})
			e.Data, _ = command.Marshal(cmd)
		}
		snap = append(snap, e)
	}
	if err := storage.NewApplier(st, nil).Apply(snap); err != nil {
		t.Fatal(err)
	}
	_, wantHash, _ := st.StateHash()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := storage.SwapCurrent(sm, storage.DBDirName(50)); err != nil { // the commit point
		t.Fatal(err)
	}

	kn, err := server.OpenKVNode(cfg, node.Options{Listener: lis, LogWriter: io.Discard})
	if err != nil {
		t.Fatalf("recovery refused to start: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = kn.Close()
		}
	}()
	if applied, h, _ := kn.Store.StateHash(); applied != 50 || h != wantHash {
		t.Fatalf("state machine applied %d (want 50), snapshot state kept: %v", applied, h == wantHash)
	}
	if first, _ := kn.FirstIndex(); first != 51 {
		t.Fatalf("log starts at %d, want 51 (the stale entries 1..10 are gone)", first)
	}
	if got := kn.SnapshotIndex(); got != 50 {
		t.Fatalf("local snapshot index = %d, want 50", got)
	}
	closed = true
	if err := kn.Close(); err != nil {
		t.Fatal(err)
	}

	// The WAL now says so on its own.
	ws, err = walstore.Open(filepath.Join(dir, node.WALDir), wal.Options{ClusterID: 1, MemberID: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ws.Close() }()
	hs, _ := ws.InitialState()
	if hs.Commit != 50 || hs.Term != 3 || hs.Vote != 0 {
		t.Fatalf("HardState after recovery = %+v, want commit 50 at term 3 with no vote", hs)
	}
	if term, err := ws.Term(50); err != nil || term != 3 {
		t.Fatalf("log start term = %d, %v; want 3", term, err)
	}
}
