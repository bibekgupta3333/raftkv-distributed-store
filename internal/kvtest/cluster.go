// Package kvtest runs an in-process RaftKV cluster for tests: real nodes (WAL,
// Pebble, Raft) talking over real localhost gRPC, with nodes that can be
// stopped and restarted and links that can be cut (partitions). It backs the
// KV integration tests and the linearizability harness (WBS 3.2.3, 3.4.3).
package kvtest

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/server"
)

// Cluster is N in-process KV nodes. Each ordered pair of nodes talks through its
// own TCP proxy so [Cluster.Partition] can cut specific links; clients use the
// nodes' real addresses and are never partitioned.
type Cluster struct {
	tb  testing.TB
	now func() time.Time
	ids []uint64

	mu      sync.Mutex
	real    map[uint64]string
	cfgs    map[uint64]node.Config
	proxies map[[2]uint64]*proxy // [from,to]
	nodes   map[uint64]*server.KVNode
	cancels map[uint64]context.CancelFunc
	dones   map[uint64]chan error
}

// Options tune a cluster.
type Options struct {
	// Tick is the Raft tick (default 10 ms: elections in 100-200 ms).
	Tick time.Duration
	// Now, if set, replaces the wall clock every node stamps into commands
	// (D-27), so tests can move session time without waiting.
	Now func() time.Time
	// LeaseReads turns on lease reads (D-25). With the fixed 100 ms margin
	// the lease is only positive for a Tick above about 13 ms; use 50 ms.
	LeaseReads bool
	// SnapshotEntries, SnapshotRetainEntries and WALSegmentBytes override
	// the snapshot policy (D-14, D-30) when non-zero; tests use small
	// values so snapshots and WAL trimming happen within seconds.
	SnapshotEntries       uint64
	SnapshotRetainEntries uint64
	WALSegmentBytes       int64
}

// New starts n nodes (IDs 1..n) and registers cleanup with tb.
func New(tb testing.TB, n int, o Options) *Cluster {
	tb.Helper()
	if o.Tick == 0 {
		o.Tick = 10 * time.Millisecond
	}
	c := &Cluster{tb: tb, now: o.Now, real: map[uint64]string{}, cfgs: map[uint64]node.Config{},
		proxies: map[[2]uint64]*proxy{}, nodes: map[uint64]*server.KVNode{},
		cancels: map[uint64]context.CancelFunc{}, dones: map[uint64]chan error{}}
	lises := map[uint64]net.Listener{}
	for i := 1; i <= n; i++ {
		id := uint64(i)
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			tb.Fatal(err)
		}
		c.ids = append(c.ids, id)
		lises[id] = l
		c.real[id] = l.Addr().String()
	}
	for _, from := range c.ids {
		for _, to := range c.ids {
			if from == to {
				continue
			}
			p, err := newProxy(c.real[to])
			if err != nil {
				tb.Fatal(err)
			}
			c.proxies[[2]uint64{from, to}] = p
		}
	}
	dir := tb.TempDir()
	for _, id := range c.ids {
		cfg := node.DefaultConfig()
		cfg.NodeID = id
		cfg.DataDir = filepath.Join(dir, fmt.Sprint("n", id))
		cfg.TickInterval = o.Tick
		cfg.LeaseReads = o.LeaseReads
		if o.SnapshotEntries > 0 {
			cfg.SnapshotEntries = o.SnapshotEntries
			cfg.SnapshotRetainEntries = o.SnapshotRetainEntries
		}
		cfg.WALSegmentBytes = o.WALSegmentBytes
		for _, to := range c.ids {
			addr := c.real[to]
			if to != id {
				addr = c.proxies[[2]uint64{id, to}].addr()
			}
			cfg.Peers = append(cfg.Peers, node.PeerConfig{ID: to, Addr: addr})
		}
		c.cfgs[id] = cfg
	}
	for _, id := range c.ids {
		c.startWith(id, lises[id])
	}
	tb.Cleanup(c.Close)
	return c
}

func (c *Cluster) startWith(id uint64, lis net.Listener) {
	c.tb.Helper()
	kn, err := server.OpenKVNode(c.cfgs[id], node.Options{Listener: lis, LogWriter: io.Discard})
	if err != nil {
		c.tb.Fatalf("OpenKVNode(%d): %v", id, err)
	}
	if c.now != nil {
		kn.KV.SetClock(c.now)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- kn.Run(ctx) }()
	c.mu.Lock()
	c.nodes[id], c.cancels[id], c.dones[id] = kn, cancel, done
	c.mu.Unlock()
}

// IDs returns the member IDs.
func (c *Cluster) IDs() []uint64 { return slices.Clone(c.ids) }

// Endpoints returns every node's client address in ID order (also when down).
func (c *Cluster) Endpoints() []string {
	out := make([]string, 0, len(c.ids))
	for _, id := range c.ids {
		out = append(out, c.real[id])
	}
	return out
}

// DataDir returns node id's data directory.
func (c *Cluster) DataDir(id uint64) string { return c.cfgs[id].DataDir }

// Addr returns node id's client address.
func (c *Cluster) Addr(id uint64) string { return c.real[id] }

// Node returns the running node or nil if it is stopped.
func (c *Cluster) Node(id uint64) *server.KVNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// Stop shuts a node down (graceful: the WAL is synced and Pebble flushed) and
// waits for it. Its address stays reserved for [Cluster.Start].
func (c *Cluster) Stop(id uint64) {
	c.tb.Helper()
	c.mu.Lock()
	cancel, done := c.cancels[id], c.dones[id]
	delete(c.nodes, id)
	delete(c.cancels, id)
	delete(c.dones, id)
	c.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			c.tb.Errorf("node %d stopped with error: %v", id, err)
		}
	case <-time.After(20 * time.Second):
		c.tb.Fatalf("node %d did not stop", id)
	}
}

// Wipe deletes a stopped node's whole data directory, as if its disk were
// replaced. Restarted, it comes back empty under the same ID and catches up
// from the leader (by snapshot once the log is compacted, D-14). Real
// clusters re-add such a node as a learner with a new ID (D-32, M6).
func (c *Cluster) Wipe(id uint64) {
	c.tb.Helper()
	if c.Node(id) != nil {
		c.tb.Fatalf("Wipe(%d): node is running", id)
	}
	if err := os.RemoveAll(c.cfgs[id].DataDir); err != nil {
		c.tb.Fatal(err)
	}
}

// Start restarts a stopped node from its data directory.
func (c *Cluster) Start(id uint64) {
	c.tb.Helper()
	var lis net.Listener
	var err error
	for range 100 { // the port may linger briefly
		if lis, err = net.Listen("tcp", c.real[id]); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		c.tb.Fatalf("relisten %s: %v", c.real[id], err)
	}
	c.startWith(id, lis)
}

// Close stops every node and proxy. Safe to call twice.
func (c *Cluster) Close() {
	for _, id := range c.ids {
		c.Stop(id)
	}
	for _, p := range c.proxies {
		p.close()
	}
}

// Partition cuts every link between different groups (nodes inside a group keep
// talking). Nodes in no group are isolated from everyone.
func (c *Cluster) Partition(groups ...[]uint64) {
	groupOf := map[uint64]int{}
	for gi, g := range groups {
		for _, id := range g {
			groupOf[id] = gi + 1
		}
	}
	for k, p := range c.proxies {
		gf, gt := groupOf[k[0]], groupOf[k[1]]
		p.setBlocked(gf == 0 || gt == 0 || gf != gt)
	}
}

// Heal restores every link.
func (c *Cluster) Heal() {
	for _, p := range c.proxies {
		p.setBlocked(false)
	}
}

// Leader waits for a running node that is leader and returns its ID. If
// several claim it (an old leader that has not noticed yet), the highest term
// wins. Only nodes in among are considered; nil means all.
func (c *Cluster) Leader(timeout time.Duration, among ...uint64) uint64 {
	c.tb.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var best raft.Status
		for _, id := range c.ids {
			if len(among) > 0 && !slices.Contains(among, id) {
				continue
			}
			if n := c.Node(id); n != nil {
				if st := n.Status(); st.State == raft.StateLeader && st.Term > best.Term {
					best = st
				}
			}
		}
		if best.ID != 0 && c.stableLeader(best.ID, best.Term, among) {
			return best.ID
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.tb.Fatalf("no leader within %v", timeout)
	return 0
}

// stableLeader: the leader's no-op must be committed so it can serve writes,
// and a majority of the considered nodes must agree on it.
func (c *Cluster) stableLeader(id, term uint64, among []uint64) bool {
	n := c.Node(id)
	if n == nil {
		return false
	}
	st := n.Status()
	return st.State == raft.StateLeader && st.Term == term && st.Commit >= 1
}

// StateHashes waits until every running node has applied the same index and
// returns each node's state hash (WBS 3.1.8). No writes may be in flight.
func (c *Cluster) StateHashes(timeout time.Duration) map[uint64][sha256.Size]byte {
	c.tb.Helper()
	deadline := time.Now().Add(timeout)
	for {
		applied := map[uint64]uint64{}
		same := true
		var first uint64
		for _, id := range c.ids {
			n := c.Node(id)
			if n == nil {
				continue
			}
			a := n.Store.Applied()
			applied[id] = a
			if first == 0 {
				first = a
			}
			same = same && a == first
		}
		if same && first > 0 {
			out := map[uint64][sha256.Size]byte{}
			for id := range applied {
				_, h, err := c.Node(id).Store.StateHash()
				if err != nil {
					c.tb.Fatal(err)
				}
				out[id] = h
			}
			return out
		}
		if time.Now().After(deadline) {
			c.tb.Fatalf("replicas never converged: applied %v", applied)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// SessionHashes waits until every running node has applied the same index and
// returns each node's session table hash and session count (WBS 4.3.3).
func (c *Cluster) SessionHashes(timeout time.Duration) map[uint64]SessionTable {
	c.tb.Helper()
	c.StateHashes(timeout) // converge first
	out := map[uint64]SessionTable{}
	for _, id := range c.ids {
		if n := c.Node(id); n != nil {
			cnt, h, err := n.Store.SessionHash()
			if err != nil {
				c.tb.Fatal(err)
			}
			out[id] = SessionTable{Sessions: cnt, Hash: h}
		}
	}
	return out
}

// SessionTable summarizes one replica's session table.
type SessionTable struct {
	Sessions int
	Hash     [sha256.Size]byte
}
