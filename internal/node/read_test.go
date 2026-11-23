package node_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

func ctxFor(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// WBS 4.2.2 (node level): every member, leader and followers, gets a read
// index covering a write acknowledged before the read, and LinearizableRead
// runs the read only once that index is applied locally.
func TestReadIndexOnLeaderAndFollowers(t *testing.T) {
	c := newTestCluster(t, 3)
	lead := c.leader()
	ctx := ctxFor(t, 10*time.Second)
	idx, err := lead.ProposeAndWait(ctx, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	for id, n := range c.nodes {
		ri, viaLease, err := n.ReadIndex(ctx)
		for errors.Is(err, node.ErrReadRejected) || errors.Is(err, raft.ErrReadIndexDropped) {
			time.Sleep(10 * time.Millisecond) // a follower may not know the leader yet
			ri, viaLease, err = n.ReadIndex(ctx)
		}
		if err != nil {
			t.Fatalf("node %d: ReadIndex: %v", id, err)
		}
		if ri < idx || viaLease {
			t.Fatalf("node %d: read index %d (lease=%v), want >= %d without a lease", id, ri, viaLease, idx)
		}
		ran := false
		if err := n.LinearizableRead(ctx, func() error {
			ran = true
			if got := n.Applier().Applied(); got < idx {
				t.Errorf("node %d: read ran at applied %d < %d", id, got, idx)
			}
			return nil
		}); err != nil || !ran {
			t.Fatalf("node %d: LinearizableRead = %v (ran=%v)", id, err, ran)
		}
	}
}

// WBS 4.2.1: WaitApplied returns as soon as the index is applied, and gives up
// at the deadline.
func TestWaitAppliedDeadlineAware(t *testing.T) {
	c := newTestCluster(t, 3)
	lead := c.leader()
	ctx := ctxFor(t, 10*time.Second)
	if _, err := lead.ProposeAndWait(ctx, []byte("settle")); err != nil { // the no-op is applied too
		t.Fatal(err)
	}
	target := lead.Applier().Applied() + 1
	done := make(chan error, 1)
	go func() { done <- lead.WaitApplied(ctx, target) }()
	select {
	case err := <-done:
		t.Fatalf("WaitApplied returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := lead.ProposeAndWait(ctx, []byte("y")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WaitApplied did not return after the index was applied")
	}

	short := ctxFor(t, 100*time.Millisecond)
	start := time.Now()
	err := lead.WaitApplied(short, lead.Applier().Applied()+1000)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitApplied past the deadline = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("WaitApplied overran its deadline: %v", d)
	}
}

// Without a known leader ReadIndex fails at once with ErrReadIndexDropped, so
// the server can tell the client to try another member.
func TestReadIndexWithoutLeader(t *testing.T) {
	c := newTestCluster(t, 3)
	lead := c.leader()
	var survivor *node.Node
	for id, n := range c.nodes {
		if n == lead {
			c.stop(id)
		} else if survivor == nil {
			survivor = n
		} else {
			c.stop(id)
		}
	}
	waitUntil(t, 10*time.Second, "the survivor to lose its leader", func() bool {
		_, _, err := survivor.ReadIndex(ctxFor(t, time.Second))
		return errors.Is(err, raft.ErrReadIndexDropped)
	})
	err := survivor.LinearizableRead(ctxFor(t, time.Second), func() error { return nil })
	if !errors.Is(err, raft.ErrReadIndexDropped) {
		t.Fatalf("LinearizableRead without a leader = %v, want ErrReadIndexDropped", err)
	}
}

// WBS 4.2.4: lease reads are off by default, and with the flag on the leader
// serves reads from its lease (no round trip) until it loses its quorum.
func TestLeaseReads(t *testing.T) {
	t.Run("off by default", func(t *testing.T) {
		if node.DefaultConfig().LeaseReads {
			t.Fatal("lease reads must be off by default (D-25)")
		}
		c := newTestCluster(t, 3)
		lead := c.leader()
		ctx := ctxFor(t, 5*time.Second)
		for range 5 {
			if _, viaLease, err := lead.ReadIndex(ctx); err != nil || viaLease {
				t.Fatalf("ReadIndex = lease %v, %v; want a ReadIndex round", viaLease, err)
			}
		}
		if lead.LeaseValid() {
			t.Fatal("LeaseValid with lease reads off")
		}
	})
	t.Run("on", func(t *testing.T) {
		c := newTestCluster(t, 3, func(o *clusterOptions) {
			o.tick = 50 * time.Millisecond // the 100 ms margin needs ticks this long (lease ~305 ms)
			o.config = func(cfg *node.Config) { cfg.LeaseReads = true }
		})
		lead := c.leader()
		waitUntil(t, 10*time.Second, "the leader to hold a lease", lead.LeaseValid)
		ctx := ctxFor(t, 10*time.Second)
		idx, err := lead.ProposeAndWait(ctx, []byte("z"))
		if err != nil {
			t.Fatal(err)
		}
		ri, viaLease, err := lead.ReadIndex(ctx)
		if err != nil || !viaLease || ri < idx {
			t.Fatalf("leader ReadIndex = %d lease=%v %v; want a lease read at >= %d", ri, viaLease, err, idx)
		}
		for id, n := range c.nodes {
			if n == lead {
				continue
			}
			if n.LeaseValid() {
				t.Errorf("follower %d claims a lease", id)
			}
			if _, viaLease, err := n.ReadIndex(ctx); err == nil && viaLease {
				t.Errorf("follower %d served a lease read", id)
			}
		}
		// Cut the leader off: its lease must run out (no renewals), well
		// before CheckQuorum would depose it.
		for id, n := range c.nodes {
			if n != lead {
				c.stop(id)
			}
		}
		waitUntil(t, 5*time.Second, "the lease to expire", func() bool { return !lead.LeaseValid() })
	})
}

func TestLeaseReadsConfigValidation(t *testing.T) {
	cfg := node.DefaultConfig()
	cfg.NodeID, cfg.DataDir = 1, t.TempDir()
	cfg.Peers = []node.PeerConfig{{ID: 1, Addr: "127.0.0.1:7001"}}
	cfg.LeaseReads = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaults with lease reads: %v", err)
	}
	if d, err := cfg.LeaseDuration(); err != nil || d != 710*time.Millisecond {
		t.Fatalf("LeaseDuration = %v, %v; want 710ms (D-25)", d, err)
	}
	cfg.TickInterval = 10 * time.Millisecond // 81 ms of safe time minus a 100 ms margin
	if err := cfg.Validate(); err == nil {
		t.Fatal("a lease shorter than zero must be rejected")
	}
}
