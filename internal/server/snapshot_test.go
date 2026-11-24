package server_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/kvtest"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/pkg/client"
)

// M5 recovery scenarios on an in-process cluster (WBS 5.3, 5.4.5, 5.5.1).

// snapOpts makes snapshots and WAL trims frequent.
var snapOpts = kvtest.Options{SnapshotEntries: 40, SnapshotRetainEntries: 10, WALSegmentBytes: 16 << 10}

func putN(t *testing.T, cl *client.Client, prefix string, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for i := range n {
		if _, err := cl.Put(ctx, fmt.Appendf(nil, "%s-%04d", prefix, i%97), fmt.Appendf(nil, "v%d", i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
}

// requireSameState waits until every running node applied the same index and
// checks their state hashes are equal (WBS 3.1.8).
func requireSameState(t *testing.T, c *kvtest.Cluster) {
	t.Helper()
	hashes := c.StateHashes(30 * time.Second)
	var first [32]byte
	for id, h := range hashes {
		if first == ([32]byte{}) {
			first = h
		} else if h != first {
			t.Fatalf("node %d state hash differs: %v", id, hashes)
		}
	}
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// WBS 5.5.1 (M5 exit criterion 2) and 6.4.2 (D-32): delete a follower's data
// directory and restart it under its old ID: the peers recognise the new
// data-directory incarnation and refuse it, so it never catches up. Removed
// and added back as a new learner, it catches up through a snapshot and its
// state hash matches. 20 runs (5 with -short).
func TestWipedFollowerRejoinsViaSnapshot(t *testing.T) {
	runs := 20
	if testing.Short() {
		runs = 5
	}
	for run := range runs {
		t.Run(fmt.Sprint("run", run), func(t *testing.T) {
			c := kvtest.New(t, 3, snapOpts)
			leader := c.Leader(15 * time.Second)
			victim := leader%3 + 1
			cl := newClient(t, c)
			putN(t, cl, "a", 60)
			c.Stop(victim)
			c.Wipe(victim)
			putN(t, cl, "b", 120) // the log moves past the trim point
			if first, _ := c.Node(leader).FirstIndex(); first <= 1 {
				t.Fatalf("leader's WAL was never trimmed (first index %d)", first)
			}
			c.Start(victim)
			old := c.Node(victim)
			putN(t, cl, "x", 5)
			time.Sleep(300 * time.Millisecond) // many heartbeat rounds at a 10 ms tick
			if old.Metrics().SnapshotInstalls() > 0 || old.Status().Commit > 0 {
				t.Fatalf("the wiped node caught up under its old ID (D-32): %+v", old.Status())
			}
			c.RemoveMember(victim, 30*time.Second)
			id := c.AddLearner(30 * time.Second)
			n := c.Node(id)
			waitFor(t, 30*time.Second, "the new learner to install a snapshot", func() bool {
				return n.Metrics().SnapshotInstalls() >= 1
			})
			c.Promote(id, 30*time.Second)
			putN(t, cl, "c", 10)
			requireSameState(t, c)
			if n.SnapshotIndex() == 0 {
				t.Fatal("the rejoined node has no local snapshot to serve others")
			}
		})
	}
}

// WBS 5.5.1: a follower partitioned away while the log is compacted past its
// position catches up after the partition heals (snapshot, then log).
func TestPartitionedFollowerCatchesUpPastCompaction(t *testing.T) {
	c := kvtest.New(t, 3, snapOpts)
	leader := c.Leader(15 * time.Second)
	lagger := leader%3 + 1
	other := 6 - leader - lagger
	cl := newClient(t, c)
	putN(t, cl, "a", 20)
	c.Partition([]uint64{leader, other}, []uint64{lagger})
	putN(t, cl, "b", 200)
	lagApplied := c.Node(lagger).Status().Applied
	first, _ := c.Node(leader).FirstIndex()
	if first <= lagApplied+1 {
		t.Fatalf("leader's log (first %d) still holds the lagging node's next entry %d", first, lagApplied+1)
	}
	c.Heal()
	waitFor(t, 30*time.Second, "the lagging node to install a snapshot", func() bool {
		return c.Node(lagger).Metrics().SnapshotInstalls() >= 1
	})
	putN(t, cl, "c", 5)
	requireSameState(t, c)
}

// WBS 5.3.1-5.3.2 (M5 exit criterion 4): under steady writes the WAL stays
// bounded: old segments are deleted after each snapshot (D-30).
func TestWALBoundedUnderSteadyWrites(t *testing.T) {
	c := kvtest.New(t, 3, snapOpts)
	c.Leader(15 * time.Second)
	cl := newClient(t, c)
	var maxBytes, maxSegs int64
	const rounds = 15
	for r := range rounds {
		putN(t, cl, fmt.Sprint("r", r), 60)
		for _, id := range c.IDs() {
			b, s := walUsage(t, filepath.Join(c.DataDir(id), node.WALDir))
			maxBytes, maxSegs = max(maxBytes, b), max(maxSegs, s)
		}
	}
	// 900 writes of ~100 bytes on disk each would be ~90 KiB of WAL per node
	// with no trimming; with snapshots every 40 entries and a 10-entry tail,
	// at most a few 16 KiB segments remain.
	t.Logf("result: %d writes, peak WAL per node %d bytes in %d segments (segment size %d)",
		rounds*60, maxBytes, maxSegs, snapOpts.WALSegmentBytes)
	if maxSegs > 4 {
		t.Fatalf("WAL grew to %d segments; trimming does not keep up", maxSegs)
	}
	requireSameState(t, c)
}

func walUsage(t *testing.T, dir string) (bytes, segs int64) {
	t.Helper()
	es, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range es {
		if fi, err := e.Info(); err == nil && !e.IsDir() {
			bytes += fi.Size()
			segs++
		}
	}
	return bytes, segs
}

// WBS 5.4.5 (M5 exit criterion 5): the session table travels in the
// snapshot. A write applied before a snapshot boundary and retried after a
// node that only knows it from an installed snapshot became leader is answered
// from the restored session table, not applied again. The restored session
// clock (m/sessionClock) keeps session time from going back.
func TestExactlyOnceAcrossSnapshotBoundary(t *testing.T) {
	var nowMs atomic.Int64
	nowMs.Store(time.Now().UnixMilli())
	o := snapOpts
	o.Now = func() time.Time { return time.UnixMilli(nowMs.Load()) }
	c := kvtest.New(t, 3, o)
	leader := c.Leader(15 * time.Second)
	lagger := leader%3 + 1
	c.Partition([]uint64{leader, 6 - leader - lagger}, []uint64{lagger})

	conn, err := grpc.NewClient(c.Addr(leader), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	kv := raftkvv1.NewKVClient(conn)
	ctx := ctx5(t)
	reg, err := kv.RegisterSession(ctx, &raftkvv1.RegisterSessionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	cid := reg.GetClientId()
	first, err := kv.Put(ctx, &raftkvv1.PutRequest{Key: []byte("x"), Value: []byte("1"),
		Session: &raftkvv1.WriteSession{ClientId: cid, Sequence: 1}})
	if err != nil {
		t.Fatal(err)
	}
	rev := first.GetRevision()

	// Move far past the write: snapshots are taken and the log is trimmed.
	putN(t, newClient(t, c), "fill", 200)
	if f, _ := c.Node(leader).FirstIndex(); f <= rev {
		t.Fatalf("the write at %d is still in the leader's log (first %d)", rev, f)
	}
	nowMs.Add(-60_000) // wall clocks go back a minute from here on
	c.Heal()
	waitFor(t, 30*time.Second, "the partitioned node to install a snapshot", func() bool {
		return c.Node(lagger).Metrics().SnapshotInstalls() >= 1
	})
	tables := c.SessionHashes(30 * time.Second)
	if tables[lagger] != tables[leader] || tables[lagger].Sessions == 0 {
		t.Fatalf("session tables differ after install: %+v", tables)
	}
	clockBefore := c.Node(lagger).Store.SessionClock()

	// Make the node that only has the snapshot the leader.
	for c.Leader(15*time.Second) != lagger {
		l := c.Leader(15 * time.Second)
		c.Stop(l)
		c.Leader(15*time.Second, lagger, 6-l-lagger)
		c.Start(l)
	}
	lconn, err := grpc.NewClient(c.Addr(lagger), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lconn.Close() }()
	lkv := raftkvv1.NewKVClient(lconn)
	// The retry carries the same (client_id, seq): it must get the cached
	// reply and must not apply (a different value makes a re-apply visible).
	// Retrying a retry is itself allowed (UNAVAILABLE while leadership
	// settles after the restarts).
	put := func(value string, seq, acked uint64) *raftkvv1.PutResponse {
		t.Helper()
		var lastErr error
		for range 50 {
			r, err := lkv.Put(ctx, &raftkvv1.PutRequest{Key: []byte("x"), Value: []byte(value),
				Session: &raftkvv1.WriteSession{ClientId: cid, Sequence: seq, AckedSequence: acked}})
			if err == nil {
				return r
			}
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("put seq %d: %v", seq, err)
			}
			lastErr = err
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("put seq %d: %v", seq, lastErr)
		return nil
	}
	retry := put("2", 1, 0)
	if retry.GetRevision() != rev {
		t.Fatalf("retry answered with revision %d, want the cached %d", retry.GetRevision(), rev)
	}
	g, err := lkv.Get(ctx, &raftkvv1.GetRequest{Key: []byte("x")})
	if err != nil || string(g.GetKv().GetValue()) != "1" || g.GetKv().GetModRevision() != rev {
		t.Fatalf("x = %q at %d (err %v), want \"1\" at %d: the retry was applied twice",
			g.GetKv().GetValue(), g.GetKv().GetModRevision(), err, rev)
	}
	// The next sequence applies normally; session time does not go back.
	if next := put("3", 2, 1); next.GetRevision() <= rev {
		t.Fatalf("seq 2 answered with revision %d", next.GetRevision())
	}
	if after := c.Node(lagger).Store.SessionClock(); after < clockBefore {
		t.Fatalf("session clock went back from %d to %d after install", clockBefore, after)
	}
	requireSameState(t, c)
}
