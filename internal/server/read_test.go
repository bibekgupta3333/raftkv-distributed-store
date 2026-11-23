package server_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/kvtest"
	"github.com/bibekgupta3333/raftkv-distributed-store/pkg/client"
)

func others(c *kvtest.Cluster, id uint64) []uint64 {
	var out []uint64
	for _, o := range c.IDs() {
		if o != id {
			out = append(out, o)
		}
	}
	return out
}

// WBS 4.2.2: a linearizable Get (the default) is served by the leader and by
// followers, and each sees every write acknowledged before it started.
func TestLinearizableGetOnLeaderAndFollowers(t *testing.T) {
	// 50 ms ticks: raw Puts do not retry, so a spurious election on a loaded
	// -race runner must not fail the test.
	c := kvtest.New(t, 3, kvtest.Options{Tick: 50 * time.Millisecond})
	lead := c.Leader(30 * time.Second)
	leader := rawClient(t, c.Addr(lead))
	members := map[uint64]raftkvv1.KVClient{}
	for _, id := range c.IDs() {
		members[id] = rawClient(t, c.Addr(id))
	}
	for i := range 30 {
		val := fmt.Sprint("v", i)
		put, err := leader.Put(ctx5(t), &raftkvv1.PutRequest{Key: []byte("k"), Value: []byte(val)})
		if err != nil {
			t.Fatal(err)
		}
		// Right after the ack, on every member: the write must be visible.
		for id, kv := range members {
			for _, cons := range []raftkvv1.Consistency{raftkvv1.Consistency_CONSISTENCY_UNSPECIFIED, raftkvv1.Consistency_LINEARIZABLE} {
				g, err := kv.Get(ctx5(t), &raftkvv1.GetRequest{Key: []byte("k"), Consistency: cons})
				if err != nil {
					t.Fatalf("member %d: Get: %v", id, err)
				}
				if !g.GetFound() || string(g.GetKv().GetValue()) != val || g.GetHeader().GetMemberId() != id {
					t.Fatalf("member %d: Get = %v, want %q written at %d (stale read)", id, g, val, put.GetRevision())
				}
				if g.GetHeader().GetRevision() < put.GetRevision() {
					t.Fatalf("member %d: revision %d < write %d", id, g.GetHeader().GetRevision(), put.GetRevision())
				}
			}
		}
	}
}

// WBS 4.2.3: STALE reads are served from local state, report the applied index
// they read at, honour min_revision, and keep working on a member cut off from
// the leader, where a linearizable read cannot be served.
func TestStaleGet(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	lead := c.Leader(15 * time.Second)
	cl := newClient(t, c)
	put, err := cl.Put(ctx5(t), []byte("k"), []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	f := others(c, lead)[0]
	fkv := rawClient(t, c.Addr(f))

	g, err := fkv.Get(ctx5(t), &raftkvv1.GetRequest{Key: []byte("k"), Consistency: raftkvv1.Consistency_STALE, MinRevision: put.Revision})
	if err != nil || !g.GetFound() || string(g.GetKv().GetValue()) != "v1" || g.GetHeader().GetRevision() < put.Revision {
		t.Fatalf("STALE Get with min_revision = %v, %v; want v1 at revision >= %d", g, err, put.Revision)
	}
	applied := c.Node(f).Store.Applied()
	if g.GetHeader().GetRevision() > applied {
		t.Fatalf("revision %d is beyond the member's applied index %d", g.GetHeader().GetRevision(), applied)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = fkv.Get(ctx, &raftkvv1.GetRequest{Key: []byte("k"), Consistency: raftkvv1.Consistency_STALE, MinRevision: put.Revision + 1_000_000})
	if info, _ := details(err); status.Code(err) != codes.DeadlineExceeded || info.GetReason() != "REVISION_NOT_REACHED" {
		t.Fatalf("unreachable min_revision: err = %v", err)
	}

	// Cut the follower off; the majority keeps writing.
	c.Partition([]uint64{f}, others(c, f))
	if _, err := cl.Put(ctx5(t), []byte("k"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	g, err = fkv.Get(ctx5(t), &raftkvv1.GetRequest{Key: []byte("k"), Consistency: raftkvv1.Consistency_STALE})
	if err != nil || string(g.GetKv().GetValue()) != "v1" {
		t.Fatalf("STALE Get on a cut-off member = %v, %v; want the old value v1", g, err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel2()
	if g, err := fkv.Get(ctx2, &raftkvv1.GetRequest{Key: []byte("k")}); err == nil {
		t.Fatalf("linearizable Get on a cut-off member succeeded: %v", g)
	}
	c.Heal()
}

// Exit criterion M4-2 (in process): a partitioned old leader never serves a
// linearizable read once a new leader exists; it fails instead of returning the
// value the new leader has overwritten.
func TestPartitionedOldLeaderServesNoStaleRead(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	old := c.Leader(15 * time.Second)
	okv := rawClient(t, c.Addr(old))
	if _, err := okv.Put(ctx5(t), &raftkvv1.PutRequest{Key: []byte("k"), Value: []byte("old")}); err != nil {
		t.Fatal(err)
	}
	rest := others(c, old)
	c.Partition([]uint64{old}, rest)
	nl := c.Leader(20*time.Second, rest...)
	if _, err := rawClient(t, c.Addr(nl)).Put(ctx5(t), &raftkvv1.PutRequest{Key: []byte("k"), Value: []byte("new")}); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		g, err := okv.Get(ctx, &raftkvv1.GetRequest{Key: []byte("k")})
		cancel()
		if err == nil {
			t.Fatalf("old leader served a linearizable read after a new leader existed: %v", g)
		}
		if c := status.Code(err); c != codes.Unavailable && c != codes.DeadlineExceeded {
			t.Fatalf("old leader read err = %v, want UNAVAILABLE or DEADLINE_EXCEEDED", err)
		}
	}
	c.Heal()
}

// WBS 4.2.4 end to end: with lease reads on, the leader serves Gets and they
// are still linearizable from every member's point of view.
func TestLeaseReadsEndToEnd(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{Tick: 50 * time.Millisecond, LeaseReads: true})
	lead := c.Leader(20 * time.Second)
	deadline := time.Now().Add(10 * time.Second)
	for !c.Node(lead).LeaseValid() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !c.Node(lead).LeaseValid() {
		t.Fatal("the leader never obtained a lease")
	}
	cl, err := client.New(client.Config{Endpoints: []string{c.Addr(lead)}, DefaultTimeout: 15 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	for i := range 10 {
		val := fmt.Sprint("v", i)
		if _, err := cl.Put(ctx5(t), []byte("k"), []byte(val)); err != nil {
			t.Fatal(err)
		}
		g, err := cl.Get(ctx5(t), []byte("k"))
		if err != nil || string(g.KV.Value) != val {
			t.Fatalf("lease Get = %+v, %v; want %s", g, err, val)
		}
	}
}
