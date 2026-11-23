package server_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/kvtest"
	"github.com/bibekgupta3333/raftkv-distributed-store/pkg/client"
)

func newClient(t *testing.T, c *kvtest.Cluster) *client.Client {
	t.Helper()
	cl, err := client.New(client.Config{Endpoints: c.Endpoints(), DefaultTimeout: 15 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

func ctx5(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// WBS 3.2.3 / 3.2.4: Put, Get through the log, Delete, end to end on 3 nodes.
func TestPutGetDelete(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	c.Leader(15 * time.Second)
	cl := newClient(t, c)
	ctx := ctx5(t)

	if r, err := cl.Get(ctx, []byte("k")); err != nil || r.Found {
		t.Fatalf("Get of a missing key = %+v, %v", r, err)
	}
	p1, err := cl.Put(ctx, []byte("k"), []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	p2, err := cl.Put(ctx, []byte("k"), []byte("v2"), client.WithPrevious())
	if err != nil {
		t.Fatal(err)
	}
	if p2.Revision <= p1.Revision || p2.Previous == nil || string(p2.Previous.Value) != "v1" {
		t.Fatalf("put results %+v %+v", p1, p2)
	}
	g, err := cl.Get(ctx, []byte("k"))
	if err != nil || !g.Found || string(g.KV.Value) != "v2" || g.KV.Version != 2 || g.KV.CreateRevision != p1.Revision {
		t.Fatalf("Get = %+v, %v", g, err)
	}
	d, err := cl.Delete(ctx, []byte("k"), client.WithPrevious())
	if err != nil || !d.Deleted || string(d.Previous.Value) != "v2" {
		t.Fatalf("Delete = %+v, %v", d, err)
	}
	if d2, err := cl.Delete(ctx, []byte("k")); err != nil || d2.Deleted {
		t.Fatalf("second Delete = %+v, %v (a missing key is OK, deleted=false)", d2, err)
	}
	if g, err := cl.Get(ctx, []byte("k")); err != nil || g.Found {
		t.Fatalf("Get after delete = %+v, %v", g, err)
	}

	// Every replica ends with the same state (WBS 3.1.8, INV-5).
	hashes := c.StateHashes(10 * time.Second)
	var first [32]byte
	for _, h := range hashes {
		if first == ([32]byte{}) {
			first = h
		}
		if h != first {
			t.Fatalf("replicas disagree: %x", hashes)
		}
	}
}

func rawClient(t *testing.T, addr string) raftkvv1.KVClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return raftkvv1.NewKVClient(conn)
}

func details(err error) (info *errdetails.ErrorInfo, hint *raftkvv1.LeaderHint) {
	st, _ := status.FromError(err)
	for _, d := range st.Details() {
		switch d := d.(type) {
		case *errdetails.ErrorInfo:
			info = d
		case *raftkvv1.LeaderHint:
			hint = d
		}
	}
	return
}

// WBS 3.2.4: a follower answers NOT_LEADER, DEFINITE, with a LeaderHint.
func TestFollowerReturnsNotLeaderWithHint(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	lead := c.Leader(15 * time.Second)
	// Wait for followers to learn the leader.
	deadline := time.Now().Add(10 * time.Second)
	for _, id := range c.IDs() {
		for c.Node(id).Status().Lead != lead && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
	}
	for _, id := range c.IDs() {
		if id == lead {
			continue
		}
		kv := rawClient(t, c.Addr(id))
		_, err := kv.Put(ctx5(t), &raftkvv1.PutRequest{Key: []byte("k"), Value: []byte("v")})
		info, hint := details(err)
		if status.Code(err) != codes.Unavailable || info == nil || info.GetReason() != "NOT_LEADER" ||
			info.GetMetadata()["outcome"] != "DEFINITE" {
			t.Fatalf("follower %d: err = %v, info = %v", id, err, info)
		}
		if hint == nil || hint.GetLeaderId() != lead || hint.GetLeaderClientUrl() == "" {
			t.Fatalf("follower %d: hint = %v, want leader %d", id, hint, lead)
		}
		// Reads through the log are leader-only too.
		if _, err := kv.Get(ctx5(t), &raftkvv1.GetRequest{Key: []byte("k")}); !strings.Contains(err.Error(), "not the leader") {
			t.Fatalf("follower Get err = %v", err)
		}
	}
	// The same request on the leader works, with a populated header.
	resp, err := rawClient(t, c.Addr(lead)).Put(ctx5(t), &raftkvv1.PutRequest{Key: []byte("k"), Value: []byte("v")})
	if err != nil || resp.GetHeader().GetMemberId() != lead || resp.GetHeader().GetClusterId() != 1 || resp.GetRevision() == 0 {
		t.Fatalf("leader Put = %+v, %v", resp, err)
	}
}

func TestValidationAndClusterID(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	lead := c.Leader(15 * time.Second)
	kv := rawClient(t, c.Addr(lead))
	for name, tc := range map[string]struct {
		req    *raftkvv1.PutRequest
		reason string
	}{
		"empty key":  {&raftkvv1.PutRequest{}, "EMPTY_KEY"},
		"huge key":   {&raftkvv1.PutRequest{Key: bytes.Repeat([]byte("k"), 4097)}, "KEY_TOO_LARGE"},
		"huge value": {&raftkvv1.PutRequest{Key: []byte("k"), Value: make([]byte, 1<<20+1)}, "VALUE_TOO_LARGE"},
	} {
		_, err := kv.Put(ctx5(t), tc.req)
		info, _ := details(err)
		if status.Code(err) != codes.InvalidArgument || info.GetReason() != tc.reason {
			t.Errorf("%s: err = %v, reason = %q, want %s", name, err, info.GetReason(), tc.reason)
		}
	}
	_, err := kv.Put(ctx5(t), &raftkvv1.PutRequest{Header: &raftkvv1.RequestHeader{ClusterId: 99}, Key: []byte("k")})
	if info, _ := details(err); status.Code(err) != codes.FailedPrecondition || info.GetReason() != "CLUSTER_ID_MISMATCH" {
		t.Errorf("cluster mismatch: %v", err)
	}
	if _, err := kv.Get(ctx5(t), &raftkvv1.GetRequest{Key: []byte("k"), MinRevision: 5}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("min_revision on a LINEARIZABLE get: %v", err)
	}
	if _, err := kv.Range(ctx5(t), &raftkvv1.RangeRequest{}); status.Code(err) != codes.Unimplemented {
		t.Errorf("Range = %v, want Unimplemented until M4", err)
	}
}

// Data written through the cluster survives a full restart (WAL + Pebble
// recovery, WBS 3.1.6): stop every node, start them again, read it all back.
func TestRestartRecoversState(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	c.Leader(15 * time.Second)
	cl := newClient(t, c)
	ctx := ctx5(t)
	for _, k := range []string{"a", "b", "c"} {
		if _, err := cl.Put(ctx, []byte(k), []byte("val-"+k)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cl.Delete(ctx, []byte("b")); err != nil {
		t.Fatal(err)
	}
	before := c.StateHashes(10 * time.Second)
	var wantHash [32]byte
	for _, h := range before {
		wantHash = h
	}
	for _, id := range c.IDs() {
		c.Stop(id)
	}
	for _, id := range c.IDs() {
		c.Start(id)
	}
	c.Leader(20 * time.Second)
	for k, want := range map[string]string{"a": "val-a", "b": "", "c": "val-c"} {
		g, err := cl.Get(ctx, []byte(k))
		if err != nil || g.Found != (want != "") || (want != "" && string(g.KV.Value) != want) {
			t.Fatalf("after restart Get(%s) = %+v, %v", k, g, err)
		}
	}
	after := c.StateHashes(10 * time.Second)
	for id, h := range after {
		if h != wantHash { // a new leader's no-op changes appliedIndex, which is not hashed
			t.Fatalf("node %d state hash changed across a restart", id)
		}
	}
}

// WBS 3.2.2 end to end: when the leader loses its quorum, a write pending on it
// fails with a retryable INDETERMINATE error instead of hanging until the
// deadline, and the cluster continues on the majority side.
func TestPendingWriteFailsRetryablyOnLeaderLoss(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	old := c.Leader(15 * time.Second)
	kv := rawClient(t, c.Addr(old))
	if _, err := kv.Put(ctx5(t), &raftkvv1.PutRequest{Key: []byte("warm"), Value: []byte("up")}); err != nil {
		t.Fatal(err)
	}

	var others []uint64
	for _, id := range c.IDs() {
		if id != old {
			others = append(others, id)
		}
	}
	c.Partition([]uint64{old}, others)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	_, err := kv.Put(ctx, &raftkvv1.PutRequest{Key: []byte("k"), Value: []byte("stuck")})
	info, _ := details(err)
	if err == nil {
		t.Fatal("a write on a partitioned leader must not succeed")
	}
	if info.GetReason() != "OUTCOME_UNKNOWN" && info.GetReason() != "NOT_LEADER" && info.GetReason() != "NO_LEADER" {
		t.Fatalf("err = %v (reason %q)", err, info.GetReason())
	}
	if time.Since(start) > 15*time.Second {
		t.Fatalf("pending write was not failed on leader change (took %v)", time.Since(start))
	}
	if info.GetReason() == "OUTCOME_UNKNOWN" && info.GetMetadata()["outcome"] != "INDETERMINATE" {
		t.Fatalf("OUTCOME_UNKNOWN must be INDETERMINATE: %v", info)
	}
	c.Heal()
	nl := c.Leader(20 * time.Second)
	if _, err := rawClient(t, c.Addr(nl)).Put(ctx5(t), &raftkvv1.PutRequest{Key: []byte("after"), Value: []byte("heal")}); err != nil {
		t.Fatalf("write after heal: %v", err)
	}
}
