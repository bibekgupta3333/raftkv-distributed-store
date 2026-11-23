package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/kvtest"
)

func clusterClient(t *testing.T, addr string) raftkvv1.ClusterClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return raftkvv1.NewClusterClient(conn)
}

// D-48: Cluster.Status reports each member's local Raft view; the leader adds
// per-follower progress. The membership RPCs stay UNIMPLEMENTED until M6.
func TestClusterStatus(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	lead := c.Leader(15 * time.Second)
	cl := newClient(t, c)
	ctx := ctx5(t)
	var last uint64
	for i := range 5 {
		r, err := cl.Put(ctx, []byte(fmt.Sprint("k", i)), []byte("v"))
		if err != nil {
			t.Fatal(err)
		}
		last = r.Revision
	}

	ls, err := clusterClient(t, c.Addr(lead)).Status(ctx, &raftkvv1.StatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if ls.State != raftkvv1.RaftState_LEADER || ls.LeaderId != lead || ls.Header.MemberId != lead ||
		ls.Header.ClusterId == 0 || ls.Term == 0 || ls.Header.RaftTerm != ls.Term {
		t.Fatalf("leader status = %v", ls)
	}
	if ls.CommitIndex < last || ls.AppliedIndex < last || ls.LastLogIndex < ls.CommitIndex ||
		ls.FirstLogIndex == 0 || ls.SnapshotIndex != 0 {
		t.Fatalf("leader indexes = %v (last write at %d)", ls, last)
	}
	if len(ls.Followers) != 2 {
		t.Fatalf("leader followers = %v, want 2 (self excluded)", ls.Followers)
	}
	for _, f := range ls.Followers {
		if f.MemberId == lead || f.MatchIndex == 0 || f.NextIndex <= f.MatchIndex {
			t.Fatalf("follower progress = %v", f)
		}
	}

	for _, id := range c.IDs() {
		if id == lead {
			continue
		}
		fs, err := clusterClient(t, c.Addr(id)).Status(ctx, &raftkvv1.StatusRequest{})
		if err != nil {
			t.Fatal(err)
		}
		// Local only, never forwarded: the reply is from the member asked.
		if fs.Header.MemberId != id || fs.State != raftkvv1.RaftState_FOLLOWER || fs.LeaderId != lead ||
			len(fs.Followers) != 0 {
			t.Fatalf("follower %d status = %v", id, fs)
		}
	}

	cc := clusterClient(t, c.Addr(lead))
	_, err = cc.Status(ctx, &raftkvv1.StatusRequest{Header: &raftkvv1.RequestHeader{ClusterId: 999}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Status with a foreign cluster ID: %v, want FAILED_PRECONDITION", err)
	}
	if _, err := cc.AddLearner(ctx, &raftkvv1.AddLearnerRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("AddLearner: %v, want UNIMPLEMENTED until M6", err)
	}
}

// Status keeps answering on a member cut off from the others (a follower in a
// minority still reports its own term and indexes).
func TestClusterStatusPartitioned(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	lead := c.Leader(15 * time.Second)
	var lone uint64
	var rest []uint64
	for _, id := range c.IDs() {
		if id != lead && lone == 0 {
			lone = id
		} else {
			rest = append(rest, id)
		}
	}
	c.Partition([]uint64{lone}, rest)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := clusterClient(t, c.Addr(lone)).Status(ctx, &raftkvv1.StatusRequest{})
	if err != nil || st.Header.MemberId != lone {
		t.Fatalf("partitioned member Status = %v, %v", st, err)
	}
}

// WBS 7.3.1: unary RPCs are counted by method and code for the dashboard.
func TestRequestMetrics(t *testing.T) {
	c := kvtest.New(t, 1, kvtest.Options{})
	id := c.Leader(15 * time.Second)
	ctx := ctx5(t)
	if _, err := clusterClient(t, c.Addr(id)).Status(ctx, &raftkvv1.StatusRequest{}); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	c.Node(id).Metrics().Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{
		`raftkv_grpc_requests_total{code="OK",method="/raftkv.v1.Cluster/Status"} 1`,
		`raftkv_grpc_request_duration_seconds_count{method="/raftkv.v1.Cluster/Status"} 1`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}
