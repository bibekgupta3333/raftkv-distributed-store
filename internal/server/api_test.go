package server_test

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/kvtest"
	"github.com/bibekgupta3333/raftkv-distributed-store/pkg/client"
)

// leaderKnown waits until every node agrees on lead and returns a follower.
func leaderKnown(t *testing.T, c *kvtest.Cluster, lead uint64) (follower uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for _, id := range c.IDs() {
		for c.Node(id).Status().Lead != lead {
			if time.Now().After(deadline) {
				t.Fatalf("node %d never learned leader %d", id, lead)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if id != lead {
			follower = id
		}
	}
	return follower
}

func wantErr(t *testing.T, err error, code codes.Code, reason, outcome string) {
	t.Helper()
	info, _ := details(err)
	if status.Code(err) != code || info.GetReason() != reason || info.GetMetadata()["outcome"] != outcome {
		t.Fatalf("err = %v (reason %q, outcome %q), want %s/%s/%q", err, info.GetReason(), info.GetMetadata()["outcome"], code, reason, outcome)
	}
}

// WBS 4.4.1 (D-28): a follower forwards writes (and session registration) to
// the leader by default and relays the reply; a request already forwarded is
// not forwarded again.
func TestFollowerForwardsWrites(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	lead := c.Leader(15 * time.Second)
	f := leaderKnown(t, c, lead)
	kv := rawClient(t, c.Addr(f))

	reg, err := kv.RegisterSession(ctx5(t), &raftkvv1.RegisterSessionRequest{})
	if err != nil || reg.GetClientId() == 0 {
		t.Fatalf("RegisterSession via a follower = %v, %v", reg, err)
	}
	put, err := kv.Put(ctx5(t), &raftkvv1.PutRequest{Key: []byte("fwd"), Value: []byte("v"),
		Session: &raftkvv1.WriteSession{ClientId: reg.GetClientId(), Sequence: 1}})
	if err != nil || put.GetRevision() == 0 {
		t.Fatalf("forwarded Put = %v, %v", put, err)
	}
	if h := put.GetHeader(); h.GetMemberId() != f || h.GetLeaderId() != lead {
		t.Fatalf("header = %v, want member %d (the follower) and leader %d", h, f, lead)
	}
	cas, err := kv.CompareAndSwap(ctx5(t), &raftkvv1.CompareAndSwapRequest{Key: []byte("fwd"),
		Expected: &raftkvv1.CompareAndSwapRequest_ExpectedValue{ExpectedValue: []byte("v")},
		Action:   &raftkvv1.CompareAndSwapRequest_Delete{Delete: true}})
	if err != nil || !cas.GetSucceeded() {
		t.Fatalf("forwarded CAS = %v, %v", cas, err)
	}
	if d, err := kv.Delete(ctx5(t), &raftkvv1.DeleteRequest{Key: []byte("fwd")}); err != nil || d.GetDeleted() {
		t.Fatalf("forwarded Delete = %v, %v", d, err)
	}
	// One hop only.
	_, err = kv.Put(ctx5(t), &raftkvv1.PutRequest{Key: []byte("k"), Header: &raftkvv1.RequestHeader{Forwarded: true}})
	wantErr(t, err, codes.Unavailable, "NOT_LEADER", "DEFINITE")

	// The library client with the default routing gets the same result.
	cl, err := client.New(client.Config{Endpoints: []string{c.Addr(f)}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cl.Close() }()
	if _, err := cl.Put(ctx5(t), []byte("lib"), []byte("x")); err != nil {
		t.Fatalf("client Put through a single follower endpoint: %v", err)
	}
	// With REDIRECT and a single follower endpoint the hint cannot be followed.
	rcl, err := client.New(client.Config{Endpoints: []string{c.Addr(f)}, Redirect: true, DefaultTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rcl.Close() }()
	if _, err := rcl.Put(context.Background(), []byte("lib"), []byte("y")); !client.IsNotLeader(err) {
		t.Fatalf("REDIRECT client on a follower: %v, want NOT_LEADER", err)
	}
}

// WBS 4.3.1 / 4.3.2 end to end: RegisterSession goes through the log and the
// ID is the entry index on every replica; a retried sequence returns the
// cached reply; unknown sessions and stale sequences are refused, DEFINITE.
func TestSessionsEndToEnd(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	lead := c.Leader(15 * time.Second)
	kv := rawClient(t, c.Addr(lead))
	reg, err := kv.RegisterSession(ctx5(t), &raftkvv1.RegisterSessionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	cid := reg.GetClientId()
	ws := func(seq, acked uint64) *raftkvv1.WriteSession {
		return &raftkvv1.WriteSession{ClientId: cid, Sequence: seq, AckedSequence: acked}
	}
	p1, err := kv.Put(ctx5(t), &raftkvv1.PutRequest{Key: []byte("k"), Value: []byte("one"), Session: ws(1, 0)})
	if err != nil {
		t.Fatal(err)
	}
	p1b, err := kv.Put(ctx5(t), &raftkvv1.PutRequest{Key: []byte("k"), Value: []byte("one"), Session: ws(1, 0)})
	if err != nil || p1b.GetRevision() != p1.GetRevision() {
		t.Fatalf("retry = %v, %v; want the cached revision %d", p1b, err, p1.GetRevision())
	}
	if g, _ := kv.Get(ctx5(t), &raftkvv1.GetRequest{Key: []byte("k")}); g.GetKv().GetVersion() != 1 {
		t.Fatalf("version = %d after a retried put, want 1", g.GetKv().GetVersion())
	}
	if _, err := kv.Put(ctx5(t), &raftkvv1.PutRequest{Key: []byte("k"), Value: []byte("two"), Session: ws(2, 1)}); err != nil {
		t.Fatal(err)
	}
	_, err = kv.Put(ctx5(t), &raftkvv1.PutRequest{Key: []byte("k"), Value: []byte("x"), Session: ws(1, 1)})
	wantErr(t, err, codes.FailedPrecondition, "SEQUENCE_STALE", "DEFINITE")
	_, err = kv.Put(ctx5(t), &raftkvv1.PutRequest{Key: []byte("k"), Value: []byte("x"), Session: ws(70, 1)})
	wantErr(t, err, codes.FailedPrecondition, "SESSION_WINDOW_EXCEEDED", "DEFINITE")
	_, err = kv.Put(ctx5(t), &raftkvv1.PutRequest{Key: []byte("k"), Value: []byte("x"),
		Session: &raftkvv1.WriteSession{ClientId: 999_999, Sequence: 1}})
	wantErr(t, err, codes.FailedPrecondition, "SESSION_EXPIRED", "DEFINITE")

	for id, st := range c.SessionHashes(10 * time.Second) {
		if st.Sessions != 1 {
			t.Fatalf("node %d has %d sessions, want 1", id, st.Sessions)
		}
		if ok, _ := c.Node(id).Store.HasSession(cid); !ok {
			t.Fatalf("node %d lacks session %d", id, cid)
		}
	}
}

// WBS 4.3.3: the leader stamps its time into each command and every replica
// expires sessions with that stamped time: after 10+ minutes of (fake) idle
// time a session is gone on all replicas at the same index, and the session
// tables hash identically.
func TestSessionExpiryReplicasAgree(t *testing.T) {
	var offset atomic.Int64
	c := kvtest.New(t, 3, kvtest.Options{Now: func() time.Time {
		return time.Now().Add(time.Duration(offset.Load()))
	}})
	c.Leader(15 * time.Second)
	idle, busy := newClient(t, c), newClient(t, c)
	ctx := ctx5(t)
	if _, err := idle.Put(ctx, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if _, err := busy.Put(ctx, []byte("b"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	idleID := idle.SessionID()
	for step := range 4 { // busy writes every 3 minutes, idle stays quiet for 12
		offset.Add(int64(3 * time.Minute))
		if _, err := busy.Put(ctx, []byte("b"), []byte(fmt.Sprint(step))); err != nil {
			t.Fatal(err)
		}
	}
	tables := c.SessionHashes(10 * time.Second)
	var first kvtest.SessionTable
	for id, st := range tables {
		if ok, _ := c.Node(id).Store.HasSession(idleID); ok {
			t.Fatalf("node %d still has the idle session", id)
		}
		if st.Sessions != 1 {
			t.Fatalf("node %d has %d sessions, want 1 (busy)", id, st.Sessions)
		}
		if first.Sessions == 0 {
			first = st
		} else if st != first {
			t.Fatalf("session tables differ across replicas: %v", tables)
		}
	}
	// The idle client's next write gets SESSION_EXPIRED on its first attempt,
	// which proves nothing applied, so the library re-registers and succeeds.
	if _, err := idle.Put(ctx, []byte("a"), []byte("2")); err != nil {
		t.Fatalf("write after expiry: %v", err)
	}
	if idle.SessionID() == idleID {
		t.Fatal("the client did not register a new session")
	}
}

// WBS 4.4.2: CompareAndSwap per api-design 3.1 (expect_absent, value and
// revision compares, delete action, mismatch returns OK with the current
// value, invalid compares rejected).
func TestCompareAndSwap(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	c.Leader(15 * time.Second)
	cl := newClient(t, c)
	ctx := ctx5(t)
	k := []byte("cas")

	r, err := cl.CompareAndSwap(ctx, k, client.Absent(), client.SetValue([]byte("v1")))
	if err != nil || !r.Succeeded || r.Current == nil || string(r.Current.Value) != "v1" {
		t.Fatalf("create-if-absent = %+v, %v", r, err)
	}
	created := r.Revision
	r, err = cl.CompareAndSwap(ctx, k, client.Absent(), client.SetValue([]byte("v2")))
	if err != nil || r.Succeeded || !r.Found || string(r.Current.Value) != "v1" || r.Revision <= created {
		t.Fatalf("mismatch = %+v, %v; want OK, succeeded=false, current v1, a new revision", r, err)
	}
	r, err = cl.CompareAndSwap(ctx, k, client.ModRevisionEquals(created), client.SetValue([]byte("v3")))
	if err != nil || !r.Succeeded || r.Current.ModRevision != r.Revision {
		t.Fatalf("revision compare = %+v, %v", r, err)
	}
	r, err = cl.CompareAndSwap(ctx, k, client.ValueEquals([]byte("v3")), client.DeleteKey())
	if err != nil || !r.Succeeded || r.Found {
		t.Fatalf("value compare + delete = %+v, %v", r, err)
	}
	if g, _ := cl.Get(ctx, k); g.Found {
		t.Fatal("key still exists")
	}
	r, err = cl.CompareAndSwap(ctx, k, client.ValueEquals(nil), client.SetValue([]byte("x")))
	if err != nil || r.Succeeded || r.Found {
		t.Fatalf("empty expected value on a missing key = %+v, %v (empty value is not absent)", r, err)
	}
	_, err = cl.CompareAndSwap(ctx, k, client.ModRevisionEquals(0), client.SetValue([]byte("x")))
	var ce *client.Error
	if !asErr(err, &ce) || ce.Code != codes.InvalidArgument || ce.Reason != "MALFORMED_REQUEST" {
		t.Fatalf("expected_mod_revision 0 = %v", err)
	}
	_, err = rawClient(t, c.Endpoints()[0]).CompareAndSwap(ctx, &raftkvv1.CompareAndSwapRequest{Key: k,
		Action: &raftkvv1.CompareAndSwapRequest_Delete{Delete: true}})
	wantErr(t, err, codes.InvalidArgument, "MALFORMED_REQUEST", "DEFINITE")
}

// WBS 4.4.3: Range with prefix or [start, end), page limit, continuation
// token, reverse, keys-only, stale pages, and paging across concurrent writes
// (each page is linearizable; the scan is not one snapshot, D-34).
func TestRangePaging(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	c.Leader(15 * time.Second)
	cl := newClient(t, c)
	ctx := ctx5(t)
	for i := range 25 {
		if _, err := cl.Put(ctx, []byte(fmt.Sprintf("p/%02d", i)), []byte(fmt.Sprint("v", i))); err != nil {
			t.Fatal(err)
		}
	}
	for _, k := range []string{"o", "q", "p"} { // around the prefix
		if _, err := cl.Put(ctx, []byte(k), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	collect := func(rr client.RangeRequest, between func(page int)) (keys []string, pages int) {
		t.Helper()
		err := cl.RangeAll(ctx, rr, func(p *client.RangePage) bool {
			for _, kv := range p.KVs {
				keys = append(keys, string(kv.Key))
				if rr.KeysOnly && kv.Value != nil {
					t.Fatalf("keys_only returned a value for %s", kv.Key)
				}
			}
			if p.More != (len(p.NextPageToken) > 0) {
				t.Fatalf("more=%v with token %x", p.More, p.NextPageToken)
			}
			pages++
			if between != nil {
				between(pages)
			}
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		return keys, pages
	}

	keys, pages := collect(client.RangeRequest{Prefix: []byte("p/"), Limit: 10}, nil)
	if len(keys) != 25 || pages != 3 || keys[0] != "p/00" || keys[24] != "p/24" {
		t.Fatalf("prefix scan: %d keys in %d pages: %v", len(keys), pages, keys)
	}
	keys, _ = collect(client.RangeRequest{Start: []byte("p/05"), End: []byte("p/08")}, nil)
	if fmt.Sprint(keys) != "[p/05 p/06 p/07]" {
		t.Fatalf("[start,end) = %v", keys)
	}
	keys, pages = collect(client.RangeRequest{Prefix: []byte("p/"), Limit: 7, Reverse: true, KeysOnly: true}, nil)
	if len(keys) != 25 || pages != 4 || keys[0] != "p/24" || keys[24] != "p/00" {
		t.Fatalf("reverse scan: %v in %d pages", keys, pages)
	}
	keys, _ = collect(client.RangeRequest{Prefix: []byte("p/"), Stale: true}, nil)
	if len(keys) != 25 {
		t.Fatalf("stale scan: %d keys", len(keys))
	}

	// Writes between pages: a key added behind the cursor is not seen, one
	// added ahead of it is, and a deleted key ahead disappears.
	keys, _ = collect(client.RangeRequest{Prefix: []byte("p/"), Limit: 10}, func(page int) {
		if page != 1 {
			return
		}
		mustDo(t, func() error { _, err := cl.Put(ctx, []byte("p/00a"), []byte("behind")); return err })
		mustDo(t, func() error { _, err := cl.Put(ctx, []byte("p/99"), []byte("ahead")); return err })
		mustDo(t, func() error { _, err := cl.Delete(ctx, []byte("p/20")); return err })
	})
	joined := fmt.Sprint(keys)
	if len(keys) != 25 || !bytes.Contains([]byte(joined), []byte("p/99")) ||
		bytes.Contains([]byte(joined), []byte("p/00a")) || bytes.Contains([]byte(joined), []byte("p/20")) {
		t.Fatalf("paging across writes: %v", keys)
	}

	// A token only works for the range it came from.
	kv := rawClient(t, c.Endpoints()[0])
	lead := c.Leader(5 * time.Second)
	kv = rawClient(t, c.Addr(lead))
	p1, err := kv.Range(ctx, &raftkvv1.RangeRequest{Prefix: []byte("p/"), Limit: 2})
	if err != nil || !p1.GetMore() {
		t.Fatalf("first page = %v, %v", p1, err)
	}
	_, err = kv.Range(ctx, &raftkvv1.RangeRequest{Prefix: []byte("q"), PageToken: p1.GetNextPageToken()})
	wantErr(t, err, codes.InvalidArgument, "INVALID_PAGE_TOKEN", "")
	_, err = kv.Range(ctx, &raftkvv1.RangeRequest{Prefix: []byte("p/"), PageToken: []byte("garbage")})
	wantErr(t, err, codes.InvalidArgument, "INVALID_PAGE_TOKEN", "")
	_, err = kv.Range(ctx, &raftkvv1.RangeRequest{Prefix: []byte("p/"), StartKey: []byte("a")})
	wantErr(t, err, codes.InvalidArgument, "MALFORMED_REQUEST", "")
	_, err = kv.Range(ctx, &raftkvv1.RangeRequest{MinRevision: 3})
	wantErr(t, err, codes.InvalidArgument, "MALFORMED_REQUEST", "")
}

func mustDo(t *testing.T, f func() error) {
	t.Helper()
	if err := f(); err != nil {
		t.Fatal(err)
	}
}

func asErr(err error, target **client.Error) bool {
	e, ok := err.(*client.Error) //nolint:errorlint // the client returns *Error directly
	if ok {
		*target = e
	}
	return ok
}
