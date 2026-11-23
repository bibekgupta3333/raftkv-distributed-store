package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

// fake is a scriptable KV server on a real localhost listener.
type fake struct {
	raftkvv1.UnimplementedKVServer
	id    uint64
	addr  string
	srv   *grpc.Server
	calls atomic.Int32

	mu   sync.Mutex
	put  func(n int) (*raftkvv1.PutResponse, error)
	get  func(n int) (*raftkvv1.GetResponse, error)
	sawH *raftkvv1.RequestHeader
}

func newFake(t *testing.T, id uint64) *fake {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fake{id: id, addr: l.Addr().String(), srv: grpc.NewServer()}
	raftkvv1.RegisterKVServer(f.srv, f)
	go func() { _ = f.srv.Serve(l) }()
	t.Cleanup(f.srv.Stop)
	return f
}

func (f *fake) hdr() *raftkvv1.ResponseHeader { return &raftkvv1.ResponseHeader{MemberId: f.id} }

func (f *fake) Put(_ context.Context, req *raftkvv1.PutRequest) (*raftkvv1.PutResponse, error) {
	n := int(f.calls.Add(1))
	f.mu.Lock()
	f.sawH = req.GetHeader()
	fn := f.put
	f.mu.Unlock()
	if fn == nil {
		return &raftkvv1.PutResponse{Header: f.hdr(), Revision: uint64(n)}, nil
	}
	return fn(n)
}

func (f *fake) Get(_ context.Context, _ *raftkvv1.GetRequest) (*raftkvv1.GetResponse, error) {
	n := int(f.calls.Add(1))
	f.mu.Lock()
	fn := f.get
	f.mu.Unlock()
	if fn == nil {
		return &raftkvv1.GetResponse{Header: f.hdr(), Found: true, Kv: &raftkvv1.KeyValue{Value: []byte("v")}}, nil
	}
	return fn(n)
}

func rpcErr(code codes.Code, reason, outcome string, hint *raftkvv1.LeaderHint) error {
	info := &errdetails.ErrorInfo{Reason: reason, Domain: "raftkv"}
	if outcome != "" {
		info.Metadata = map[string]string{"outcome": outcome}
	}
	ds := []protoadapt.MessageV1{info}
	if hint != nil {
		ds = append(ds, hint)
	}
	st, _ := status.New(code, reason).WithDetails(ds...)
	return st.Err()
}

func notLeader(f *fake, leader *fake) error {
	var hint *raftkvv1.LeaderHint
	if leader != nil {
		hint = &raftkvv1.LeaderHint{LeaderId: leader.id, LeaderClientUrl: leader.addr, Term: 1}
	}
	return rpcErr(codes.Unavailable, "NOT_LEADER", "DEFINITE", hint)
}

func fastClient(t *testing.T, eps []string, mod func(*Config)) *Client {
	t.Helper()
	cfg := Config{Endpoints: eps, DefaultTimeout: 5 * time.Second,
		Backoff: func(int) time.Duration { return time.Millisecond }}
	if mod != nil {
		mod(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// WBS 3.3.1: a follower redirects with a hint; the client follows it at once and caches the leader.
func TestFollowsNotLeaderHint(t *testing.T) {
	a, b, c := newFake(t, 1), newFake(t, 2), newFake(t, 3)
	a.put = func(int) (*raftkvv1.PutResponse, error) { return nil, notLeader(a, c) }
	b.put = a.put
	cl := fastClient(t, []string{a.addr, b.addr, c.addr}, nil)
	// Teach the client who is who via a successful read on each endpoint.
	for range 3 {
		if _, err := cl.Get(context.Background(), []byte("k")); err != nil {
			t.Fatal(err)
		}
	}
	cl.setLeader(0) // force the first write to start at a follower

	res, err := cl.Put(context.Background(), []byte("k"), []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Revision == 0 || c.calls.Load() == 0 {
		t.Fatalf("write did not reach the leader: %+v", res)
	}
	if cl.Leader() != c.addr {
		t.Fatalf("cached leader = %q, want %q", cl.Leader(), c.addr)
	}
	// The next write goes straight to the cached leader.
	before := a.calls.Load()
	if _, err := cl.Put(context.Background(), []byte("k"), []byte("v")); err != nil || a.calls.Load() != before {
		t.Fatalf("second write wandered: err=%v follower calls %d -> %d", err, before, a.calls.Load())
	}
	if h := c.sawH; h.GetWriteRouting() != raftkvv1.WriteRouting_REDIRECT {
		t.Fatalf("header = %v, want REDIRECT routing", h)
	}
}

// A hint to an unknown address, and no-hint NOT_LEADER, fall back to the next endpoint.
func TestNotLeaderWithoutUsableHintTriesNextEndpoint(t *testing.T) {
	a, b := newFake(t, 1), newFake(t, 2)
	a.put = func(int) (*raftkvv1.PutResponse, error) {
		return nil, rpcErr(codes.Unavailable, "NOT_LEADER", "DEFINITE", &raftkvv1.LeaderHint{LeaderId: 9, LeaderClientUrl: "10.0.0.1:1"})
	}
	cl := fastClient(t, []string{a.addr, b.addr}, nil)
	if _, err := cl.Put(context.Background(), []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if b.calls.Load() != 1 {
		t.Fatalf("b calls = %d", b.calls.Load())
	}
}

// WBS 3.3.2: retryable codes are retried with backoff until the call succeeds.
func TestRetriesRetryableReasonsWithBackoff(t *testing.T) {
	a := newFake(t, 1)
	a.put = func(n int) (*raftkvv1.PutResponse, error) {
		switch n {
		case 1:
			return nil, rpcErr(codes.Unavailable, "NO_LEADER", "DEFINITE", nil)
		case 2:
			return nil, rpcErr(codes.ResourceExhausted, "OVERLOADED", "DEFINITE", nil)
		case 3:
			return nil, rpcErr(codes.DeadlineExceeded, "NOT_PROPOSED", "DEFINITE", nil)
		}
		return &raftkvv1.PutResponse{Header: a.hdr(), Revision: 7}, nil
	}
	var slept []int
	cl := fastClient(t, []string{a.addr}, func(c *Config) {
		c.Backoff = func(attempt int) time.Duration { slept = append(slept, attempt); return time.Millisecond }
	})
	res, err := cl.Put(context.Background(), []byte("k"), []byte("v"))
	if err != nil || res.Revision != 7 {
		t.Fatalf("res = %+v, err = %v", res, err)
	}
	if len(slept) != 3 || slept[0] != 0 || slept[2] != 2 {
		t.Fatalf("backoff attempts = %v, want [0 1 2]", slept)
	}
}

func TestDefaultBackoffBounds(t *testing.T) {
	for attempt := range 12 {
		limit := min(time.Second, 25*time.Millisecond<<min(attempt, 6))
		for range 50 {
			if d := defaultBackoff(attempt); d < 0 || d > limit {
				t.Fatalf("attempt %d: %v outside [0, %v]", attempt, d, limit)
			}
		}
	}
}

// Without sessions an indeterminate write is not resent; a read is.
func TestIndeterminateWriteNotRetriedButReadIs(t *testing.T) {
	a, b := newFake(t, 1), newFake(t, 2)
	unknown := func() error { return rpcErr(codes.Unavailable, "OUTCOME_UNKNOWN", "INDETERMINATE", nil) }
	a.put = func(int) (*raftkvv1.PutResponse, error) { return nil, unknown() }
	a.get = func(int) (*raftkvv1.GetResponse, error) { return nil, unknown() }
	cl := fastClient(t, []string{a.addr, b.addr}, nil)

	_, err := cl.Put(context.Background(), []byte("k"), []byte("v"))
	var ce *Error
	if !errors.As(err, &ce) || !ce.Indeterminate() || ce.Reason != "OUTCOME_UNKNOWN" {
		t.Fatalf("Put err = %v, want an indeterminate *Error", err)
	}
	if a.calls.Load() != 1 || b.calls.Load() != 0 {
		t.Fatalf("write was resent: a=%d b=%d", a.calls.Load(), b.calls.Load())
	}

	cl.setLeader(0)
	if r, err := cl.Get(context.Background(), []byte("k")); err != nil || !r.Found {
		t.Fatalf("Get = %+v, %v; a read retries on the next endpoint", r, err)
	}

	// Opt-in retry for idempotent writes.
	a.calls.Store(0)
	cl2 := fastClient(t, []string{a.addr, b.addr}, func(c *Config) { c.RetryIndeterminate = true })
	cl2.setLeader(0)
	if _, err := cl2.Put(context.Background(), []byte("k"), []byte("v")); err != nil {
		t.Fatalf("RetryIndeterminate Put: %v", err)
	}
}

// Non-retryable codes fail at once.
func TestNonRetryableFailsImmediately(t *testing.T) {
	a := newFake(t, 1)
	a.put = func(int) (*raftkvv1.PutResponse, error) {
		return nil, rpcErr(codes.InvalidArgument, "EMPTY_KEY", "DEFINITE", nil)
	}
	cl := fastClient(t, []string{a.addr}, nil)
	_, err := cl.Put(context.Background(), nil, nil)
	var ce *Error
	if !errors.As(err, &ce) || ce.Code != codes.InvalidArgument || ce.Indeterminate() || a.calls.Load() != 1 {
		t.Fatalf("err = %v, calls = %d", err, a.calls.Load())
	}
}

// The retry loop is bounded by the caller's deadline and reports the last error.
func TestRetryBoundedByDeadline(t *testing.T) {
	a := newFake(t, 1)
	a.put = func(int) (*raftkvv1.PutResponse, error) {
		return nil, rpcErr(codes.Unavailable, "NO_LEADER", "DEFINITE", nil)
	}
	cl := fastClient(t, []string{a.addr}, func(c *Config) {
		c.Backoff = func(int) time.Duration { return 5 * time.Millisecond }
	})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := cl.Put(ctx, []byte("k"), []byte("v"))
	var ce *Error
	if !errors.As(err, &ce) || ce.Reason != "NO_LEADER" || ce.Attempts < 3 {
		t.Fatalf("err = %v", err)
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("took %v with a 150ms deadline", el)
	}
}

// A dead endpoint is skipped without sending (DEFINITE), so a write still succeeds.
func TestSkipsUnreachableEndpoint(t *testing.T) {
	dead, b := newFake(t, 1), newFake(t, 2)
	deadAddr := dead.addr
	dead.srv.Stop()
	cl := fastClient(t, []string{deadAddr, b.addr}, func(c *Config) { c.ConnectTimeout = 100 * time.Millisecond })
	if _, err := cl.Put(context.Background(), []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put with one dead endpoint: %v", err)
	}
	if cl.Leader() != b.addr {
		t.Fatalf("leader = %q", cl.Leader())
	}
	if _, err := New(Config{}); err == nil {
		t.Fatal("New without endpoints must fail")
	}
}
