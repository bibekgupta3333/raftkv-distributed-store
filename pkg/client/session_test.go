package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

// WBS 4.3.4: the first write registers a session; every logical write gets the
// next sequence; acked_sequence trails the finished writes.
func TestSessionAutoRegisterAndSequence(t *testing.T) {
	a := newFake(t, 1)
	cl := fastClient(t, []string{a.addr}, nil)
	for range 3 {
		if _, err := cl.Put(context.Background(), []byte("k"), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	if a.regs.Load() != 1 || cl.SessionID() != 100 {
		t.Fatalf("registrations = %d, session = %d; want one registration, client 100", a.regs.Load(), cl.SessionID())
	}
	got := a.seen()
	for i, ws := range got {
		want := uint64(i + 1)
		if ws.GetClientId() != 100 || ws.GetSequence() != want || ws.GetAckedSequence() != want-1 {
			t.Fatalf("write %d session = %v, want seq %d acked %d", i, ws, want, want-1)
		}
	}
}

// WBS 4.3.4: an indeterminate write is retried with the SAME sequence (safe:
// the server deduplicates), and a later write moves on.
func TestSessionRetryKeepsSequence(t *testing.T) {
	a, b := newFake(t, 1), newFake(t, 2)
	a.put = func(n int) (*raftkvv1.PutResponse, error) {
		return nil, rpcErr(codes.Unavailable, "OUTCOME_UNKNOWN", "INDETERMINATE", nil)
	}
	cl := fastClient(t, []string{a.addr, b.addr}, nil)
	cl.setLeader(0)
	if _, err := cl.Put(context.Background(), []byte("k"), []byte("v")); err != nil {
		t.Fatalf("a session write must be retried after OUTCOME_UNKNOWN: %v", err)
	}
	sa, sb := a.seen(), b.seen()
	if len(sa) == 0 || len(sb) != 1 || sa[0].GetSequence() != 1 || sb[0].GetSequence() != 1 ||
		sa[0].GetClientId() != sb[0].GetClientId() {
		t.Fatalf("retry changed the session: first %v, retry %v", sa, sb)
	}
	if _, err := cl.Put(context.Background(), []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if s := b.seen(); s[len(s)-1].GetSequence() != 2 || s[len(s)-1].GetAckedSequence() != 1 {
		t.Fatalf("next write = %v, want seq 2 acked 1", s[len(s)-1])
	}
}

// SESSION_EXPIRED on a first attempt proves nothing applied: the client
// registers a new session and retries. After an indeterminate attempt it must
// instead report the write as indeterminate (api-design 5).
func TestSessionExpiredHandling(t *testing.T) {
	expired := func() error { return rpcErr(codes.FailedPrecondition, "SESSION_EXPIRED", "DEFINITE", nil) }

	a := newFake(t, 1)
	a.put = func(n int) (*raftkvv1.PutResponse, error) {
		if n == 1 {
			return nil, expired()
		}
		return &raftkvv1.PutResponse{Header: a.hdr(), Revision: 9}, nil
	}
	cl := fastClient(t, []string{a.addr}, nil)
	if res, err := cl.Put(context.Background(), []byte("k"), []byte("v")); err != nil || res.Revision != 9 {
		t.Fatalf("Put = %+v, %v; want a retry on a new session", res, err)
	}
	if s := a.seen(); a.regs.Load() != 2 || s[1].GetClientId() != 101 || s[1].GetSequence() != 1 {
		t.Fatalf("regs = %d, sessions = %v", a.regs.Load(), s)
	}

	b := newFake(t, 2)
	b.put = func(n int) (*raftkvv1.PutResponse, error) {
		if n == 1 {
			return nil, rpcErr(codes.Unavailable, "OUTCOME_UNKNOWN", "INDETERMINATE", nil)
		}
		return nil, expired()
	}
	cl2 := fastClient(t, []string{b.addr}, nil)
	_, err := cl2.Put(context.Background(), []byte("k"), []byte("v"))
	var ce *Error
	if !errors.As(err, &ce) || ce.Reason != "SESSION_EXPIRED" || !ce.Indeterminate() {
		t.Fatalf("err = %v, want an INDETERMINATE SESSION_EXPIRED", err)
	}
	if b.calls.Load() != 2 {
		t.Fatalf("calls = %d; the write must not be resent on a new session", b.calls.Load())
	}
}

// Once any attempt of a write was indeterminate, a later DEFINITE error does
// not make the whole write definite: the first attempt may have applied.
func TestIndeterminateIsSticky(t *testing.T) {
	a := newFake(t, 1)
	a.put = func(n int) (*raftkvv1.PutResponse, error) {
		if n == 1 {
			return nil, rpcErr(codes.Unavailable, "OUTCOME_UNKNOWN", "INDETERMINATE", nil)
		}
		return nil, rpcErr(codes.InvalidArgument, "MALFORMED_REQUEST", "DEFINITE", nil)
	}
	cl := fastClient(t, []string{a.addr}, nil)
	_, err := cl.Put(context.Background(), []byte("k"), []byte("v"))
	var ce *Error
	if !errors.As(err, &ce) || !ce.Indeterminate() {
		t.Fatalf("err = %v, want INDETERMINATE", err)
	}
}

// A registration failure means the write was never sent: DEFINITE.
func TestRegistrationFailureIsDefinite(t *testing.T) {
	a := newFake(t, 1)
	a.reg = func(int) (*raftkvv1.RegisterSessionResponse, error) {
		return nil, rpcErr(codes.Unavailable, "OUTCOME_UNKNOWN", "INDETERMINATE", nil)
	}
	ctx, stop := context.WithCancel(context.Background())
	cl := fastClient(t, []string{a.addr}, func(c *Config) {
		c.Backoff = func(attempt int) time.Duration {
			stop()
			return time.Hour
		}
	})
	_, err := cl.Put(ctx, []byte("k"), []byte("v"))
	var ce *Error
	if !errors.As(err, &ce) || ce.Indeterminate() || a.calls.Load() != 0 {
		t.Fatalf("err = %v, puts = %d", err, a.calls.Load())
	}
}
