package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/command"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
)

// fakeMember is a follower (or leader) whose proposals always fail with err.
type fakeMember struct {
	st    raft.Status
	addrs map[uint64]string
	err   error
}

func (m *fakeMember) Propose(context.Context, []byte) (uint64, uint64, error) { return 0, 0, m.err }
func (m *fakeMember) Status() raft.Status                                       { return m.st }
func (m *fakeMember) ID() uint64                                                { return m.st.ID }
func (m *fakeMember) ClusterID() uint64                                         { return 1 }
func (m *fakeMember) PeerAddr(id uint64) string                                 { return m.addrs[id] }

type fakeReader struct{}

func (fakeReader) Get([]byte) (storage.Value, bool, error) { return storage.Value{}, false, nil }
func (fakeReader) Range(storage.RangeOptions) ([]storage.KV, bool, error) {
	return nil, false, nil
}
func (fakeReader) Applied() uint64      { return 0 }
func (fakeReader) SessionClock() uint64 { return 0 }

// fakeLeader records what a forwarded request looked like and answers with
// reply.
type fakeLeader struct {
	raftkvv1.UnimplementedKVServer
	mu       sync.Mutex
	header   *raftkvv1.RequestHeader
	session  *raftkvv1.WriteSession
	deadline time.Duration
	reply    func() (*raftkvv1.PutResponse, error)
}

func (f *fakeLeader) Put(ctx context.Context, req *raftkvv1.PutRequest) (*raftkvv1.PutResponse, error) {
	f.mu.Lock()
	f.header, f.session = req.GetHeader(), req.GetSession()
	if d, ok := ctx.Deadline(); ok {
		f.deadline = time.Until(d)
	}
	reply := f.reply
	f.mu.Unlock()
	return reply()
}

func startLeader(t *testing.T, f *fakeLeader) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	raftkvv1.RegisterKVServer(srv, f)
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(srv.Stop)
	return l.Addr().String()
}

func info(err error) (*errdetails.ErrorInfo, *raftkvv1.LeaderHint) {
	st, _ := status.FromError(err)
	var i *errdetails.ErrorInfo
	var h *raftkvv1.LeaderHint
	for _, d := range st.Details() {
		switch d := d.(type) {
		case *errdetails.ErrorInfo:
			i = d
		case *raftkvv1.LeaderHint:
			h = d
		}
	}
	return i, h
}

// WBS 4.4.1 / 4.5.2: the routing rules of api-design 7 and the error
// relaying of 6.1, including that the caller's deadline reaches the leader.
func TestRouteWrite(t *testing.T) {
	leader := &fakeLeader{}
	addr := startLeader(t, leader)
	m := &fakeMember{st: raft.Status{ID: 2, State: raft.StateFollower, Lead: 1, Term: 3}, addrs: map[uint64]string{1: addr, 9: "127.0.0.1:1"}}
	s := New(m, fakeReader{}, NewTracker())
	defer s.Close()
	req := &raftkvv1.PutRequest{Key: []byte("k"), Value: []byte("v"),
		Session: &raftkvv1.WriteSession{ClientId: 7, Sequence: 3, AckedSequence: 2}}

	// Forwarded with the same session, forwarded=true and the deadline.
	leader.reply = func() (*raftkvv1.PutResponse, error) {
		return &raftkvv1.PutResponse{Header: &raftkvv1.ResponseHeader{MemberId: 1, LeaderId: 1, Revision: 42}, Revision: 41}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := s.Put(ctx, req)
	if err != nil || resp.GetRevision() != 41 || resp.GetHeader().GetMemberId() != 2 || resp.GetHeader().GetRevision() != 42 {
		t.Fatalf("forwarded Put = %v, %v", resp, err)
	}
	if !leader.header.GetForwarded() || leader.session.GetSequence() != 3 || leader.session.GetClientId() != 7 {
		t.Fatalf("leader saw header %v session %v", leader.header, leader.session)
	}
	if leader.deadline <= 0 || leader.deadline > 3*time.Second {
		t.Fatalf("leader saw deadline %v, want the caller's remaining ~3s", leader.deadline)
	}

	type row struct {
		name     string
		setup    func()
		header   *raftkvv1.RequestHeader
		code     codes.Code
		reason   string
		outcome  string
		wantHint bool
	}
	unknown := func() (*raftkvv1.PutResponse, error) { return nil, errors.New("unused") }
	rows := []row{
		{name: "redirect", header: &raftkvv1.RequestHeader{WriteRouting: raftkvv1.WriteRouting_REDIRECT},
			code: codes.Unavailable, reason: ReasonNotLeader, outcome: Definite, wantHint: true},
		{name: "already forwarded", header: &raftkvv1.RequestHeader{Forwarded: true},
			code: codes.Unavailable, reason: ReasonNotLeader, outcome: Definite, wantHint: true},
		{name: "no leader", setup: func() { m.st.Lead = 0 },
			code: codes.Unavailable, reason: ReasonNoLeader, outcome: Definite},
		{name: "leader unreachable (nothing sent)", setup: func() { m.st.Lead = 9 },
			code: codes.Unavailable, reason: ReasonNoLeader, outcome: Definite},
		{name: "leader's labelled error is relayed", setup: func() {
			m.st.Lead = 1
			leader.reply = func() (*raftkvv1.PutResponse, error) {
				return nil, newError(codes.FailedPrecondition, ReasonSessionExpired, Definite, "gone", nil)
			}
		}, code: codes.FailedPrecondition, reason: ReasonSessionExpired, outcome: Definite},
		{name: "unlabelled failure after sending", setup: func() {
			leader.reply = func() (*raftkvv1.PutResponse, error) { return nil, status.Error(codes.Internal, "boom") }
		}, code: codes.Unavailable, reason: ReasonOutcomeUnknown, outcome: Indeterminate},
		{name: "deadline expires at the leader", setup: func() {
			leader.reply = func() (*raftkvv1.PutResponse, error) { time.Sleep(400 * time.Millisecond); return unknown() }
		}, code: codes.DeadlineExceeded, reason: ReasonOutcomeUnknown, outcome: Indeterminate},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			if r.setup != nil {
				r.setup()
			}
			q := proto.CloneOf(req)
			q.Header = r.header
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_, err := s.Put(ctx, q)
			i, h := info(err)
			if status.Code(err) != r.code || i.GetReason() != r.reason || i.GetMetadata()[OutcomeKey] != r.outcome {
				t.Fatalf("err = %v (%v), want %s/%s/%s", err, i, r.code, r.reason, r.outcome)
			}
			if r.wantHint && (h == nil || h.GetLeaderId() != m.st.Lead) {
				t.Fatalf("hint = %v", h)
			}
		})
	}
}

// WBS 4.5.2: status codes, reasons and outcomes for errors decided on this
// node (api-design 6.1), as a table.
func TestErrorMapping(t *testing.T) {
	s := New(&fakeMember{st: raft.Status{ID: 1, State: raft.StateLeader}}, fakeReader{}, NewTracker())
	defer s.Close()
	for _, tc := range []struct {
		err             error
		code            codes.Code
		reason, outcome string
	}{
		{command.ErrEmptyKey, codes.InvalidArgument, ReasonEmptyKey, Definite},
		{command.ErrKeyTooLarge, codes.InvalidArgument, ReasonKeyTooLarge, Definite},
		{command.ErrValueTooLarge, codes.InvalidArgument, ReasonValueTooLarge, Definite},
		{command.ErrMalformed, codes.InvalidArgument, ReasonMalformed, Definite},
		{command.ErrSessionExpired, codes.FailedPrecondition, ReasonSessionExpired, Definite},
		{command.ErrSequenceStale, codes.FailedPrecondition, ReasonSequenceStale, Definite},
		{command.ErrSessionWindow, codes.FailedPrecondition, ReasonSessionWindow, Definite},
	} {
		err := mapValidation(fmt.Errorf("wrapped: %w", tc.err))
		i, _ := info(err)
		if status.Code(err) != tc.code || i.GetReason() != tc.reason || i.GetMetadata()[OutcomeKey] != tc.outcome || i.GetDomain() != ErrorDomain {
			t.Errorf("%v -> %v (%v)", tc.err, err, i)
		}
	}
	for _, tc := range []struct {
		err             error
		code            codes.Code
		reason, outcome string
	}{
		{&raft.NotLeaderError{Leader: 2}, codes.Unavailable, ReasonNotLeader, Definite},
		{raft.ErrProposalDropped, codes.Unavailable, ReasonNoLeader, Definite},
		{context.DeadlineExceeded, codes.DeadlineExceeded, ReasonOutcomeUnknown, Indeterminate},
	} {
		err := s.mapProposeError(tc.err)
		i, _ := info(err)
		if status.Code(err) != tc.code || i.GetReason() != tc.reason || i.GetMetadata()[OutcomeKey] != tc.outcome {
			t.Errorf("propose %v -> %v", tc.err, err)
		}
	}
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{ErrLeaderChanged, codes.Unavailable},
		{ErrTrackerClosed, codes.Unavailable},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
		{context.Canceled, codes.Canceled},
	} {
		err := mapWaitError(tc.err)
		i, _ := info(err)
		if status.Code(err) != tc.code || i.GetReason() != ReasonOutcomeUnknown || i.GetMetadata()[OutcomeKey] != Indeterminate {
			t.Errorf("wait %v -> %v", tc.err, err)
		}
	}
	// Deadline already gone before proposing: NOT_PROPOSED, DEFINITE.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.Put(ctx, &raftkvv1.PutRequest{Key: []byte("k")})
	if i, _ := info(err); status.Code(err) != codes.DeadlineExceeded || i.GetReason() != ReasonNotProposed || i.GetMetadata()[OutcomeKey] != Definite {
		t.Fatalf("expired before propose = %v", err)
	}
}
