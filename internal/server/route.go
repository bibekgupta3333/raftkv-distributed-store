package server

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// forwardMsgBytes bounds forwarded messages like the peer listener (8 MiB).
const forwardMsgBytes = 8 << 20

// forwardConnectTimeout is how long a follower waits for a connection to the
// leader before answering NO_LEADER. Nothing has been sent at that point, so
// the answer is DEFINITE.
const forwardConnectTimeout = 500 * time.Millisecond

type headed interface {
	GetHeader() *raftkvv1.ResponseHeader
}

// routeWrite runs a write here if this member leads, and otherwise applies
// the routing rules of api-design 7 (D-28):
//
//   - FORWARD (default): send it to the leader with forwarded=true, the same
//     session and the caller's remaining deadline (gRPC carries it), then
//     relay the reply. One hop only.
//   - REDIRECT, or a request that was already forwarded: NOT_LEADER with a
//     LeaderHint (DEFINITE).
//   - No known leader: NO_LEADER (DEFINITE).
//
// A forwarded call that fails after it was sent is OUTCOME_UNKNOWN: the
// leader may have applied it. With a session the client's retry is safe.
func routeWrite[R headed](ctx context.Context, s *KV, hdr *raftkvv1.RequestHeader,
	local func(context.Context) (R, error),
	remote func(context.Context, raftkvv1.KVClient, *raftkvv1.RequestHeader) (R, error),
) (R, error) {
	var zero R
	st := s.member.Status()
	switch {
	case st.State == raft.StateLeader:
		return local(ctx)
	case st.Lead == 0:
		return zero, newError(codes.Unavailable, ReasonNoLeader, Definite, "no known leader", nil)
	case hdr.GetForwarded() || hdr.GetWriteRouting() == raftkvv1.WriteRouting_REDIRECT:
		return zero, newError(codes.Unavailable, ReasonNotLeader, Definite, "not the leader", s.hint(st.Lead))
	}
	if ctx.Err() != nil {
		return zero, newError(codes.DeadlineExceeded, ReasonNotProposed, Definite, "deadline passed before forwarding", nil)
	}
	kv, err := s.fwd.client(ctx, s.member.PeerAddr(st.Lead))
	if err != nil {
		return zero, newError(codes.Unavailable, ReasonNoLeader, Definite, "leader unreachable: "+err.Error(), nil)
	}
	fh := proto.CloneOf(hdr)
	if fh == nil {
		fh = &raftkvv1.RequestHeader{}
	}
	fh.Forwarded, fh.WriteRouting = true, raftkvv1.WriteRouting_FORWARD
	resp, err := remote(ctx, kv, fh)
	if err != nil {
		return zero, relayForwardError(ctx, err)
	}
	// The reply is the leader's (its revision and term); member_id names the
	// member the client talked to, so the client's endpoint map stays right.
	if h := resp.GetHeader(); h != nil {
		h.MemberId = s.member.ID()
	}
	return resp, nil
}

// relayForwardError passes the leader's labelled error through unchanged (its
// outcome is authoritative) and labels everything else OUTCOME_UNKNOWN.
func relayForwardError(ctx context.Context, err error) error {
	if st, ok := status.FromError(err); ok {
		for _, d := range st.Details() {
			if info, ok := d.(*errdetails.ErrorInfo); ok && info.GetDomain() == ErrorDomain {
				return err
			}
		}
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
		return newError(codes.DeadlineExceeded, ReasonOutcomeUnknown, Indeterminate, "forwarded write timed out: "+err.Error(), nil)
	}
	return newError(codes.Unavailable, ReasonOutcomeUnknown, Indeterminate, "forwarded write failed: "+err.Error(), nil)
}

// forwarder keeps one client connection per leader address.
type forwarder struct {
	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	closed bool
}

func newForwarder() *forwarder { return &forwarder{conns: map[string]*grpc.ClientConn{}} }

var errForwarderClosed = errors.New("server: shutting down")

// client returns a KV client for addr once its connection is ready. A
// connection that is not ready cannot have carried the request.
func (f *forwarder) client(ctx context.Context, addr string) (raftkvv1.KVClient, error) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil, errForwarderClosed
	}
	conn := f.conns[addr]
	if conn == nil {
		var err error
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(forwardMsgBytes), grpc.MaxCallSendMsgSize(forwardMsgBytes)))
		if err != nil {
			f.mu.Unlock()
			return nil, err
		}
		f.conns[addr] = conn
	}
	f.mu.Unlock()

	state := conn.GetState()
	if state == connectivity.TransientFailure {
		conn.ResetConnectBackoff()
	}
	conn.Connect()
	wctx, cancel := context.WithTimeout(ctx, forwardConnectTimeout)
	defer cancel()
	for state = conn.GetState(); state != connectivity.Ready; state = conn.GetState() {
		if state == connectivity.Shutdown || !conn.WaitForStateChange(wctx, state) {
			return nil, errors.New("no connection to " + addr)
		}
	}
	return raftkvv1.NewKVClient(conn), nil
}

func (f *forwarder) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	for addr, c := range f.conns {
		_ = c.Close()
		delete(f.conns, addr)
	}
}
