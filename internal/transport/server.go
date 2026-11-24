package transport

import (
	"context"
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// raftServer is the server side of the Raft peer service (WBS 2.2.1).
// Replicate and RequestVote exist since M2, ReadIndex since M4,
// InstallSnapshot since M5 and TimeoutNow since M6 (D-36).
type raftServer struct {
	raftkvv1.UnimplementedRaftServer
	t *Transport
}

// inboundStream is a Replicate stream opened by a leader. Responses for that
// leader are queued on out and written by the stream's handler goroutine.
type inboundStream struct {
	out    chan raft.Message
	cancel context.CancelFunc
}

type recvResult struct {
	req *raftkvv1.AppendEntriesRequest
	err error
}

// Replicate implements the follower side of the long-lived append stream
// (D-12). The first request identifies the leader; later requests must come
// from the same leader. A sender may open the stream long before it has
// anything to say, so the handler must stay interruptible while it waits for
// the first request (Close and a superseding stream cancel ctx).
func (s *raftServer) Replicate(stream raftkvv1.Raft_ReplicateServer) error {
	t := s.t
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	go func() {
		select {
		case <-t.closing:
			cancel()
		case <-ctx.Done():
		}
	}()

	// Receive in a goroutine so the handler can return even while Recv blocks.
	recv := make(chan recvResult)
	go func() {
		for {
			req, err := stream.Recv()
			select {
			case recv <- recvResult{req, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	var (
		leader uint64
		in     *inboundStream
		wg     sync.WaitGroup
	)
	defer func() {
		cancel()
		wg.Wait() // no Send may outlive the handler
		if in != nil {
			t.mu.Lock()
			if t.inbound[leader] == in {
				delete(t.inbound, leader)
			}
			t.mu.Unlock()
		}
	}()

	for {
		var req *raftkvv1.AppendEntriesRequest
		select {
		case <-ctx.Done():
			return nil
		case r := <-recv:
			if r.err != nil {
				if errors.Is(r.err, io.EOF) {
					return nil
				}
				return r.err
			}
			req = r.req
		}
		if in == nil { // first request: authenticate the leader and register
			leader = req.GetLeaderId()
			if err := t.checkSender(ctx, req.GetClusterId(), leader); err != nil {
				return refusedStatus(err) // D-31, D-32: removed or wiped senders are refused
			}
			if t.cfg.Incarnation != 0 {
				_ = stream.SendHeader(t.incarnationMD())
			}
			in = &inboundStream{out: make(chan raft.Message, t.cfg.QueueSize), cancel: cancel}
			t.mu.Lock()
			if t.closed {
				t.mu.Unlock()
				return status.Error(codes.Unavailable, "transport closed")
			}
			old := t.inbound[leader]
			t.inbound[leader] = in
			t.mu.Unlock()
			if old != nil {
				old.cancel() // a leader that reconnects supersedes its previous stream
			}
			wg.Add(1)
			go func() { // single writer: responses leave in the order the core made them
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case m := <-in.out:
						if err := stream.Send(AppendResponseToProto(m)); err != nil {
							cancel()
							return
						}
						t.cfg.Observer.MessageSent(leader, m.Type)
					}
				}
			}()
		} else if req.GetLeaderId() != leader {
			return status.Error(codes.InvalidArgument, "leader id changed on an open stream")
		}
		if err := t.cfg.Handler.Step(ctx, AppendRequestFromProto(req, t.cfg.ID)); err != nil {
			return status.FromContextError(err).Err()
		}
	}
}

// RequestVote implements the vote and pre-vote RPC (D-36). The core answers
// through Ready.Messages; the response is matched back to this call by
// (candidate, pre_vote). The core may legitimately not answer (a follower with
// a live leader ignores votes, D-08); the call then times out and the candidate
// treats it as no vote.
func (s *raftServer) RequestVote(ctx context.Context, req *raftkvv1.RequestVoteRequest) (*raftkvv1.RequestVoteResponse, error) {
	t := s.t
	if err := t.checkSender(ctx, req.GetClusterId(), req.GetCandidateId()); err != nil {
		return nil, refusedStatus(err)
	}
	t.setIncarnationHeader(ctx)
	k := voteKey{candidate: req.GetCandidateId(), pre: req.GetPreVote()}
	ch, done := t.registerVote(k)
	defer done()
	ctx, cancel := context.WithTimeout(ctx, t.cfg.VoteTimeout)
	defer cancel()
	if err := t.cfg.Handler.Step(ctx, VoteRequestFromProto(req, t.cfg.ID)); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	select {
	case m := <-ch:
		t.cfg.Observer.MessageSent(req.GetCandidateId(), m.Type)
		return VoteResponseToProto(m), nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

// ReadIndex implements the follower-to-leader ReadIndex RPC (D-05, D-36). The
// core answers through Ready.Messages with a MsgReadIndexResp, matched back to
// this call by (follower, request context). A rejection (this node is not the
// leader, or lost leadership before confirming) is returned as UNAVAILABLE; if
// the leader cannot confirm within the timeout (no quorum) the call fails with
// DEADLINE_EXCEEDED. Either way the follower retries.
func (s *raftServer) ReadIndex(ctx context.Context, req *raftkvv1.ReadIndexRequest) (*raftkvv1.ReadIndexResponse, error) {
	t := s.t
	if err := t.checkSender(ctx, req.GetClusterId(), req.GetFrom()); err != nil {
		return nil, refusedStatus(err)
	}
	t.setIncarnationHeader(ctx)
	ch, done := t.registerReadIndex(readKey{from: req.GetFrom(), ctx: string(req.GetRequestCtx())})
	defer done()
	ctx, cancel := context.WithTimeout(ctx, t.cfg.ReadIndexTimeout)
	defer cancel()
	if err := t.cfg.Handler.Step(ctx, ReadIndexRequestFromProto(req, t.cfg.ID)); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	select {
	case m := <-ch:
		if m.Reject {
			return nil, status.Error(codes.Unavailable, "not the leader")
		}
		t.cfg.Observer.MessageSent(req.GetFrom(), m.Type)
		return ReadIndexResponseToProto(m), nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}
