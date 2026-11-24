package transport

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Dynamic membership (D-06, D-31, D-32).
//
// Peers come and go with the configuration: the driver calls [Transport.AddPeer]
// when a member is added (or its address learned) and [Transport.RemovePeer]
// once a removal is committed; from then on the removed ID is refused, and so
// are its messages (D-31).
//
// Every node also stamps its data directory's incarnation (a random number
// written once, when the directory is created) on every request and stream it
// opens, and on every answer it gives. A peer whose incarnation changes while
// this process runs has come back with an empty data directory under its old
// ID: it may have forgotten a vote or acknowledged entries, so D-32 forbids it
// to rejoin under that ID. The transport refuses it from then on, in both
// directions, and tells the driver; the operator removes it and adds the
// machine back as a new learner with a new ID.

// incarnationKey is the gRPC metadata key carrying the sender's data-directory
// incarnation.
const incarnationKey = "raftkv-incarnation"

// ErrRefused is wrapped by the errors for senders this node will not talk to:
// removed members (D-31) and members whose data directory was wiped (D-32).
var ErrRefused = errors.New("transport: peer refused")

func (t *Transport) incarnationMD() metadata.MD {
	return metadata.Pairs(incarnationKey, strconv.FormatUint(t.cfg.Incarnation, 10))
}

// withIncarnation adds this node's incarnation to an outgoing context.
func (t *Transport) withIncarnation(ctx context.Context) context.Context {
	if t.cfg.Incarnation == 0 {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, incarnationKey, strconv.FormatUint(t.cfg.Incarnation, 10))
}

// setIncarnationHeader sends this node's incarnation in a unary answer's header.
func (t *Transport) setIncarnationHeader(ctx context.Context) {
	if t.cfg.Incarnation != 0 {
		_ = grpc.SetHeader(ctx, t.incarnationMD())
	}
}

func parseIncarnation(md metadata.MD) uint64 {
	v := md.Get(incarnationKey)
	if len(v) == 0 {
		return 0
	}
	n, _ := strconv.ParseUint(v[0], 10, 64)
	return n
}

// observeIncarnation records peer id's data-directory incarnation and refuses
// the peer if it changed (D-32). An unknown (0) incarnation is not checked.
func (t *Transport) observeIncarnation(id, inc uint64) error {
	t.mu.Lock()
	if t.refused[id] != "" {
		reason := t.refused[id]
		t.mu.Unlock()
		return fmt.Errorf("%w: node %d %s", ErrRefused, id, reason)
	}
	if inc == 0 {
		t.mu.Unlock()
		return nil
	}
	known := t.incarnations[id]
	if known == 0 || known == inc {
		t.incarnations[id] = inc
		t.mu.Unlock()
		return nil
	}
	reason := "came back with an empty data directory under its old ID (D-32): remove it and add it again as a new learner"
	t.refused[id] = reason
	t.mu.Unlock()
	t.cfg.Logger.Error("refusing a wiped member", "component", "transport", "peer", id,
		"old_incarnation", known, "new_incarnation", inc)
	if t.cfg.OnPeerWiped != nil {
		go t.cfg.OnPeerWiped(id)
	}
	return fmt.Errorf("%w: node %d %s", ErrRefused, id, reason)
}

// isRefused reports whether messages to or from id are dropped.
func (t *Transport) isRefused(id uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.refused[id] != ""
}

// WipedPeers returns the members refused because their data directory was
// wiped (D-32), for status reporting.
func (t *Transport) WipedPeers() []uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []uint64
	for id, r := range t.refused {
		if !t.removed[id] && r != "" {
			out = append(out, id)
		}
	}
	return out
}

// AddPeer starts (or re-addresses) the outbound side for member id at addr
// (D-06: a learner added by AddLearner, or a member learned from the log). A
// removed ID is never re-added (D-38).
func (t *Transport) AddPeer(id uint64, addr string) error {
	if id == t.cfg.ID {
		return t.cfg.Peers.Set(id, addr)
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.removed[id] || t.closed {
		return nil
	}
	old, _ := t.cfg.Peers.Addr(id)
	if err := t.cfg.Peers.Set(id, addr); err != nil {
		return err
	}
	if p := t.peers[id]; p != nil {
		if old != addr {
			p.closeConn() // the next send dials the new address
		}
		return nil
	}
	p := newPeer(t, id)
	t.peers[id] = p
	if t.started {
		t.startPeer(p)
	}
	return nil
}

// RemovePeer stops talking to member id for good: its removal is committed
// (D-31). Its messages are refused from now on.
func (t *Transport) RemovePeer(id uint64) {
	if id == t.cfg.ID {
		return
	}
	t.mu.Lock()
	t.removed[id] = true
	t.refused[id] = "was removed from the cluster (D-31)"
	p := t.peers[id]
	delete(t.peers, id)
	in := t.inbound[id]
	t.mu.Unlock()
	if p != nil {
		p.stop()
	}
	if in != nil {
		in.cancel()
	}
}

// startPeer launches p's sender goroutine. t.mu must be held and t started.
func (t *Transport) startPeer(p *peer) {
	ctx, cancel := context.WithCancel(t.baseCtx)
	p.cancel = cancel
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		p.run(ctx)
	}()
}

// isTransferVote reports whether m is a vote ordered by MsgTimeoutNow.
func isTransferVote(m raft.Message) bool {
	return m.Type == raft.MsgVote && string(m.Context) == string(raft.TransferContext)
}

// IsTimeoutNow reports whether m is carried by the TimeoutNow RPC.
func IsTimeoutNow(t raft.MessageType) bool { return t == raft.MsgTimeoutNow }

// TimeoutNowToProto converts a MsgTimeoutNow (system-design 7.3).
func TimeoutNowToProto(clusterID uint64, m raft.Message) *raftkvv1.TimeoutNowRequest {
	return &raftkvv1.TimeoutNowRequest{ClusterId: clusterID, Term: m.Term, LeaderId: m.From}
}

// TimeoutNowFromProto converts a wire TimeoutNow received by node `to`.
func TimeoutNowFromProto(r *raftkvv1.TimeoutNowRequest, to uint64) raft.Message {
	return raft.Message{Type: raft.MsgTimeoutNow, To: to, From: r.GetLeaderId(), Term: r.GetTerm()}
}

// sendTimeoutNow delivers a MsgTimeoutNow on its own goroutine.
func (p *peer) sendTimeoutNow(m raft.Message) {
	cfg := &p.t.cfg
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.VoteTimeout)
		defer cancel()
		conn, err := p.getConn(ctx)
		if err == nil {
			_, err = raftkvv1.NewRaftClient(conn).TimeoutNow(p.t.withIncarnation(ctx), TimeoutNowToProto(cfg.ClusterID, m))
		}
		if err != nil {
			cfg.Observer.SendFailure(p.id)
			cfg.Logger.Warn("TimeoutNow failed", "component", "transport", "peer", p.id, "err", err)
			return
		}
		cfg.Observer.MessageSent(p.id, m.Type)
	}()
}

// TimeoutNow implements the leadership-transfer RPC (system-design 7.3): the
// leader orders this caught-up voter to campaign at once.
func (s *raftServer) TimeoutNow(ctx context.Context, req *raftkvv1.TimeoutNowRequest) (*raftkvv1.TimeoutNowResponse, error) {
	t := s.t
	if err := t.checkSender(ctx, req.GetClusterId(), req.GetLeaderId()); err != nil {
		return nil, refusedStatus(err)
	}
	t.setIncarnationHeader(ctx)
	if err := t.cfg.Handler.Step(ctx, TimeoutNowFromProto(req, t.cfg.ID)); err != nil {
		return nil, statusFromContext(err)
	}
	return &raftkvv1.TimeoutNowResponse{Term: req.GetTerm(), Accepted: true}, nil
}
