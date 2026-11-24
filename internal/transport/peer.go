package transport

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// peer is the outbound side for one remote node: a bounded send queue, one
// Replicate stream at a time (D-12), reconnect with backoff and jitter
// (WBS 2.2.4), and the in-flight cap (D-29, WBS 2.2.3).
type peer struct {
	t     *Transport
	id    uint64
	queue chan raft.Message

	// inflight counts entry-bearing MsgApp written but not yet answered.
	inflight atomic.Int64

	connMu sync.Mutex
	conn   *grpc.ClientConn

	// cancel stops run (set when the transport starts the peer).
	cancel context.CancelFunc
}

// stop ends the peer's sender goroutine and closes its connection.
func (p *peer) stop() {
	if p.cancel != nil {
		p.cancel()
	}
	p.closeConn()
}

func newPeer(t *Transport, id uint64) *peer {
	return &peer{t: t, id: id, queue: make(chan raft.Message, t.cfg.QueueSize)}
}

func (p *peer) enqueue(m raft.Message) {
	select {
	case p.queue <- m:
	default:
		p.t.cfg.Observer.Dropped(p.id) // Raft retransmits (docs/architecture.md 5)
	}
}

// getConn returns the shared client connection, dialing (and re-resolving the
// peer's DNS name, WBS 2.2.6) if there is none.
func (p *peer) getConn(ctx context.Context) (*grpc.ClientConn, error) {
	p.connMu.Lock()
	defer p.connMu.Unlock()
	if p.conn != nil {
		return p.conn, nil
	}
	addr, err := p.t.cfg.Peers.Resolve(ctx, p.id)
	if err != nil {
		return nil, err
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(p.t.cfg.MaxMsgBytes),
			grpc.MaxCallSendMsgSize(p.t.cfg.MaxMsgBytes),
		),
	}
	opts = append(opts, p.t.cfg.DialOptions...)
	// passthrough: Resolve already did the lookup, and the next reconnect
	// calls Resolve again, so we never cache a stale address.
	conn, err := grpc.NewClient("passthrough:///"+addr, opts...)
	if err != nil {
		return nil, err
	}
	p.conn = conn
	return conn, nil
}

// closeConn drops the connection so the next getConn re-resolves.
func (p *peer) closeConn() {
	p.connMu.Lock()
	defer p.connMu.Unlock()
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
}

// run keeps a Replicate stream open until ctx ends.
func (p *peer) run(ctx context.Context) {
	cfg := &p.t.cfg
	backoff := cfg.BackoffMin
	first := true
	for ctx.Err() == nil {
		if !first {
			cfg.Observer.Reconnect(p.id)
		}
		first = false
		established, err := p.session(ctx)
		if ctx.Err() != nil {
			return
		}
		p.closeConn() // force a fresh DNS lookup and dial
		cfg.Observer.SendFailure(p.id)
		cfg.OnUnreachable(p.id)
		cfg.Logger.Debug("replicate stream down", "component", "transport", "peer", p.id, "err", err)
		if established {
			backoff = cfg.BackoffMin
		}
		wait := jitter(backoff)
		backoff = min(backoff*2, cfg.BackoffMax)
		timer := time.NewTimer(wait)
		p.drainUntil(ctx, timer.C)
		timer.Stop()
	}
}

// jitter returns a duration in [d/2, d).
func jitter(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(half)
}

// drainUntil discards queued messages while the link is down (Raft resends
// what matters) until the backoff timer fires.
func (p *peer) drainUntil(ctx context.Context, timer <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer:
			return
		case <-p.queue:
			p.t.cfg.Observer.Dropped(p.id)
		}
	}
}

// session opens one stream and pumps messages until it breaks. established
// reports whether the stream was opened at all.
func (p *peer) session(ctx context.Context) (established bool, err error) {
	conn, err := p.getConn(ctx)
	if err != nil {
		return false, err
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := raftkvv1.NewRaftClient(conn).Replicate(p.t.withIncarnation(sctx))
	if err != nil {
		return false, err
	}
	p.inflight.Store(0)

	recvErr := make(chan error, 1)
	go func() { recvErr <- p.recvLoop(sctx, stream) }()

	for {
		select {
		case <-sctx.Done():
			return true, sctx.Err()
		case err := <-recvErr:
			return true, err
		case m := <-p.queue:
			if err := p.write(stream, m); err != nil {
				cancel()
				rerr := <-recvErr // wait for the receiver before returning
				_ = rerr
				return true, err
			}
		}
	}
}

func (p *peer) write(stream raftkvv1.Raft_ReplicateClient, m raft.Message) error {
	cfg := &p.t.cfg
	withEntries := m.Type == raft.MsgApp && len(m.Entries) > 0
	if withEntries && p.inflight.Load() >= int64(cfg.MaxInflight) {
		cfg.Observer.Dropped(p.id) // D-29: never exceed the in-flight cap
		return nil
	}
	if err := stream.Send(AppendRequestToProto(cfg.ClusterID, m)); err != nil {
		return err
	}
	if withEntries {
		p.inflight.Add(1)
	}
	cfg.Observer.MessageSent(p.id, m.Type)
	return nil
}

// recvLoop turns follower replies into inbound messages and releases in-flight
// slots. A heartbeat reply proves the stream is alive and, because replies are
// ordered, that every earlier append was handled, so it resets the counter; an
// append the follower ignored would otherwise leak a slot forever.
func (p *peer) recvLoop(ctx context.Context, stream raftkvv1.Raft_ReplicateClient) error {
	// The follower sends its data-directory incarnation in the stream
	// header once it accepted the first request (D-32).
	if md, err := stream.Header(); err == nil {
		if err := p.t.observeIncarnation(p.id, parseIncarnation(md)); err != nil {
			return err
		}
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		m := AppendResponseFromProto(resp, p.t.cfg.ID)
		if m.From != p.id {
			continue // a peer may only speak for itself
		}
		if m.Type == raft.MsgHeartbeatResp {
			p.inflight.Store(0)
		} else if p.inflight.Load() > 0 {
			p.inflight.Add(-1)
		}
		if err := p.t.cfg.Handler.Step(ctx, m); err != nil {
			return err
		}
	}
}

// sendVote sends a (pre-)vote request on its own goroutine so a slow vote never
// stalls the append stream, and feeds the response back as an inbound message.
func (p *peer) sendVote(m raft.Message) {
	cfg := &p.t.cfg
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.VoteTimeout)
		defer cancel()
		conn, err := p.getConn(ctx)
		if err == nil {
			var (
				resp *raftkvv1.RequestVoteResponse
				hdr  metadata.MD
			)
			resp, err = raftkvv1.NewRaftClient(conn).RequestVote(p.t.withIncarnation(ctx),
				VoteRequestToProto(cfg.ClusterID, m), grpc.Header(&hdr))
			if err == nil {
				// D-32: a vote from a member that lost its disk does not
				// count; it may have voted already in this term.
				err = p.t.observeIncarnation(p.id, parseIncarnation(hdr))
			}
			if err == nil {
				cfg.Observer.MessageSent(p.id, m.Type)
				if r := VoteResponseFromProto(resp, cfg.ID); r.From == p.id {
					_ = cfg.Handler.Step(ctx, r)
				}
				return
			}
		}
		cfg.Observer.SendFailure(p.id)
		cfg.OnUnreachable(p.id)
	}()
}

// sendReadIndex asks this peer (the leader) for a read index on its own
// goroutine (D-05, D-36) and feeds the answer back as a MsgReadIndexResp. Any
// failure (refused because the peer is not the leader, timeout, unreachable)
// comes back as a rejected response, so the requester retries at once
// instead of waiting for its deadline.
func (p *peer) sendReadIndex(m raft.Message) {
	cfg := &p.t.cfg
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ReadIndexTimeout)
		defer cancel()
		resp, err := p.readIndex(ctx, m)
		if err != nil {
			cfg.Observer.SendFailure(p.id)
			resp = raft.Message{Type: raft.MsgReadIndexResp, To: cfg.ID, From: p.id, Context: m.Context, Reject: true}
		}
		sctx, scancel := context.WithTimeout(context.Background(), cfg.ReadIndexTimeout)
		defer scancel()
		_ = cfg.Handler.Step(sctx, resp)
	}()
}

func (p *peer) readIndex(ctx context.Context, m raft.Message) (raft.Message, error) {
	cfg := &p.t.cfg
	conn, err := p.getConn(ctx)
	if err != nil {
		return raft.Message{}, err
	}
	resp, err := raftkvv1.NewRaftClient(conn).ReadIndex(p.t.withIncarnation(ctx), ReadIndexRequestToProto(cfg.ClusterID, m))
	if err != nil {
		return raft.Message{}, err
	}
	cfg.Observer.MessageSent(p.id, m.Type)
	return ReadIndexResponseFromProto(resp, p.id, cfg.ID), nil
}
