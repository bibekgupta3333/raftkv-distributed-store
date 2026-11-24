package transport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Defaults for [Config].
const (
	// DefaultMaxMsgBytes is the gRPC message-size limit in both directions. It
	// matches the WAL's 8 MiB record cap; the core batches at most about 1 MiB
	// of entries per MsgApp (D-11, D-29).
	DefaultMaxMsgBytes = 8 << 20
	// DefaultQueueSize bounds the per-peer outbound queue.
	DefaultQueueSize = 4096
	// DefaultMaxInflight is the per-follower in-flight cap (D-29).
	DefaultMaxInflight = 256
	// DefaultVoteTimeout bounds one RequestVote round trip.
	DefaultVoteTimeout = time.Second
	// DefaultReadIndexTimeout bounds one ReadIndex round trip. The leader
	// answers within a heartbeat round; a longer wait means it cannot reach a
	// quorum, and the follower should retry (maybe at a new leader).
	DefaultReadIndexTimeout = time.Second
	// DefaultBackoffMin and DefaultBackoffMax bound reconnect backoff.
	DefaultBackoffMin = 50 * time.Millisecond
	DefaultBackoffMax = 2 * time.Second
)

// Handler receives inbound peer messages. Step must hand m to the driver and
// return promptly (it may block only while the driver's inbox is full, and must
// return when ctx is done). It must never call the Raft core directly.
type Handler interface {
	Step(ctx context.Context, m raft.Message) error
}

// HandlerFunc adapts a function to [Handler].
type HandlerFunc func(ctx context.Context, m raft.Message) error

// Step implements [Handler].
func (f HandlerFunc) Step(ctx context.Context, m raft.Message) error { return f(ctx, m) }

// Observer receives transport events for metrics (D-17, WBS 2.4.3). All methods
// must be cheap and safe for concurrent use.
type Observer interface {
	// MessageSent counts a message written to a peer (or, for responses, to
	// the inbound stream it answers).
	MessageSent(peer uint64, t raft.MessageType)
	// SendFailure counts a failed stream or RPC to peer.
	SendFailure(peer uint64)
	// Reconnect counts an attempt to re-establish the stream to peer.
	Reconnect(peer uint64)
	// Dropped counts a message discarded because of a full queue, the
	// in-flight cap, or a dead link.
	Dropped(peer uint64)
}

type nopObserver struct{}

func (nopObserver) MessageSent(uint64, raft.MessageType) {}
func (nopObserver) SendFailure(uint64)                   {}
func (nopObserver) Reconnect(uint64)                     {}
func (nopObserver) Dropped(uint64)                       {}

// Config configures a [Transport]. ID, Peers and Handler are required.
type Config struct {
	// ID is this node's ID; ClusterID is stamped on every request and, when
	// non-zero, checked on every inbound one.
	ID, ClusterID uint64
	// Peers is the address book (WBS 2.2.6): the bootstrap members, then
	// whatever [Transport.AddPeer] adds (D-06).
	Peers   *AddressBook
	Handler Handler
	Logger  *slog.Logger
	// Observer receives metrics events; nil means none.
	Observer Observer
	// OnUnreachable is called (from a transport goroutine, never blocking
	// long) when the link to a peer fails, so the driver can report it to the
	// core. nil means ignore.
	OnUnreachable func(peer uint64)

	MaxMsgBytes int // 0: DefaultMaxMsgBytes
	QueueSize   int // 0: DefaultQueueSize
	MaxInflight int // 0: DefaultMaxInflight
	VoteTimeout time.Duration
	// ReadIndexTimeout bounds one ReadIndex round trip to the leader (D-05).
	ReadIndexTimeout time.Duration
	// Snapshots sends and receives InstallSnapshot streams (D-14); nil
	// means this node cannot send or accept snapshots.
	Snapshots SnapshotHandler
	// SnapshotTimeout bounds one outgoing snapshot transfer.
	SnapshotTimeout time.Duration
	BackoffMin      time.Duration
	BackoffMax      time.Duration
	// Incarnation is this node's data-directory incarnation, sent with
	// every request and answer so peers can tell a wiped member (D-32). 0
	// sends none.
	Incarnation uint64
	// OnPeerWiped is called (on its own goroutine) when a peer comes back
	// with a different incarnation: its data directory was wiped (D-32).
	OnPeerWiped func(peer uint64)
	// DialOptions are extra grpc dial options (credentials, for example).
	// Without any, the transport uses insecure credentials: mTLS is out of
	// scope for this learning project (removed D-15).
	DialOptions []grpc.DialOption
}

func (c *Config) fill() error {
	switch {
	case c.ID == 0:
		return errors.New("transport: config: ID must be non-zero")
	case c.Peers == nil:
		return errors.New("transport: config: Peers must be set")
	case c.Handler == nil:
		return errors.New("transport: config: Handler is required")
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.DiscardHandler)
	}
	if c.Observer == nil {
		c.Observer = nopObserver{}
	}
	if c.OnUnreachable == nil {
		c.OnUnreachable = func(uint64) {}
	}
	if c.MaxMsgBytes <= 0 {
		c.MaxMsgBytes = DefaultMaxMsgBytes
	}
	if c.QueueSize <= 0 {
		c.QueueSize = DefaultQueueSize
	}
	if c.MaxInflight <= 0 {
		c.MaxInflight = DefaultMaxInflight
	}
	if c.VoteTimeout <= 0 {
		c.VoteTimeout = DefaultVoteTimeout
	}
	if c.ReadIndexTimeout <= 0 {
		c.ReadIndexTimeout = DefaultReadIndexTimeout
	}
	if c.SnapshotTimeout <= 0 {
		c.SnapshotTimeout = DefaultSnapshotTimeout
	}
	if c.BackoffMin <= 0 {
		c.BackoffMin = DefaultBackoffMin
	}
	if c.BackoffMax < c.BackoffMin {
		c.BackoffMax = max(DefaultBackoffMax, c.BackoffMin)
	}
	return nil
}

// Transport owns the outbound peers and the inbound Raft gRPC service of one
// node.
type Transport struct {
	cfg   Config
	peers map[uint64]*peer

	mu      sync.Mutex
	inbound map[uint64]*inboundStream
	votes   map[voteKey]chan raft.Message
	reads   map[readKey]chan raft.Message
	// D-31, D-32: data-directory incarnation seen per peer, peers refused
	// (with the reason) and peers removed from the configuration.
	incarnations map[uint64]uint64
	refused      map[uint64]string
	removed      map[uint64]bool
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	started      bool
	closed       bool
	baseCtx      context.Context
	closing      chan struct{} // closed by Close; unblocks idle inbound streams
}

// New validates cfg and builds a Transport. Call [Transport.Start] to begin
// dialing peers and register the service with a gRPC server.
func New(cfg Config) (*Transport, error) {
	if err := cfg.fill(); err != nil {
		return nil, err
	}
	t := &Transport{
		cfg:     cfg,
		peers:   make(map[uint64]*peer),
		inbound: make(map[uint64]*inboundStream),
		votes:   make(map[voteKey]chan raft.Message),
		reads:   make(map[readKey]chan raft.Message),
		closing: make(chan struct{}),

		incarnations: make(map[uint64]uint64),
		refused:      make(map[uint64]string),
		removed:      make(map[uint64]bool),
	}
	for _, id := range cfg.Peers.IDs() {
		if id != cfg.ID {
			t.peers[id] = newPeer(t, id)
		}
	}
	return t, nil
}

// NewGRPCServer returns a gRPC server with the transport's message-size limits
// and the Raft service registered (WBS 2.2.1). Extra options are appended.
func (t *Transport) NewGRPCServer(extra ...grpc.ServerOption) *grpc.Server {
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(t.cfg.MaxMsgBytes),
		grpc.MaxSendMsgSize(t.cfg.MaxMsgBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	}
	srv := grpc.NewServer(append(opts, extra...)...)
	raftkvv1.RegisterRaftServer(srv, &raftServer{t: t})
	return srv
}

// Start launches one sender goroutine per peer. ctx bounds their lifetime;
// [Transport.Close] also stops them.
func (t *Transport) Start(ctx context.Context) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.started || t.closed {
		return
	}
	t.started = true
	ctx, t.cancel = context.WithCancel(ctx)
	t.baseCtx = ctx
	for _, p := range t.peers {
		t.startPeer(p)
	}
}

// Close stops all peers, closes their connections and waits for the sender
// goroutines. Inbound handlers end when the gRPC server stops.
func (t *Transport) Close() {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	close(t.closing)
	cancel := t.cancel
	for _, in := range t.inbound {
		in.cancel()
	}
	t.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	t.wg.Wait()
	for _, p := range t.peers {
		p.closeConn()
	}
}

// Send routes messages produced by one Ready. The driver calls it only after
// the Ready's entries and HardState are durable (D-35). It never blocks: a full
// queue drops the message and Raft retries.
func (t *Transport) Send(msgs []raft.Message) {
	for _, m := range msgs {
		if m.To == t.cfg.ID || t.isRefused(m.To) {
			continue // D-31, D-32: nothing goes to a removed or wiped member
		}
		t.mu.Lock()
		p := t.peers[m.To]
		t.mu.Unlock()
		switch {
		case IsAppendRequest(m.Type):
			if p != nil {
				p.enqueue(m)
			}
		case IsVoteRequest(m.Type):
			if p != nil {
				p.sendVote(m)
			}
		case IsAppendResponse(m.Type):
			t.sendOnInbound(m)
		case IsVoteResponse(m.Type):
			t.completeVote(m)
		case IsReadIndexRequest(m.Type):
			if p != nil {
				p.sendReadIndex(m)
			}
		case IsReadIndexResponse(m.Type):
			t.completeReadIndex(m)
		case m.Type == raft.MsgSnap:
			if p != nil {
				p.sendSnapshot(m)
			}
		case IsTimeoutNow(m.Type):
			if p != nil {
				p.sendTimeoutNow(m)
			}
		}
	}
}

// sendOnInbound queues a response on the inbound Replicate stream opened by
// the leader m.To. No stream means the leader is gone; drop (Raft retries).
func (t *Transport) sendOnInbound(m raft.Message) {
	t.mu.Lock()
	in := t.inbound[m.To]
	t.mu.Unlock()
	if in == nil {
		t.cfg.Observer.Dropped(m.To)
		return
	}
	select {
	case in.out <- m:
	default:
		t.cfg.Observer.Dropped(m.To)
	}
}

type voteKey struct {
	candidate uint64
	pre       bool
}

func (t *Transport) completeVote(m raft.Message) {
	k := voteKey{candidate: m.To, pre: m.Type == raft.MsgPreVoteResp}
	t.mu.Lock()
	ch := t.votes[k]
	t.mu.Unlock()
	if ch == nil {
		t.cfg.Observer.Dropped(m.To)
		return
	}
	select {
	case ch <- m:
	default:
	}
}

// registerVote makes a waiter for the response to a vote request from
// candidate. A newer request from the same candidate replaces the older waiter.
func (t *Transport) registerVote(k voteKey) (<-chan raft.Message, func()) {
	ch := make(chan raft.Message, 1)
	t.mu.Lock()
	t.votes[k] = ch
	t.mu.Unlock()
	return ch, func() {
		t.mu.Lock()
		if t.votes[k] == ch {
			delete(t.votes, k)
		}
		t.mu.Unlock()
	}
}

// readKey matches a ReadIndex answer to the RPC that asked: the follower and
// its request context (unique among that follower's requests in flight).
type readKey struct {
	from uint64
	ctx  string
}

func (t *Transport) completeReadIndex(m raft.Message) {
	k := readKey{from: m.To, ctx: string(m.Context)}
	t.mu.Lock()
	ch := t.reads[k]
	t.mu.Unlock()
	if ch == nil {
		t.cfg.Observer.Dropped(m.To) // the RPC already gave up
		return
	}
	select {
	case ch <- m:
	default:
	}
}

func (t *Transport) registerReadIndex(k readKey) (<-chan raft.Message, func()) {
	ch := make(chan raft.Message, 1)
	t.mu.Lock()
	t.reads[k] = ch
	t.mu.Unlock()
	return ch, func() {
		t.mu.Lock()
		if t.reads[k] == ch {
			delete(t.reads, k)
		}
		t.mu.Unlock()
	}
}

// checkSender authenticates an inbound request: right cluster, not ourselves,
// not a removed member (D-31), and the same data-directory incarnation as
// before (D-32). Members this node has not heard of yet are accepted: a
// learner learns its peers from the log, and the core decides what a message
// from a stranger may do.
func (t *Transport) checkSender(ctx context.Context, cluster, from uint64) error {
	if t.cfg.ClusterID != 0 && cluster != t.cfg.ClusterID {
		return fmt.Errorf("cluster id mismatch: got %d, want %d", cluster, t.cfg.ClusterID)
	}
	if from == 0 || from == t.cfg.ID {
		return fmt.Errorf("bad sender %d", from)
	}
	md, _ := metadata.FromIncomingContext(ctx)
	return t.observeIncarnation(from, parseIncarnation(md))
}

// refusedStatus is the gRPC status for a request checkSender rejected.
func refusedStatus(err error) error { return status.Error(codes.PermissionDenied, err.Error()) }

// statusFromContext turns a context error into its gRPC status.
func statusFromContext(err error) error { return status.FromContextError(err).Err() }
