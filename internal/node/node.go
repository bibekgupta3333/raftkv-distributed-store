package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/lease"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/observability"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/snapshot"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/transport"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/wal"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/walstore"
)

// Errors returned by proposals.
var (
	// ErrStopped means the node is shutting down.
	ErrStopped = errors.New("node: stopped")
	// ErrProposalLost means the entry at the proposed index was replaced by a
	// different leader's entry before it committed. The command did not run;
	// retrying is safe only for idempotent commands (sessions arrive in M4, D-09).
	ErrProposalLost = errors.New("node: proposal replaced by a different entry")
)

const (
	inboxSize = 4096
	// maxBatch bounds how many queued events one loop iteration absorbs before
	// handling a Ready, so one fsync covers many entries (WAL 1.5) without
	// starving ticks.
	maxBatch = 256
)

// Options are the node's injectable dependencies. The zero value is a normal
// production node: it listens on the configured addresses, logs JSON to stderr
// and applies into memory.
type Options struct {
	// Listener and MetricsListener replace listening on the configured
	// addresses; tests use them to get free ports.
	Listener        net.Listener
	MetricsListener net.Listener
	// Applier defaults to a [MemoryApplier]. raftkvd passes the Pebble applier
	// (M3). If it implements io.Closer, Run (or Close) closes it after the loop
	// stops and the last apply has finished.
	Applier Applier
	// Observer is told about role/term changes and shutdown; the KV
	// proposal tracker uses it to fail waiters on leader change (WBS 3.2.2).
	Observer Observer
	// Logger replaces the default JSON logger (which writes to LogWriter).
	Logger    *slog.Logger
	LogWriter io.Writer // default os.Stderr
	// TracerProvider defaults to the global OpenTelemetry provider, a no-op
	// unless the process installed an SDK.
	TracerProvider trace.TracerProvider
	// Metrics defaults to a fresh set.
	Metrics *observability.Metrics
	// WALFS and OnFatal are passed to the WAL (fault injection in tests, D-33).
	WALFS   wal.FS
	OnFatal func(error)
	// Snapshots enables snapshots and log compaction (D-14, D-30). It must
	// manage the same state machine as Applier. nil (the default, and the
	// only choice with a MemoryApplier) keeps the whole log.
	Snapshots *snapshot.Manager
}

// AppliedTermer is implemented by appliers that know the term of the entry at
// their applied index (the Pebble applier). Recovery uses it to detect a
// snapshot install that crashed before its WAL marker (storage-design 5).
type AppliedTermer interface {
	AppliedTerm() uint64
}

// Observer watches the node from the driver loop. Methods run on the loop
// goroutine and must not block.
type Observer interface {
	// OnStatus is called after every handled Ready with the fresh status.
	OnStatus(st raft.Status)
	// OnStop is called once when the node is shutting down.
	OnStop()
}

// Node is one RaftKV member: the driver loop around the pure core (D-35).
//
// A single goroutine (the loop in [Node.Run]) owns the RawNode. Every other
// goroutine talks to it through channels, or reads the atomically published
// [Node.Status] (docs/architecture.md section 5).
type Node struct {
	cfg     Config
	log     *slog.Logger
	metrics *observability.Metrics
	tracer  trace.Tracer

	store   *walstore.Store
	rn      *raft.RawNode
	tr      *transport.Transport
	applier Applier

	lis        net.Listener
	metricsLis net.Listener
	srv        *grpc.Server

	inbox  chan raft.Message
	propCh chan *proposal
	readCh chan readRequest

	// Snapshots (snapshot.go, D-14).
	snaps          *snapshot.Manager
	installCh      chan *installReq
	snapReportCh   chan snapReport
	pendingInstall *installReq // stepped, waiting for its Ready
	installed      *installReq // installed in the current Ready
	needSnapshot   bool        // take a local snapshot at the next chance
	unreach        chan uint64
	pingCh         chan chan struct{} // /livez pings (D-46)
	stopping       chan struct{}

	observer Observer
	services []func(grpc.ServiceRegistrar)

	// Membership (membership.go, D-06, D-23). members is the applied member
	// table; removedAt is when this node applied its own removal.
	identity   Identity
	book       *transport.AddressBook
	members    atomic.Pointer[raftkvv1.ClusterConfig]
	confCh     chan *confReq
	transferCh chan *transferReq
	removedAt  time.Time
	lastHeard  map[uint64]time.Time // loop goroutine only

	status atomic.Pointer[raft.Status]
	term   atomic.Uint64

	waitMu  sync.Mutex
	waiters map[uint64]*waiter

	// Read path (read.go, D-05, D-25).
	readSeq   atomic.Uint64
	readMu    sync.Mutex
	readWait  map[string]chan readResult
	appliedMu sync.Mutex
	appliedCh chan struct{} // closed and replaced after every apply
	epoch     time.Time     // origin of the monotonic lease clock
	leaseMu   sync.Mutex    // guards lease.l
	lease     *leaseState   // nil unless lease reads are on

	started chan struct{}
}

type proposal struct {
	data []byte
	wait bool
	resp chan proposalResult
}

type proposalResult struct {
	index, term uint64
	w           *waiter
	err         error
}

type waiter struct {
	term uint64
	done chan error
}

// New validates cfg, enforces the data directory's identity (WBS 2.3.4), opens
// the WAL, and builds the core and transport. It does not start any goroutine;
// call [Node.Run].
func New(cfg Config, opts Options) (*Node, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	ident, err := EnsureIdentity(cfg.DataDir, Identity{NodeID: cfg.NodeID, ClusterID: cfg.ClusterID, Joined: cfg.Join})
	if err != nil {
		return nil, err
	}

	n := &Node{
		identity:     ident,
		confCh:       make(chan *confReq),
		lastHeard:    make(map[uint64]time.Time),
		transferCh:   make(chan *transferReq),
		cfg:          cfg,
		inbox:        make(chan raft.Message, inboxSize),
		propCh:       make(chan *proposal, inboxSize),
		readCh:       make(chan readRequest, inboxSize),
		readWait:     make(map[string]chan readResult),
		appliedCh:    make(chan struct{}),
		epoch:        time.Now(),
		unreach:      make(chan uint64, 64),
		pingCh:       make(chan chan struct{}),
		snaps:        opts.Snapshots,
		installCh:    make(chan *installReq),
		snapReportCh: make(chan snapReport, 64),
		stopping:     make(chan struct{}),
		started:      make(chan struct{}),
		waiters:      make(map[uint64]*waiter),
		applier:      opts.Applier,
		observer:     opts.Observer,
		metrics:      opts.Metrics,
	}
	if n.applier == nil {
		n.applier = NewMemoryApplier()
	}
	if cfg.LeaseReads {
		d, err := cfg.LeaseDuration()
		if err != nil {
			return nil, err
		}
		n.lease = &leaseState{l: lease.New(d)} // D-25: off unless configured
	}
	if n.metrics == nil {
		n.metrics = observability.NewMetrics()
	}
	tp := opts.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	n.tracer = tp.Tracer("github.com/bibekgupta3333/raftkv-distributed-store/internal/node")
	n.log = opts.Logger
	if n.log == nil {
		w := opts.LogWriter
		if w == nil {
			w = os.Stderr
		}
		lvl, _ := observability.ParseLevel(cfg.LogLevel)
		n.log = observability.NewLogger(w, lvl, cfg.NodeID, n.term.Load)
	}

	store, err := walstore.Open(filepath.Join(cfg.DataDir, WALDir), wal.Options{
		FS: opts.WALFS, ClusterID: cfg.ClusterID, MemberID: cfg.NodeID, OnFatal: opts.OnFatal,
		SegmentSize: cfg.WALSegmentBytes,
	})
	if err != nil {
		return nil, err
	}
	n.store = store
	if n.snaps != nil {
		if l := n.snaps.Latest(); l != nil {
			store.SetSnapshot(snapshotOf(l))
		}
	}
	if store.TruncatedBytes > 0 {
		n.log.Warn("cut a torn WAL tail", "component", "wal", "bytes", store.TruncatedBytes) // D-33
	}

	if err := n.reconcileApplied(); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err := n.ensureSnapshotCoversLog(); err != nil {
		_ = store.Close()
		return nil, err
	}

	if err := n.loadMembers(ident.Joined); err != nil {
		_ = store.Close()
		return nil, err
	}
	for _, id := range n.members.Load().GetRemoved() {
		if id == cfg.NodeID {
			_ = store.Close()
			return nil, fmt.Errorf("%w: node %d; start a new node with -join under the ID AddLearner assigns", ErrRemoved, id)
		}
	}

	rcfg := raft.DefaultConfig(cfg.NodeID, n.bootstrapIDs(ident.Joined), store,
		rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), cfg.NodeID))) // D-24: randomness is injected, here
	rcfg.ElectionTick, rcfg.HeartbeatTick = cfg.ElectionTick, cfg.HeartbeatTick
	rcfg.Applied = n.applier.Applied()
	if n.rn, err = raft.NewRawNode(rcfg); err != nil {
		_ = store.Close()
		return nil, err
	}

	book, err := transport.NewAddressBook(cfg.PeerAddrs())
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	n.book = book
	if n.tr, err = transport.New(transport.Config{
		ID: cfg.NodeID, ClusterID: cfg.ClusterID, Peers: book, Handler: n,
		Incarnation: ident.Incarnation, OnPeerWiped: n.onPeerWiped, // D-32
		Logger:   observability.Component(n.log, "transport"),
		Observer: n.metrics, MaxMsgBytes: cfg.MaxMsgBytes,
		OnUnreachable: n.reportUnreachable,
		Snapshots:     n.snapshotHandler(),
	}); err != nil {
		_ = store.Close()
		return nil, err
	}
	n.syncTransport(n.members.Load()) // members added at run time (D-06)

	if n.lis = opts.Listener; n.lis == nil {
		if n.lis, err = net.Listen("tcp", cfg.EffectiveListenAddr()); err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("node: listen: %w", err)
		}
	}
	if n.metricsLis = opts.MetricsListener; n.metricsLis == nil && cfg.MetricsAddr != "" {
		if n.metricsLis, err = net.Listen("tcp", cfg.MetricsAddr); err != nil {
			_ = n.lis.Close()
			_ = store.Close()
			return nil, fmt.Errorf("node: listen metrics: %w", err)
		}
	}
	return n, nil
}

// ErrAppliedOutsideLog means the state machine's applied index and the WAL
// disagree in a way that cannot happen without breaking D-30 or losing WAL
// data: the node refuses to start (storage-design section 5, step 6).
var ErrAppliedOutsideLog = errors.New("node: applied index is outside the WAL")

// reconcileApplied checks the applier's durable appliedIndex A against the WAL
// (storage-design 5, steps 6-7). A < firstIndex-1 means entries (A, first) were
// trimmed before the state machine made them durable: D-30 was broken. A above
// the last WAL entry means committed entries were lost. If the WAL's
// persisted commit lags A (it is written lazily), commit is raised to A because
// only committed entries are ever applied (storage-design 2.2).
func (n *Node) reconcileApplied() error {
	applied := n.applier.Applied()
	if applied == 0 {
		return nil
	}
	first, err := n.store.FirstIndex()
	if err != nil {
		return err
	}
	last, err := n.store.LastIndex()
	if err != nil {
		return err
	}
	if applied+1 < first {
		return fmt.Errorf("%w: applied %d but the WAL starts at %d (D-30 violated)", ErrAppliedOutsideLog, applied, first)
	}
	hs, err := n.store.InitialState()
	if err != nil {
		return err
	}
	if at, ok := n.applier.(AppliedTermer); ok && n.snaps != nil {
		// storage-design 5 step 6: a snapshot was installed (sm/CURRENT
		// swapped) but the crash came before its WAL marker. The state
		// machine is ahead of the log, or disagrees with it at A: the log
		// must restart after (A, At), Raft section 7.
		term, terr := n.store.Term(applied)
		if applied > last || terr != nil || term != at.AppliedTerm() {
			s := raft.Snapshot{Index: applied, Term: at.AppliedTerm()}
			n.log.Warn("snapshot install finished by recovery: writing the missing WAL marker",
				"component", "snapshot", "index", s.Index, "term", s.Term)
			if err := n.store.ApplySnapshot(s); err != nil {
				return err
			}
			if s.Term > hs.Term {
				// Never hold entries newer than the current term.
				hs.Term, hs.Vote = s.Term, raft.None
			}
			hs.Commit = max(hs.Commit, applied)
			if err := n.store.SetHardState(hs); err != nil {
				return err
			}
			return n.store.Sync()
		}
	}
	if applied > last {
		return fmt.Errorf("%w: applied %d but the WAL ends at %d (committed entries lost)", ErrAppliedOutsideLog, applied, last)
	}
	if hs.Commit < applied {
		hs.Commit = applied
		if err := n.store.SetHardState(hs); err != nil {
			return err
		}
		return n.store.Sync()
	}
	return nil
}

// ensureSnapshotCoversLog takes a local snapshot at start-up if the newest one
// does not cover the log's compaction point, so this node can always serve a
// follower that needs compacted entries (raft.Storage.Snapshot contract). That
// happens after recovery finished an install (the received copy became the
// live database) or if snap/ was lost.
func (n *Node) ensureSnapshotCoversLog() error {
	if n.snaps == nil {
		return nil
	}
	first, err := n.store.FirstIndex()
	if err != nil {
		return err
	}
	if first <= 1 {
		return nil
	}
	if s, _ := n.store.Snapshot(); s.Index+1 >= first {
		return nil
	}
	n.needSnapshot = true
	return n.maybeSnapshot()
}

// snapshotHandler is the transport's snapshot handler, or nil when snapshots
// are off.
func (n *Node) snapshotHandler() transport.SnapshotHandler {
	if n.snaps == nil {
		return nil
	}
	return n
}

// RegisterService lets a service (the KV API) register on the node's gRPC
// server. It must be called before [Node.Run].
func (n *Node) RegisterService(f func(grpc.ServiceRegistrar)) { n.services = append(n.services, f) }

// ClusterID is this node's cluster ID.
func (n *Node) ClusterID() uint64 { return n.cfg.ClusterID }

// PeerAddr returns the peer address of member id, or "": a configured peer or
// one learned from the membership (D-06).
func (n *Node) PeerAddr(id uint64) string {
	a, _ := n.book.Addr(id)
	return a
}

// Addr is the peer gRPC address the node listens on.
func (n *Node) Addr() string { return n.lis.Addr().String() }

// MetricsAddr is the address serving /metrics, /livez and /readyz, or "" if metrics are disabled.
func (n *Node) MetricsAddr() string {
	if n.metricsLis == nil {
		return ""
	}
	return n.metricsLis.Addr().String()
}

// Metrics returns the node's metrics.
func (n *Node) Metrics() *observability.Metrics { return n.metrics }

// Applier returns the node's applier.
func (n *Node) Applier() Applier { return n.applier }

// ID is this node's ID.
func (n *Node) ID() uint64 { return n.cfg.NodeID }

// FirstIndex is the first index still in the WAL. Safe from any goroutine.
func (n *Node) FirstIndex() (uint64, error) { return n.store.FirstIndex() }

// Status returns the most recently published status. It is safe from any
// goroutine; before the first loop iteration it is the zero value.
func (n *Node) Status() raft.Status {
	if s := n.status.Load(); s != nil {
		return *s
	}
	return raft.Status{ID: n.cfg.NodeID}
}

// Step implements [transport.Handler]: it queues an inbound peer message for
// the driver loop.
func (n *Node) Step(ctx context.Context, m raft.Message) error {
	if m.Type == raft.MsgSnap {
		// Snapshots arrive only through ReceiveSnapshot, with their files.
		return fmt.Errorf("node: MsgSnap without a snapshot stream")
	}
	select {
	case n.inbox <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.stopping:
		return ErrStopped
	}
}

func (n *Node) reportUnreachable(id uint64) {
	select {
	case n.unreach <- id:
	default: // the loop is backed up; the next failure reports again
	}
}

// Propose appends data to the log if this node is the leader and returns the
// index and term it will have if it commits. It does not wait for commit; see
// [Node.ProposeAndWait]. Errors are the core's: [raft.ErrProposalDropped] and
// *[raft.NotLeaderError].
func (n *Node) Propose(ctx context.Context, data []byte) (index, term uint64, err error) {
	ctx, span := n.tracer.Start(ctx, "raft.propose", trace.WithAttributes(attribute.Int("raft.data_bytes", len(data))))
	defer span.End()
	res, err := n.submit(ctx, data, false)
	span.SetAttributes(attribute.Int64("raft.index", int64(res.index)))
	return res.index, res.term, err
}

// ProposeAndWait proposes data and blocks until it has been applied on this
// node (which implies it committed) and returns its log index. The span
// raft.propose covers the whole wait.
func (n *Node) ProposeAndWait(ctx context.Context, data []byte) (uint64, error) {
	ctx, span := n.tracer.Start(ctx, "raft.propose", trace.WithAttributes(attribute.Int("raft.data_bytes", len(data))))
	defer span.End()
	res, err := n.submit(ctx, data, true)
	if err != nil {
		span.RecordError(err)
		return 0, err
	}
	span.SetAttributes(attribute.Int64("raft.index", int64(res.index)))
	select {
	case err := <-res.w.done:
		if err != nil {
			span.RecordError(err)
		}
		return res.index, err
	case <-ctx.Done():
		n.dropWaiter(res.index, res.w)
		return 0, ctx.Err()
	}
}

func (n *Node) submit(ctx context.Context, data []byte, wait bool) (proposalResult, error) {
	p := &proposal{data: data, wait: wait, resp: make(chan proposalResult, 1)}
	select {
	case n.propCh <- p:
	case <-ctx.Done():
		return proposalResult{}, ctx.Err()
	case <-n.stopping:
		return proposalResult{}, ErrStopped
	}
	select {
	case res := <-p.resp:
		return res, res.err
	case <-ctx.Done():
		return proposalResult{}, ctx.Err()
	case <-n.stopping:
		return proposalResult{}, ErrStopped
	}
}

func (n *Node) dropWaiter(index uint64, w *waiter) {
	n.waitMu.Lock()
	defer n.waitMu.Unlock()
	if n.waiters[index] == w {
		delete(n.waiters, index)
	}
}

// Started is closed once [Node.Run] is serving peers and driving the core.
func (n *Node) Started() <-chan struct{} { return n.started }

// Close releases the listeners and the WAL of a node that was built but never
// run. It is not needed after [Node.Run], which cleans up itself.
func (n *Node) Close() error {
	_ = n.lis.Close()
	if n.metricsLis != nil {
		_ = n.metricsLis.Close()
	}
	return errors.Join(n.store.Close(), n.closeApplier())
}

func (n *Node) closeApplier() error {
	if c, ok := n.applier.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// Run serves peers and drives the core until ctx is cancelled (graceful
// shutdown, WBS 2.3.5) or a fatal error occurs. On cancellation it returns nil
// after stopping the transport and syncing and closing the WAL.
func (n *Node) Run(ctx context.Context) error {
	n.srv = n.tr.NewGRPCServer(grpc.ChainUnaryInterceptor(n.metrics.UnaryServerInterceptor()))
	registerDebug(n.srv, n)
	for _, f := range n.services {
		f(n.srv)
	}
	go func() { _ = n.srv.Serve(n.lis) }()
	var metricsSrv *http.Server
	if n.metricsLis != nil {
		metricsSrv = &http.Server{Handler: n.httpHandler(), ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = metricsSrv.Serve(n.metricsLis) }()
	}
	n.tr.Start(ctx)
	close(n.started)
	n.log.Info("node started", "component", "node", "addr", n.Addr(), "peers", len(n.cfg.Peers))

	err := n.loop(ctx)

	n.log.Info("node stopping", "component", "node", "err", err)
	close(n.stopping)
	n.tr.Close()
	stopped := make(chan struct{})
	go func() { n.srv.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		n.srv.Stop()
		<-stopped
	}
	if metricsSrv != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = metricsSrv.Shutdown(sctx)
		cancel()
	}
	n.failWaiters(ErrStopped)
	if n.observer != nil {
		n.observer.OnStop()
	}
	if cerr := n.store.Close(); err == nil { // Close fsyncs the WAL tail
		err = cerr
	}
	if cerr := n.closeApplier(); err == nil {
		err = cerr
	}
	return err
}

// loop is the Ready loop of docs/architecture.md 4.2 (D-35).
func (n *Node) loop(ctx context.Context) error {
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()
	done := ctx.Done()
	var handoverUntil time.Time // set while a shutdown hands leadership over
	for {
		select {
		case <-done:
			// WBS 6.2.3: a leader hands over before it stops, so the
			// cluster does not wait an election timeout for a new one.
			if !n.startHandover() {
				return nil
			}
			done, handoverUntil = nil, time.Now().Add(n.transferTimeout())
		case <-ticker.C:
			n.rn.Tick()
			n.maybeRenewLease()
			if !handoverUntil.IsZero() && (n.rn.Status().State != raft.StateLeader || time.Now().After(handoverUntil)) {
				n.log.Info("shutdown handover finished", "component", "node", "leader", n.rn.Status().Lead)
				return nil
			}
			if !n.removedAt.IsZero() && time.Since(n.removedAt) > 3*n.cfg.TickInterval {
				return ErrRemoved // system-design 7.4: give the handover a few ticks
			}
		case req := <-n.confCh:
			n.handleConfChange(req)
		case req := <-n.transferCh:
			n.handleTransfer(req)
		case m := <-n.inbox:
			n.step(m)
		case id := <-n.unreach:
			n.rn.ReportUnreachable(id)
		case p := <-n.propCh:
			n.handleProposal(p)
		case pong := <-n.pingCh:
			close(pong) // D-46: alive = the loop answers; nothing to process
			continue
		case rq := <-n.readCh:
			n.handleRead(rq)
		case req := <-n.installCh:
			n.handleInstall(req)
		case r := <-n.snapReportCh:
			n.rn.ReportSnapshot(r.to, r.failed)
		}
		n.absorb()
		if err := n.processReady(); err != nil {
			return err
		}
		if n.snaps != nil {
			n.finishInstall()
			if err := n.maybeSnapshot(); err != nil {
				return err
			}
		}
	}
}

// startHandover begins a leadership transfer at shutdown if this node leads
// and another voter exists. It reports whether the loop should wait for it.
func (n *Node) startHandover() bool {
	st := n.rn.Status()
	if st.State != raft.StateLeader || len(st.Config.Voters) < 2 {
		return false
	}
	n.revokeLease() // D-25
	if err := n.rn.TransferLeadership(raft.None); err != nil {
		n.log.Warn("shutdown handover not started", "component", "node", "err", err)
		return false
	}
	n.log.Info("handing leadership over before shutdown", "component", "node", "to", n.rn.Status().LeadTransferee)
	if err := n.processReady(); err != nil {
		n.log.Warn("shutdown handover", "component", "node", "err", err)
		return false
	}
	return true
}

func (n *Node) step(m raft.Message) {
	n.lastHeard[m.From] = time.Now() // WOULD_LOSE_QUORUM check (membership.go)
	if err := n.rn.Step(m); err != nil {
		n.log.Debug("step failed", "component", "raft", "type", m.Type.String(), "from", m.From, "err", err)
	}
}

// absorb takes already-queued events without blocking, so a burst of proposals
// or messages shares one Ready and one fsync.
func (n *Node) absorb() {
	for range maxBatch {
		select {
		case m := <-n.inbox:
			n.step(m)
		case p := <-n.propCh:
			n.handleProposal(p)
		case rq := <-n.readCh:
			n.handleRead(rq)
		case id := <-n.unreach:
			n.rn.ReportUnreachable(id)
		case r := <-n.snapReportCh:
			n.rn.ReportSnapshot(r.to, r.failed)
		default:
			return
		}
	}
}

func (n *Node) handleProposal(p *proposal) {
	idx, term, err := n.rn.Propose(p.data)
	res := proposalResult{index: idx, term: term, err: err}
	if err == nil && p.wait {
		// Registered inside the loop, before any Ready can apply the entry.
		res.w = &waiter{term: term, done: make(chan error, 1)}
		n.waitMu.Lock()
		n.waiters[idx] = res.w
		n.waitMu.Unlock()
	}
	p.resp <- res
}

func (n *Node) processReady() error {
	for n.rn.HasReady() {
		rd := n.rn.Ready()
		if n.lease != nil && hasConfChange(rd.Entries) {
			n.revokeLease() // D-25: the voter set may change
		}
		// (1) Persist before anything is acknowledged (D-33, storage-design 2.1).
		if err := n.persist(rd); err != nil {
			return fmt.Errorf("persist: %w", err)
		}
		// (2) Only now send messages.
		n.tr.Send(rd.Messages)
		// (3) Apply committed entries in order (D-04).
		if err := n.apply(rd.CommittedEntries); err != nil {
			return fmt.Errorf("apply: %w", err)
		}
		// (4) Answer reads (D-05). They wait for the applied index themselves.
		n.handleReadStates(rd)
		n.rn.Advance(rd)
		n.publish(rd)
	}
	return nil
}

// persist appends entries and HardState, then fsyncs once if rd.MustSync.
func (n *Node) persist(rd raft.Ready) error {
	if rd.Snapshot != nil {
		if err := n.installSnapshot(*rd.Snapshot); err != nil {
			return err
		}
	}
	if err := n.store.Append(rd.Entries); err != nil {
		return err
	}
	if !rd.HardState.IsEmpty() {
		if err := n.store.SetHardState(rd.HardState); err != nil {
			return err
		}
	}
	if !rd.MustSync {
		return nil
	}
	_, span := n.tracer.Start(context.Background(), "wal.fsync",
		trace.WithAttributes(attribute.Int("wal.entries", len(rd.Entries))))
	start := time.Now()
	err := n.store.Sync()
	n.metrics.ObserveFsync(time.Since(start), err)
	if err != nil {
		span.RecordError(err)
	}
	span.End()
	return err
}

func (n *Node) apply(ents []raft.Entry) error {
	if len(ents) == 0 {
		return nil
	}
	_, span := n.tracer.Start(context.Background(), "apply", trace.WithAttributes(
		attribute.Int64("raft.first_index", int64(ents[0].Index)),
		attribute.Int64("raft.last_index", int64(ents[len(ents)-1].Index)),
		attribute.Int("raft.entries", len(ents))))
	err := n.applier.Apply(ents)
	if err != nil {
		span.RecordError(err)
	}
	span.End()
	if err != nil {
		return err
	}
	n.applyMembership(ents) // D-06: the member table and the transport follow
	n.notifyApplied()
	n.waitMu.Lock()
	defer n.waitMu.Unlock()
	if len(n.waiters) == 0 {
		return nil
	}
	for _, e := range ents {
		if w, ok := n.waiters[e.Index]; ok {
			delete(n.waiters, e.Index)
			if w.term == e.Term {
				w.done <- nil
			} else {
				w.done <- ErrProposalLost
			}
		}
	}
	return nil
}

func hasConfChange(ents []raft.Entry) bool {
	for _, e := range ents {
		if e.Type == raft.EntryConfChange {
			return true
		}
	}
	return false
}

func (n *Node) failWaiters(err error) {
	n.waitMu.Lock()
	defer n.waitMu.Unlock()
	for idx, w := range n.waiters {
		w.done <- err
		delete(n.waiters, idx)
	}
}

// publish refreshes the shared status, metrics and role-change logs after a
// Ready has been fully handled.
func (n *Node) publish(rd raft.Ready) {
	st := n.rn.Status()
	n.status.Store(&st)
	n.term.Store(st.Term)
	n.metrics.ObserveStatus(st)
	if n.observer != nil {
		n.observer.OnStatus(st)
	}
	if rd.SoftState != nil {
		n.log.Info("raft state changed", "component", "raft",
			"state", rd.SoftState.State.String(), "leader", rd.SoftState.Lead, "commit", st.Commit)
	}
}
