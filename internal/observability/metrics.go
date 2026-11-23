package observability

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Metrics is the node's Prometheus instrumentation (D-17, WBS 2.4.2-2.4.3). It
// owns a private registry so tests and in-process clusters never collide. The
// transport reports through the methods that satisfy transport.Observer.
type Metrics struct {
	reg *prometheus.Registry

	term, state, commit, applied, lastIndex prometheus.Gauge
	leaderChanges                           prometheus.Counter

	walFsync       prometheus.Histogram
	walFsyncErrors prometheus.Counter

	msgSent      *prometheus.CounterVec
	sendFailures *prometheus.CounterVec
	reconnects   *prometheus.CounterVec
	dropped      *prometheus.CounterVec

	rpcRequests *prometheus.CounterVec
	rpcDuration *prometheus.HistogramVec

	// Snapshots (D-14, WBS 5.9.1).
	snapIndex     prometheus.Gauge
	snapSize      prometheus.Gauge
	snapDuration  *prometheus.HistogramVec // op: create, install
	snapInstalls  prometheus.Counter
	snapTransfers *prometheus.CounterVec // result: ok, failed
	walTrimmed    prometheus.Gauge

	mu       sync.Mutex
	lastLead uint64
}

// NewMetrics builds the collectors and registers them.
func NewMetrics() *Metrics {
	g := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	}
	m := &Metrics{
		reg:       prometheus.NewRegistry(),
		term:      g("raftkv_raft_term", "Current Raft term."),
		state:     g("raftkv_raft_state", "Raft role: 0 follower, 1 candidate, 2 pre-candidate, 3 leader."),
		commit:    g("raftkv_raft_commit_index", "Highest log index known committed."),
		applied:   g("raftkv_raft_applied_index", "Highest log index applied to the state machine."),
		lastIndex: g("raftkv_raft_last_index", "Last log index."),
		leaderChanges: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "raftkv_raft_leader_changes_total", Help: "Times this node saw a different leader."}),
		walFsync: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "raftkv_wal_fsync_duration_seconds", Help: "WAL fsync latency.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14)}),
		walFsyncErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "raftkv_wal_fsync_errors_total", Help: "Failed WAL fsyncs (each one is fatal, D-33)."}),
		msgSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raftkv_transport_messages_sent_total", Help: "Raft messages written to peers."}, []string{"type"}),
		sendFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raftkv_transport_send_failures_total", Help: "Failed peer streams or RPCs."}, []string{"peer"}),
		reconnects: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raftkv_transport_reconnects_total", Help: "Peer stream reconnect attempts."}, []string{"peer"}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raftkv_transport_dropped_total", Help: "Messages dropped (queue full, in-flight cap, dead link)."}, []string{"peer"}),
		rpcRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raftkv_grpc_requests_total", Help: "Unary gRPC requests served, by full method and status code."},
			[]string{"method", "code"}),
		rpcDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "raftkv_grpc_request_duration_seconds", Help: "Unary gRPC request latency, by full method.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14)}, []string{"method"}),
		snapIndex: g("raftkv_snapshot_index", "Log index of the newest local snapshot."),
		snapSize:  g("raftkv_snapshot_size_bytes", "Total file size of the newest local snapshot."),
		snapDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "raftkv_snapshot_duration_seconds", Help: "Time to create or install a snapshot, by op.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 16)}, []string{"op"}),
		snapInstalls: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "raftkv_snapshot_installs_total", Help: "Snapshots installed from the leader."}),
		snapTransfers: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "raftkv_snapshot_transfers_total", Help: "Snapshot transfers sent to followers, by result."}, []string{"result"}),
		walTrimmed: g("raftkv_wal_first_index", "First index still in the WAL (moves up when the log is trimmed, D-30)."),
	}
	m.reg.MustRegister(m.term, m.state, m.commit, m.applied, m.lastIndex, m.leaderChanges,
		m.walFsync, m.walFsyncErrors, m.msgSent, m.sendFailures, m.reconnects, m.dropped,
		m.rpcRequests, m.rpcDuration,
		m.snapIndex, m.snapSize, m.snapDuration, m.snapInstalls, m.snapTransfers, m.walTrimmed)
	return m
}

// Registry returns the underlying registry (for tests).
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Handler serves /metrics.
func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	return mux
}

// ObserveStatus records the Raft gauges from a status snapshot and counts a
// leader change whenever a different non-zero leader is seen.
func (m *Metrics) ObserveStatus(s raft.Status) {
	m.term.Set(float64(s.Term))
	m.state.Set(float64(s.State))
	m.commit.Set(float64(s.Commit))
	m.applied.Set(float64(s.Applied))
	m.lastIndex.Set(float64(s.LastIndex))
	m.mu.Lock()
	defer m.mu.Unlock()
	if s.Lead != raft.None && s.Lead != m.lastLead {
		m.leaderChanges.Inc()
	}
	m.lastLead = s.Lead
}

// ObserveFsync records one WAL fsync.
func (m *Metrics) ObserveFsync(d time.Duration, err error) {
	m.walFsync.Observe(d.Seconds())
	if err != nil {
		m.walFsyncErrors.Inc()
	}
}

// SnapshotCreated records a local snapshot at index with total file size
// bytes, taken in d.
func (m *Metrics) SnapshotCreated(index, bytes uint64, d time.Duration) {
	m.snapIndex.Set(float64(index))
	m.snapSize.Set(float64(bytes))
	m.snapDuration.WithLabelValues("create").Observe(d.Seconds())
}

// SnapshotInstalled records a snapshot installed from the leader in d.
func (m *Metrics) SnapshotInstalled(d time.Duration) {
	m.snapInstalls.Inc()
	m.snapDuration.WithLabelValues("install").Observe(d.Seconds())
}

// SnapshotSent records the outcome of a snapshot transfer to a follower.
func (m *Metrics) SnapshotSent(failed bool) {
	r := "ok"
	if failed {
		r = "failed"
	}
	m.snapTransfers.WithLabelValues(r).Inc()
}

// WALFirstIndex records the first index still in the WAL.
func (m *Metrics) WALFirstIndex(i uint64) { m.walTrimmed.Set(float64(i)) }

// MessageSent implements transport.Observer.
func (m *Metrics) MessageSent(_ uint64, t raft.MessageType) {
	m.msgSent.WithLabelValues(t.String()).Inc()
}

// SendFailure implements transport.Observer.
func (m *Metrics) SendFailure(peer uint64) { m.sendFailures.WithLabelValues(id(peer)).Inc() }

// Reconnect implements transport.Observer.
func (m *Metrics) Reconnect(peer uint64) { m.reconnects.WithLabelValues(id(peer)).Inc() }

// Dropped implements transport.Observer.
func (m *Metrics) Dropped(peer uint64) { m.dropped.WithLabelValues(id(peer)).Inc() }

func id(peer uint64) string { return strconv.FormatUint(peer, 10) }

// UnaryServerInterceptor counts every unary RPC (KV, Cluster, debug) by method
// and status code and times it; the Grafana dashboard's request-rate and error
// panels read these (WBS 7.3.1). Raft peer traffic is a stream and is counted
// by the transport metrics instead.
func (m *Metrics) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := h(ctx, req)
		m.rpcDuration.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		m.rpcRequests.WithLabelValues(info.FullMethod, status.Code(err).String()).Inc()
		return resp, err
	}
}
