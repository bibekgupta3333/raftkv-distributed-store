package node_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// WBS 2.3.1: three in-process nodes elect a leader and commit entries over real
// localhost gRPC; every replica applies the same entries in the same order.
func TestThreeNodesCommitEntries(t *testing.T) {
	c := newTestCluster(t, 3)
	lead := c.leader()

	var want []string
	for i := range 50 {
		cmd := fmt.Sprintf("cmd-%03d", i)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if _, err := lead.ProposeAndWait(ctx, []byte(cmd)); err != nil {
			cancel()
			t.Fatalf("propose %d: %v", i, err)
		}
		cancel()
		want = append(want, cmd)
	}
	for id, n := range c.nodes {
		waitUntil(t, 10*time.Second, fmt.Sprintf("node %d to apply everything", id), func() bool {
			return len(commandsOf(n)) == len(want)
		})
		if got := commandsOf(n); !reflect.DeepEqual(got, want) {
			t.Fatalf("node %d applied %v, want %v", id, got, want)
		}
	}
}

// A follower refuses proposals (forwarding arrives in M4, D-28).
func TestFollowerRejectsProposal(t *testing.T) {
	c := newTestCluster(t, 3)
	lead := c.leader()
	for id, n := range c.nodes {
		if n == lead {
			continue
		}
		waitUntil(t, 10*time.Second, "follower to learn the leader", func() bool { return n.Status().Lead == lead.ID() })
		_, _, err := n.Propose(context.Background(), []byte("x"))
		if !errors.Is(err, raft.ErrNotLeader) {
			t.Fatalf("node %d: err = %v, want ErrNotLeader", id, err)
		}
	}
}

// A killed leader is replaced and the cluster keeps committing; the old leader
// restarts from its WAL, rejoins, and catches up.
func TestLeaderFailoverAndRestartFromWAL(t *testing.T) {
	c := newTestCluster(t, 3)
	lead := c.leader()
	propose := func(n *node.Node, s string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := n.ProposeAndWait(ctx, []byte(s)); err != nil {
			t.Fatalf("propose %q: %v", s, err)
		}
	}
	propose(lead, "before")
	oldID := lead.ID()
	c.stop(oldID)

	newLead := c.leader()
	if newLead.ID() == oldID {
		t.Fatal("stopped node is still the leader")
	}
	propose(newLead, "during")

	c.start(oldID, clusterOptions{})
	waitUntil(t, 15*time.Second, "restarted node to catch up", func() bool {
		return len(commandsOf(c.nodes[oldID])) == 2
	})
	if got := commandsOf(c.nodes[oldID]); !reflect.DeepEqual(got, []string{"before", "during"}) {
		t.Fatalf("restarted node applied %v", got)
	}
}

// WBS 2.3.5: SIGTERM stops the node gracefully; the WAL is intact afterwards.
func TestGracefulShutdownOnSIGTERM(t *testing.T) {
	c := newTestCluster(t, 3)
	lead := c.leader()
	if _, err := lead.ProposeAndWait(context.Background(), []byte("x")); err != nil {
		t.Fatal(err)
	}

	// Hand the node its own context from RunUntilSignal and deliver a real SIGTERM.
	cfg := c.cfgs[1]
	c.stop(1)
	n, err := node.New(cfg, node.Options{LogWriter: io.Discard, Listener: mustListen(t, c.addr(1))})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- node.RunUntilSignal(n, syscall.SIGTERM) }()
	select {
	case <-n.Started(): // the signal handler is installed before Run starts
	case <-time.After(10 * time.Second):
		t.Fatal("node did not start")
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunUntilSignal = %v, want nil after SIGTERM", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("node did not stop on SIGTERM")
	}
	// Proposals after shutdown fail cleanly.
	if _, _, err := n.Propose(context.Background(), []byte("late")); !errors.Is(err, node.ErrStopped) {
		t.Fatalf("Propose after stop = %v, want ErrStopped", err)
	}
}

// WBS 2.4.1: JSON logs carry node_id, term and component.
func TestJSONLogFields(t *testing.T) {
	c := newTestCluster(t, 3)
	c.leader()
	var found bool
	for id, buf := range c.logs {
		for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			var rec map[string]any
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatalf("node %d log line is not JSON: %q", id, line)
			}
			if rec["node_id"] != float64(id) || rec["component"] == nil {
				t.Fatalf("node %d log line misses fields: %q", id, line)
			}
			if _, ok := rec["term"]; !ok {
				t.Fatalf("node %d log line has no term: %q", id, line)
			}
			if rec["msg"] == "raft state changed" && rec["component"] == "raft" && rec["term"].(float64) > 0 {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("no 'raft state changed' record with a positive term was logged")
	}
}

// WBS 2.4.2 / 2.4.3: /metrics exposes Raft, WAL and transport metrics.
func TestMetricsEndpoint(t *testing.T) {
	c := newTestCluster(t, 3)
	lead := c.leader()
	for i := range 5 {
		if _, err := lead.ProposeAndWait(context.Background(), []byte(fmt.Sprint("m", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, 5*time.Second, "leader applied index", func() bool { return lead.Status().Applied >= 6 })

	// The nodes were built without metrics listeners: serve the registry
	// through the node's own handler to check exposition format end to end.
	srv := newMetricsServer(t, lead)
	body := scrape(t, srv)
	for _, want := range []string{
		"raftkv_raft_term ", "raftkv_raft_state 3", "raftkv_raft_leader_changes_total 1",
		"raftkv_raft_commit_index ", "raftkv_raft_applied_index ",
		"raftkv_wal_fsync_duration_seconds_count ",
		`raftkv_transport_messages_sent_total{type="MsgApp"}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
	if v := metricValue(t, body, "raftkv_raft_applied_index"); v < 6 {
		t.Errorf("applied index metric = %v, want >= 6 (no-op + 5 commands)", v)
	}
	if v := metricValue(t, body, "raftkv_wal_fsync_duration_seconds_count"); v < 1 {
		t.Errorf("fsync count = %v, want >= 1", v)
	}
}

// WBS 2.4.2: a real /metrics listener serves the same registry.
func TestMetricsListener(t *testing.T) {
	cfg := node.DefaultConfig()
	cfg.NodeID, cfg.DataDir = 1, t.TempDir()
	cfg.Peers = []node.PeerConfig{{ID: 1, Addr: "127.0.0.1:0"}}
	cfg.TickInterval = 5 * time.Millisecond
	cfg.MetricsAddr = "127.0.0.1:0"
	n, err := node.New(cfg, node.Options{LogWriter: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	waitUntil(t, 10*time.Second, "single node to lead", func() bool { return n.Status().State == raft.StateLeader })

	resp, err := http.Get("http://" + n.MetricsAddr() + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "raftkv_raft_state 3") {
		t.Fatalf("metrics did not show the leader state:\n%s", b)
	}
}

// WBS 2.4.4: spans raft.propose, wal.fsync and apply reach an in-memory exporter.
func TestTracingSpans(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	defer func() { _ = tp.Shutdown(context.Background()) }()
	c := newTestCluster(t, 3, func(o *clusterOptions) { o.tracerProvider = tp })
	lead := c.leader()
	if _, err := lead.ProposeAndWait(context.Background(), []byte("traced")); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, s := range exp.GetSpans() {
		seen[s.Name] = true
	}
	for _, name := range []string{"raft.propose", "wal.fsync", "apply"} {
		if !seen[name] {
			t.Errorf("no %q span exported; got %v", name, seen)
		}
	}
}

// A single node is its own quorum and works with no peers (bootstrap, WBS 2.3.4).
func TestSingleNodeCluster(t *testing.T) {
	cfg := node.DefaultConfig()
	cfg.NodeID, cfg.DataDir = 1, filepath.Join(t.TempDir(), "d")
	cfg.Peers = []node.PeerConfig{{ID: 1, Addr: "127.0.0.1:0"}}
	cfg.TickInterval = 5 * time.Millisecond
	n, err := node.New(cfg, node.Options{LogWriter: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	}()
	waitUntil(t, 10*time.Second, "leadership", func() bool { return n.Status().State == raft.StateLeader })
	if _, err := n.ProposeAndWait(context.Background(), []byte("solo")); err != nil {
		t.Fatal(err)
	}
}

func mustListen(t *testing.T, addr string) net.Listener {
	t.Helper()
	var err error
	for range 50 {
		var l net.Listener
		if l, err = net.Listen("tcp", addr); err == nil {
			return l
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(err)
	return nil
}

func newMetricsServer(t *testing.T, n *node.Node) string {
	t.Helper()
	srv := httptest.NewServer(n.Metrics().Handler())
	t.Cleanup(srv.Close)
	return srv.URL
}

func scrape(t *testing.T, base string) string {
	t.Helper()
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// metricValue returns the value of an unlabelled sample.
func metricValue(t *testing.T, body, name string) float64 {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if rest, ok := strings.CutPrefix(line, name+" "); ok {
			v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}
