package node_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
)

// probe returns the status code and body of GET base+path.
func probe(t *testing.T, base, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(base + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// WBS 7.1.4 / D-46: /livez follows the driver loop, /readyz needs a known
// leader. Both are served on the metrics listener next to /metrics.
func TestHealthProbes(t *testing.T) {
	cfg := node.DefaultConfig()
	cfg.DataDir = t.TempDir()
	// Node 1 of a 3-node cluster whose peers never start: it can never learn
	// a leader, so it stays live but never becomes ready.
	cfg.NodeID = 1
	cfg.Peers = []node.PeerConfig{{ID: 1, Addr: "127.0.0.1:0"}, {ID: 2, Addr: "127.0.0.1:1"}, {ID: 3, Addr: "127.0.0.1:1"}}
	cfg.TickInterval = 5 * time.Millisecond
	cfg.MetricsAddr = "127.0.0.1:0"
	n, err := node.New(cfg, node.Options{LogWriter: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Live(context.Background()); !errors.Is(err, node.ErrNotRunning) {
		t.Errorf("Live before Run = %v, want ErrNotRunning", err)
	}
	if err := n.Ready(); !errors.Is(err, node.ErrNotRunning) {
		t.Errorf("Ready before Run = %v, want ErrNotRunning", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()
	<-n.Started()
	base := "http://" + n.MetricsAddr()

	if code, body := probe(t, base, "/livez"); code != http.StatusOK || strings.TrimSpace(body) != "ok" {
		t.Errorf("/livez = %d %q, want 200 ok", code, body)
	}
	time.Sleep(100 * time.Millisecond) // several election timeouts: still no leader possible
	if code, body := probe(t, base, "/readyz"); code != http.StatusServiceUnavailable || !strings.Contains(body, "no known leader") {
		t.Errorf("/readyz without a leader = %d %q, want 503 no known leader", code, body)
	}
	if code, _ := probe(t, base, "/metrics"); code != http.StatusOK {
		t.Errorf("/metrics = %d, want 200", code)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := n.Live(context.Background()); !errors.Is(err, node.ErrNotRunning) {
		t.Errorf("Live after shutdown = %v, want ErrNotRunning", err)
	}
	if err := n.Ready(); !errors.Is(err, node.ErrNotRunning) {
		t.Errorf("Ready after shutdown = %v, want ErrNotRunning", err)
	}
}

// Every member of a healthy cluster becomes ready once a leader is elected and
// it has applied what it knows is committed.
func TestHealthReadyWithLeader(t *testing.T) {
	c := newTestCluster(t, 3)
	lead := c.leader()
	if _, err := lead.ProposeAndWait(context.Background(), []byte("x")); err != nil {
		t.Fatal(err)
	}
	for id, n := range c.nodes {
		waitUntil(t, 5*time.Second, "node ready", func() bool { return n.Ready() == nil })
		if err := n.Live(context.Background()); err != nil {
			t.Errorf("node %d Live = %v", id, err)
		}
	}
	srv := httptest.NewServer(lead.HealthHandler())
	defer srv.Close()
	if code, body := probe(t, srv.URL, "/readyz"); code != http.StatusOK {
		t.Errorf("leader /readyz = %d %q, want 200", code, body)
	}
	if code, body := probe(t, srv.URL, "/livez"); code != http.StatusOK {
		t.Errorf("leader /livez = %d %q, want 200", code, body)
	}
}
