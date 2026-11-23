package node_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// testCluster is N in-process nodes talking over real localhost gRPC.
type testCluster struct {
	t       *testing.T
	nodes   map[uint64]*node.Node
	cancels map[uint64]context.CancelFunc
	done    map[uint64]chan error
	logs    map[uint64]*syncBuffer
	cfgs    map[uint64]node.Config
	lises   map[uint64]net.Listener
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type clusterOptions struct {
	tracerProvider trace.TracerProvider
	tick           time.Duration // 0: 10ms
	config         func(*node.Config)
}

func newTestCluster(t *testing.T, n int, co ...func(*clusterOptions)) *testCluster {
	t.Helper()
	var opts clusterOptions
	for _, f := range co {
		f(&opts)
	}
	c := &testCluster{t: t,
		nodes: map[uint64]*node.Node{}, cancels: map[uint64]context.CancelFunc{}, done: map[uint64]chan error{},
		logs: map[uint64]*syncBuffer{}, cfgs: map[uint64]node.Config{}, lises: map[uint64]net.Listener{}}
	var peers []node.PeerConfig
	for i := 1; i <= n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		id := uint64(i)
		c.lises[id] = l
		peers = append(peers, node.PeerConfig{ID: id, Addr: l.Addr().String()})
	}
	dir := t.TempDir()
	for _, p := range peers {
		cfg := node.DefaultConfig()
		cfg.NodeID = p.ID
		cfg.DataDir = filepath.Join(dir, fmt.Sprint("n", p.ID))
		cfg.Peers = peers
		cfg.TickInterval = 10 * time.Millisecond
		if opts.tick > 0 {
			cfg.TickInterval = opts.tick
		}
		if opts.config != nil {
			opts.config(&cfg)
		}
		c.cfgs[p.ID] = cfg
		c.start(p.ID, opts)
	}
	t.Cleanup(c.stopAll)
	return c
}

func (c *testCluster) start(id uint64, opts clusterOptions) {
	c.t.Helper()
	lis := c.lises[id]
	if lis == nil {
		var err error
		if lis, err = net.Listen("tcp", c.addr(id)); err != nil {
			c.t.Fatal(err)
		}
	}
	delete(c.lises, id)
	c.logs[id] = &syncBuffer{}
	n, err := node.New(c.cfgs[id], node.Options{Listener: lis, LogWriter: c.logs[id], TracerProvider: opts.tracerProvider})
	if err != nil {
		c.t.Fatalf("node.New(%d): %v", id, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Run(ctx) }()
	c.nodes[id], c.cancels[id], c.done[id] = n, cancel, done
}

func (c *testCluster) addr(id uint64) string {
	for _, p := range c.cfgs[id].Peers {
		if p.ID == id {
			return p.Addr
		}
	}
	return ""
}

// stop shuts one node down gracefully and checks Run returned nil.
func (c *testCluster) stop(id uint64) {
	c.t.Helper()
	if c.cancels[id] == nil {
		return
	}
	c.cancels[id]()
	select {
	case err := <-c.done[id]:
		if err != nil {
			c.t.Errorf("node %d Run returned %v", id, err)
		}
	case <-time.After(10 * time.Second):
		c.t.Errorf("node %d did not shut down", id)
	}
	c.cancels[id] = nil
}

func (c *testCluster) stopAll() {
	for id := range c.cancels {
		c.stop(id)
	}
}

// leader waits for exactly one running node to claim leadership of the highest term.
func (c *testCluster) leader() *node.Node {
	c.t.Helper()
	var lead *node.Node
	waitUntil(c.t, 15*time.Second, "a leader", func() bool {
		lead = nil
		for id, n := range c.nodes {
			if c.cancels[id] == nil {
				continue
			}
			if st := n.Status(); st.State == raft.StateLeader {
				if lead != nil {
					return false
				}
				lead = n
			}
		}
		return lead != nil
	})
	return lead
}

func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// commandsOf returns the node's applied command payloads as strings.
func commandsOf(n *node.Node) []string {
	ma := n.Applier().(*node.MemoryApplier)
	var out []string
	for _, c := range ma.Commands() {
		out = append(out, string(c))
	}
	return out
}
