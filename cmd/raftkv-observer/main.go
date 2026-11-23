// Command raftkv-observer is the read-only live view of a RaftKV cluster
// (D-48, WBS 7.2). It polls Cluster.Status on every configured node at a fixed
// interval, infers events (elections, leader changes, snapshots, nodes going
// down or coming back) by diffing successive polls, and pushes the result to
// one embedded HTML page over server-sent events. It calls nothing but Status
// and has no auth, so it binds to localhost by default.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/version"
)

// defaultEndpoints are the compose cluster's host-published ports
// (deploy/compose/docker-compose.yml).
const defaultEndpoints = "node1=127.0.0.1:17001,node2=127.0.0.1:17002,node3=127.0.0.1:17003," +
	"node4=127.0.0.1:17004,node5=127.0.0.1:17005"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

type config struct {
	endpoints  []Endpoint
	listen     string
	interval   time.Duration
	rpcTimeout time.Duration
}

func parseFlags(args []string, stderr io.Writer) (config, error) {
	fs := flag.NewFlagSet("raftkv-observer", flag.ContinueOnError)
	fs.SetOutput(stderr)
	eps := fs.String("endpoints", defaultEndpoints, "comma-separated nodes to poll, each name=host:port or host:port")
	listen := fs.String("listen", "127.0.0.1:18080", "HTTP listen address (no auth: keep it on localhost)")
	interval := fs.Duration("interval", 500*time.Millisecond, "Cluster.Status poll interval")
	timeout := fs.Duration("rpc-timeout", 0, "per-node Status timeout (default 80% of -interval)")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	cfg := config{listen: *listen, interval: *interval, rpcTimeout: *timeout}
	if cfg.interval <= 0 {
		return cfg, errors.New("-interval must be positive")
	}
	if cfg.rpcTimeout <= 0 {
		cfg.rpcTimeout = cfg.interval * 4 / 5
	}
	var err error
	if cfg.endpoints, err = parseEndpoints(*eps); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// parseEndpoints reads "name=host:port,host:port,...". A bare address is named
// after its host.
func parseEndpoints(s string) ([]Endpoint, error) {
	var out []Endpoint
	seen := map[string]bool{}
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, addr, ok := strings.Cut(part, "=")
		if !ok {
			addr = part
			name, _, _ = net.SplitHostPort(addr)
			if name == "" {
				name = addr
			}
		}
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", part, err)
		}
		if seen[name] {
			return nil, fmt.Errorf("duplicate endpoint name %q", name)
		}
		seen[name] = true
		out = append(out, Endpoint{Name: name, Addr: addr})
	}
	if len(out) == 0 {
		return nil, errors.New("no endpoints")
	}
	return out, nil
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && (args[0] == "-version" || args[0] == "--version") {
		_, _ = fmt.Fprintln(stdout, version.String("raftkv-observer"))
		return 0
	}
	cfg, err := parseFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		_, _ = fmt.Fprintln(stderr, "raftkv-observer:", err)
		return 2
	}
	targets := make([]Target, 0, len(cfg.endpoints))
	for _, ep := range cfg.endpoints {
		// Connections are lazy: an unreachable node only fails its own polls.
		conn, err := grpc.NewClient(ep.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "raftkv-observer:", err)
			return 2
		}
		defer func() { _ = conn.Close() }()
		cl := raftkvv1.NewClusterClient(conn)
		targets = append(targets, Target{Endpoint: ep, Status: func(ctx context.Context) (*raftkvv1.StatusResponse, error) {
			return cl.Status(ctx, &raftkvv1.StatusRequest{})
		}})
	}

	lis, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "raftkv-observer:", err)
		return 1
	}
	h := newHub()
	srv := &http.Server{Handler: h.handler(), ReadHeaderTimeout: 5 * time.Second,
		// Request contexts end with ctx, so open SSE streams close on shutdown.
		BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() { _ = srv.Serve(lis) }()
	_, _ = fmt.Fprintf(stderr, "raftkv-observer: serving http://%s, polling %d nodes every %s\n",
		lis.Addr(), len(targets), cfg.interval)

	pollLoop(ctx, targets, cfg.interval, cfg.rpcTimeout, NewObserver(cfg.interval), h)

	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
	return 0
}
