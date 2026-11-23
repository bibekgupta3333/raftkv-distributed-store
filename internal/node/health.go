package node

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Health probes for Kubernetes (D-46, WBS 7.1.4). They are served next to
// /metrics on the metrics listener:
//
//	/livez   alive: the driver loop answers a ping within livezTimeout.
//	/readyz  ready: this node knows the current leader and has applied every
//	         entry it knows to be committed.
//
// Liveness deliberately says nothing about leadership: a node in a minority
// partition is healthy and must not be restarted (that would only add churn).
// Readiness gates the client Service, so clients are not sent to a node that
// cannot route or serve a request yet.

// livezTimeout bounds how long /livez waits for the loop. A loop stuck longer
// than this (e.g. on a hung fsync) is reported dead.
const livezTimeout = time.Second

// Errors returned by [Node.Live] and [Node.Ready].
var (
	ErrNotRunning = errors.New("node: driver loop not running")
	ErrNoLeader   = errors.New("node: no known leader")
	ErrLagging    = errors.New("node: applied index behind commit index")
)

// Live reports whether the driver loop is running and responsive: it sends a
// ping through the loop and waits for the answer (D-46).
func (n *Node) Live(ctx context.Context) error {
	select {
	case <-n.started:
	default:
		return ErrNotRunning
	}
	pong := make(chan struct{})
	select {
	case n.pingCh <- pong:
	case <-n.stopping:
		return ErrNotRunning
	case <-ctx.Done():
		return fmt.Errorf("node: driver loop did not take a ping: %w", ctx.Err())
	}
	select {
	case <-pong:
		return nil
	case <-n.stopping:
		return ErrNotRunning
	case <-ctx.Done():
		return fmt.Errorf("node: driver loop did not answer a ping: %w", ctx.Err())
	}
}

// Ready reports whether the node knows the leader and has caught up: its
// applied index has reached the commit index it knows of (D-46). It reads the
// published status only, so it never waits on the loop.
func (n *Node) Ready() error {
	select {
	case <-n.started:
	default:
		return ErrNotRunning
	}
	select {
	case <-n.stopping:
		return ErrNotRunning
	default:
	}
	st := n.Status()
	if st.Lead == raft.None {
		return ErrNoLeader
	}
	if st.Applied < st.Commit {
		return fmt.Errorf("%w: applied %d, commit %d", ErrLagging, st.Applied, st.Commit)
	}
	return nil
}

// HealthHandler serves /livez and /readyz: 200 "ok", or 503 with the reason.
func (n *Node) HealthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), livezTimeout)
		defer cancel()
		writeProbe(w, n.Live(ctx))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		writeProbe(w, n.Ready())
	})
	return mux
}

func writeProbe(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintln(w, err)
		return
	}
	_, _ = fmt.Fprintln(w, "ok")
}

// httpHandler is everything the metrics listener serves.
func (n *Node) httpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", n.metrics.Handler())
	health := n.HealthHandler()
	mux.Handle("/livez", health)
	mux.Handle("/readyz", health)
	return mux
}
