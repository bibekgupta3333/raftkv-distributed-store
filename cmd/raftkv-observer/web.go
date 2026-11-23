package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

//go:embed web/index.html
var webFS embed.FS

// StatusFunc calls Cluster.Status on one node.
type StatusFunc func(ctx context.Context) (*raftkvv1.StatusResponse, error)

// Target is an endpoint and the function that polls it.
type Target struct {
	Endpoint
	Status StatusFunc
}

// pollAll calls Status on every target concurrently, each bounded by timeout,
// and returns the results in target order.
func pollAll(ctx context.Context, targets []Target, timeout time.Duration) []Poll {
	out := make([]Poll, len(targets))
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			resp, err := t.Status(cctx)
			out[i] = Poll{Endpoint: t.Endpoint, Resp: resp, Err: err}
		}()
	}
	wg.Wait()
	return out
}

// hub fans the latest State (as JSON) out to SSE subscribers. A slow browser
// only ever misses intermediate states: each subscriber channel holds one
// message and a newer one replaces it.
type hub struct {
	mu   sync.Mutex
	last []byte
	subs map[chan []byte]struct{}
}

func newHub() *hub { return &hub{subs: map[chan []byte]struct{}{}} }

func (h *hub) publish(b []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.last = b
	for ch := range h.subs {
		select { // drop the stale message, if any, then send
		case <-ch:
		default:
		}
		ch <- b
	}
}

func (h *hub) subscribe() (chan []byte, []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := make(chan []byte, 1)
	h.subs[ch] = struct{}{}
	return ch, h.last
}

func (h *hub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subs, ch)
}

func (h *hub) latest() []byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.last
}

// handler serves the read-only observer: the page, the SSE stream and the
// current state as JSON. Everything else is 404 or 405 (D-48: read-only).
func (h *hub) handler() http.Handler {
	page, err := webFS.ReadFile("web/index.html")
	if err != nil {
		panic(err) // embedded at build time
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(page)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, _ *http.Request) {
		b := h.latest()
		if b == nil {
			http.Error(w, "no poll yet", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	})
	mux.HandleFunc("GET /events", h.serveSSE)
	return mux
}

func (h *hub) serveSSE(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	ch, last := h.subscribe()
	defer h.unsubscribe(ch)
	write := func(b []byte) bool {
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return false
		}
		fl.Flush()
		return true
	}
	if last != nil && !write(last) {
		return
	}
	fl.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case b := <-ch:
			if !write(b) {
				return
			}
		}
	}
}

// pollLoop polls every interval until ctx ends, folding each round into obs and
// publishing the result.
func pollLoop(ctx context.Context, targets []Target, interval, timeout time.Duration, obs *Observer, h *hub) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		st := obs.Update(time.Now(), pollAll(ctx, targets, timeout))
		if b, err := json.Marshal(st); err == nil {
			h.publish(b)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
