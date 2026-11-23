package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/kvtest"
)

// readEvent returns the payload of the next SSE "data:" message.
func readEvent(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("reading SSE stream: %v", err)
		}
		if data, ok := strings.CutPrefix(line, "data: "); ok {
			return strings.TrimSpace(data)
		}
	}
}

// WBS 7.2.1: the page and the SSE stream; the endpoint is read-only.
func TestHTTPAndSSE(t *testing.T) {
	h := newHub()
	srv := httptest.NewServer(h.handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), "RaftKV Observer") ||
		!strings.Contains(string(body), `EventSource("events")`) {
		t.Fatalf("GET / = %d\n%s", resp.StatusCode, body)
	}
	if resp, err := http.Post(srv.URL+"/", "text/plain", nil); err != nil || resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST / = %v, %v; want 405", resp, err)
	}
	if resp, err := http.Get(srv.URL + "/api/state"); err != nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("GET /api/state before a poll = %v, %v; want 503", resp, err)
	}

	h.publish([]byte(`{"n":1}`))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/events", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}
	r := bufio.NewReader(resp.Body)
	if got := readEvent(t, r); got != `{"n":1}` {
		t.Fatalf("first event (latest state on connect) = %s", got)
	}
	h.publish([]byte(`{"n":2}`))
	if got := readEvent(t, r); got != `{"n":2}` {
		t.Fatalf("second event = %s", got)
	}
	if resp, err := http.Get(srv.URL + "/api/state"); err != nil || resp.StatusCode != 200 {
		t.Fatalf("GET /api/state = %v, %v", resp, err)
	}
}

// WBS 7.2.1/7.2.2 end to end against a real in-process cluster: the observer
// finds the leader, and a killed leader shows up in the timeline within one
// poll (well under 1 s).
func TestObserverAgainstCluster(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	lead := c.Leader(15 * time.Second)
	var targets []Target
	for _, id := range c.IDs() {
		conn, err := grpc.NewClient(c.Addr(id), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		cl := raftkvv1.NewClusterClient(conn)
		name := "n" + string(rune('0'+id))
		targets = append(targets, Target{Endpoint: Endpoint{name, c.Addr(id)},
			Status: func(ctx context.Context) (*raftkvv1.StatusResponse, error) {
				return cl.Status(ctx, &raftkvv1.StatusRequest{})
			}})
	}
	const interval = 500 * time.Millisecond
	o := NewObserver(interval)
	ctx := context.Background()

	s := o.Update(time.Now(), pollAll(ctx, targets, 400*time.Millisecond))
	if s.LeaderID != lead || s.Term == 0 || s.ClusterID == 0 {
		t.Fatalf("observer header = term %d leader %d cluster %d; want leader %d", s.Term, s.LeaderID, s.ClusterID, lead)
	}
	for _, n := range s.Nodes {
		if !n.Up || n.Role == "" {
			t.Fatalf("node %+v not up", n)
		}
	}
	b, err := json.Marshal(s)
	if err != nil || !strings.Contains(string(b), `"leaderName":"n`) {
		t.Fatalf("state JSON = %s, %v", b, err)
	}

	c.Stop(lead)
	start := time.Now()
	s = o.Update(time.Now(), pollAll(ctx, targets, 400*time.Millisecond))
	if took := time.Since(start); took > time.Second {
		t.Fatalf("poll took %v", took)
	}
	leadName := targets[lead-1].Name
	found := false
	for _, e := range s.Events {
		found = found || (e.Kind == EventUnreachable && e.Node == leadName)
	}
	if !found {
		t.Fatalf("leader kill missing from timeline: %q", texts(s.Events))
	}
}
