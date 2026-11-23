package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestVersionAndConfigErrors(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"-version"}, &out, &errOut); code != exitOK || !strings.Contains(out.String(), "raftkvd") {
		t.Fatalf("-version: exit %d, out %q", code, out.String())
	}
	errOut.Reset()
	if code := run([]string{"-node-id", "1"}, &out, &errOut); code != exitConfig || !strings.Contains(errOut.String(), "data_dir") {
		t.Fatalf("invalid config: exit %d, stderr %q", code, errOut.String())
	}
}

// A data directory that belongs to another node makes raftkvd refuse to start
// with the config exit code (WBS 2.3.4, D-32).
func TestRefusesIdentityMismatch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "d")
	// First run as node 1 and stop it with SIGTERM (WBS 2.3.5).
	args := func(id string) []string {
		return []string{"-node-id", id, "-data-dir", dir, "-peers", "1=127.0.0.1:0,2=127.0.0.1:0",
			"-listen", "127.0.0.1:0", "-tick-interval", "10ms"}
	}
	var out bytes.Buffer
	errOut := &syncBuffer{}
	done := make(chan int, 1)
	go func() { done <- run(args("1"), &out, errOut) }()
	waitFor(t, func() bool { return strings.Contains(errOut.String(), "node started") })
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-done:
		if code != exitOK {
			t.Fatalf("exit %d after SIGTERM, stderr: %s", code, errOut.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("raftkvd did not stop on SIGTERM")
	}

	errOut.Reset()
	if code := run(args("2"), &out, errOut); code != exitConfig || !strings.Contains(errOut.String(), "identity mismatch") {
		t.Fatalf("foreign data dir: exit %d, stderr %q", code, errOut.String())
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

// syncBuffer is a bytes.Buffer the node's log goroutine and the test can share.
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

func (s *syncBuffer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.b.Reset()
}
