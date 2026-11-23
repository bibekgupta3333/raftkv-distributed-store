package main

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
)

func TestWALDumpCommand(t *testing.T) {
	cfg := node.DefaultConfig()
	cfg.NodeID, cfg.DataDir = 1, t.TempDir()
	cfg.Peers = []node.PeerConfig{{ID: 1, Addr: "127.0.0.1:0"}}
	n, err := node.New(cfg, node.Options{LogWriter: io.Discard}) // creates identity and an empty WAL
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	var out, errOut bytes.Buffer
	if code := run([]string{"debug", "wal-dump", "-data-dir", cfg.DataDir}, &out, &errOut); code != 0 {
		t.Fatalf("exit %d: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "node_id=1 cluster_id=1") || !strings.Contains(out.String(), "summary entries=0") {
		t.Fatalf("unexpected output:\n%s", out.String())
	}
}

func TestUsageAndErrors(t *testing.T) {
	for name, args := range map[string][]string{
		"no args":         nil,
		"unknown command": {"frobnicate"},
		"no data dir":     {"debug", "wal-dump"},
		"bad debug":       {"debug", "nope"},
	} {
		var out, errOut bytes.Buffer
		if code := run(args, &out, &errOut); code != 2 {
			t.Errorf("%s: exit %d, want 2", name, code)
		}
	}
	var out, errOut bytes.Buffer
	if code := run([]string{"debug", "wal-dump", "-data-dir", t.TempDir()}, &out, &errOut); code != 1 {
		t.Errorf("foreign dir: exit %d, want 1", code)
	}
	if code := run([]string{"-version"}, &out, &errOut); code != 0 {
		t.Errorf("-version: exit %d", code)
	}
}
