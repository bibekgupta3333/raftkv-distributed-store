package main

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/kvtest"
)

func ctl(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = run(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

// WBS 3.3.3 smoke: put, get, del against a live 3-node cluster, then
// `debug state-hash` on every stopped replica (WBS 3.1.8).
func TestPutGetDelAndStateHash(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	c.Leader(15 * time.Second)
	ep := "--endpoints=" + strings.Join(c.Endpoints(), ",")

	if code, out, errOut := ctl(t, "put", ep, "color", "blue"); code != 0 || !strings.HasPrefix(out, "OK revision=") {
		t.Fatalf("put: exit %d out %q err %q", code, out, errOut)
	}
	if code, out, _ := ctl(t, "get", ep, "color"); code != 0 || out != "blue\n" {
		t.Fatalf("get: exit %d out %q", code, out)
	}
	if code, out, _ := ctl(t, "get", ep, "--hex", "color"); code != 0 || out != "626c7565\n" {
		t.Fatalf("get --hex: exit %d out %q", code, out)
	}
	f := filepath.Join(t.TempDir(), "val")
	if err := os.WriteFile(f, []byte("from file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, out, errOut := ctl(t, "put", ep, "--prev", "--value-file", f, "color"); code != 0 || !strings.Contains(out, "previous=blue") {
		t.Fatalf("put --prev: exit %d out %q err %q", code, out, errOut)
	}
	if code, out, _ := ctl(t, "del", ep, "color"); code != 0 || !strings.Contains(out, "deleted=true") {
		t.Fatalf("del: exit %d out %q", code, out)
	}
	if code, _, errOut := ctl(t, "get", ep, "color"); code != 3 || !strings.Contains(errOut, "not found") {
		t.Fatalf("get missing: exit %d err %q, want 3", code, errOut)
	}
	if code, _, _ := ctl(t, "put", ep, "", "x"); code != 1 {
		t.Fatalf("put with an empty key: exit %d, want 1 (INVALID_ARGUMENT)", code)
	}
	if code, _, _ := ctl(t, "put", ep, "only-key"); code != 2 {
		t.Fatalf("put with missing value: exit %d, want 2 (usage)", code)
	}
	ctl(t, "put", ep, "keep", "me")

	c.StateHashes(10 * time.Second) // wait for the replicas to converge
	for _, id := range c.IDs() {
		c.Stop(id)
	}
	re := regexp.MustCompile(`^applied_index=(\d+) state_hash=([0-9a-f]{64})\n$`)
	var first string
	for _, id := range c.IDs() {
		code, out, errOut := ctl(t, "debug", "state-hash", "-data-dir", c.DataDir(id))
		m := re.FindStringSubmatch(out)
		if code != 0 || m == nil {
			t.Fatalf("state-hash node %d: exit %d out %q err %q", id, code, out, errOut)
		}
		if first == "" {
			first = m[2]
		} else if m[2] != first {
			t.Fatalf("node %d hash %s differs from %s", id, m[2], first)
		}
	}
	if code, _, _ := ctl(t, "debug", "state-hash"); code != 2 {
		t.Fatalf("state-hash without -data-dir: exit %d", code)
	}
	if code, _, _ := ctl(t, "debug", "state-hash", "-data-dir", t.TempDir()); code != 1 {
		t.Fatalf("state-hash of a foreign dir: exit %d", code)
	}
}
