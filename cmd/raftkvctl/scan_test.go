package main

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/kvtest"
)

// WBS 4.4.4 smoke: cas and range against a live 3-node cluster.
func TestCASAndRange(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	c.Leader(15 * time.Second)
	ep := "--endpoints=" + strings.Join(c.Endpoints(), ",")

	if code, out, errOut := ctl(t, "cas", ep, "--expect-absent", "--new-value", "v1", "lock"); code != 0 || !strings.HasPrefix(out, "OK revision=") {
		t.Fatalf("cas create: exit %d out %q err %q", code, out, errOut)
	}
	if code, out, _ := ctl(t, "cas", ep, "--expect-absent", "--new-value", "v2", "lock"); code != 4 || !strings.Contains(out, "current=v1") {
		t.Fatalf("cas mismatch: exit %d out %q, want 4 and the current value", code, out)
	}
	if code, out, _ := ctl(t, "cas", ep, "--expect-value", "v1", "--delete", "lock"); code != 0 {
		t.Fatalf("cas delete: exit %d out %q", code, out)
	}
	if code, _, _ := ctl(t, "cas", ep, "--expect-value", "x", "lock"); code != 2 {
		t.Fatalf("cas without an action: exit %d, want 2 (usage)", code)
	}
	if code, _, _ := ctl(t, "cas", ep, "--expect-revision", "0", "--new-value", "x", "lock"); code != 1 {
		t.Fatalf("cas with revision 0: exit %d, want 1 (INVALID_ARGUMENT)", code)
	}

	for _, k := range []string{"a/1", "a/2", "a/3", "b/1"} {
		if code, _, errOut := ctl(t, "put", ep, k, "val-"+k); code != 0 {
			t.Fatalf("put %s: %s", k, errOut)
		}
	}
	if code, out, _ := ctl(t, "range", ep, "--prefix", "a/"); code != 0 || out != "a/1 val-a/1\na/2 val-a/2\na/3 val-a/3\n" {
		t.Fatalf("range --prefix: exit %d out %q", code, out)
	}
	code, out, _ := ctl(t, "range", ep, "--limit", "2", "--keys-only", "a/", "b/")
	m := regexp.MustCompile(`^a/1\na/2\nmore=true next=([0-9a-f]+)\n$`).FindStringSubmatch(out)
	if code != 0 || m == nil {
		t.Fatalf("one page: exit %d out %q", code, out)
	}
	if code, out, _ := ctl(t, "range", ep, "--limit", "2", "--keys-only", "--page-token", m[1], "a/", "b/"); code != 0 || out != "a/3\n" {
		t.Fatalf("next page: exit %d out %q", code, out)
	}
	if code, out, _ := ctl(t, "range", ep, "--all", "--limit", "1", "--reverse", "--keys-only", "a"); code != 0 || out != "b/1\na/3\na/2\na/1\n" {
		t.Fatalf("range --all --reverse: exit %d out %q", code, out)
	}
	if code, _, _ := ctl(t, "range", ep, "--limit", "1001", "a"); code != 1 {
		t.Fatalf("range --limit 1001: exit %d, want 1", code)
	}
}
