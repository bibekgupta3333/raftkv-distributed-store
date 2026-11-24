package node_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
)

// WBS 2.3.6: wal-dump prints a data dir's entries, and replicas that agree have
// identical digests.
func TestDumpWALComparesReplicas(t *testing.T) {
	c := newTestCluster(t, 3)
	lead := c.leader()
	for i := range 5 {
		if _, err := lead.ProposeAndWait(context.Background(), []byte(fmt.Sprintf("dump-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for id, n := range c.nodes {
		waitUntil(t, 10*time.Second, fmt.Sprintf("node %d applied", id), func() bool { return len(commandsOf(n)) == 5 })
	}
	// Followers first: a stopping leader hands leadership over (WBS 6.2.3),
	// and the new leader's no-op would make the logs differ.
	for id := range c.nodes {
		if id != lead.ID() {
			c.stop(id)
		}
	}
	c.stop(lead.ID())

	var summaries []string
	for id := uint64(1); id <= 3; id++ {
		var out bytes.Buffer
		if err := node.DumpWAL(&out, c.cfgs[id].DataDir, node.DumpOptions{}); err != nil {
			t.Fatalf("DumpWAL(%d): %v", id, err)
		}
		text := out.String()
		if !strings.Contains(text, fmt.Sprintf("node_id=%d cluster_id=1", id)) ||
			!strings.Contains(text, `data="dump-4"`) || !strings.Contains(text, "hard_state term=") {
			t.Fatalf("node %d dump is missing expected content:\n%s", id, text)
		}
		lines := strings.Split(strings.TrimSpace(text), "\n")
		summaries = append(summaries, lines[len(lines)-1])
	}
	// Every node holds the leader's no-op plus 5 commands; compare through the
	// first 6 entries via the digest of the full log, which must match when the
	// logs are equal (a follower may lag by a trailing commit marker only in
	// HardState, never in entries).
	for _, s := range summaries[1:] {
		if s != summaries[0] {
			t.Fatalf("replica logs differ:\n%s\n%s", summaries[0], s)
		}
	}

	var ranged bytes.Buffer
	if err := node.DumpWAL(&ranged, c.cfgs[1].DataDir, node.DumpOptions{From: 3, To: 4, MaxData: 4}); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(ranged.String(), "entry index="); n != 2 {
		t.Fatalf("range dump printed %d entries, want 2:\n%s", n, ranged.String())
	}
	if !strings.Contains(ranged.String(), `data="dump"...`) {
		t.Fatalf("data was not truncated to MaxData:\n%s", ranged.String())
	}
}

func TestDumpWALRejectsForeignDirectory(t *testing.T) {
	var out bytes.Buffer
	if err := node.DumpWAL(&out, t.TempDir(), node.DumpOptions{}); err == nil {
		t.Fatal("a directory with no identity file must be rejected")
	}
}
