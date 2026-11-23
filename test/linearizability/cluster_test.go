package linearizability

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/kvtest"
	"github.com/bibekgupta3333/raftkv-distributed-store/pkg/client"
)

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// WBS 3.4.3: concurrent clients hammer a real 5-node cluster (real gRPC on
// localhost) while a nemesis kills the leader, kills a follower, and
// partitions the leader and then a minority away. Every operation is recorded
// and the history must check as linearizable: 0 violations. The checker's
// timeout counts as failure.
func TestClusterIsLinearizableUnderFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second fault run")
	}
	c := kvtest.New(t, 5, kvtest.Options{})
	c.Leader(15 * time.Second)

	rec := NewRecorder()
	wl := NewWorkload(rec, WorkloadConfig{Workers: 6, Keys: 4, OpTime: 4 * time.Second, Seed: 1})

	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	nemesisDone := make(chan struct{})
	go func() {
		defer close(nemesisDone)
		defer stop()
		nemesis(t, runCtx, c)
	}()

	wl.Run(runCtx, func(int) KV {
		cl, err := client.New(client.Config{Endpoints: c.Endpoints(), AttemptTimeout: 1500 * time.Millisecond,
			ConnectTimeout: 200 * time.Millisecond})
		if err != nil {
			t.Error(err)
			return nil
		}
		t.Cleanup(func() { _ = cl.Close() })
		return cl
	})
	<-nemesisDone

	// Everything is healed; the cluster must still serve and converge.
	c.Heal()
	for _, id := range c.IDs() {
		if c.Node(id) == nil {
			c.Start(id)
		}
	}
	c.Leader(20 * time.Second)

	rep := CheckRecorder(rec, DefaultTimeout)
	t.Logf("history: %d events, %d ops checked, %s, verdict %s", len(rec.Events()), rep.Ops, rep.Stats, rep.Verdict)
	if !rep.OK() {
		dir := t.ArtifactDir()
		html := filepath.Join(dir, "linearizability-failure.html")
		if err := rep.SaveHTML(html); err != nil {
			t.Errorf("save visualization: %v", err)
		}
		if f, err := os.Create(filepath.Join(dir, "history.jsonl")); err == nil {
			_ = rec.WriteJSONL(f)
			_ = f.Close()
		}
		t.Fatalf("history is not linearizable (%s); visualization: %s", rep.Verdict, html)
	}
	if rep.Stats.OK < 100 {
		t.Fatalf("only %d operations succeeded; the run proves little", rep.Stats.OK)
	}
	if rep.Stats.Info == 0 {
		t.Log("note: no indeterminate operations occurred in this run")
	}

	// INV-5: replicas that applied the same entries hold the same state.
	hashes := c.StateHashes(20 * time.Second)
	var first [32]byte
	for id, h := range hashes {
		if first == ([32]byte{}) {
			first = h
		} else if h != first {
			t.Fatalf("node %d state differs after the run: %x", id, hashes)
		}
	}
}

// nemesis injects the faults of WBS 3.4.3, one at a time, with quiet gaps.
func nemesis(t *testing.T, ctx context.Context, c *kvtest.Cluster) {
	t.Helper()
	step := func(d time.Duration) bool { return sleepCtx(ctx, d) }
	others := func(except ...uint64) []uint64 {
		var out []uint64
	next:
		for _, id := range c.IDs() {
			for _, e := range except {
				if id == e {
					continue next
				}
			}
			out = append(out, id)
		}
		return out
	}

	if !step(700 * time.Millisecond) {
		return
	}
	// 1. kill the leader, run without it, restart it
	lead := c.Leader(15 * time.Second)
	t.Logf("nemesis: kill leader %d", lead)
	c.Stop(lead)
	if !step(1500 * time.Millisecond) {
		return
	}
	c.Start(lead)
	if !step(700 * time.Millisecond) {
		return
	}

	// 2. kill a follower
	lead = c.Leader(15 * time.Second)
	victim := others(lead)[0]
	t.Logf("nemesis: kill follower %d (leader %d)", victim, lead)
	c.Stop(victim)
	if !step(1200 * time.Millisecond) {
		return
	}
	c.Start(victim)
	if !step(700 * time.Millisecond) {
		return
	}

	// 3. partition the leader away from the other four
	lead = c.Leader(15 * time.Second)
	t.Logf("nemesis: isolate leader %d", lead)
	c.Partition([]uint64{lead}, others(lead))
	if !step(2500 * time.Millisecond) {
		c.Heal()
		return
	}
	c.Heal()
	if !step(1000 * time.Millisecond) {
		return
	}

	// 4. partition a two-node minority from the three-node majority
	lead = c.Leader(15 * time.Second)
	minority := []uint64{lead, others(lead)[0]}
	t.Logf("nemesis: minority partition %v", minority)
	c.Partition(minority, others(minority...))
	if !step(2500 * time.Millisecond) {
		c.Heal()
		return
	}
	c.Heal()
	step(1000 * time.Millisecond)
}
