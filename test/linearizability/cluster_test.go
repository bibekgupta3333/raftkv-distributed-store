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
// partitions the leader and then a minority away. Since M4 reads use ReadIndex
// and half the workers read from a fixed member, so followers and partitioned
// old leaders serve (or refuse) linearizable reads too. Every operation is recorded
// and the history must check as linearizable: 0 violations. The checker's
// timeout counts as failure.
func TestClusterIsLinearizableUnderFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second fault run")
	}
	c := kvtest.New(t, 5, kvtest.Options{})
	checkUnderFaults(t, c, nemesis)
}

// WBS 5.5.2 (M5 exit criterion 5): the same check with a very low snapshot
// threshold while a nemesis wipes followers' data directories and partitions
// a follower until the log is compacted past it. Wiped and lagging followers
// come back through InstallSnapshot (D-14) while clients keep going: 0
// violations, and every replica ends with the same state.
func TestClusterIsLinearizableWithFollowerWipes(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second fault run")
	}
	c := kvtest.New(t, 5, kvtest.Options{SnapshotEntries: 30, SnapshotRetainEntries: 5, WALSegmentBytes: 16 << 10})
	checkUnderFaults(t, c, wipeNemesis)
	installs := uint64(0)
	for _, id := range c.IDs() {
		installs += c.Node(id).Metrics().SnapshotInstalls()
	}
	t.Logf("snapshots installed during the run: %d", installs)
	if installs == 0 {
		t.Fatal("no follower was brought back by a snapshot; the run proves little")
	}
}

// wipeNemesis wipes a follower twice, replacing it each time by a new learner
// (D-32: a wiped node may not come back under its old ID), and partitions one
// past compaction.
func wipeNemesis(t *testing.T, ctx context.Context, c *kvtest.Cluster) {
	t.Helper()
	step := func(d time.Duration) bool { return sleepCtx(ctx, d) }
	follower := func(k int) uint64 {
		lead := c.Leader(15 * time.Second)
		var fs []uint64
		for _, id := range c.IDs() {
			if id != lead {
				fs = append(fs, id)
			}
		}
		return fs[k%len(fs)]
	}
	for k := range 2 {
		if !step(600 * time.Millisecond) {
			return
		}
		v := follower(k)
		t.Logf("nemesis: wipe follower %d and replace it by a new learner", v)
		c.Stop(v)
		c.Wipe(v)
		c.RemoveMember(v, 15*time.Second)
		if !step(500 * time.Millisecond) {
			return
		}
		id := c.AddLearner(15 * time.Second)
		c.Promote(id, 15*time.Second)
		t.Logf("nemesis: %d joined as %d", v, id)
	}
	if !step(600 * time.Millisecond) {
		return
	}
	v := follower(2)
	var rest []uint64
	for _, id := range c.IDs() {
		if id != v {
			rest = append(rest, id)
		}
	}
	t.Logf("nemesis: partition follower %d past compaction", v)
	c.Partition([]uint64{v}, rest)
	if !step(2000 * time.Millisecond) {
		c.Heal()
		return
	}
	c.Heal()
	step(1500 * time.Millisecond)
}

// checkUnderFaults runs the recorded workload against c while nem injects
// faults, then checks the history and the replicas' state hashes.
func checkUnderFaults(t *testing.T, c *kvtest.Cluster, nem func(*testing.T, context.Context, *kvtest.Cluster)) {
	t.Helper()
	c.Leader(15 * time.Second)

	rec := NewRecorder()
	const workers = 6
	wl := NewWorkload(rec, WorkloadConfig{Workers: workers, Groups: 2, Keys: 3, OpTime: 4 * time.Second, Seed: 1})

	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	nemesisDone := make(chan struct{})
	go func() {
		defer close(nemesisDone)
		defer stop()
		nem(t, runCtx, c)
	}()

	newClient := func(endpoints []string, noSessions bool) *client.Client {
		cl, err := client.New(client.Config{Endpoints: endpoints, AttemptTimeout: 1500 * time.Millisecond,
			ConnectTimeout: 200 * time.Millisecond, DisableSessions: noSessions})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cl.Close() })
		return cl
	}
	eps := c.Endpoints()
	kvs := make([]KV, workers)
	for i := range kvs {
		// Worker 1 writes without a session, so a write whose outcome is
		// unknown surfaces as indeterminate ("info") instead of being
		// retried; the others retry exactly-once (D-09).
		cl := newClient(eps, i == 1)
		kvs[i] = cl
		if i%2 == 0 {
			// WBS 4.2.2: half the workers read from one fixed member,
			// so linearizable reads are served by followers as well as
			// the leader (ReadIndex via the leader, D-05).
			kvs[i] = PinnedReads{KV: cl, Reads: newClient([]string{eps[(i/2)%len(eps)]}, false)}
		}
	}
	wl.Run(runCtx, func(worker int) KV { return kvs[worker] })
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
	// WBS 4.6.1: every kind of operation took part in the checked history.
	mix := Breakdown(rec.Events())
	t.Logf("checked operations by kind: %v", mix)
	for _, kind := range []string{FPut, FGet, FDelete, "cas-swapped", "cas-failed", "range-page"} {
		if mix[kind] == 0 {
			t.Errorf("no successful %s in the history", kind)
		}
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
