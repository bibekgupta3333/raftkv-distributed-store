//go:build e2e

// Run with `make e2e`. Environment:
//
//	E2E_PROFILE  pr (default) or nightly: picks round/step counts
//	E2E_SEED     seed for victim choice and the fault schedule (default 1)
//	E2E_ROUNDS   override the kill/restart round count (pr 20, nightly 200)
//	E2E_STEPS    override the schedule length (pr 6, nightly 40)
//	E2E_LIN_CYCLES override the linearizability fault cycles (pr 1, nightly 6)
//	E2E_OUT      artifact directory (default out/e2e)
//	E2E_KEEP     keep the cluster running after the test
package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
	"time"
)

func seed() uint64 {
	n, _ := strconv.ParseUint(envOr("E2E_SEED", "1"), 10, 64)
	return n
}

func nightly() bool { return envOr("E2E_PROFILE", "pr") == "nightly" }

func pick(prDefault, nightlyDefault int, env string) int {
	if nightly() {
		return envInt(env, nightlyDefault)
	}
	return envInt(env, prDefault)
}

// WBS 2.6.1 / 2.6.5: a leader is elected on start and the cluster commits.
func TestLeaderElectedOnStart(t *testing.T) {
	c := Start(t, "leader-on-start")
	lead := c.WaitLeader(30 * time.Second)
	t.Logf("leader is node %d", lead)
	// Exactly one node is leader, all others follow.
	leaders := 0
	for _, s := range c.States() {
		if s.State == stateLeader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("%d leaders, want 1: %+v", leaders, c.States())
	}
	idx := c.MustPropose("smoke")
	if got := c.WaitConverged(30 * time.Second); got < idx {
		t.Fatalf("converged at %d, below acked index %d", got, idx)
	}
}

// WBS 2.6.5: killing the leader elects a different one, which still commits.
func TestNewLeaderAfterLeaderKill(t *testing.T) {
	c := Start(t, "leader-kill")
	old := c.WaitLeader(30 * time.Second)
	c.MustPropose("before-kill")
	c.Kill(old)
	var rest []int
	for id := 1; id <= NumNodes; id++ {
		if id != old {
			rest = append(rest, id)
		}
	}
	nl := c.WaitNewLeader(30*time.Second, old, rest...)
	t.Logf("leader moved %d -> %d", old, nl)
	idx := c.MustPropose("after-kill", rest...)
	c.Restart(old)
	if got := c.WaitConverged(60 * time.Second); got < idx {
		t.Fatalf("converged at %d, below %d", got, idx)
	}
}

// WBS 2.6.5: a minority (including the old leader) cannot commit, the majority
// can, and after the partition heals all five logs are identical.
func TestMinorityStopsCommittingAndLogsMatchAfterHeal(t *testing.T) {
	c := Start(t, "partition")
	lead := c.WaitLeader(30 * time.Second)
	c.MustPropose("pre-partition")
	c.WaitConverged(30 * time.Second)

	buddy := lead%NumNodes + 1
	minority := []int{lead, buddy}
	var majority []int
	for id := 1; id <= NumNodes; id++ {
		if id != lead && id != buddy {
			majority = append(majority, id)
		}
	}
	c.Partition(minority...)

	// The majority elects its own leader and keeps committing.
	nl := c.WaitLeader(30*time.Second, majority...)
	t.Logf("old leader %d, majority leader %d", lead, nl)
	majIdx := c.MustPropose("majority-side", majority...)

	// The minority cannot commit. Whether or not the old leader still believes
	// it leads, an acknowledged proposal would be a safety violation.
	for _, id := range minority {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		idx, err := c.Propose(ctx, id, []byte(fmt.Sprintf("minority-%d", id)))
		cancel()
		if err == nil {
			t.Fatalf("minority node %d acknowledged a commit at index %d", id, idx)
		}
	}
	for _, id := range minority {
		s, err := c.State(id)
		if err != nil {
			t.Fatal(err)
		}
		if s.Commit >= majIdx {
			t.Fatalf("minority node %d commit index %d advanced to majority's %d", id, s.Commit, majIdx)
		}
	}

	c.Heal()
	final := c.MustPropose("post-heal")
	got := c.WaitConverged(60 * time.Second)
	if got < final {
		t.Fatalf("converged at %d, below %d", got, final)
	}

	// Logs match: byte-for-byte digests of every node's WAL are equal.
	c.StopAll()
	dumps := c.DumpWALs()
	ref := dumps[1].digestOf()
	for id, d := range dumps {
		if d.digestOf() != ref {
			t.Errorf("node %d log differs:\n  node 1: %s\n  node %d: %s", id, ref, id, d.digestOf())
		}
	}
}

// WBS 2.6.6: repeated kill -9 and restart of a random node (sometimes the
// leader) while proposing; every acknowledged entry is still in every log.
func TestKillRestartRoundsLoseNoCommittedEntry(t *testing.T) {
	rounds := pick(20, 200, "E2E_ROUNDS")
	c := Start(t, "kill-restart")
	r := rand.New(rand.NewPCG(seed(), 1))
	c.WaitLeader(30 * time.Second)

	acked := map[uint64]string{}
	propose := func(tag string, ids ...int) {
		t.Helper()
		data := fmt.Sprintf("%s-%d", tag, len(acked))
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		idx, err := c.ProposeAmong(ctx, 5*time.Second, []byte(data), ids...)
		if err != nil {
			t.Fatalf("round proposal: %v", err)
		}
		if prev, dup := acked[idx]; dup {
			t.Fatalf("index %d acknowledged twice: %q and %q", idx, prev, data)
		}
		acked[idx] = data
	}

	for round := 1; round <= rounds; round++ {
		propose(fmt.Sprintf("r%03da", round))
		victim := r.IntN(NumNodes) + 1
		if lead, ok := c.Leader(); ok && r.IntN(3) == 0 {
			victim = lead
		}
		c.Faults.Logf("round %d/%d victim=%d acked=%d", round, rounds, victim, len(acked))
		c.Kill(victim)
		var rest []int
		for id := 1; id <= NumNodes; id++ {
			if id != victim {
				rest = append(rest, id)
			}
		}
		propose(fmt.Sprintf("r%03db", round), rest...)
		c.Restart(victim)
		Eventually(t, 60*time.Second, fmt.Sprintf("round %d: node %d to rejoin", round, victim), func() bool {
			lead, ok := c.Leader()
			if !ok {
				return false
			}
			ls, _ := c.State(lead)
			vs, err := c.State(victim)
			return err == nil && vs.Applied >= ls.Applied-1 && vs.Term == ls.Term
		})
	}

	c.WaitConverged(60 * time.Second)
	c.StopAll()
	dumps := c.DumpWALs()
	idxs := make([]uint64, 0, len(acked))
	for idx := range acked {
		idxs = append(idxs, idx)
	}
	slices.Sort(idxs)
	for id, d := range dumps {
		for _, idx := range idxs {
			if got := d.Entries[idx]; got != acked[idx] {
				t.Fatalf("node %d: committed entry %d is %q, want %q (acked entries lost or rewritten)", id, idx, got, acked[idx])
			}
		}
	}
	ref := dumps[1].digestOf()
	for id, d := range dumps {
		if d.digestOf() != ref {
			t.Errorf("node %d log differs from node 1: %s vs %s", id, d.digestOf(), ref)
		}
	}
	t.Logf("%d rounds, %d acknowledged entries all present on 5 nodes", rounds, len(acked))
}

// WBS 2.6.4: a seeded mixed-fault schedule (kill, pause, partition, delay,
// loss) with continuous proposals; no acknowledged entry is lost and the
// cluster converges afterwards.
func TestSeededFaultSchedule(t *testing.T) {
	n := pick(6, 40, "E2E_STEPS")
	c := Start(t, "schedule")
	c.WaitLeader(30 * time.Second)
	kinds := DefaultKinds
	if nightly() {
		kinds = NightlyKinds // WBS 5.5.2: follower wipes too
	}
	steps := ScheduleKinds(seed(), n, kinds)
	c.Faults.Logf("schedule seed=%d steps=%d", seed(), n)

	acked := map[uint64]string{}
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			data := fmt.Sprintf("sched-%d", i)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			idx, err := c.ProposeAmong(ctx, 3*time.Second, []byte(data))
			cancel()
			if err == nil {
				acked[idx] = data // only this goroutine writes until it stops
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	for i, s := range steps {
		t.Logf("step %d/%d: %+v", i+1, n, s)
		c.Run(s)
	}
	close(done)
	<-stopped
	c.Heal()
	for id := 1; id <= NumNodes; id++ {
		c.NetemClear(id)
	}
	c.MustPropose("final")
	c.WaitConverged(90 * time.Second)

	c.StopAll()
	dumps := c.DumpWALs()
	for id, d := range dumps {
		for idx, data := range acked {
			if got := d.Entries[idx]; got != data {
				t.Fatalf("node %d: acked entry %d is %q, want %q", id, idx, got, data)
			}
		}
	}
	t.Logf("%d acknowledged entries survived %d faults", len(acked), n)
}

// WBS 5.5.1-5.5.2 (M5 exit criterion 2): with a low snapshot threshold, a
// follower's data volume is deleted and it restarts empty under its ID. It
// catches up through InstallSnapshot (the leader's log no longer reaches back
// to its start) and, once the cluster is stopped, every replica's state hash
// is identical. E2E_WIPE_RUNS rounds (pr 2, nightly 20), a different follower
// each time.
func TestWipedFollowerRejoinsViaSnapshot(t *testing.T) {
	runs := pick(2, 20, "E2E_WIPE_RUNS")
	c := StartEnv(t, "wipe-follower", "RAFTKV_SNAPSHOT_ENTRIES=100", "RAFTKV_SNAPSHOT_RETAIN=10")
	c.WaitLeader(30 * time.Second)
	r := rand.New(rand.NewPCG(seed(), 5))
	next := 0
	write := func(n int) {
		for range n {
			c.MustPropose(fmt.Sprintf("w-%d", next))
			next++
		}
	}
	write(150)
	for run := range runs {
		lead := c.WaitLeader(30 * time.Second)
		victim := 1 + r.IntN(NumNodes)
		for victim == lead {
			victim = 1 + r.IntN(NumNodes)
		}
		c.Kill(victim)
		write(150) // the leader snapshots and trims past the victim's log
		c.Wipe(victim)
		Eventually(t, 60*time.Second, fmt.Sprintf("node %d to install a snapshot", victim), func() bool {
			v, err := c.Metric(victim, "raftkv_snapshot_installs_total")
			return err == nil && v >= 1
		})
		write(20)
		c.WaitConverged(60 * time.Second)
		t.Logf("run %d/%d: wiped node %d rejoined via snapshot", run+1, runs, victim)
	}
	c.StopAll()
	hashes := c.StateHashes()
	for id, h := range hashes {
		if h != hashes[1] {
			t.Fatalf("state hashes differ: node %d %q vs node 1 %q", id, h, hashes[1])
		}
	}
	t.Logf("all 5 replicas: %s", hashes[1])
}
