package sim

import (
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Read-path scenarios (WBS 4.1.4, 4.2.5). The READ-1 and READ-2 checks run in
// every seed of every profile (reads.go); these tests drive the dangerous
// situations on purpose and assert on them directly.

// WBS 4.1.4: a partitioned old leader never completes a ReadIndex once a newer
// leader exists. Reads keep arriving at the old leader throughout; the ones
// issued after the new leader was elected must all fail or hang.
func TestSimReadPartitionedOldLeader(t *testing.T) {
	for _, seed := range []uint64{11, 12, 13} {
		c := newScenario(t, seed)
		oldID, oldTerm := c.Leader()
		if oldID == 0 {
			t.Fatal("no leader on a healthy cluster")
		}
		c.Net().Isolate(oldID, c.IDs())
		var after []uint64 // reads on the old leader issued after a newer leader existed
		newLeaderAt := int64(-1)
		for range 200 { // 10 s in 50 ms steps
			rid := c.Read(oldID)
			if newLeaderAt >= 0 {
				after = append(after, rid)
			}
			c.RunFor(50)
			if id, term := c.Leader(); newLeaderAt < 0 && id != oldID && term > oldTerm {
				newLeaderAt = c.sched.Now()
			}
		}
		if v := c.Violation(); v != nil {
			t.Fatalf("seed %d: %v", seed, v)
		}
		if newLeaderAt < 0 {
			t.Fatalf("seed %d: the majority never elected a new leader", seed)
		}
		if len(after) == 0 {
			t.Fatalf("seed %d: no read was issued after the new leader appeared", seed)
		}
		for _, rid := range after {
			if o := c.ReadOutcome(rid); o.Done && !o.Rejected {
				t.Fatalf("seed %d: the partitioned old leader served read %d at index %d after a newer leader existed", seed, rid, o.Index)
			}
		}
	}
}

// WBS 4.1.4: a read issued at a new leader before its no-op commits waits for
// the no-op and is served at or after it.
func TestSimReadRightAfterElectionWaitsForNoop(t *testing.T) {
	tested := 0
	for seed := uint64(20); seed < 40 && tested < 3; seed++ {
		c := newScenario(t, seed)
		oldID, oldTerm := c.Leader()
		c.Crash(oldID)
		var newID uint64
		for range 10_000 { // until a new leader appears, 1 ms at a time
			c.RunFor(1)
			if id, term := c.Leader(); id != 0 && term > oldTerm {
				newID = id
				break
			}
		}
		if newID == 0 {
			t.Fatalf("seed %d: no new leader", seed)
		}
		st, _ := c.Status(newID)
		if st.Commit >= st.LastIndex {
			continue // the no-op already committed; this seed cannot test the wait
		}
		noop := st.LastIndex
		rid := c.Read(newID)
		c.RunFor(2_000)
		if v := c.Violation(); v != nil {
			t.Fatalf("seed %d: %v", seed, v)
		}
		o := c.ReadOutcome(rid)
		if !o.Done || o.Rejected || o.Index < noop {
			t.Fatalf("seed %d: read at a fresh leader = %+v, want served at index >= no-op %d", seed, o, noop)
		}
		tested++
	}
	if tested == 0 {
		t.Fatal("no seed caught a leader before its no-op committed")
	}
}

// leaseScenario is a 5-node cluster with lease reads on. The leader's clock
// is then set to run at 100/period of real time, the leader is cut off, and
// reads keep arriving at it while the majority elects a new leader.
func leaseScenario(t *testing.T, seed uint64, leaderPeriod int64) (c *Cluster, leaseReads int) {
	t.Helper()
	p := mustProfile(t, "lease")
	p.Sizes, p.Weights = []int{5}, Weights{}
	c = NewCluster(seed, p, Hooks{}, false)
	c.Boot()
	c.StartWorkload()
	c.RunFor(5_000)
	old, _ := c.Leader()
	if old == 0 {
		t.Fatalf("seed %d: no leader", seed)
	}
	c.SetPeriod(old, leaderPeriod)
	c.RunFor(3 * leaderPeriod) // a few renewals on the drifted clock
	c.Net().Isolate(old, c.IDs())
	for range 300 { // 6 s in 20 ms steps
		if c.Violation() != nil {
			break
		}
		if o := c.ReadOutcome(c.Read(old)); o.Lease {
			leaseReads++
		}
		c.RunFor(20)
	}
	return c, leaseReads
}

// WBS 4.2.5: with clock drift inside the D-25 bound the lease is safe: the old
// leader serves lease reads while cut off, but none of them is stale.
func TestSimLeaseDriftWithinBound(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3, 4, 5} {
		c, leaseReads := leaseScenario(t, seed, 105) // ~5% slow, as slow as the default range allows
		if v := c.Violation(); v != nil {
			t.Fatalf("seed %d: violation within the drift bound: %v", seed, v)
		}
		if leaseReads == 0 {
			t.Fatalf("seed %d: the cut-off leader served no lease reads; the test exercised nothing", seed)
		}
	}
}

// WBS 4.2.5: with the leader's clock 5x slow (far beyond ρ = 0.1) its lease
// outlives the election of a new leader, and the simulator finds the stale
// read. This is the check that the lease check works.
func TestSimLeaseDriftBeyondBoundIsCaught(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3, 4, 5} {
		c, _ := leaseScenario(t, seed, 500)
		v := c.Violation()
		if v == nil || v.Kind != KindStaleRead {
			t.Fatalf("seed %d: want a %s violation with a 5x slow leader clock, got %v", seed, KindStaleRead, v)
		}
		t.Logf("seed %d: found as expected: %v", seed, v)
	}
}

// Lease reads stay off unless a profile asks for them (D-25).
func TestSimLeaseOffByDefault(t *testing.T) {
	for _, name := range ProfileNames() {
		p := mustProfile(t, name)
		if name != "lease" && p.LeaseReads {
			t.Errorf("profile %s has lease reads on", name)
		}
	}
	r := Run(1, RunOpts{Profile: mustProfile(t, "full")})
	if r.Stats.LeaseReads != 0 || r.Stats.ReadsServed == 0 {
		t.Fatalf("full profile: lease=%d served=%d, want 0 lease reads and some ReadIndex reads", r.Stats.LeaseReads, r.Stats.ReadsServed)
	}
}

// Test the read check itself: a node that answers reads at an index lower than
// the leader confirmed (a planted stale read) is caught as READ-1.
func TestSimPlantedStaleRead(t *testing.T) {
	planted(t, "none", 5, KindStaleRead, func() Hooks {
		return Hooks{Ready: func(_ uint64, rd *raft.Ready) {
			for i := range rd.ReadStates {
				if rs := &rd.ReadStates[i]; !rs.Rejected && rs.Index > 1 {
					rs.Index = 1
				}
			}
		}}
	})
}
