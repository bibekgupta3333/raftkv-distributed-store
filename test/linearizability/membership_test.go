package linearizability

import (
	"context"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/kvtest"
)

// Membership changes under load (WBS 6.5.1, 6.5.2, in process). The docker
// versions live in test/e2e; these run in `make test` and check the same
// thing: concurrent clients see a linearizable history while the
// configuration changes one server at a time (D-06).

func leaderAndFollower(c *kvtest.Cluster) (leader, follower uint64) {
	leader = c.Leader(15 * time.Second)
	for _, id := range c.IDs() {
		if id != leader {
			return leader, id
		}
	}
	return leader, 0
}

// WBS 6.5.1: replace a follower, then the leader itself (add a learner,
// let it catch up, promote it, remove the old member) while clients run.
func TestReplaceNodeUnderLoadIsLinearizable(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second membership run")
	}
	c := kvtest.New(t, 3, kvtest.Options{SnapshotEntries: 50, SnapshotRetainEntries: 10})
	checkUnderFaults(t, c, func(t *testing.T, ctx context.Context, c *kvtest.Cluster) {
		if !sleepCtx(ctx, 500*time.Millisecond) {
			return
		}
		leader, follower := leaderAndFollower(c)
		id := c.Replace(follower, 15*time.Second)
		t.Logf("membership: replaced follower %d by %d", follower, id)
		if !sleepCtx(ctx, 500*time.Millisecond) {
			return
		}
		id = c.Replace(leader, 15*time.Second)
		t.Logf("membership: replaced leader %d by %d (now led by %d)", leader, id, c.Leader(15*time.Second))
	})
	if n := len(c.IDs()); n != 3 {
		t.Fatalf("%d members after the replacements, want 3", n)
	}
}

// WBS 6.5.2: scale 3 -> 5 -> 3 under load, removing the leader on the way
// down, with a leadership transfer in between.
func TestScale3To5To3UnderLoadIsLinearizable(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second membership run")
	}
	c := kvtest.New(t, 3, kvtest.Options{SnapshotEntries: 50, SnapshotRetainEntries: 10})
	checkUnderFaults(t, c, func(t *testing.T, ctx context.Context, c *kvtest.Cluster) {
		if !sleepCtx(ctx, 300*time.Millisecond) {
			return
		}
		for range 2 {
			id := c.AddLearner(15 * time.Second)
			c.Promote(id, 15*time.Second)
			t.Logf("membership: added voter %d (%d members)", id, len(c.IDs()))
		}
		if !sleepCtx(ctx, 300*time.Millisecond) {
			return
		}
		leader, follower := leaderAndFollower(c)
		c.RemoveMember(leader, 15*time.Second)
		t.Logf("membership: removed leader %d", leader)
		c.RemoveMember(follower, 15*time.Second)
		t.Logf("membership: removed follower %d (%d members)", follower, len(c.IDs()))
	})
	if n := len(c.IDs()); n != 3 {
		t.Fatalf("%d members after scaling back, want 3", n)
	}
}
