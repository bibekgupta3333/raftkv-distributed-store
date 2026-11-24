//go:build e2e

package e2e

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/test/linearizability"
)

// M6 e2e: membership changes and leadership transfer on the compose cluster
// (WBS 6.2.3, 6.4.7, 6.5.1, 6.5.2).

func otherLive(c *Cluster, except ...int) []int {
	var out []int
	for _, k := range c.Live() {
		skip := false
		for _, e := range except {
			skip = skip || k == e
		}
		if !skip {
			out = append(out, k)
		}
	}
	return out
}

// WBS 6.2.3: SIGTERM on the leader (docker stop) hands leadership over before
// the process exits, so a new leader is visible almost at once instead of
// after an election timeout (1-2 s at 100 ms ticks). Repeated a few times,
// each on the then-current leader.
func TestSIGTERMLeaderHandsOver(t *testing.T) {
	rounds := pick(2, 5, "E2E_SIGTERM_ROUNDS")
	c := Start(t, "sigterm-handover")
	for round := range rounds {
		lead := c.WaitLeader(30 * time.Second)
		c.MustPropose(fmt.Sprintf("before-stop-%d", round))
		rest := otherLive(c, lead)
		if _, err := c.docker("stop", "-t", "10", ContainerName(lead)); err != nil {
			t.Fatalf("docker stop leader %d: %v", lead, err)
		}
		stopped := time.Now()
		var nl int
		for time.Since(stopped) < 10*time.Second {
			if l, ok := c.Leader(rest...); ok {
				nl = l
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		took := time.Since(stopped)
		if nl == 0 {
			t.Fatalf("round %d: no new leader after stopping %d", round, lead)
		}
		// Without a handover the followers would wait at least one minimum
		// election timeout (1 s) after the leader went silent.
		if took > 800*time.Millisecond {
			t.Fatalf("round %d: new leader %d took %v after the old leader %d stopped; a handover takes a round trip",
				round, nl, took, lead)
		}
		logs, _ := exec.Command("docker", "logs", ContainerName(lead)).CombinedOutput()
		if !strings.Contains(string(logs), "handing leadership over before shutdown") {
			t.Fatalf("round %d: old leader %d did not log a handover", round, lead)
		}
		t.Logf("round %d: leader %d -> %d, visible %v after the stop returned", round, lead, nl, took)
		c.MustPropose(fmt.Sprintf("after-stop-%d", round), rest...)
		c.Restart(lead)
		c.WaitConverged(60 * time.Second)
	}
}

// buildCtl builds raftkvctl for the host once per test.
func buildCtl(t *testing.T, c *Cluster) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "raftkvctl")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/raftkvctl")
	cmd.Dir = c.root
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build raftkvctl: %v\n%s", err, out)
	}
	return bin
}

// WBS 6.4.7: `raftkvctl member ...` smoke against the compose cluster: list,
// remove, add-learner, promote --wait, transfer (and `leader transfer`),
// list again.
func TestRaftkvctlMemberSmoke(t *testing.T) {
	c := Start(t, "raftkvctl-member")
	ctl := buildCtl(t, c)
	c.WaitLeader(30 * time.Second)
	c.MustPropose("smoke")
	var eps []string
	for k := 1; k <= NumNodes; k++ {
		eps = append(eps, clientAddr(k))
	}
	run := func(want int, args ...string) string {
		t.Helper()
		full := append([]string{args[0], args[1], "--endpoints", strings.Join(eps, ","), "--timeout", "90s"}, args[2:]...)
		cmd := exec.Command(ctl, full...)
		out, err := cmd.CombinedOutput()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatalf("raftkvctl %v: %v", full, err)
		}
		if code != want {
			t.Fatalf("raftkvctl %v: exit %d, want %d\n%s", full, code, want, out)
		}
		c.Faults.Logf("raftkvctl %v -> %d: %s", full, code, strings.TrimSpace(string(out)))
		return string(out)
	}
	out := run(0, "member", "list")
	if n := strings.Count(out, "VOTER"); n != NumNodes {
		t.Fatalf("member list shows %d voters, want %d:\n%s", n, NumNodes, out)
	}
	// Free slot 5, then bring it back as a new learner.
	run(0, "member", "remove", strconv.FormatUint(c.MemberID(5), 10))
	c.removed[5] = true
	_, _ = c.composeCmd("stop", "-t", "10", "node5")
	out = run(0, "member", "add-learner", "--peer-url", "node5:7000", "node5")
	m := regexp.MustCompile(`added learner id=(\d+)`).FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("add-learner printed no ID:\n%s", out)
	}
	id, _ := strconv.ParseUint(m[1], 10, 64)
	c.Rejoin(5, id)
	if out := run(0, "member", "list", "--stale"); !strings.Contains(out, "LEARNER") {
		t.Fatalf("the new learner is not listed:\n%s", out)
	}
	run(0, "member", "promote", "--wait", m[1])
	out = run(0, "member", "transfer", m[1])
	if !strings.Contains(out, "leader="+m[1]) {
		t.Fatalf("transfer did not move leadership to %s:\n%s", m[1], out)
	}
	c.WaitLeader(30 * time.Second)
	out = run(0, "leader", "transfer")
	if strings.Contains(out, "leader="+m[1]+" ") {
		t.Fatalf("leader transfer (any voter) kept leader %s:\n%s", m[1], out)
	}
	out = run(0, "member", "list")
	if n := strings.Count(out, "VOTER"); n != NumNodes || !strings.Contains(out, m[1]) {
		t.Fatalf("final member list:\n%s", out)
	}
	// Errors map to exit codes: removing an unknown member is NOT_FOUND (3).
	run(3, "member", "remove", "12345")
	c.MustPropose("after-smoke")
	c.WaitConverged(60 * time.Second)
}

// WBS 6.5.1 (M6 exit criterion 2): replace a node under load, once a
// follower and once the leader itself (add a learner, let it catch up,
// promote it, remove the old member), while the Porcupine workload runs: 0
// linearizability violations, and the final members' state hashes match.
// Slot 5 is freed first so there is room for the newcomer.
func TestReplaceNodeUnderLoad(t *testing.T) {
	c := StartEnv(t, "replace-node", "RAFTKV_SNAPSHOT_ENTRIES=500", "RAFTKV_SNAPSHOT_RETAIN=50")
	runLinearizable(t, c, 150, func(_ *linearizability.Workload) {
		c.RemoveSlot(5) // 4 voters, slot 5 free
		time.Sleep(2 * time.Second)
		lead := c.WaitLeader(30 * time.Second)
		f := otherLive(c, lead)[0]
		id := c.ReplaceSlot(f, 5)
		c.Faults.Logf("replaced follower slot %d by member %d in slot 5", f, id)
		time.Sleep(3 * time.Second)
		lead = c.WaitLeader(30 * time.Second)
		id = c.ReplaceSlot(lead, f)
		c.Faults.Logf("replaced leader slot %d by member %d in slot %d", lead, id, f)
		time.Sleep(3 * time.Second)
	})
	requireMembersAndHashes(t, c, 4)
}

// WBS 6.5.2 (M6 exit criterion 3): scale under load. Compose always starts
// five, so: 5 -> 3 (remove two followers), 3 -> 5 (both slots rejoin as new
// learners and are promoted), then 5 -> 3 again, removing the leader among
// others, with a leadership transfer in between. 0 violations.
func TestScale3To5To3UnderLoad(t *testing.T) {
	c := StartEnv(t, "scale-3-5-3", "RAFTKV_SNAPSHOT_ENTRIES=500", "RAFTKV_SNAPSHOT_RETAIN=50")
	runLinearizable(t, c, 150, func(_ *linearizability.Workload) {
		lead := c.WaitLeader(30 * time.Second)
		down := otherLive(c, lead)[:2]
		for _, k := range down {
			c.RemoveSlot(k)
		}
		c.Faults.Logf("scaled to 3: %v", c.Live())
		time.Sleep(3 * time.Second)
		for _, k := range down {
			id := c.AddLearnerSlot(k)
			c.PromoteSlot(k)
			c.Faults.Logf("slot %d rejoined as voter %d", k, id)
		}
		c.Faults.Logf("scaled to 5: %v", c.Live())
		time.Sleep(3 * time.Second)
		c.TransferTo(down[0])
		time.Sleep(2 * time.Second)
		lead = c.WaitLeader(30 * time.Second)
		c.RemoveSlot(lead) // the leader removes itself and hands over (WBS 6.1.5)
		c.RemoveSlot(otherLive(c)[0])
		c.Faults.Logf("scaled back to 3: %v", c.Live())
		time.Sleep(3 * time.Second)
	})
	requireMembersAndHashes(t, c, 3)
}

// requireMembersAndHashes checks the member table has want voters, all live,
// and that the stopped members' state hashes match.
func requireMembersAndHashes(t *testing.T, c *Cluster, want int) {
	t.Helper()
	members := c.Members()
	voters := 0
	for _, m := range members {
		if m.GetRole() == raftkvv1.MemberRole_VOTER {
			voters++
		}
		if c.SlotOf(m.GetId()) == 0 {
			t.Fatalf("member %d runs in no live slot (slots %v -> %v)", m.GetId(), c.Live(), c.ids)
		}
	}
	if voters != want || len(members) != want {
		t.Fatalf("%d members (%d voters), want %d voters", len(members), voters, want)
	}
	c.StopAll()
	hashes := c.StateHashes()
	ref := hashes[c.Live()[0]]
	for k, h := range hashes {
		if h != ref {
			t.Fatalf("state hashes differ: slot %d %q vs %q", k, h, ref)
		}
	}
	t.Logf("%d members, state %s", len(hashes), ref)
}
