package sim

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

func newScenario(t *testing.T, seed uint64) *Cluster {
	t.Helper()
	p := mustProfile(t, "none")
	p.Sizes = []int{5}
	c := NewCluster(seed, p, Hooks{}, false)
	c.Boot()
	c.StartWorkload()
	c.RunFor(5_000)
	if v := c.Violation(); v != nil {
		t.Fatal(v)
	}
	return c
}

// WBS 1.5.3: isolate the leader; the majority elects a new one in a higher
// term; after the heal the old leader steps down and every node converges.
func TestSimScenarioPartitionIsolateLeaderAndHeal(t *testing.T) {
	c := newScenario(t, 11)
	oldID, oldTerm := c.Leader()
	if oldID == 0 {
		t.Fatal("no leader after 5s on a healthy cluster")
	}
	committedBefore := c.Committed()

	c.Net().Isolate(oldID, c.IDs())
	c.RunFor(10_000)
	newID, newTerm := c.Leader()
	if newID == oldID || newTerm <= oldTerm {
		t.Fatalf("majority side did not elect a new leader: old=%d@%d now=%d@%d", oldID, oldTerm, newID, newTerm)
	}
	if st, _ := c.Status(oldID); st.State == raft.StateLeader {
		t.Error("CheckQuorum: the isolated leader should have stepped down")
	}
	if c.Committed() <= committedBefore {
		t.Error("the majority side made no progress while the old leader was cut off")
	}

	c.Net().Heal()
	c.RunFor(10_000)
	if v := c.Violation(); v != nil {
		t.Fatal(v)
	}
	st, _ := c.Status(oldID)
	if st.State == raft.StateLeader || st.Term < newTerm {
		t.Errorf("old leader after heal: state=%v term=%d (want follower, term >= %d)", st.State, st.Term, newTerm)
	}
	if id, _ := c.Leader(); id != newID {
		t.Errorf("leadership moved from %d to %d after the heal; PreVote/CheckQuorum should keep it", newID, id)
	}
}

// WBS 1.5.4: crash a follower and the leader. Unpersisted state is lost, the
// node restarts from its disk, replays committed entries and catches up.
func TestSimScenarioCrashRestart(t *testing.T) {
	c := newScenario(t, 21)
	leader, _ := c.Leader()
	victim := leader%5 + 1 // some other node
	before := c.PersistedLogLen(victim)
	if before == 0 {
		t.Fatal("victim has an empty disk after 5s of load")
	}
	oldSt, _ := c.Status(victim)

	c.Crash(victim)
	c.Crash(leader)
	if _, up := c.Status(victim); up {
		t.Fatal("crashed node still reports status")
	}
	c.RunFor(6_000)
	if id, _ := c.Leader(); id == 0 || id == leader || id == victim {
		t.Fatalf("remaining majority did not elect a leader (got %d)", id)
	}
	if got := c.PersistedLogLen(victim); got != before {
		t.Errorf("a down node's disk changed: %d -> %d entries", before, got)
	}

	c.Restart(victim)
	c.Restart(leader)
	st, _ := c.Status(victim)
	if st.Applied != 0 || st.LastIndex != uint64(before) {
		t.Errorf("restarted node: applied=%d last=%d, want applied 0 (volatile state machine) and last %d from disk", st.Applied, st.LastIndex, before)
	}
	if st.Term < oldSt.Term {
		t.Errorf("term went back across restart: %d -> %d", oldSt.Term, st.Term)
	}
	c.RunFor(8_000)
	if v := c.Violation(); v != nil {
		t.Fatal(v)
	}
	for _, id := range c.IDs() {
		st, _ := c.Status(id)
		lid, _ := c.Leader()
		ls, _ := c.Status(lid)
		if st.Commit+5 < ls.Commit {
			t.Errorf("node %d did not catch up: commit %d vs leader %d", id, st.Commit, ls.Commit)
		}
	}
}

func TestSimTraceJSONLRoundTripAndPretty(t *testing.T) {
	p := mustProfile(t, "full")
	r := Run(7, RunOpts{Profile: p, Record: true})
	var buf bytes.Buffer
	if err := r.WriteTrace(&buf); err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(buf.String(), "\n"); uint64(lines) != r.Stats.Events {
		t.Fatalf("%d JSONL lines for %d events", lines, r.Stats.Events)
	}
	var pretty bytes.Buffer
	if err := PrettyPrint(&pretty, bytes.NewReader(buf.Bytes()), 50); err != nil {
		t.Fatal(err)
	}
	out := pretty.String()
	if !strings.Contains(out, "earlier events omitted") || strings.Count(out, "\n") != 51 {
		t.Fatalf("pretty printer tail handling wrong:\n%s", out)
	}
	if !strings.Contains(out, "ms #") {
		t.Fatalf("pretty output is not readable:\n%s", out)
	}
	// Without recording there is nothing to dump.
	if err := Run(7, RunOpts{Profile: p}).WriteTrace(&bytes.Buffer{}); err == nil {
		t.Error("WriteTrace without recording should fail")
	}
}

func TestSimRegressionFileParsing(t *testing.T) {
	dir := t.TempDir()
	good := "# comment\n\n12 full # why\n7 crash\n"
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte(good), 0o600); err != nil {
		t.Fatal(err)
	}
	seeds, err := LoadRegressions(dir)
	if err != nil || len(seeds) != 2 || seeds[0].Seed != 12 || seeds[0].Comment != "why" || seeds[1].Profile != "crash" {
		t.Fatalf("got %+v, %v", seeds, err)
	}
	for _, bad := range []string{"x full\n", "5\n", "5 nosuchprofile\n"} {
		if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte(bad), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadRegressions(dir); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
}
