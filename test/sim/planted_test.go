package sim

import (
	"bytes"
	"path/filepath"
	"slices"
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// "Test the tests" (docs/testing.md section 1, principle 4): break the system
// on purpose and require the simulator to notice. Each planted bug is a Hooks
// value that corrupts a message, a Ready or the checker's view; the same seed
// without the plant must pass, so the detection is the plant's doing.

func flip(b []byte) []byte {
	out := slices.Clone(b)
	if len(out) > 0 {
		out[len(out)-1] ^= 0xff
	}
	return out
}

// planted runs seeds 1..maxSeeds with a fresh Hooks per seed (hooks are
// stateful) and requires at least one run to report a violation of kind.
// The same seed with no hooks must pass.
func planted(t *testing.T, profile string, maxSeeds int, kind string, mk func() Hooks) {
	t.Helper()
	p := mustProfile(t, profile)
	seen := map[string]int{}
	for seed := uint64(1); seed <= uint64(maxSeeds); seed++ {
		r := Run(seed, RunOpts{Profile: p, Hooks: mk()})
		if r.Violation == nil {
			continue
		}
		seen[r.Violation.Kind]++
		if r.Violation.Kind != kind {
			continue
		}
		if clean := Run(seed, RunOpts{Profile: p}); !clean.OK() {
			t.Fatalf("seed %d fails even without the planted bug: %s", seed, clean)
		}
		t.Logf("detected at seed %d: %v", seed, r.Violation)
		return
	}
	t.Fatalf("no seed in 1..%d reported %s (violations seen: %v)", maxSeeds, kind, seen)
}

// A corrupted commit on one node: it applies a different command than its
// peers (the model of the applied sequence catches it, WBS 1.5.5).
func TestSimPlantedStateMachineDivergence(t *testing.T) {
	planted(t, "none", 5, KindStateMachineSafety, func() Hooks {
		done := false
		return Hooks{Ready: func(id uint64, rd *raft.Ready) {
			if id != 2 || done {
				return
			}
			for i, e := range rd.CommittedEntries {
				if len(e.Data) >= 8 {
					ce := slices.Clone(rd.CommittedEntries)
					ce[i].Data = flip(e.Data)
					rd.CommittedEntries = ce
					done = true
					return
				}
			}
		}}
	})
}

// A message that arrives with a flipped payload byte: the follower stores an
// entry with the leader's index and term but different content.
func TestSimPlantedLogMatching(t *testing.T) {
	planted(t, "none", 5, KindLogMatching, func() Hooks {
		done := false
		return Hooks{Message: func(m *raft.Message) {
			if done || m.Type != raft.MsgApp || m.To != 3 {
				return
			}
			for i, e := range m.Entries {
				if len(e.Data) >= 8 {
					es := slices.Clone(m.Entries)
					es[i].Data = flip(e.Data)
					m.Entries = es
					done = true
					return
				}
			}
		}}
	})
}

// Voters that grant every vote: two candidates of one term can both win.
func TestSimPlantedElectionSafety(t *testing.T) {
	planted(t, "full", 300, KindElectionSafety, func() Hooks {
		return Hooks{Message: func(m *raft.Message) {
			if m.Type == raft.MsgVoteResp || m.Type == raft.MsgPreVoteResp {
				m.Reject = false
			}
		}}
	})
}

// Voters that skip the "candidate's log is up to date" check (candidates
// look maximally up to date): a stale node can win and lacks committed
// entries.
func TestSimPlantedLeaderCompleteness(t *testing.T) {
	planted(t, "full", 300, KindLeaderCompleteness, func() Hooks {
		return Hooks{Message: func(m *raft.Message) {
			if m.Type == raft.MsgVote || m.Type == raft.MsgPreVote {
				m.LogTerm, m.Index = 1<<40, 1<<40
			}
		}}
	})
}

// A leader whose persisted log shrinks during its term (as the checker sees
// it, on every second observation).
func TestSimPlantedLeaderAppendOnly(t *testing.T) {
	planted(t, "none", 5, KindLeaderAppendOnly, func() Hooks {
		calls := 0
		return Hooks{View: func(v *NodeView) {
			if v.State != raft.StateLeader || len(v.Log) < 3 {
				return
			}
			if calls++; calls%2 == 0 {
				v.Log, v.Hashes = v.Log[:1], v.Hashes[:1]
			}
		}}
	})
}

// A node whose commit index goes back and forth.
func TestSimPlantedCommitRegression(t *testing.T) {
	planted(t, "none", 5, KindMonotonicity, func() Hooks {
		calls := 0
		return Hooks{View: func(v *NodeView) {
			if v.ID != 1 || v.Commit < 5 {
				return
			}
			if calls++; calls%2 == 0 {
				v.Commit -= 2
				v.Applied = min(v.Applied, v.Commit)
			}
		}}
	})
}

// A network that never recovers after the faults heal: safety holds, and the
// liveness check reports it (separately from safety, LIVE-1).
func TestSimPlantedLiveness(t *testing.T) {
	p := mustProfile(t, "none")
	h := Hooks{Filter: func(now int64, m raft.Message) bool { return now < p.FaultMs }}
	r := Run(1, RunOpts{Profile: p, Hooks: h})
	if r.Violation == nil || r.Violation.Kind != KindLiveness {
		t.Fatalf("a dead network after heal was not reported as a liveness failure: %s", r)
	}
	if r.Violation.IsSafety() {
		t.Fatal("liveness failures must not count as safety failures")
	}
}

// The regression machinery: a planted failure goes through the same failure
// path as a real one (repro command, JSONL trace), and a regression file
// entry replays it (WBS 1.7.3).
func TestSimPlantedRegressionReplay(t *testing.T) {
	p := mustProfile(t, "none")
	hooks := Hooks{Ready: func(id uint64, rd *raft.Ready) {
		if id != 2 {
			return
		}
		for i, e := range rd.CommittedEntries {
			if len(e.Data) >= 8 {
				ce := slices.Clone(rd.CommittedEntries)
				ce[i].Data = flip(e.Data)
				rd.CommittedEntries = ce
				return
			}
		}
	}}
	// The regression file says seed 3/none; with the plant it must fail, and
	// without it it must pass.
	seeds, err := LoadRegressions(filepath.Join("testdata", "planted"))
	if err != nil || len(seeds) != 1 || seeds[0].Seed != 3 || seeds[0].Profile != "none" {
		t.Fatalf("testdata/planted/regressions.txt: %+v, %v", seeds, err)
	}
	rs := seeds[0]
	if r := Run(rs.Seed, RunOpts{Profile: p}); !r.OK() {
		t.Fatalf("clean replay of regression seed failed: %s", r)
	}
	r := Run(rs.Seed, RunOpts{Profile: p, Hooks: hooks, Record: true})
	if r.OK() {
		t.Fatal("planted regression seed passed")
	}
	var buf bytes.Buffer
	if err := r.WriteTrace(&buf); err != nil {
		t.Fatal(err)
	}
	evs, err := ReadJSONL(&buf)
	if err != nil || len(evs) == 0 || evs[len(evs)-1].Kind != "VIOLATION" {
		t.Fatalf("trace must end with the violation (len=%d, err=%v)", len(evs), err)
	}
	if got := reproCommand(rs.Seed, rs.Profile); !bytes.Contains([]byte(got), []byte("-start=3")) {
		t.Fatalf("repro command lacks the seed: %s", got)
	}
}

// A snapshot transfer that corrupts one entry of the state it carries: the
// follower installs state that differs from what the others applied (WBS
// 5.1.4). The checker compares every installed snapshot with the model.
func TestSimPlantedCorruptSnapshot(t *testing.T) {
	planted(t, "snapshot", 20, KindStateMachineSafety, func() Hooks {
		done := false
		return Hooks{Install: func(_ uint64, ents []raft.Entry) []raft.Entry {
			if done {
				return ents
			}
			for i := len(ents) - 1; i >= 0; i-- {
				if len(ents[i].Data) >= 8 {
					out := slices.Clone(ents)
					out[i].Data = flip(out[i].Data)
					done = true
					return out
				}
			}
			return ents
		}}
	})
}

// The snapshot profile must actually exercise snapshots: they are taken,
// sent, and installed (WBS 5.1.4).
func TestSimSnapshotProfileExercisesInstalls(t *testing.T) {
	var taken, sent, installed int
	for _, r := range runSeeds(1, 20, mustProfile(t, "snapshot")) {
		if !r.OK() {
			t.Fatalf("seed %d: %v", r.Seed, r.Violation)
		}
		taken += r.Stats.Snapshots
		sent += r.Stats.SnapsSent
		installed += r.Stats.SnapsInstalled
	}
	if taken == 0 || sent == 0 || installed == 0 {
		t.Fatalf("snapshots taken=%d sent=%d installed=%d; want all > 0", taken, sent, installed)
	}
	if installed >= sent {
		t.Logf("note: every transfer completed (sent=%d installed=%d)", sent, installed)
	}
}
