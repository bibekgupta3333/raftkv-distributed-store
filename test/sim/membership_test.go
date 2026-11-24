package sim

import (
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// A leader that lets a second membership change through while the first is
// still uncommitted (WBS 6.3.1): two configurations two servers apart are in
// play at once, and CONF-1 ("no two disjoint majorities") reports it.
func TestSimPlantedConcurrentConfChanges(t *testing.T) {
	planted(t, "membership", 200, KindConfigOverlap, func() Hooks {
		return Hooks{Config: func(_ uint64, cfg *raft.Config) { cfg.Unsafe.AllowConcurrentConfChanges = true }}
	})
}

// A new leader that changes the configuration before committing an entry of
// its own term (the single-server-change bug, WBS 6.1.3): CONF-2 reports it.
func TestSimPlantedEarlyConfChange(t *testing.T) {
	planted(t, "membership", 200, KindConfigEarly, func() Hooks {
		return Hooks{Config: func(_ uint64, cfg *raft.Config) { cfg.Unsafe.AllowEarlyConfChange = true }}
	})
}

// The membership profile must exercise the changes it claims to: many
// accepted changes, leaders removing themselves, transfers, and learners
// catching up by snapshot.
func TestSimMembershipProfileExercisesChanges(t *testing.T) {
	var conf, transfers, installed int
	for _, r := range runSeeds(1, 20, mustProfile(t, "membership")) {
		if !r.OK() {
			t.Fatalf("seed %d: %v", r.Seed, r.Violation)
		}
		conf += r.Stats.ConfChanges
		transfers += r.Stats.Transfers
		installed += r.Stats.SnapsInstalled
	}
	if conf < 100 || transfers == 0 || installed == 0 {
		t.Fatalf("conf changes=%d transfers=%d snapshot installs=%d; want many, >0, >0", conf, transfers, installed)
	}
}
