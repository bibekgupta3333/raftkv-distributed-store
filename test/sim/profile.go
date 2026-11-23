package sim

import (
	"fmt"
	"slices"
)

// Weights are relative frequencies of the chaos actions in a fault plan.
type Weights struct {
	Partition     int // random bipartition
	IsolateLeader int // cut the current leader off
	OneWay        int // asymmetric block of one directed link
	Heal          int // remove partitions
	Crash         int // crash a random node, restart it later
	CrashLeader   int // crash the current leader, restart it later
	Degrade       int // make one link slow, lossy, duplicating, reordering
	ResetLinks    int // remove per-link degradation
}

func (w Weights) total() int {
	return w.Partition + w.IsolateLeader + w.OneWay + w.Heal + w.Crash + w.CrashLeader + w.Degrade + w.ResetLinks
}

// Profile is a named fault mix (docs/testing.md 2.2, -faults flag).
type Profile struct {
	Name string
	// Sizes are the cluster sizes a seed may pick from.
	Sizes []int
	// FaultMs is the length of the chaos phase in virtual ms; the cluster is
	// then healed and the liveness check runs.
	FaultMs int64
	// Base is the default link behaviour during the chaos phase.
	Base Link
	// Degraded is what a degraded link looks like.
	Degraded Link
	Weights  Weights
}

// Profiles by name. "full" is the one the M1 exit criterion uses (WBS 1.9.1).
var profiles = []Profile{
	{
		Name: "none", Sizes: []int{3, 5}, FaultMs: 10_000,
		Base: ReliableLink,
	},
	{
		Name: "lossy", Sizes: []int{3, 5}, FaultMs: 20_000,
		Base:     Link{Drop: 0.05, Dup: 0.03, Reorder: 0.10, MinDelay: 1, MaxDelay: 50},
		Degraded: Link{Drop: 0.30, Dup: 0.10, Reorder: 0.30, MinDelay: 5, MaxDelay: 400},
		Weights:  Weights{Degrade: 3, ResetLinks: 2},
	},
	{
		Name: "partition", Sizes: []int{3, 5}, FaultMs: 20_000,
		Base:    ReliableLink,
		Weights: Weights{Partition: 3, IsolateLeader: 3, OneWay: 2, Heal: 3},
	},
	{
		Name: "crash", Sizes: []int{3, 5}, FaultMs: 20_000,
		Base:    ReliableLink,
		Weights: Weights{Crash: 4, CrashLeader: 4},
	},
	{
		Name: "full", Sizes: []int{3, 5}, FaultMs: 25_000,
		Base:     Link{Drop: 0.03, Dup: 0.03, Reorder: 0.08, MinDelay: 1, MaxDelay: 40},
		Degraded: Link{Drop: 0.30, Dup: 0.10, Reorder: 0.30, MinDelay: 5, MaxDelay: 400},
		Weights: Weights{
			Partition: 3, IsolateLeader: 3, OneWay: 2, Heal: 4,
			Crash: 3, CrashLeader: 3, Degrade: 2, ResetLinks: 2,
		},
	},
}

// ProfileNames lists the known profile names.
func ProfileNames() []string {
	out := make([]string, len(profiles))
	for i, p := range profiles {
		out[i] = p.Name
	}
	return out
}

// LookupProfile finds a profile by name.
func LookupProfile(name string) (Profile, error) {
	i := slices.IndexFunc(profiles, func(p Profile) bool { return p.Name == name })
	if i < 0 {
		return Profile{}, fmt.Errorf("sim: unknown fault profile %q (have %v)", name, ProfileNames())
	}
	return profiles[i], nil
}
