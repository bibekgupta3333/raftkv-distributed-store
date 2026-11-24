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
	Membership    int // a random single-server change or leadership transfer (D-06)
}

func (w Weights) total() int {
	return w.Partition + w.IsolateLeader + w.OneWay + w.Heal + w.Crash + w.CrashLeader + w.Degrade + w.ResetLinks +
		w.Membership
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
	// LeaseReads makes leaders answer reads from a lease (D-25) instead of a
	// ReadIndex round while the lease is valid.
	LeaseReads bool
	// Periods is the [min, max] tick period in ms a node draws at creation;
	// it is also the node's clock rate (100/period). Zero means [95, 105],
	// i.e. clocks within about 5% of each other.
	Periods [2]int64
	// Snapshots, when Every[1] > 0, makes nodes snapshot their state machine
	// and compact their logs (D-14, WBS 5.1.4).
	Snapshots SnapshotPolicy
}

// SnapshotPolicy is how often simulated nodes snapshot. A seed draws the
// threshold from Every and the retained log tail from Retain (both [min, max],
// inclusive). Low values make followers fall behind the leader's first index
// all the time, so snapshots are sent, interrupted and installed constantly.
type SnapshotPolicy struct {
	// Every is the number of applied entries between two snapshots.
	Every [2]uint64
	// Retain is how many entries below the snapshot index stay in the log
	// (D-30's retention tail).
	Retain [2]uint64
}

func (p SnapshotPolicy) enabled() bool { return p.Every[1] > 0 }

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
	{
		// Lease reads (WBS 4.2.5) with clock drift inside the D-25 bound.
		// No crashes: a restarted voter forgets the leader it heard from
		// and may vote at once, which the lease does not survive (see
		// internal/lease).
		Name: "lease", Sizes: []int{3, 5}, FaultMs: 20_000,
		Base:     Link{Drop: 0.03, Dup: 0.03, Reorder: 0.08, MinDelay: 1, MaxDelay: 40},
		Degraded: Link{Drop: 0.30, Dup: 0.10, Reorder: 0.30, MinDelay: 5, MaxDelay: 400},
		Weights: Weights{
			Partition: 3, IsolateLeader: 4, OneWay: 2, Heal: 4, Degrade: 2, ResetLinks: 2,
		},
		LeaseReads: true,
	},
	{
		// Snapshots (WBS 5.1.4, D-14): a very low snapshot threshold and
		// retention tail, so lagging and restarted followers are brought
		// back by InstallSnapshot, plus crashes (of sender and receiver)
		// that interrupt transfers and installs.
		Name: "snapshot", Sizes: []int{3, 5}, FaultMs: 25_000,
		Base:     Link{Drop: 0.03, Dup: 0.03, Reorder: 0.08, MinDelay: 1, MaxDelay: 40},
		Degraded: Link{Drop: 0.30, Dup: 0.10, Reorder: 0.30, MinDelay: 5, MaxDelay: 400},
		Weights: Weights{
			Partition: 3, IsolateLeader: 2, OneWay: 1, Heal: 4,
			Crash: 5, CrashLeader: 2, Degrade: 2, ResetLinks: 2,
		},
		Snapshots: SnapshotPolicy{Every: [2]uint64{3, 25}, Retain: [2]uint64{0, 5}},
	},
	{
		// Membership changes (WBS 6.3.1, D-06, D-31): learners added,
		// promoted and removed, voters removed (the leader included) and
		// leadership transferred, mixed with crashes, partitions and lossy
		// links. Snapshots are on so new learners usually catch up by
		// InstallSnapshot.
		Name: "membership", Sizes: []int{3, 5}, FaultMs: 25_000,
		Base:     Link{Drop: 0.03, Dup: 0.03, Reorder: 0.08, MinDelay: 1, MaxDelay: 40},
		Degraded: Link{Drop: 0.30, Dup: 0.10, Reorder: 0.30, MinDelay: 5, MaxDelay: 400},
		Weights: Weights{
			Partition: 2, IsolateLeader: 2, OneWay: 1, Heal: 4,
			Crash: 3, CrashLeader: 2, Degrade: 1, ResetLinks: 2, Membership: 8,
		},
		Snapshots: SnapshotPolicy{Every: [2]uint64{10, 60}, Retain: [2]uint64{0, 10}},
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
