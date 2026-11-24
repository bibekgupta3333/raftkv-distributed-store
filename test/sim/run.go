package sim

import (
	"fmt"
	"io"
)

// Result is the outcome of one seed.
type Result struct {
	Seed      uint64
	Profile   string
	Violation *Violation // nil: all invariants held and liveness was met
	TraceHash uint64
	Stats     Stats

	trace *Trace
}

// OK reports whether the run passed.
func (r Result) OK() bool { return r.Violation == nil }

// String is a one-line summary.
func (r Result) String() string {
	s := fmt.Sprintf("seed=%d profile=%s nodes=%d events=%d hash=%016x terms<=%d committed=%d acked=%d/%d reads=%d/%d (rejected=%d lease=%d) crashes=%d partitions=%d leader+%dms commit+%dms",
		r.Seed, r.Profile, r.Stats.Nodes, r.Stats.Events, r.TraceHash, r.Stats.MaxTerm,
		r.Stats.Committed, r.Stats.Acked, r.Stats.Accepted, r.Stats.ReadsServed, r.Stats.Reads,
		r.Stats.ReadsRejected, r.Stats.LeaseReads, r.Stats.Crashes, r.Stats.Partitions,
		r.Stats.LeaderAfterMs, r.Stats.CommitAfterMs)
	if r.Stats.Snapshots > 0 {
		s += fmt.Sprintf(" snapshots=%d sent=%d installed=%d", r.Stats.Snapshots, r.Stats.SnapsSent, r.Stats.SnapsInstalled)
	}
	if r.Stats.ConfChanges+r.Stats.ConfRejected+r.Stats.Transfers > 0 {
		s += fmt.Sprintf(" conf=%d refused=%d transfers=%d", r.Stats.ConfChanges, r.Stats.ConfRejected, r.Stats.Transfers)
	}
	if r.Violation != nil {
		s += " FAIL: " + r.Violation.Error()
	}
	return s
}

// WriteTrace dumps the recorded trace as JSONL. It needs a result produced
// with recording on (see [RunOpts.Record]).
func (r Result) WriteTrace(w io.Writer) error {
	if r.trace == nil || len(r.trace.Events()) == 0 {
		return fmt.Errorf("sim: seed %d was run without trace recording", r.Seed)
	}
	return r.trace.WriteJSONL(w)
}

// RunOpts configures [Run].
type RunOpts struct {
	Profile Profile
	Hooks   Hooks
	// Record keeps the full trace in memory so it can be dumped. The trace
	// hash is computed either way.
	Record bool
}

// Run executes one seed and returns its result. It is a pure function of
// (seed, opts): running it twice gives identical trace hashes (D-24).
func Run(seed uint64, opts RunOpts) Result {
	return NewCluster(seed, opts.Profile, opts.Hooks, opts.Record).Run()
}
