package raft

import (
	"fmt"
	"slices"
)

// ProgressState is the replication mode the leader uses for one follower
// (system-design 2.1).
type ProgressState uint8

const (
	// ProgressProbe sends at most one MsgApp per round trip. Used after
	// election, after a rejection and when the follower's position is unknown.
	ProgressProbe ProgressState = iota
	// ProgressReplicate streams entries optimistically up to MaxInflight.
	ProgressReplicate
	// ProgressSnapshot means the follower needs entries the leader has
	// compacted away, so a snapshot is being sent (D-14, system-design 2.1).
	// No appends are sent until the driver reports the transfer's outcome
	// ([RawNode.ReportSnapshot]) or the follower acknowledges the snapshot.
	ProgressSnapshot
)

func (s ProgressState) String() string {
	switch s {
	case ProgressProbe:
		return "Probe"
	case ProgressReplicate:
		return "Replicate"
	case ProgressSnapshot:
		return "Snapshot"
	default:
		return fmt.Sprintf("ProgressState(%d)", uint8(s))
	}
}

// Progress is the leader's view of one follower.
type Progress struct {
	// Match is the highest index known replicated (and durable) on the node.
	Match uint64
	// Next is the next index to send.
	Next  uint64
	State ProgressState

	// RecentActive is set on any response and cleared by each CheckQuorum
	// round (D-08).
	RecentActive bool

	// PendingSnapshot is the index of the snapshot being sent while State
	// is ProgressSnapshot (D-14); 0 otherwise.
	PendingSnapshot uint64

	probeSent bool
	inflights inflights
}

func newProgress(next uint64, maxInflight int) *Progress {
	return &Progress{Next: next, inflights: inflights{cap: maxInflight}}
}

func (p *Progress) becomeProbe() {
	switch p.State {
	case ProgressReplicate:
		p.Next = p.Match + 1
	case ProgressSnapshot:
		// If the snapshot was installed the follower holds everything up to
		// it; if it failed, PendingSnapshot was cleared and we probe from
		// Match (D-14: an interrupted transfer starts over).
		p.Next = max(p.Match+1, p.PendingSnapshot+1)
		p.PendingSnapshot = 0
	}
	p.State = ProgressProbe
	p.probeSent = false
	p.inflights.reset()
}

func (p *Progress) becomeReplicate() {
	p.State = ProgressReplicate
	p.Next = p.Match + 1
	p.PendingSnapshot = 0
	p.probeSent = false
	p.inflights.reset()
}

// becomeSnapshot starts sending the snapshot at index snap (D-14). Appends
// pause until the transfer is reported or acknowledged.
func (p *Progress) becomeSnapshot(snap uint64) {
	p.State = ProgressSnapshot
	p.PendingSnapshot = snap
	p.Next = snap + 1
	p.probeSent = false
	p.inflights.reset()
}

// maybeUpdate records that the follower has everything up to n. It reports
// whether Match advanced.
func (p *Progress) maybeUpdate(n uint64) bool {
	updated := p.Match < n
	if updated {
		p.Match = n
		p.probeSent = false
	}
	p.Next = max(p.Next, n+1)
	return updated
}

// maybeDecrTo handles a rejection of the append whose prevIndex was rejected,
// given the follower's hint (the last index it believes could match). It
// reports whether the rejection was current (not stale).
func (p *Progress) maybeDecrTo(rejected, hint uint64) bool {
	if p.State == ProgressSnapshot {
		// No append is outstanding while a snapshot is in flight, so any
		// rejection answers an older probe.
		return false
	}
	if p.State == ProgressReplicate {
		if rejected <= p.Match {
			return false // stale: already matched past this
		}
		p.Next = p.Match + 1
		return true
	}
	if p.Next-1 != rejected {
		return false // stale: we have probed a different index since
	}
	p.Next = max(min(rejected, hint+1), 1)
	p.probeSent = false
	return true
}

// isPaused reports whether the leader should hold further appends (D-29).
func (p *Progress) isPaused() bool {
	switch p.State {
	case ProgressProbe:
		return p.probeSent
	case ProgressSnapshot:
		return true
	default:
		return p.inflights.full()
	}
}

// inflights tracks last-indexes of unacknowledged MsgApp (D-29).
type inflights struct {
	cap   int
	items []uint64
}

func (in *inflights) full() bool   { return len(in.items) >= in.cap }
func (in *inflights) reset()       { in.items = in.items[:0] }
func (in *inflights) add(i uint64) { in.items = append(in.items, i) }

// freeLE releases every in-flight append whose last index is <= i.
func (in *inflights) freeLE(i uint64) {
	n, _ := slices.BinarySearchFunc(in.items, i, func(e, t uint64) int {
		if e <= t {
			return -1
		}
		return 1
	})
	in.items = in.items[n:]
}

// freeFirstOne releases the oldest in-flight append.
func (in *inflights) freeFirstOne() {
	if len(in.items) > 0 {
		in.items = in.items[1:]
	}
}

// quorumSize is the number of voters needed for a majority of n.
func quorumSize(n int) int { return n/2 + 1 }

// quorumIndex returns the largest index replicated on a majority, given each
// voter's match index (including the leader's own durable index). It does not
// modify matches. This is the "counting" half of the commit rule; the caller
// must still check that the entry has the current term (Figure 8).
func quorumIndex(matches []uint64) uint64 {
	if len(matches) == 0 {
		return 0
	}
	sorted := slices.Clone(matches)
	slices.SortFunc(sorted, func(a, b uint64) int {
		switch {
		case a > b:
			return -1
		case a < b:
			return 1
		}
		return 0
	})
	return sorted[quorumSize(len(sorted))-1]
}

// tallyVotes counts granted and rejected votes among voters. Voters that have
// not answered count as neither.
func tallyVotes(voters []uint64, votes map[uint64]bool) (granted, rejected int) {
	for _, id := range voters {
		if v, ok := votes[id]; ok {
			if v {
				granted++
			} else {
				rejected++
			}
		}
	}
	return granted, rejected
}
