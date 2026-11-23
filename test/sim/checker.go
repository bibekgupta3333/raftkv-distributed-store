package sim

import (
	"fmt"
	"sort"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Violation kinds. Safety kinds stop a run as a bug; KindLiveness is reported
// separately (docs/testing.md 2.2, LIVE-1).
const (
	KindElectionSafety     = "ElectionSafety"     // INV-1
	KindLeaderAppendOnly   = "LeaderAppendOnly"   // INV-2
	KindLogMatching        = "LogMatching"        // INV-3
	KindLeaderCompleteness = "LeaderCompleteness" // INV-4
	KindStateMachineSafety = "StateMachineSafety" // INV-5
	KindMonotonicity       = "Monotonicity"       // INV-6
	KindLiveness           = "Liveness"           // LIVE-1
)

// Violation is a failed invariant. At is virtual time in ms.
type Violation struct {
	Kind   string
	Detail string
	At     int64
	Node   uint64
}

func (v *Violation) Error() string {
	return fmt.Sprintf("%s violated at %dms (node %d): %s", v.Kind, v.At, v.Node, v.Detail)
}

// IsSafety reports whether v is a safety violation (anything but liveness).
func (v *Violation) IsSafety() bool { return v.Kind != KindLiveness }

// NodeView is what the checker sees of one node after an event: the volatile
// status of the Raft core plus the node's persisted log. Log holds entries
// 1..len(Log), the compacted prefix included (taken from the snapshot); Hashes[i] is the chained hash of
// entries 1..i+1, so two logs agree on a prefix iff the hashes at its end are
// equal.
type NodeView struct {
	ID          uint64
	Incarnation uint64
	State       raft.StateType
	Term        uint64
	Commit      uint64
	Applied     uint64
	LastIndex   uint64
	// Synced is true when the core holds no unpersisted entries, so Log is
	// exactly the core's log and Commit refers to it. Committed-prefix and
	// Leader Completeness checks only run on synced views, because a
	// follower's persisted log may still hold stale entries that an
	// outstanding Ready is about to overwrite.
	Synced bool
	Log    []raft.Entry
	Hashes []uint64
}

// HashEntry is the content hash of one entry (term, index, type and data).
func HashEntry(e raft.Entry) uint64 {
	h := uint64(fnvOffset)
	mix := func(v uint64) {
		for range 8 {
			h ^= v & 0xff
			h *= fnvPrime
			v >>= 8
		}
	}
	mix(e.Term)
	mix(e.Index)
	mix(uint64(e.Type))
	for _, b := range e.Data {
		h ^= uint64(b)
		h *= fnvPrime
	}
	mix(uint64(len(e.Data)))
	return h
}

// ChainHash extends the prefix hash prev with an entry hash.
func ChainHash(prev, entry uint64) uint64 {
	h := prev ^ 0x9e3779b97f4a7c15
	h *= fnvPrime
	h ^= entry
	h *= fnvPrime
	return h
}

// ChainHashes computes the prefix hashes of a log.
func ChainHashes(log []raft.Entry) []uint64 {
	out := make([]uint64, len(log))
	prev := uint64(0)
	for i, e := range log {
		prev = ChainHash(prev, HashEntry(e))
		out[i] = prev
	}
	return out
}

type canonEntry struct {
	term uint64
	// commitTerm bounds from above the term of the leader that committed the
	// entry: the observer's term when the commit was first seen, made
	// monotone. Leader Completeness is about the commit term, not the
	// entry's own term.
	commitTerm uint64
	entry      uint64 // HashEntry
	prefix     uint64 // chained hash up to and including this entry
}

type leaderMark struct {
	term, inc uint64
	last      int
	hash      uint64
}

type applyKey struct{ node, inc uint64 }

// Checker evaluates the Raft safety invariants (D-18, docs/testing.md 2.2)
// from NodeViews and apply events. It keeps a canonical committed sequence
// (the "reference model" of the applied log) against which every node is
// compared. It is fed one touched node at a time, so it costs O(1) per event.
type Checker struct {
	ids   []uint64
	views map[uint64]NodeView

	leaders    map[uint64]uint64 // term -> leader id (INV-1)
	lastLeader map[uint64]leaderMark
	canon      []canonEntry // committed prefix, observed from persisted logs
	model      []uint64     // entry hash per applied index (reference model)
	applied    map[applyKey]uint64
}

// NewChecker returns a checker for the given node IDs.
func NewChecker(ids []uint64) *Checker {
	return &Checker{
		ids:        append([]uint64(nil), ids...),
		views:      map[uint64]NodeView{},
		leaders:    map[uint64]uint64{},
		lastLeader: map[uint64]leaderMark{},
		applied:    map[applyKey]uint64{},
	}
}

// CommittedLen is the length of the canonical committed sequence seen so far.
func (c *Checker) CommittedLen() int { return len(c.canon) }

func viol(kind string, at int64, node uint64, format string, args ...any) *Violation {
	return &Violation{Kind: kind, At: at, Node: node, Detail: fmt.Sprintf(format, args...)}
}

// OnApply records that node (in incarnation inc) applied e, and checks State
// Machine Safety (INV-5): every node applies the same entry at each index, in
// order, with no gaps.
func (c *Checker) OnApply(at int64, node, inc uint64, e raft.Entry) *Violation {
	k := applyKey{node, inc}
	if last := c.applied[k]; e.Index != last+1 {
		return viol(KindStateMachineSafety, at, node, "applied index %d after %d (gap or repeat)", e.Index, last)
	}
	c.applied[k] = e.Index
	h := HashEntry(e)
	switch {
	case e.Index <= uint64(len(c.model)):
		if c.model[e.Index-1] != h {
			return viol(KindStateMachineSafety, at, node,
				"applied different entry at index %d (term %d) than another node", e.Index, e.Term)
		}
	case e.Index == uint64(len(c.model))+1:
		c.model = append(c.model, h)
	default:
		return viol(KindStateMachineSafety, at, node, "applied index %d beyond model length %d", e.Index, len(c.model))
	}
	if e.Index <= uint64(len(c.canon)) && c.canon[e.Index-1].entry != h {
		return viol(KindStateMachineSafety, at, node,
			"applied entry at index %d differs from the committed entry in the logs", e.Index)
	}
	return nil
}

// OnRestore records that node (in incarnation inc) replaced its state machine
// with a snapshot holding ents, the applied entries 1..len(ents) (D-14):
// either installed from the leader or loaded at restart. The snapshot must
// hold exactly the entries the reference model applied (INV-5), and must
// never roll the node's state machine back.
func (c *Checker) OnRestore(at int64, node, inc uint64, ents []raft.Entry) *Violation {
	k := applyKey{node, inc}
	last := uint64(len(ents))
	if last == 0 {
		return nil
	}
	if prev := c.applied[k]; prev >= last {
		return viol(KindStateMachineSafety, at, node, "snapshot at %d would roll back applied index %d", last, prev)
	}
	if last > uint64(len(c.model)) {
		return viol(KindStateMachineSafety, at, node, "snapshot at %d holds entries no node applied (model length %d)", last, len(c.model))
	}
	for i, e := range ents {
		if e.Index != uint64(i+1) || HashEntry(e) != c.model[i] {
			return viol(KindStateMachineSafety, at, node, "snapshot at %d differs from the applied entry at index %d", last, i+1)
		}
	}
	c.applied[k] = last
	return nil
}

// Observe checks the invariants that involve node view v and records v. It
// returns the first violation found.
func (c *Checker) Observe(at int64, v NodeView) *Violation {
	prev, hadPrev := c.views[v.ID]
	if vi := c.checkBounds(at, v, prev, hadPrev); vi != nil {
		return vi
	}
	if vi := c.checkLeader(at, v); vi != nil {
		return vi
	}
	c.views[v.ID] = v
	if vi := c.checkLogMatching(at, v); vi != nil {
		return vi
	}
	if vi := c.checkCommitted(at, v); vi != nil {
		return vi
	}
	return c.checkLeaderCompleteness(at, v)
}

// INV-6: commit and applied never go backwards inside one incarnation, and
// applied <= committed <= lastIndex. Across a restart the commit index may
// fall back to the persisted one (HardState.Commit is lazy and not needed for
// safety), so only the within-incarnation rule is checked.
func (c *Checker) checkBounds(at int64, v, prev NodeView, hadPrev bool) *Violation {
	if v.Applied > v.Commit || v.Commit > v.LastIndex {
		return viol(KindMonotonicity, at, v.ID, "applied=%d commit=%d last=%d not ordered", v.Applied, v.Commit, v.LastIndex)
	}
	if hadPrev && prev.Incarnation == v.Incarnation {
		if v.Commit < prev.Commit {
			return viol(KindMonotonicity, at, v.ID, "commit went back from %d to %d", prev.Commit, v.Commit)
		}
		if v.Applied < prev.Applied {
			return viol(KindMonotonicity, at, v.ID, "applied went back from %d to %d", prev.Applied, v.Applied)
		}
		if v.Term < prev.Term {
			return viol(KindMonotonicity, at, v.ID, "term went back from %d to %d", prev.Term, v.Term)
		}
	}
	return nil
}

// INV-1 Election Safety and INV-2 Leader Append-Only.
func (c *Checker) checkLeader(at int64, v NodeView) *Violation {
	if v.State != raft.StateLeader {
		delete(c.lastLeader, v.ID)
		return nil
	}
	if other, ok := c.leaders[v.Term]; ok && other != v.ID {
		return viol(KindElectionSafety, at, v.ID, "nodes %d and %d are both leader of term %d", other, v.ID, v.Term)
	}
	c.leaders[v.Term] = v.ID
	if m, ok := c.lastLeader[v.ID]; ok && m.term == v.Term && m.inc == v.Incarnation && m.last > 0 {
		if len(v.Log) < m.last || v.Hashes[m.last-1] != m.hash {
			return viol(KindLeaderAppendOnly, at, v.ID,
				"leader of term %d changed its own log: had %d entries, entry %d differs or vanished (now %d entries)",
				v.Term, m.last, m.last, len(v.Log))
		}
	}
	mark := leaderMark{term: v.Term, inc: v.Incarnation, last: len(v.Log)}
	if mark.last > 0 {
		mark.hash = v.Hashes[mark.last-1]
	}
	c.lastLeader[v.ID] = mark
	return nil
}

// INV-3 Log Matching: if two logs hold an entry with the same index and term,
// they are identical up to that index. For each peer we find the highest
// index with equal terms and compare the prefix hashes there.
func (c *Checker) checkLogMatching(at int64, v NodeView) *Violation {
	for _, id := range c.ids {
		if id == v.ID {
			continue
		}
		o, ok := c.views[id]
		if !ok {
			continue
		}
		for i := min(len(v.Log), len(o.Log)); i >= 1; i-- {
			if v.Log[i-1].Term != o.Log[i-1].Term {
				continue
			}
			if v.Hashes[i-1] != o.Hashes[i-1] {
				return viol(KindLogMatching, at, v.ID,
					"logs of nodes %d and %d both hold term %d at index %d but their prefixes differ",
					v.ID, o.ID, v.Log[i-1].Term, i)
			}
			break
		}
	}
	return nil
}

// checkCommitted extends the canonical committed sequence from v's persisted
// log up to its commit index and verifies that v's committed prefix agrees
// with it (INV-5, as observed from logs rather than applies).
func (c *Checker) checkCommitted(at int64, v NodeView) *Violation {
	if !v.Synced {
		return nil
	}
	n := min(int(v.Commit), len(v.Log))
	have := len(c.canon)
	if both := min(n, have); both > 0 && c.canon[both-1].prefix != v.Hashes[both-1] {
		return viol(KindStateMachineSafety, at, v.ID,
			"committed prefix up to index %d differs from the committed prefix of another node", both)
	}
	for i := have; i < n; i++ {
		eh := HashEntry(v.Log[i])
		if i < len(c.model) && c.model[i] != eh {
			return viol(KindStateMachineSafety, at, v.ID,
				"committed entry at index %d differs from the entry that was applied", i+1)
		}
		ct := v.Term
		if i > 0 {
			ct = max(ct, c.canon[i-1].commitTerm)
		}
		c.canon = append(c.canon, canonEntry{term: v.Log[i].Term, commitTerm: ct, entry: eh, prefix: v.Hashes[i]})
	}
	return nil
}

// INV-4 Leader Completeness: a leader of term T holds every entry committed
// in a term below T (the commit term, not the entry term: an old-term entry
// may be committed later by a newer leader). Commit terms are non-decreasing,
// so those entries are a prefix of the canonical sequence. The check waits until the leader has no
// unpersisted entries, because its log is only compared on stable storage.
func (c *Checker) checkLeaderCompleteness(at int64, v NodeView) *Violation {
	if !v.Synced || v.State != raft.StateLeader || uint64(len(v.Log)) != v.LastIndex {
		return nil
	}
	k := sort.Search(len(c.canon), func(i int) bool { return c.canon[i].commitTerm >= v.Term })
	if k == 0 {
		return nil
	}
	if len(v.Log) < k || v.Hashes[k-1] != c.canon[k-1].prefix {
		return viol(KindLeaderCompleteness, at, v.ID,
			"leader of term %d lacks committed entries of earlier terms (needs the first %d, has %d, prefix match=%v)",
			v.Term, k, len(v.Log), len(v.Log) >= k && v.Hashes[k-1] == c.canon[k-1].prefix)
	}
	return nil
}
