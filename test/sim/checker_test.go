package sim

import (
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// ents builds a log whose entry i+1 has term terms[i] and data {tag, i+1}.
func ents(tag byte, terms ...uint64) []raft.Entry {
	out := make([]raft.Entry, len(terms))
	for i, t := range terms {
		out[i] = raft.Entry{Term: t, Index: uint64(i + 1), Data: []byte{tag, byte(i + 1)}}
	}
	return out
}

// view builds a synced NodeView with the given log, commit == applied.
func view(id uint64, state raft.StateType, term, commit uint64, log []raft.Entry) NodeView {
	return NodeView{
		ID: id, Incarnation: 1, State: state, Term: term, Commit: commit, Applied: commit,
		LastIndex: uint64(len(log)), Synced: true, Log: log, Hashes: ChainHashes(log),
	}
}

func mustObserve(t *testing.T, c *Checker, v NodeView) {
	t.Helper()
	if viol := c.Observe(1, v); viol != nil {
		t.Fatalf("unexpected violation on a legal history: %v", viol)
	}
}

func wantKind(t *testing.T, got *Violation, kind string) {
	t.Helper()
	if got == nil {
		t.Fatalf("checker missed a planted %s bug", kind)
	}
	if got.Kind != kind {
		t.Fatalf("planted %s bug reported as %s: %v", kind, got.Kind, got)
	}
}

// Planted-bug tests on the checker itself (WBS 1.6.x): each invariant gets a
// crafted history that is legal up to one step, then illegal. The legal part
// guards against a checker that fires on everything.

func TestSimCheckerElectionSafety(t *testing.T) {
	c := NewChecker([]uint64{1, 2, 3})
	mustObserve(t, c, view(1, raft.StateLeader, 2, 0, ents('a', 1)))
	mustObserve(t, c, view(2, raft.StateLeader, 3, 0, ents('a', 1))) // another term: fine
	wantKind(t, c.Observe(2, view(3, raft.StateLeader, 2, 0, ents('a', 1))), KindElectionSafety)
}

func TestSimCheckerLeaderAppendOnly(t *testing.T) {
	log := ents('a', 1, 1, 2)
	c := NewChecker([]uint64{1, 2, 3})
	mustObserve(t, c, view(1, raft.StateLeader, 2, 0, log[:2]))
	mustObserve(t, c, view(1, raft.StateLeader, 2, 0, log)) // append: fine

	t.Run("truncate", func(t *testing.T) {
		wantKind(t, c.Observe(2, view(1, raft.StateLeader, 2, 0, log[:2])), KindLeaderAppendOnly)
	})
	t.Run("overwrite", func(t *testing.T) {
		c := NewChecker([]uint64{1, 2, 3})
		mustObserve(t, c, view(1, raft.StateLeader, 2, 0, log))
		other := ents('b', 1, 1, 2)
		wantKind(t, c.Observe(2, view(1, raft.StateLeader, 2, 0, other)), KindLeaderAppendOnly)
	})
	t.Run("new term may truncate", func(t *testing.T) {
		c := NewChecker([]uint64{1, 2, 3})
		mustObserve(t, c, view(1, raft.StateLeader, 2, 0, log))
		mustObserve(t, c, view(1, raft.StateFollower, 3, 0, log[:1]))
		mustObserve(t, c, view(1, raft.StateLeader, 4, 0, log[:1]))
	})
}

func TestSimCheckerLogMatching(t *testing.T) {
	c := NewChecker([]uint64{1, 2, 3})
	mustObserve(t, c, view(1, raft.StateFollower, 2, 0, ents('a', 1, 1, 2)))
	// Same prefix, different suffix terms: allowed.
	mustObserve(t, c, view(2, raft.StateFollower, 3, 0, append(ents('a', 1, 1), raft.Entry{Term: 3, Index: 3, Data: []byte{'x'}})))
	// Same (index, term) at 3 but different earlier data: Log Matching broken.
	wantKind(t, c.Observe(2, view(3, raft.StateFollower, 2, 0, ents('b', 1, 1, 2))), KindLogMatching)
}

func TestSimCheckerLeaderCompleteness(t *testing.T) {
	log := ents('a', 1, 1, 1)
	c := NewChecker([]uint64{1, 2, 3})
	// Node 1 commits three term-1 entries.
	mustObserve(t, c, view(1, raft.StateFollower, 1, 3, log))
	// A term-2 leader that holds them (plus its no-op) is fine.
	mustObserve(t, c, view(2, raft.StateLeader, 2, 0, append(ents('a', 1, 1, 1), raft.Entry{Term: 2, Index: 4})))
	// A term-3 leader missing entry 3 is not.
	wantKind(t, c.Observe(2, view(3, raft.StateLeader, 3, 0, append(ents('a', 1, 1), raft.Entry{Term: 3, Index: 3}))), KindLeaderCompleteness)
}

func TestSimCheckerStateMachineSafety(t *testing.T) {
	a, b := raft.Entry{Term: 1, Index: 1, Data: []byte("a")}, raft.Entry{Term: 1, Index: 1, Data: []byte("b")}
	t.Run("different apply", func(t *testing.T) {
		c := NewChecker([]uint64{1, 2})
		if v := c.OnApply(1, 1, 1, a); v != nil {
			t.Fatal(v)
		}
		wantKind(t, c.OnApply(2, 2, 1, b), KindStateMachineSafety)
	})
	t.Run("same apply on both is fine", func(t *testing.T) {
		c := NewChecker([]uint64{1, 2})
		if v := c.OnApply(1, 1, 1, a); v != nil {
			t.Fatal(v)
		}
		if v := c.OnApply(2, 2, 1, a); v != nil {
			t.Fatal(v)
		}
	})
	t.Run("gap", func(t *testing.T) {
		c := NewChecker([]uint64{1, 2})
		wantKind(t, c.OnApply(1, 1, 1, raft.Entry{Term: 1, Index: 2}), KindStateMachineSafety)
	})
	t.Run("replay in a new incarnation is fine", func(t *testing.T) {
		c := NewChecker([]uint64{1, 2})
		if v := c.OnApply(1, 1, 1, a); v != nil {
			t.Fatal(v)
		}
		if v := c.OnApply(2, 1, 2, a); v != nil {
			t.Fatal(v)
		}
	})
	t.Run("committed prefixes diverge", func(t *testing.T) {
		c := NewChecker([]uint64{1, 2})
		mustObserve(t, c, view(1, raft.StateFollower, 2, 2, ents('a', 1, 1)))
		// Terms differ at index 2, so Log Matching is silent, but both
		// claim index 2 committed.
		wantKind(t, c.Observe(2, view(2, raft.StateFollower, 3, 2, ents('a', 1, 2))), KindStateMachineSafety)
	})
	t.Run("apply contradicts committed log", func(t *testing.T) {
		c := NewChecker([]uint64{1, 2})
		mustObserve(t, c, view(1, raft.StateFollower, 1, 1, []raft.Entry{a}))
		wantKind(t, c.OnApply(2, 2, 1, b), KindStateMachineSafety)
	})
}

func TestSimCheckerMonotonicity(t *testing.T) {
	log := ents('a', 1, 1, 1, 1, 1)
	c := NewChecker([]uint64{1})
	mustObserve(t, c, view(1, raft.StateFollower, 1, 4, log))

	t.Run("commit back", func(t *testing.T) {
		wantKind(t, c.Observe(2, view(1, raft.StateFollower, 1, 3, log)), KindMonotonicity)
	})
	t.Run("applied back", func(t *testing.T) {
		v := view(1, raft.StateFollower, 1, 4, log)
		v.Applied = 4
		c := NewChecker([]uint64{1})
		mustObserve(t, c, v)
		v.Applied = 2
		wantKind(t, c.Observe(2, v), KindMonotonicity)
	})
	t.Run("applied beyond commit", func(t *testing.T) {
		v := view(1, raft.StateFollower, 1, 2, log)
		v.Applied = 3
		wantKind(t, NewChecker([]uint64{1}).Observe(2, v), KindMonotonicity)
	})
	t.Run("commit beyond last index", func(t *testing.T) {
		v := view(1, raft.StateFollower, 1, 9, log)
		wantKind(t, NewChecker([]uint64{1}).Observe(2, v), KindMonotonicity)
	})
	t.Run("restart may lower commit", func(t *testing.T) {
		v := view(1, raft.StateFollower, 1, 1, log)
		v.Incarnation = 2
		if viol := c.Observe(3, v); viol != nil {
			t.Fatalf("a restart legitimately resets the commit index: %v", viol)
		}
	})
	t.Run("term back", func(t *testing.T) {
		c := NewChecker([]uint64{1})
		mustObserve(t, c, view(1, raft.StateFollower, 5, 0, nil))
		wantKind(t, c.Observe(2, view(1, raft.StateFollower, 4, 0, nil)), KindMonotonicity)
	})
}

func TestSimChainHashes(t *testing.T) {
	a, b := ChainHashes(ents('a', 1, 1, 2)), ChainHashes(ents('a', 1, 1, 2))
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("hash %d not stable", i)
		}
	}
	d := ChainHashes(ents('b', 1, 1, 2))
	for i := range a {
		if a[i] == d[i] {
			t.Errorf("prefix hash %d ignores entry data", i)
		}
	}
}
