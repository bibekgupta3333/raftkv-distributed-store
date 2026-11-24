package sim

import (
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Membership invariants (D-06, D-31, WBS 6.3.1).
//
//	CONF-1 "no two disjoint majorities": for a leader, the configuration in
//	effect at the highest index known to be committed and every
//	configuration appended after it in the leader's log pairwise have
//	intersecting majorities (raft.QuorumsOverlap over the voters). With one
//	change at a time there is at most one such later configuration and it
//	is one server away from the committed one; two pending changes (the
//	planted bug) put two configurations two servers apart in play at once.
//
//	CONF-2 current-term commit first: a leader of term T never holds a
//	conf-change entry of term T while its commit index is below its first
//	entry of term T (its no-op). Without this rule a new leader can stack
//	a change on a configuration an earlier leader already left behind (the
//	single-server-change bug, Ongaro, raft-dev 2015).
//
// Why CONF-1 is checked per leader and not over every node's configuration:
// a stale node may still hold an appended-but-doomed configuration (an entry
// the next leader will truncate) that is two servers away from the leader's
// current one. That pair has disjoint majorities on paper, but the stale
// node cannot win an election with it: the voters that moved on have a
// higher-term log and refuse it. A global pairwise check would report such
// harmless states; the configurations that can actually gather a quorum
// together are the committed one and those a leader appends on top of it.
//
// "Highest index known to be committed" is the leader's own commit index,
// raised to the highest commit index any node has reported when the leader
// is of the newest term (its log then holds every committed entry, INV-4).
// The raise matters for a fresh leader whose commit index still lags a
// change its predecessor committed before appending the next one.
const (
	KindConfigOverlap = "ConfigOverlap" // CONF-1
	KindConfigEarly   = "ConfigEarly"   // CONF-2
)

// confPos is one conf-change entry in a log and the configuration it sets.
type confPos struct {
	index, term uint64
	cs          raft.ConfState
}

// confCache remembers the conf-change entries of a node's log prefix, so a
// leader's growing log is scanned once.
type confCache struct {
	upto  int    // entries 1..upto scanned
	hash  uint64 // prefix hash at upto, to detect truncation
	confs []confPos
}

// confsOf returns the conf-change entries of v.Log, in order.
func (c *Checker) confsOf(v NodeView) ([]confPos, error) {
	cc := c.confCaches[v.ID]
	if cc == nil {
		cc = &confCache{}
		c.confCaches[v.ID] = cc
	}
	if cc.upto > len(v.Log) || (cc.upto > 0 && v.Hashes[cc.upto-1] != cc.hash) {
		*cc = confCache{} // the log was truncated or replaced: rescan
	}
	for i := cc.upto; i < len(v.Log); i++ {
		e := v.Log[i]
		if e.Type != raft.EntryConfChange {
			continue
		}
		_, cs, err := raft.DecodeConfChange(e.Data)
		if err != nil {
			return nil, err
		}
		cc.confs = append(cc.confs, confPos{index: e.Index, term: e.Term, cs: cs})
	}
	cc.upto = len(v.Log)
	if cc.upto > 0 {
		cc.hash = v.Hashes[cc.upto-1]
	}
	return cc.confs, nil
}

// checkConfig runs CONF-1 and CONF-2 on a leader view.
func (c *Checker) checkConfig(at int64, v NodeView, maxCommit uint64) *Violation {
	if v.State != raft.StateLeader {
		return nil
	}
	confs, err := c.confsOf(v)
	if err != nil {
		return viol(KindConfigOverlap, at, v.ID, "undecodable conf-change entry: %v", err)
	}
	if len(confs) == 0 {
		return nil
	}
	// CONF-2.
	first := uint64(0)
	for _, e := range v.Log {
		if e.Term == v.Term {
			first = e.Index
			break
		}
	}
	for _, p := range confs {
		if p.term == v.Term && first != 0 && v.Commit < first {
			return viol(KindConfigEarly, at, v.ID,
				"leader of term %d holds conf change at %d before committing its first entry %d (commit %d)",
				v.Term, p.index, first, v.Commit)
		}
	}
	// CONF-1.
	known := v.Commit
	if c.isNewestTerm(v.Term) {
		known = max(known, min(maxCommit, uint64(len(v.Log))))
	}
	inPlay := []raft.ConfState{{Voters: c.bootstrap}}
	for _, p := range confs {
		if p.index <= known {
			inPlay[0] = p.cs
			continue
		}
		inPlay = append(inPlay, p.cs)
	}
	for i := range inPlay {
		for j := i + 1; j < len(inPlay); j++ {
			if !raft.QuorumsOverlap(inPlay[i].Voters, inPlay[j].Voters) {
				return viol(KindConfigOverlap, at, v.ID,
					"leader of term %d has configurations voters=%v and voters=%v in play at once (commit %d): their majorities can be disjoint",
					v.Term, inPlay[i].Voters, inPlay[j].Voters, known)
			}
		}
	}
	return nil
}

// isNewestTerm reports whether no node has shown a term above term.
func (c *Checker) isNewestTerm(term uint64) bool {
	for _, v := range c.views {
		if v.Term > term {
			return false
		}
	}
	return true
}
