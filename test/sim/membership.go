package sim

import (
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Membership changes in the simulator (D-06, D-31, WBS 6.3.1).
//
// The "membership" chaos action asks the current leader for one random
// single-server change: add a learner (a brand-new node with the next
// sequential ID, empty disk and no bootstrap voters, D-32), promote a learner,
// remove a voter (the leader itself a third of the time), remove a learner,
// or transfer leadership. A third of the time an operator immediately asks
// for a second change, which a correct leader refuses while the first is
// pending (and which the planted bug lets through). A new leader is also
// asked for a change right after it wins, which a correct leader refuses
// until its no-op commits. Refusals are traced, not errors.
//
// A node whose removal commits (some node applies it) is either stopped for
// good after a while, or, half of the time, left running: it never learns
// it was removed and keeps campaigning, and the others ignore it (D-31).
//
// Liveness (LIVE-1) is judged on the members (voters and learners) of the
// newest committed configuration only: removed nodes and nodes whose
// AddLearner entry was truncated are not part of the cluster.

const (
	maxSimNodes   = 12 // nodes ever created per run, bootstrap ones included
	maxSimMembers = 8  // voters + learners
	minSimVoters  = 2  // no voter removal below this
)

// membershipAction runs one membership chaos action on the leader.
func (c *Cluster) membershipAction() {
	if l := c.leader(); l != nil {
		c.touch(l)
		l.do(func() { c.changeMembership(l, true) })
	}
}

// membershipLoop is the operator: besides the chaos action, a change every
// 200-1200 ms of the chaos phase, so a run sees dozens of them.
func (c *Cluster) membershipLoop() {
	if c.sched.Now() >= c.faultEnd || c.violation != nil {
		return
	}
	c.membershipAction()
	c.sched.After(200+c.rng.Int64N(1001), c.membershipLoop)
}

// changeMembership asks leader l for one random change. It must run inside
// l.do. again allows an immediate second request.
func (c *Cluster) changeMembership(l *node, again bool) {
	st := l.rn.Status()
	if st.State != raft.StateLeader {
		return
	}
	cs := st.Config
	pickOf := func(ids []uint64, skip uint64) uint64 {
		var cand []uint64
		for _, id := range ids {
			if id != skip {
				cand = append(cand, id)
			}
		}
		if len(cand) == 0 {
			return raft.None
		}
		return cand[c.rng.IntN(len(cand))]
	}
	var cc raft.ConfChange
	r := c.rng.IntN(10)
	switch {
	case r < 3 && len(cs.Voters)+len(cs.Learners) < maxSimMembers && len(c.nodes) < maxSimNodes:
		cc = raft.ConfChange{Type: raft.ConfChangeAddLearner, NodeID: uint64(len(c.nodes) + 1)}
	case r < 6 && len(cs.Learners) > 0:
		cc = raft.ConfChange{Type: raft.ConfChangePromoteLearner, NodeID: pickOf(cs.Learners, raft.None)}
	case r < 8 && len(cs.Voters) > minSimVoters:
		id := pickOf(cs.Voters, raft.None)
		if cs.IsVoter(l.id) && c.rng.IntN(3) == 0 {
			id = l.id // 6.1.5: the leader removes itself
		}
		cc = raft.ConfChange{Type: raft.ConfChangeRemoveNode, NodeID: id}
	case r < 9 && len(cs.Learners) > 0:
		cc = raft.ConfChange{Type: raft.ConfChangeRemoveNode, NodeID: pickOf(cs.Learners, raft.None)}
	default:
		to := pickOf(cs.Voters, l.id)
		if to == raft.None {
			return
		}
		err := l.rn.TransferLeadership(to)
		c.stats.Transfers++
		c.addTrace(Event{Kind: "transfer", Node: l.id, Peer: to, Note: errNote(err)})
		return
	}
	idx, term, err := l.rn.ProposeConfChange(cc)
	c.addTrace(Event{Kind: "conf-" + cc.Type.String(), Node: l.id, Peer: cc.NodeID, Index: idx, Term: term, Note: errNote(err)})
	if err != nil {
		c.stats.ConfRejected++
	} else {
		c.stats.ConfChanges++
		if cc.Type == raft.ConfChangeAddLearner {
			c.addJoiner(cc.NodeID)
		}
	}
	if again && c.rng.IntN(3) == 0 {
		c.changeMembership(l, false)
	}
}

func errNote(err error) string {
	if err != nil {
		return err.Error()
	}
	return "ok"
}

// addJoiner creates and starts node id with an empty disk and no bootstrap
// voters: it waits for the leader's log or snapshot (D-32).
func (c *Cluster) addJoiner(id uint64) {
	if id != uint64(len(c.nodes)+1) {
		panic("sim: joiner IDs must be sequential")
	}
	n := newNode(c, id)
	n.joiner = true
	c.nodes = append(c.nodes, n)
	c.ids = append(c.ids, id)
	c.checker.AddNode(id)
	n.start()
	c.touch(n)
}

// onConfApplied tracks the newest committed configuration: an applied
// conf-change entry is committed. Newly removed nodes are retired.
func (c *Cluster) onConfApplied(e raft.Entry) {
	if e.Index <= c.confIndex {
		return
	}
	_, cs, err := raft.DecodeConfChange(e.Data)
	if err != nil {
		panic("sim: undecodable conf-change entry: " + err.Error())
	}
	c.conf, c.confIndex = cs, e.Index
	c.addTrace(Event{Kind: "conf-committed", Index: e.Index, Note: cs.String()})
	for _, id := range cs.Removed {
		n := c.nodes[id-1]
		if n.removed {
			continue
		}
		n.removed = true
		if c.rng.IntN(2) == 0 {
			continue // left running, ignored by the others (D-31)
		}
		c.sched.After(50+c.rng.Int64N(2000), func() {
			n.retired = true
			if n.up {
				n.crash()
			}
			c.addTrace(Event{Kind: "retire", Node: n.id})
		})
	}
}

// members returns the nodes of the newest committed configuration.
func (c *Cluster) members() []*node {
	var out []*node
	for _, id := range c.conf.Members() {
		out = append(out, c.nodes[id-1])
	}
	return out
}

// onNewLeader asks a freshly elected leader for a membership change soon,
// before its no-op is likely to have committed: a correct leader refuses
// (ErrConfChangeTooEarly), a planted early-change bug does not.
func (c *Cluster) onNewLeader(n *node, term uint64) {
	if term <= c.lastLeaderTerm {
		return
	}
	c.lastLeaderTerm = term
	if c.profile.Weights.Membership == 0 || c.sched.Now() >= c.faultEnd || c.rng.IntN(2) != 0 {
		return
	}
	inc := n.inc
	c.sched.After(c.rng.Int64N(3), func() {
		if !n.up || n.inc != inc || c.sched.Now() >= c.faultEnd {
			return
		}
		c.touch(n)
		n.do(func() { c.changeMembership(n, true) })
	})
}
