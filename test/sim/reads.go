package sim

import (
	"encoding/binary"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/lease"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Linearizable reads in the simulator (WBS 4.1.4, 4.2.5, D-05, D-25).
//
// The read workload sends ReadIndex requests to random nodes (leader and
// followers). Each read records, when it is issued, the highest commit index
// any node has reached so far. Everything at or below that index was committed
// (and may have been acknowledged to a client) before the read started, so a
// linearizable read must be served at an index at least that high:
//
//	READ-1: readIndex >= max commit index seen anywhere when the read started.
//
// A stale leader (partitioned, or deposed by a newer leader) that answered a
// read would break READ-1 as soon as the newer leader commits anything.
//
//	READ-2: a read answered by a leader is served at an entry of that
//	leader's own term, i.e. not before its no-op committed (T6).
//
// With lease reads on (Profile.LeaseReads), a leader holding a valid lease
// answers from its commit index without a round, mirroring the node. Lease
// time is measured on each node's local clock, which runs at 100/period of
// virtual time, so per-node tick periods are also clock-rate drift.

// Read invariant kinds.
const (
	KindStaleRead   = "LinearizableRead" // READ-1
	KindReadNoNoop  = "ReadBeforeNoop"   // READ-2
	nominalPeriodMs = 100                // the nominal tick period; clocks run at nominal/period
)

// ReadOutcome is what happened to one read issued with [Cluster.Read].
type ReadOutcome struct {
	Issued   bool
	Done     bool // answered: served (Index valid) or rejected
	Rejected bool
	Lease    bool // served from the lease, without a round
	Index    uint64
	MinIndex uint64 // max commit index seen anywhere when issued
}

type simRead struct {
	id         uint64
	node, inc  uint64
	issuedAt   int64
	minIndex   uint64
	leaderTerm uint64 // term if issued on the leader, else 0
	liveness   bool
}

// leaseCtx marks a node's own lease-renewal request; reads use readCtx.
func readCtx(id uint64) []byte  { return binary.BigEndian.AppendUint64([]byte{'R'}, id) }
func leaseCtx(id uint64) []byte { return binary.BigEndian.AppendUint64([]byte{'L'}, id) }

func parseCtx(b []byte) (kind byte, id uint64, ok bool) {
	if len(b) != 9 {
		return 0, 0, false
	}
	return b[0], binary.BigEndian.Uint64(b[1:]), true
}

// localNow is node n's clock in ms: it advances at nominal/period of virtual
// time, so a node with a long tick period has a slow clock.
func (n *node) localNow() int64 {
	return n.clockBase + (n.c.sched.Now()-n.clockAt)*nominalPeriodMs/n.period
}

// setPeriod changes n's tick period (and so its clock rate) from now on.
func (n *node) setPeriod(p int64) {
	n.clockBase, n.clockAt = n.localNow(), n.c.sched.Now()
	n.period = p
}

func (n *node) newLease() *lease.Lease {
	d, err := lease.Config{
		ElectionTick: raft.DefaultElectionTick, Tick: nominalPeriodMs * time.Millisecond,
		Drift: lease.DefaultDrift, Margin: lease.DefaultMargin,
	}.Duration()
	if err != nil {
		panic(err)
	}
	return lease.New(d)
}

func ms(v int64) time.Duration { return time.Duration(v) * time.Millisecond }

// maybeRenewLease runs on every tick of a leader with lease reads on: one
// renewal round at a time, timed by the leader's own clock (D-25).
func (n *node) maybeRenewLease() {
	c := n.c
	if !c.profile.LeaseReads || n.renewing {
		return
	}
	st := n.rn.Status()
	if st.State != raft.StateLeader {
		return
	}
	c.nextRead++
	if n.rn.ReadIndex(leaseCtx(c.nextRead)) == nil {
		n.renewing, n.renewID, n.renewAt, n.renewTerm = true, c.nextRead, n.localNow(), st.Term
	}
}

// Read issues a linearizable read on node id (scenario tests) and returns its
// ID for [Cluster.ReadOutcome].
func (c *Cluster) Read(id uint64) uint64 {
	n := c.nodes[id-1]
	c.nextRead++
	rid := c.nextRead
	c.touch(n)
	n.do(func() { c.issueRead(n, rid, false) })
	return rid
}

// ReadOutcome reports what happened to read rid.
func (c *Cluster) ReadOutcome(rid uint64) ReadOutcome { return c.outcomes[rid] }

// SetPeriod changes node id's tick period (its clock rate) from now on.
func (c *Cluster) SetPeriod(id uint64, periodMs int64) { c.nodes[id-1].setPeriod(periodMs) }

// issueRead starts read rid on n. It must run inside n.do.
func (c *Cluster) issueRead(n *node, rid uint64, liveness bool) {
	st := n.rn.Status()
	r := &simRead{id: rid, node: n.id, inc: n.inc, issuedAt: c.sched.Now(), minIndex: c.maxCommit, liveness: liveness}
	if st.State == raft.StateLeader {
		r.leaderTerm = st.Term
	}
	c.stats.Reads++
	out := ReadOutcome{Issued: true, MinIndex: r.minIndex}
	if c.profile.LeaseReads && st.State == raft.StateLeader && n.lease.Valid(st.Term, ms(n.localNow())) {
		// D-25: no round trip; the commit index is the read index.
		out.Lease = true
		c.outcomes[rid] = out
		c.stats.LeaseReads++
		c.serveRead(n, r, st.Commit)
		return
	}
	c.outcomes[rid] = out
	if err := n.rn.ReadIndex(readCtx(rid)); err != nil {
		out.Done, out.Rejected = true, true
		c.outcomes[rid] = out
		return
	}
	c.reads[rid] = r
}

// onReadStates handles a Ready's answers on n, then revokes n's lease if it
// stopped leading. ReadStates first: a round that finished before a step-down
// in the same Ready was confirmed while n still led, and the lease term check
// stops it from outliving the step-down.
func (c *Cluster) onReadStates(n *node, rd raft.Ready) {
	for _, rs := range rd.ReadStates {
		kind, id, ok := parseCtx(rs.RequestCtx)
		if !ok {
			continue
		}
		if kind == 'L' {
			if id == n.renewID && n.renewing {
				n.renewing = false
				if !rs.Rejected {
					n.lease.Extend(n.renewTerm, ms(n.renewAt))
				}
			}
			continue
		}
		r, ok := c.reads[id]
		if !ok || r.node != n.id || r.inc != n.inc {
			continue // a duplicate answer, or one from before a crash
		}
		delete(c.reads, id)
		out := c.outcomes[id]
		out.Done = true
		if rs.Rejected {
			out.Rejected = true
			c.outcomes[id] = out
			c.stats.ReadsRejected++
			continue
		}
		c.outcomes[id] = out
		c.serveRead(n, r, rs.Index)
	}
	if rd.SoftState != nil && rd.SoftState.State != raft.StateLeader {
		n.lease.Revoke()
	}
}

// serveRead checks READ-1 and READ-2 for a read answered at index.
func (c *Cluster) serveRead(n *node, r *simRead, index uint64) {
	out := c.outcomes[r.id]
	out.Done, out.Index = true, index
	c.outcomes[r.id] = out
	c.stats.ReadsServed++
	if r.liveness {
		c.readChecked[n.id] = true
	}
	c.addTrace(Event{Kind: "read", Node: n.id, Index: index, Count: r.minIndex, Note: "issued=" + itoa(uint64(r.issuedAt))})
	if index < r.minIndex {
		c.fail(&Violation{Kind: KindStaleRead, At: c.sched.Now(), Node: n.id,
			Detail: "read issued at " + itoa(uint64(r.issuedAt)) + "ms served at index " + itoa(index) +
				", but index " + itoa(r.minIndex) + " was already committed"})
		return
	}
	if r.leaderTerm != 0 {
		if index == 0 || index > uint64(len(n.log)) || n.log[index-1].Term != r.leaderTerm {
			c.fail(&Violation{Kind: KindReadNoNoop, At: c.sched.Now(), Node: n.id,
				Detail: "leader of term " + itoa(r.leaderTerm) + " served a read at index " + itoa(index) +
					", which is not an entry of its term (no-op not committed)"})
		}
	}
}

// readLoop is the read workload: a read every 30-120 ms on a random up node.
func (c *Cluster) readLoop() {
	if c.done || c.violation != nil {
		return
	}
	if ups := c.up(); len(ups) > 0 {
		n := ups[c.rng.IntN(len(ups))]
		c.nextRead++
		rid := c.nextRead
		c.touch(n)
		n.do(func() { c.issueRead(n, rid, false) })
	}
	c.sched.After(30+c.rng.Int64N(91), c.readLoop)
}

// readLiveness is the second half of LIVE-1 once the marker is applied: a
// linearizable read must complete on every node (followers via the leader).
// It reports whether every node has served one.
func (c *Cluster) readLiveness(now int64) bool {
	if len(c.readChecked) == len(c.nodes) {
		return true
	}
	if c.readSent != 0 && now-c.readSent < 1000 {
		return false
	}
	c.readSent = now
	for _, n := range c.nodes {
		if c.readChecked[n.id] || !n.up {
			continue
		}
		c.nextRead++
		rid := c.nextRead
		c.touch(n)
		n.do(func() { c.issueRead(n, rid, true) })
	}
	return false
}
