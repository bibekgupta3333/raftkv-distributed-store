package sim

import (
	"encoding/binary"
	"errors"
	"math/rand/v2"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Liveness limits (LIVE-1): after the faults heal, a leader must appear
// within 10 maximum election timeouts and a new proposal must then commit on
// every node within the same budget. One maximum election timeout is
// 2*ElectionTick ticks of at most 101 ms.
const (
	maxElectionMs = 2 * raft.DefaultElectionTick * 101
	livenessMs    = 10 * maxElectionMs
)

// Hooks let tests plant bugs by corrupting the system under test or the
// checker's view of it (WBS 1.6, docs/testing.md 3.4). All are optional.
type Hooks struct {
	// Ready may rewrite a Ready before the driver handles it.
	Ready func(id uint64, rd *raft.Ready)
	// Message may rewrite a message just before delivery.
	Message func(m *raft.Message)
	// Filter may drop a message at send time (return false).
	Filter func(now int64, m raft.Message) bool
	// View may rewrite what the checker sees of a node.
	View func(v *NodeView)
}

// Stats summarises one run.
type Stats struct {
	Nodes          int
	Events         uint64
	Crashes        int
	Partitions     int
	MaxTerm        uint64
	Proposed       int
	Accepted       int
	Acked          int
	Committed      int
	Reads          int // issued
	ReadsServed    int
	ReadsRejected  int
	LeaseReads     int   // served from a lease
	LeaderAfterMs  int64 // heal to stable leader, -1 if never
	CommitAfterMs  int64 // heal to marker applied on all nodes, -1 if never
	Net            NetStats
	VirtualEndTime int64
}

// Cluster is the simulated cluster: nodes, network, workload, fault plan and
// checker, all driven by one [Scheduler] and one seeded PRNG (D-18, D-24).
type Cluster struct {
	seed    uint64
	rng     *rand.Rand
	sched   *Scheduler
	net     *Network
	trace   *Trace
	checker *Checker
	hooks   Hooks
	profile Profile

	ids   []uint64
	nodes []*node

	touched   []*node
	violation *Violation
	done      bool

	// workload
	nextCmd uint64
	pending map[uint64]pendingCmd
	stats   Stats

	// reads (reads.go)
	nextRead    uint64
	reads       map[uint64]*simRead // awaiting a ReadState
	outcomes    map[uint64]ReadOutcome
	maxCommit   uint64 // highest commit index any node has shown
	readChecked map[uint64]bool
	readSent    int64

	// liveness
	faultEnd      int64
	leaderAt      int64
	markerID      uint64
	markerSent    int64
	markerApplied map[uint64]bool
}

type pendingCmd struct {
	node, inc, index, term uint64
}

// NewCluster builds a cluster for seed. The cluster size comes from the
// profile and the seed. Nothing runs until [Cluster.Run].
func NewCluster(seed uint64, p Profile, h Hooks, record bool) *Cluster {
	rng := rand.New(rand.NewPCG(seed, 0x9e3779b97f4a7c15))
	c := &Cluster{
		seed: seed, rng: rng, sched: &Scheduler{}, trace: NewTrace(record), hooks: h, profile: p,
		pending: map[uint64]pendingCmd{}, markerApplied: map[uint64]bool{}, leaderAt: -1,
		reads: map[uint64]*simRead{}, outcomes: map[uint64]ReadOutcome{}, readChecked: map[uint64]bool{},
	}
	size := p.Sizes[rng.IntN(len(p.Sizes))]
	for i := 1; i <= size; i++ {
		c.ids = append(c.ids, uint64(i))
	}
	c.checker = NewChecker(c.ids)
	c.net = NewNetwork(c.sched, rng, p.Base, c.deliver)
	c.net.Filter = h.Filter
	if h.Message != nil {
		c.net.Tamper = h.Message
	}
	c.net.Trace = func(kind string, m raft.Message, note string) {
		c.addTrace(Event{
			Kind: kind, Node: m.From, Peer: m.To, Msg: m.Type.String(),
			Term: m.Term, Index: m.Index, Count: uint64(len(m.Entries)), Note: note,
		})
	}
	for _, id := range c.ids {
		c.nodes = append(c.nodes, newNode(c, id))
	}
	c.stats.Nodes = size
	c.stats.LeaderAfterMs, c.stats.CommitAfterMs = -1, -1
	return c
}

// IDs returns the node IDs.
func (c *Cluster) IDs() []uint64 { return c.ids }

func (c *Cluster) addTrace(e Event) {
	e.At = c.sched.Now()
	c.trace.Add(e)
}

func (c *Cluster) touch(n *node) {
	if !n.touched {
		n.touched = true
		c.touched = append(c.touched, n)
	}
}

func (c *Cluster) fail(v *Violation) {
	if c.violation == nil {
		c.violation = v
		c.addTrace(Event{Kind: "VIOLATION", Node: v.Node, Note: v.Kind + ": " + v.Detail})
	}
}

// deliver hands an arrived message to its destination node (if it is up).
func (c *Cluster) deliver(m raft.Message) {
	n := c.nodes[m.To-1]
	if !n.up {
		c.addTrace(Event{Kind: "lost", Node: m.From, Peer: m.To, Msg: m.Type.String(), Note: "node down"})
		return
	}
	c.touch(n)
	c.addTrace(Event{
		Kind: "deliver", Node: m.From, Peer: m.To, Msg: m.Type.String(),
		Term: m.Term, Index: m.Index, Count: uint64(len(m.Entries)),
	})
	n.do(func() { _ = n.rn.Step(m) })
}

// onApply is the state machine: it feeds the reference model and acks client
// commands (WBS 1.5.5).
func (c *Cluster) onApply(n *node, e raft.Entry) {
	if v := c.checker.OnApply(c.sched.Now(), n.id, n.inc, e); v != nil {
		c.fail(v)
	}
	if len(e.Data) < 8 {
		return // leader no-op
	}
	id := binary.BigEndian.Uint64(e.Data)
	if id == c.markerID && id != 0 {
		c.markerApplied[n.id] = true
	}
	if p, ok := c.pending[id]; ok && p.node == n.id && p.inc == n.inc && p.index == e.Index && p.term == e.Term {
		delete(c.pending, id)
		c.stats.Acked++
		c.addTrace(Event{Kind: "ack", Node: n.id, Index: e.Index, Term: e.Term})
	}
}

// observe runs the checker over every node touched since the last event.
func (c *Cluster) observe() {
	for _, n := range c.touched {
		n.touched = false
		if c.violation != nil || !n.up {
			continue
		}
		v := n.view()
		c.maxCommit = max(c.maxCommit, v.Commit) // READ-1 baseline, before any planted rewrite
		if c.hooks.View != nil {
			c.hooks.View(&v)
		}
		c.stats.MaxTerm = max(c.stats.MaxTerm, v.Term)
		if vi := c.checker.Observe(c.sched.Now(), v); vi != nil {
			c.fail(vi)
		}
	}
	c.touched = c.touched[:0]
}

// Boot starts every node. Run calls it; scenario tests that drive the
// cluster by hand call it themselves.
func (c *Cluster) Boot() {
	for _, n := range c.nodes {
		n.start()
		c.touch(n)
	}
}

// StartWorkload begins the client command and read generators.
func (c *Cluster) StartWorkload() {
	c.sched.After(50, c.clientLoop)
	c.sched.After(70, c.readLoop)
}

// RunFor advances virtual time by d ms (or until an invariant fails),
// checking invariants after every event.
func (c *Cluster) RunFor(d int64) {
	until := c.sched.Now() + d
	for c.violation == nil && c.sched.Pending() > 0 && c.sched.Now() <= until && c.sched.Step() {
		c.observe()
	}
}

// Net exposes the virtual network, for scenario tests.
func (c *Cluster) Net() *Network { return c.net }

// Violation is the first invariant failure so far, or nil.
func (c *Cluster) Violation() *Violation { return c.violation }

// Crash stops node id immediately, dropping its volatile state. It stays down
// until [Cluster.Restart].
func (c *Cluster) Crash(id uint64) {
	n := c.nodes[id-1]
	if n.up {
		n.crash()
		c.stats.Crashes++
	}
}

// Restart brings node id back from its disk.
func (c *Cluster) Restart(id uint64) { c.restartNode(c.nodes[id-1]) }

// Leader returns the ID and term of the up node that leads in the highest
// term, or zeros.
func (c *Cluster) Leader() (id, term uint64) {
	if l := c.leader(); l != nil {
		return l.id, l.rn.Status().Term
	}
	return 0, 0
}

// Status returns node id's Raft status; ok is false while it is down.
func (c *Cluster) Status(id uint64) (st raft.Status, ok bool) {
	n := c.nodes[id-1]
	if !n.up {
		return raft.Status{}, false
	}
	return n.rn.Status(), true
}

// PersistedLogLen is the number of entries on node id's disk.
func (c *Cluster) PersistedLogLen(id uint64) int { return len(c.nodes[id-1].log) }

// Committed is the length of the committed sequence the checker has seen.
func (c *Cluster) Committed() int { return c.checker.CommittedLen() }

// Run executes the whole scenario: chaos phase, heal, liveness check. It
// returns when the scenario ends or an invariant fails.
func (c *Cluster) Run() Result {
	c.Boot()
	c.faultEnd = c.profile.FaultMs
	c.StartWorkload()
	if c.profile.Weights.total() > 0 {
		c.sched.After(500, c.chaos)
	}
	c.sched.After(c.faultEnd, c.heal)
	horizon := c.faultEnd + 2*livenessMs + 5_000
	for c.violation == nil && !c.done && c.sched.Now() <= horizon && c.sched.Step() {
		c.observe()
	}
	c.stats.Events = c.trace.Len()
	c.stats.Net = c.net.Stats
	c.stats.VirtualEndTime = c.sched.Now()
	c.stats.Committed = c.checker.CommittedLen()
	return Result{
		Seed: c.seed, Profile: c.profile.Name, Violation: c.violation,
		TraceHash: c.trace.Hash(), Stats: c.stats, trace: c.trace,
	}
}

// ---------------------------------------------------------------- workload

func (c *Cluster) up() []*node {
	var out []*node
	for _, n := range c.nodes {
		if n.up {
			out = append(out, n)
		}
	}
	return out
}

// leader returns the up node that is leader in the highest term, or nil.
func (c *Cluster) leader() *node {
	var best *node
	var bestTerm uint64
	for _, n := range c.nodes {
		if !n.up {
			continue
		}
		if st := n.rn.Status(); st.State == raft.StateLeader && (best == nil || st.Term > bestTerm) {
			best, bestTerm = n, st.Term
		}
	}
	return best
}

func cmdData(id uint64, pad int) []byte {
	b := make([]byte, 8+pad)
	binary.BigEndian.PutUint64(b, id)
	return b
}

// propose submits data at n. It must run inside n.do. A follower that knows
// the leader redirects the client once.
func (c *Cluster) propose(n *node, data []byte, redirect bool) {
	idx, term, err := n.rn.Propose(data)
	id := binary.BigEndian.Uint64(data)
	c.stats.Proposed++
	var nl *raft.NotLeaderError
	switch {
	case err == nil:
		c.stats.Accepted++
		c.pending[id] = pendingCmd{node: n.id, inc: n.inc, index: idx, term: term}
		c.addTrace(Event{Kind: "propose", Node: n.id, Index: idx, Term: term, Count: id & 0xffff})
	case errors.As(err, &nl) && redirect && nl.Leader >= 1 && int(nl.Leader) <= len(c.nodes):
		t := c.nodes[nl.Leader-1]
		c.touch(t)
		t.do(func() { c.propose(t, data, false) })
	}
}

// clientLoop is the workload generator: a command every 20-100 ms sent to a
// random node, from the start of the run until the liveness check ends.
func (c *Cluster) clientLoop() {
	if c.done || c.violation != nil {
		return
	}
	if ups := c.up(); len(ups) > 0 {
		n := ups[c.rng.IntN(len(ups))]
		c.nextCmd++
		data := cmdData(c.nextCmd, c.rng.IntN(16))
		c.touch(n)
		n.do(func() { c.propose(n, data, true) })
	}
	c.sched.After(20+c.rng.Int64N(81), c.clientLoop)
}

// ------------------------------------------------------------------- faults

func (c *Cluster) crashNode(n *node) {
	if !n.up {
		return
	}
	n.crash()
	c.stats.Crashes++
	delay := 300 + c.rng.Int64N(4700)
	c.sched.After(delay, func() { c.restartNode(n) })
}

func (c *Cluster) restartNode(n *node) {
	if n.up {
		return
	}
	n.start()
	c.touch(n)
}

func (c *Cluster) pickNode() *node { return c.nodes[c.rng.IntN(len(c.nodes))] }

// chaos applies one random fault from the profile and reschedules itself
// until the chaos phase ends (WBS 1.5.3).
func (c *Cluster) chaos() {
	if c.sched.Now() >= c.faultEnd || c.violation != nil {
		return
	}
	w := c.profile.Weights
	r := c.rng.IntN(w.total())
	pick := func(weight int) bool {
		if r < weight {
			return true
		}
		r -= weight
		return false
	}
	switch {
	case pick(w.Partition):
		perm := c.rng.Perm(len(c.ids))
		cut := 1 + c.rng.IntN(len(c.ids)-1)
		var a, b []uint64
		for i, p := range perm {
			if i < cut {
				a = append(a, c.ids[p])
			} else {
				b = append(b, c.ids[p])
			}
		}
		c.net.Partition(a, b)
		c.stats.Partitions++
		c.addTrace(Event{Kind: "partition", Note: fmtGroups(a, b)})
	case pick(w.IsolateLeader):
		id := c.pickNode().id
		if l := c.leader(); l != nil {
			id = l.id
		}
		c.net.Isolate(id, c.ids)
		c.stats.Partitions++
		c.addTrace(Event{Kind: "isolate", Node: id})
	case pick(w.OneWay):
		a, b := c.pickNode().id, c.pickNode().id
		if a != b {
			c.net.BlockOneWay(a, b)
			c.addTrace(Event{Kind: "oneway", Node: a, Peer: b})
		}
	case pick(w.Heal):
		c.net.Heal()
		c.addTrace(Event{Kind: "heal-net"})
	case pick(w.Crash):
		c.crashNode(c.pickNode())
	case pick(w.CrashLeader):
		if l := c.leader(); l != nil {
			c.crashNode(l)
		}
	case pick(w.Degrade):
		a, b := c.pickNode().id, c.pickNode().id
		if a != b {
			c.net.SetLink(a, b, c.profile.Degraded)
			c.addTrace(Event{Kind: "degrade", Node: a, Peer: b})
		}
	default:
		c.net.ResetLinks()
		c.addTrace(Event{Kind: "reset-links"})
	}
	c.sched.After(200+c.rng.Int64N(2300), c.chaos)
}

func fmtGroups(a, b []uint64) string {
	s := "["
	for _, x := range a {
		s += itoa(x) + " "
	}
	s += "| "
	for _, x := range b {
		s += itoa(x) + " "
	}
	return s + "]"
}

// heal ends the chaos phase: the network becomes reliable, every node is
// restarted, and the liveness monitor starts (WBS 1.6.4).
func (c *Cluster) heal() {
	c.net.Heal()
	c.net.ResetLinks()
	c.net.SetDefault(ReliableLink)
	c.addTrace(Event{Kind: "heal"})
	for _, n := range c.nodes {
		c.restartNode(n)
	}
	c.sched.After(100, c.monitor)
}

// monitor is the LIVE-1 check: after heal, exactly one leader of the highest
// term must emerge, and then a new marker proposal must apply on every node,
// each within livenessMs of virtual time.
func (c *Cluster) monitor() {
	if c.done || c.violation != nil {
		return
	}
	now, healAt := c.sched.Now(), c.faultEnd
	if c.leaderAt < 0 {
		if l := c.leader(); l != nil && c.isHighestTerm(l) {
			c.leaderAt = now
			c.stats.LeaderAfterMs = now - healAt
			c.addTrace(Event{Kind: "stable", Node: l.id, Term: l.rn.Status().Term})
		} else if now-healAt > livenessMs {
			c.fail(&Violation{Kind: KindLiveness, At: now,
				Detail: "no stable leader " + itoa(uint64(now-healAt)) + "ms after the faults healed"})
			return
		}
	}
	if c.leaderAt >= 0 {
		if len(c.markerApplied) == len(c.nodes) {
			if c.stats.CommitAfterMs < 0 {
				c.stats.CommitAfterMs = now - healAt
			}
			// Then a linearizable read must complete on every node.
			if c.readLiveness(now) {
				c.done = true
				return
			}
			if now-c.leaderAt > livenessMs {
				c.fail(&Violation{Kind: KindLiveness, At: now,
					Detail: "linearizable reads did not complete on all nodes " + itoa(uint64(now-c.leaderAt)) + "ms after a leader was elected"})
				return
			}
			c.sched.After(100, c.monitor)
			return
		}
		if now-c.leaderAt > livenessMs {
			c.fail(&Violation{Kind: KindLiveness, At: now,
				Detail: "marker proposal not applied on all nodes " + itoa(uint64(now-c.leaderAt)) + "ms after a leader was elected"})
			return
		}
		if c.markerID == 0 || now-c.markerSent >= 1000 {
			if c.markerID == 0 {
				c.markerID = 1 << 62
			}
			c.markerSent = now
			if l := c.leader(); l != nil {
				c.touch(l)
				data := cmdData(c.markerID, 0)
				l.do(func() { c.propose(l, data, false) })
			}
		}
	}
	c.sched.After(100, c.monitor)
}

func (c *Cluster) isHighestTerm(l *node) bool {
	lt := l.rn.Status().Term
	for _, n := range c.nodes {
		if n.up && n.rn.Status().Term > lt {
			return false
		}
	}
	return true
}
