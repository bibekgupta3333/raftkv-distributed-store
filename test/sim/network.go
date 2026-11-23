package sim

import (
	"math/rand/v2"
	"slices"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Link describes the faults of one directed link. Delays are uniform in
// [MinDelay, MaxDelay] virtual ms. Without Reorder, a link is FIFO (a message
// never overtakes an earlier one); with Reorder, that fraction of messages
// ignores FIFO and so may arrive out of order.
type Link struct {
	Drop     float64
	Dup      float64
	Reorder  float64
	MinDelay int64
	MaxDelay int64
}

// ReliableLink is a healthy link: 1-10 ms, no loss.
var ReliableLink = Link{MinDelay: 1, MaxDelay: 10}

type pair struct{ from, to uint64 }

// NetStats counts what the network did, for tests and reports.
type NetStats struct {
	Sent, Delivered, Dropped, Duplicated uint64
}

// Network is the virtual network (D-18). It delivers raft.Messages through
// the [Scheduler] with per-link faults and partitions. All randomness comes
// from the one seeded source.
type Network struct {
	sched   *Scheduler
	rng     *rand.Rand
	deliver func(raft.Message)

	def     Link
	links   map[pair]Link
	blocked map[pair]bool
	last    map[pair]int64

	// Filter, if set, may veto a message at send time (planted-bug tests).
	Filter func(now int64, m raft.Message) bool
	// Tamper, if set, may rewrite a message just before delivery
	// (planted-bug tests).
	Tamper func(m *raft.Message)
	// Trace, if set, observes network events: "send", "drop", "dup".
	Trace func(kind string, m raft.Message, note string)

	Stats NetStats
}

// NewNetwork returns a network whose deliveries call deliver.
func NewNetwork(s *Scheduler, rng *rand.Rand, def Link, deliver func(raft.Message)) *Network {
	return &Network{
		sched: s, rng: rng, deliver: deliver, def: def,
		links: map[pair]Link{}, blocked: map[pair]bool{}, last: map[pair]int64{},
	}
}

func cloneMsg(m raft.Message) raft.Message {
	m.Entries = slices.Clone(m.Entries)
	m.Context = slices.Clone(m.Context)
	return m
}

func (n *Network) trace(kind string, m raft.Message, note string) {
	if n.Trace != nil {
		n.Trace(kind, m, note)
	}
}

func (n *Network) link(p pair) Link {
	if l, ok := n.links[p]; ok {
		return l
	}
	return n.def
}

// SetDefault replaces the fault profile of every link without an override.
func (n *Network) SetDefault(l Link) { n.def = l }

// SetLink overrides one directed link.
func (n *Network) SetLink(from, to uint64, l Link) { n.links[pair{from, to}] = l }

// ResetLinks removes all per-link overrides.
func (n *Network) ResetLinks() { clear(n.links) }

// BlockOneWay drops everything sent from -> to (an asymmetric partition).
func (n *Network) BlockOneWay(from, to uint64) { n.blocked[pair{from, to}] = true }

// Partition blocks every link between different groups, in both directions.
// Nodes not listed in any group are unaffected.
func (n *Network) Partition(groups ...[]uint64) {
	for i, a := range groups {
		for j, b := range groups {
			if i == j {
				continue
			}
			for _, x := range a {
				for _, y := range b {
					n.blocked[pair{x, y}] = true
				}
			}
		}
	}
}

// Isolate cuts id off from every other node in all, in both directions.
func (n *Network) Isolate(id uint64, all []uint64) {
	for _, o := range all {
		if o != id {
			n.blocked[pair{id, o}] = true
			n.blocked[pair{o, id}] = true
		}
	}
}

// Heal removes all partitions and one-way blocks.
func (n *Network) Heal() { clear(n.blocked) }

// Blocked reports whether from -> to is currently cut.
func (n *Network) Blocked(from, to uint64) bool { return n.blocked[pair{from, to}] }

// Send puts m on the wire. The message is copied, so later changes by the
// sender do not affect it.
func (n *Network) Send(m raft.Message) {
	m = cloneMsg(m)
	n.Stats.Sent++
	n.trace("send", m, "")
	p := pair{m.From, m.To}
	if n.blocked[p] {
		n.drop(m, "partition")
		return
	}
	if n.Filter != nil && !n.Filter(n.sched.Now(), m) {
		n.drop(m, "filter")
		return
	}
	l := n.link(p)
	if n.rng.Float64() < l.Drop {
		n.drop(m, "loss")
		return
	}
	copies := 1
	if n.rng.Float64() < l.Dup {
		copies = 2
		n.Stats.Duplicated++
		n.trace("dup", m, "")
	}
	for range copies {
		n.schedule(p, l, cloneMsg(m))
	}
}

func (n *Network) drop(m raft.Message, why string) {
	n.Stats.Dropped++
	n.trace("drop", m, why)
}

func (n *Network) schedule(p pair, l Link, m raft.Message) {
	delay := l.MinDelay
	if l.MaxDelay > l.MinDelay {
		delay += n.rng.Int64N(l.MaxDelay - l.MinDelay + 1)
	}
	at := n.sched.Now() + delay
	if !(l.Reorder > 0 && n.rng.Float64() < l.Reorder) {
		at = max(at, n.last[p])
		n.last[p] = at
	}
	n.sched.After(at-n.sched.Now(), func() {
		if n.blocked[p] {
			n.drop(m, "partition-inflight")
			return
		}
		n.Stats.Delivered++
		if n.Tamper != nil {
			n.Tamper(&m)
		}
		n.deliver(m)
	})
}
