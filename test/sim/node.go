package sim

import (
	"math/rand/v2"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/lease"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// node is one simulated server: a RawNode plus its disk and driver state.
//
// The disk (store) only ever holds what a Ready asked to persist, and only
// after the simulated fsync delay has passed. A crash therefore loses exactly
// the state that was not yet durable (WBS 1.5.4). The state machine is
// volatile and rebuilt by replaying committed entries after a restart.
type node struct {
	id uint64
	c  *Cluster

	store *raft.MemoryStorage
	rn    *raft.RawNode // nil while down
	up    bool
	inc   uint64 // incarnation: bumped on every (re)start
	busy  bool   // a Ready is outstanding (persist pending)
	inbox []func()
	// period is this node's tick interval in ms, so nodes drift apart (D-08).
	// It is also its clock rate: the local clock runs at 100/period of
	// virtual time (see localNow), measured from (clockAt, clockBase).
	period             int64
	clockAt, clockBase int64

	// Lease reads (D-25), when the profile enables them. Volatile.
	lease     *lease.Lease
	renewing  bool
	renewID   uint64
	renewAt   int64 // local clock when the renewal round was requested
	renewTerm uint64

	// log mirrors the persisted entries 1..len(log), with chained hashes, for
	// the checker. It always equals what store holds.
	log     []raft.Entry
	hashes  []uint64
	touched bool
}

func newNode(c *Cluster, id uint64) *node {
	lo, hi := c.profile.Periods[0], c.profile.Periods[1]
	if lo == 0 {
		lo, hi = 95, 105
	}
	n := &node{id: id, c: c, store: raft.NewMemoryStorage(), period: lo + c.rng.Int64N(hi-lo+1)}
	n.lease = n.newLease()
	return n
}

// start (re)creates the RawNode from the disk. D-24: its randomness is a PCG
// seeded from the cluster source.
func (n *node) start() {
	c := n.c
	cfg := raft.DefaultConfig(n.id, c.ids, n.store, rand.New(rand.NewPCG(c.rng.Uint64(), n.id)))
	rn, err := raft.NewRawNode(cfg)
	if err != nil {
		panic("sim: NewRawNode: " + err.Error())
	}
	n.rn, n.up, n.busy, n.inbox = rn, true, false, nil
	n.renewing = false
	n.lease.Revoke()
	n.inc++
	n.reloadMirror()
	c.addTrace(Event{Kind: "start", Node: n.id, Note: "inc=" + itoa(n.inc)})
	n.scheduleTick()
}

// reloadMirror rebuilds the log mirror from storage.
func (n *node) reloadMirror() {
	last, _ := n.store.LastIndex()
	n.log, n.hashes = nil, nil
	if last >= 1 {
		ents, err := n.store.Entries(1, last+1, ^uint64(0))
		if err != nil {
			panic("sim: reading own storage: " + err.Error())
		}
		n.log = ents
		n.hashes = ChainHashes(ents)
	}
}

// crash drops all volatile state, including a Ready whose persist is pending.
func (n *node) crash() {
	n.up, n.rn, n.busy, n.inbox = false, nil, false, nil
	n.inc++ // invalidates pending persist and tick events
	n.c.addTrace(Event{Kind: "crash", Node: n.id})
}

func (n *node) scheduleTick() {
	inc := n.inc
	jitter := n.c.rng.Int64N(7) - 3
	n.c.sched.After(n.period+jitter, func() {
		if !n.up || n.inc != inc {
			return
		}
		n.c.touch(n)
		n.do(func() {
			n.rn.Tick()
			n.maybeRenewLease()
		})
		n.scheduleTick()
	})
}

// do runs f against the RawNode, or queues it while a Ready is outstanding
// (the core panics on use between Ready and Advance), then drives the Ready
// loop.
func (n *node) do(f func()) {
	if !n.up {
		return
	}
	if n.busy {
		n.inbox = append(n.inbox, f)
		return
	}
	f()
	n.process()
}

// process is the driver loop of internal/raft/doc.go: persist, send, apply,
// Advance.
func (n *node) process() {
	c := n.c
	for n.up && !n.busy && n.rn.HasReady() {
		rd := n.rn.Ready()
		if c.hooks.Ready != nil {
			c.hooks.Ready(n.id, &rd)
		}
		n.busy = true
		inc := n.inc
		finish := func() {
			if n.inc != inc || !n.up {
				return // crashed before the fsync finished: Ready is lost
			}
			c.touch(n)
			n.persist(rd)
			for _, m := range rd.Messages {
				c.net.Send(m)
			}
			for _, e := range rd.CommittedEntries {
				c.onApply(n, e)
			}
			c.onReadStates(n, rd)
			n.rn.Advance(rd)
			n.busy = false
			for len(n.inbox) > 0 && n.up {
				f := n.inbox[0]
				n.inbox = n.inbox[1:]
				f()
			}
		}
		if len(rd.Entries) > 0 || !rd.HardState.IsEmpty() {
			c.sched.After(1+c.rng.Int64N(5), func() {
				finish()
				n.process()
			})
			return
		}
		finish()
	}
}

// persist applies rd.Entries and rd.HardState to the disk (D-01 step 1).
func (n *node) persist(rd raft.Ready) {
	if len(rd.Entries) > 0 {
		n.store.Append(rd.Entries)
		first := int(rd.Entries[0].Index)
		if first-1 < len(n.log) {
			n.log = n.log[: first-1 : first-1]
			n.hashes = n.hashes[: first-1 : first-1]
		}
		prev := uint64(0)
		if len(n.hashes) > 0 {
			prev = n.hashes[len(n.hashes)-1]
		}
		for _, e := range rd.Entries {
			prev = ChainHash(prev, HashEntry(e))
			n.log = append(n.log, e)
			n.hashes = append(n.hashes, prev)
		}
		n.c.addTrace(Event{Kind: "persist", Node: n.id, Index: rd.Entries[0].Index, Count: uint64(len(rd.Entries))})
	}
	if !rd.HardState.IsEmpty() {
		n.store.SetHardState(rd.HardState)
	}
}

func (n *node) view() NodeView {
	v := NodeView{ID: n.id, Incarnation: n.inc, Log: n.log, Hashes: n.hashes, Synced: !n.busy}
	st := n.rn.Status()
	v.State, v.Term, v.Commit, v.Applied, v.LastIndex = st.State, st.Term, st.Commit, st.Applied, st.LastIndex
	return v
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}
