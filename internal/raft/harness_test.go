package raft

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// testNode couples a RawNode with its MemoryStorage and applied entries.
type testNode struct {
	id      uint64
	rn      *RawNode
	st      *MemoryStorage
	applied []Entry
}

// network is a tiny deterministic in-test cluster: FIFO message queue, nodes
// processed in ID order, no goroutines, no clock.
type network struct {
	t       *testing.T
	ids     []uint64
	nodes   map[uint64]*testNode
	queue   []Message
	blocked map[[2]uint64]bool // directed links that drop messages
	sent    []Message          // every message ever sent (for assertions)
}

type netOpt func(*Config)

func withPreVote(v bool) netOpt     { return func(c *Config) { c.PreVote = v } }
func withCheckQuorum(v bool) netOpt { return func(c *Config) { c.CheckQuorum = v } }

func testRand(id uint64) *rand.Rand { return rand.New(rand.NewPCG(42, id)) }

func baseConfig(id uint64, peers []uint64, st Storage, opts ...netOpt) Config {
	cfg := DefaultConfig(id, peers, st, testRand(id))
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// newNetwork builds n nodes with IDs 1..n and empty storage.
func newNetwork(t *testing.T, n int, opts ...netOpt) *network {
	t.Helper()
	stores := make(map[uint64]*MemoryStorage, n)
	for i := 1; i <= n; i++ {
		stores[uint64(i)] = NewMemoryStorage()
	}
	return newNetworkWithStorage(t, stores, opts...)
}

func newNetworkWithStorage(t *testing.T, stores map[uint64]*MemoryStorage, opts ...netOpt) *network {
	t.Helper()
	nw := &network{t: t, nodes: map[uint64]*testNode{}, blocked: map[[2]uint64]bool{}}
	for id := range stores {
		nw.ids = append(nw.ids, id)
	}
	slices.Sort(nw.ids)
	for _, id := range nw.ids {
		rn, err := NewRawNode(baseConfig(id, nw.ids, stores[id], opts...))
		if err != nil {
			t.Fatalf("NewRawNode(%d): %v", id, err)
		}
		nw.nodes[id] = &testNode{id: id, rn: rn, st: stores[id]}
	}
	return nw
}

// storageWith returns storage holding entries with the given terms at
// indexes 1..len(terms) and the given hard-state term.
func storageWith(hardTerm uint64, terms ...uint64) *MemoryStorage {
	st := NewMemoryStorage()
	var ents []Entry
	for i, term := range terms {
		ents = append(ents, Entry{Term: term, Index: uint64(i + 1)})
	}
	st.Append(ents)
	st.SetHardState(HardState{Term: hardTerm})
	return st
}

func (nw *network) node(id uint64) *testNode { return nw.nodes[id] }

// drain handles every pending Ready of one node as a correct driver would:
// persist, then queue messages, then record applied entries, then Advance.
func (nw *network) drain(n *testNode) {
	for n.rn.HasReady() {
		rd := n.rn.Ready()
		n.st.Append(rd.Entries)
		if !rd.HardState.IsEmpty() {
			n.st.SetHardState(rd.HardState)
		}
		n.applied = append(n.applied, rd.CommittedEntries...)
		for _, m := range rd.Messages {
			nw.sent = append(nw.sent, m)
			if !nw.blocked[[2]uint64{m.From, m.To}] {
				nw.queue = append(nw.queue, m)
			}
		}
		n.rn.Advance(rd)
		if err := n.rn.raft.log.checkInvariants(); err != nil {
			nw.t.Fatalf("node %d log invariant: %v", n.id, err)
		}
	}
}

// settle delivers messages until the network is quiet.
func (nw *network) settle() {
	nw.t.Helper()
	for round := 0; ; round++ {
		if round > 10000 {
			nw.t.Fatal("network did not settle")
		}
		for _, id := range nw.ids {
			nw.drain(nw.nodes[id])
		}
		if len(nw.queue) == 0 {
			return
		}
		q := nw.queue
		nw.queue = nil
		for _, m := range q {
			if err := nw.nodes[m.To].rn.Step(m); err != nil {
				nw.t.Fatalf("Step: %v", err)
			}
		}
	}
}

func (nw *network) campaign(id uint64) {
	nw.t.Helper()
	nw.nodes[id].rn.Campaign()
	nw.settle()
}

func (nw *network) propose(id uint64, data string) (uint64, uint64, error) {
	nw.t.Helper()
	idx, term, err := nw.nodes[id].rn.Propose([]byte(data))
	nw.settle()
	return idx, term, err
}

// tick ticks every node once, in ID order, settling after each tick round.
func (nw *network) tick(rounds int) {
	nw.t.Helper()
	for range rounds {
		for _, id := range nw.ids {
			nw.nodes[id].rn.Tick()
		}
		nw.settle()
	}
}

// tickOnly ticks only the given node.
func (nw *network) tickOnly(id uint64, rounds int) {
	nw.t.Helper()
	for range rounds {
		nw.nodes[id].rn.Tick()
		nw.settle()
	}
}

func (nw *network) isolate(id uint64) {
	for _, o := range nw.ids {
		if o != id {
			nw.blocked[[2]uint64{id, o}] = true
			nw.blocked[[2]uint64{o, id}] = true
		}
	}
}

func (nw *network) heal() { nw.blocked = map[[2]uint64]bool{} }

func (nw *network) leaders() []uint64 {
	var out []uint64
	for _, id := range nw.ids {
		if nw.nodes[id].rn.Status().State == StateLeader {
			out = append(out, id)
		}
	}
	return out
}

// logOf returns the full stored log of a node (entries 1..last).
func (n *testNode) logOf() []Entry {
	last, _ := n.st.LastIndex()
	if last == 0 {
		return nil
	}
	ents, err := n.st.Entries(1, last+1, noLimit)
	if err != nil {
		panic(err)
	}
	return ents
}

func termsOf(ents []Entry) []uint64 {
	out := make([]uint64, len(ents))
	for i, e := range ents {
		out[i] = e.Term
	}
	return out
}
