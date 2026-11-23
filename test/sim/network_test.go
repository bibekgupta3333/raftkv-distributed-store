package sim

import (
	"math/rand/v2"
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

type delivery struct {
	at  int64
	msg raft.Message
}

// netHarness is a Network plus a log of deliveries.
type netHarness struct {
	s   *Scheduler
	n   *Network
	got []delivery
}

func newNetHarness(seed uint64, l Link) *netHarness {
	h := &netHarness{s: &Scheduler{}}
	h.n = NewNetwork(h.s, rand.New(rand.NewPCG(seed, 7)), l, func(m raft.Message) {
		h.got = append(h.got, delivery{h.s.Now(), m})
	})
	return h
}

func (h *netHarness) sendSeq(from, to uint64, count int) {
	for i := 1; i <= count; i++ {
		h.n.Send(raft.Message{Type: raft.MsgApp, From: from, To: to, Index: uint64(i)})
	}
}

func (h *netHarness) drain() {
	for h.s.Step() {
	}
}

func TestSimNetworkReliableFIFO(t *testing.T) {
	h := newNetHarness(1, Link{MinDelay: 1, MaxDelay: 30})
	h.sendSeq(1, 2, 200)
	h.drain()
	if len(h.got) != 200 {
		t.Fatalf("delivered %d of 200 on a reliable link", len(h.got))
	}
	for i, d := range h.got {
		if d.msg.Index != uint64(i+1) {
			t.Fatalf("FIFO link reordered: position %d holds message %d", i, d.msg.Index)
		}
		if d.at < 1 || d.at > 30 {
			t.Fatalf("delivery at %dms outside the delay range [1,30]", d.at)
		}
	}
}

func TestSimNetworkDrop(t *testing.T) {
	t.Run("all", func(t *testing.T) {
		h := newNetHarness(1, Link{Drop: 1, MinDelay: 1, MaxDelay: 2})
		h.sendSeq(1, 2, 50)
		h.drain()
		if len(h.got) != 0 || h.n.Stats.Dropped != 50 {
			t.Fatalf("delivered=%d dropped=%d, want 0 and 50", len(h.got), h.n.Stats.Dropped)
		}
	})
	t.Run("some", func(t *testing.T) {
		h := newNetHarness(1, Link{Drop: 0.5, MinDelay: 1, MaxDelay: 2})
		h.sendSeq(1, 2, 1000)
		h.drain()
		if len(h.got) < 350 || len(h.got) > 650 {
			t.Fatalf("delivered %d of 1000 at 50%% loss", len(h.got))
		}
	})
}

func TestSimNetworkDuplicate(t *testing.T) {
	h := newNetHarness(1, Link{Dup: 1, MinDelay: 1, MaxDelay: 5})
	h.sendSeq(1, 2, 20)
	h.drain()
	if len(h.got) != 40 {
		t.Fatalf("delivered %d, want every message twice (40)", len(h.got))
	}
	count := map[uint64]int{}
	for _, d := range h.got {
		count[d.msg.Index]++
	}
	for i := uint64(1); i <= 20; i++ {
		if count[i] != 2 {
			t.Errorf("message %d delivered %d times", i, count[i])
		}
	}
}

func TestSimNetworkReorder(t *testing.T) {
	h := newNetHarness(1, Link{Reorder: 1, MinDelay: 1, MaxDelay: 100})
	h.sendSeq(1, 2, 200)
	h.drain()
	inversions := 0
	for i := 1; i < len(h.got); i++ {
		if h.got[i].msg.Index < h.got[i-1].msg.Index {
			inversions++
		}
	}
	if len(h.got) != 200 || inversions == 0 {
		t.Fatalf("delivered=%d inversions=%d; expected all messages and some reordering", len(h.got), inversions)
	}
}

func TestSimNetworkPartitions(t *testing.T) {
	all := []uint64{1, 2, 3}
	h := newNetHarness(1, ReliableLink)
	h.n.Partition([]uint64{1}, []uint64{2, 3})
	h.sendSeq(1, 2, 1)
	h.sendSeq(2, 1, 1)
	h.sendSeq(2, 3, 1) // same side: delivered
	h.drain()
	if len(h.got) != 1 || h.got[0].msg.To != 3 {
		t.Fatalf("partition leaked: %+v", h.got)
	}

	h.n.Heal()
	h.sendSeq(1, 2, 1)
	h.drain()
	if len(h.got) != 2 {
		t.Fatal("heal did not restore the link")
	}

	h.n.BlockOneWay(1, 2)
	h.sendSeq(1, 2, 1)
	h.sendSeq(2, 1, 1)
	h.drain()
	if len(h.got) != 3 || h.got[2].msg.To != 1 {
		t.Fatalf("one-way block should only stop 1->2: %+v", h.got[2:])
	}

	h.n.Heal()
	h.n.Isolate(3, all)
	h.sendSeq(1, 3, 1)
	h.sendSeq(3, 1, 1)
	h.sendSeq(1, 2, 1)
	h.drain()
	if len(h.got) != 4 {
		t.Fatalf("isolating node 3 should leave only 1->2: %+v", h.got[3:])
	}
}

func TestSimNetworkInFlightDroppedByPartition(t *testing.T) {
	h := newNetHarness(1, Link{MinDelay: 50, MaxDelay: 50})
	h.sendSeq(1, 2, 1)
	h.s.After(10, func() { h.n.Partition([]uint64{1}, []uint64{2}) })
	h.drain()
	if len(h.got) != 0 {
		t.Fatal("a message in flight when the link was cut must be lost")
	}
}

func TestSimNetworkPerLinkOverride(t *testing.T) {
	h := newNetHarness(1, ReliableLink)
	h.n.SetLink(1, 2, Link{Drop: 1, MinDelay: 1, MaxDelay: 1})
	h.sendSeq(1, 2, 5)
	h.sendSeq(1, 3, 5)
	h.drain()
	if len(h.got) != 5 {
		t.Fatalf("delivered %d, want only the 5 messages to node 3", len(h.got))
	}
	h.n.ResetLinks()
	h.sendSeq(1, 2, 5)
	h.drain()
	if len(h.got) != 10 {
		t.Fatal("ResetLinks did not clear the override")
	}
}

func TestSimNetworkDeterministic(t *testing.T) {
	run := func() []delivery {
		h := newNetHarness(42, Link{Drop: 0.2, Dup: 0.2, Reorder: 0.3, MinDelay: 1, MaxDelay: 50})
		h.sendSeq(1, 2, 300)
		h.drain()
		return h.got
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatalf("lengths differ: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].at != b[i].at || a[i].msg.Index != b[i].msg.Index {
			t.Fatalf("delivery %d differs between identical runs", i)
		}
	}
}

func TestSimNetworkMessageIsolation(t *testing.T) {
	h := newNetHarness(1, ReliableLink)
	m := raft.Message{Type: raft.MsgApp, From: 1, To: 2, Entries: []raft.Entry{{Term: 1, Index: 1, Data: []byte("x")}}}
	h.n.Send(m)
	m.Entries[0].Term = 99 // sender mutates after Send
	h.drain()
	if h.got[0].msg.Entries[0].Term != 1 {
		t.Fatal("network shares the sender's entry slice")
	}
}
