package transport_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/transport"
)

// WBS 6.4.3: a member added at run time (a learner) is reachable once
// AddPeer records its address, without restarting anything.
func TestAddPeerAtRuntime(t *testing.T) {
	c := newCluster(t, 2)
	l := listen(t, "127.0.0.1:0")
	book3, err := transport.NewAddressBook(map[uint64]string{3: l.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	n3 := startNode(t, 3, book3, l)
	t.Cleanup(n3.stop)
	if err := c[1].tr.AddPeer(3, n3.addr); err != nil {
		t.Fatal(err)
	}
	c[1].tr.Send([]raft.Message{{Type: raft.MsgApp, From: 1, To: 3, Term: 2, Index: 0,
		Entries: []raft.Entry{{Term: 2, Index: 1, Data: []byte("x")}}}})
	if m := recvMsg(t, n3); m.Type != raft.MsgApp || m.From != 1 {
		t.Fatalf("got %+v", m)
	}
	// The learner answers on the leader's stream without knowing its address.
	n3.tr.Send([]raft.Message{{Type: raft.MsgAppResp, From: 3, To: 1, Term: 2, Index: 1}})
	if m := recvMsg(t, c[1]); m.Type != raft.MsgAppResp || m.From != 3 {
		t.Fatalf("got %+v", m)
	}
}

// D-31: after RemovePeer nothing is sent to the removed member and its
// messages are refused.
func TestRemovePeerStopsTraffic(t *testing.T) {
	c := newCluster(t, 2)
	c[1].tr.RemovePeer(2)
	c[1].tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 1}})
	select {
	case m := <-c[2].inbox:
		t.Fatalf("a removed member got %+v", m)
	case <-time.After(200 * time.Millisecond):
	}
	c[2].tr.Send([]raft.Message{{Type: raft.MsgPreVote, From: 2, To: 1, Term: 5}})
	select {
	case m := <-c[1].inbox:
		t.Fatalf("a removed member's message was delivered: %+v", m)
	case <-time.After(300 * time.Millisecond):
	}
}

// System-design 7.3: MsgTimeoutNow travels over the TimeoutNow RPC, and the
// vote it triggers keeps its transfer mark so voters skip T5.
func TestTimeoutNowAndTransferVote(t *testing.T) {
	c := newCluster(t, 2)
	c[1].tr.Send([]raft.Message{{Type: raft.MsgTimeoutNow, From: 1, To: 2, Term: 4}})
	if m := recvMsg(t, c[2]); m.Type != raft.MsgTimeoutNow || m.From != 1 || m.Term != 4 {
		t.Fatalf("got %+v", m)
	}
	c[2].tr.Send([]raft.Message{{Type: raft.MsgVote, From: 2, To: 1, Term: 5, Context: raft.TransferContext}})
	if m := recvMsg(t, c[1]); m.Type != raft.MsgVote || string(m.Context) != string(raft.TransferContext) {
		t.Fatalf("got %+v", m)
	}
	c[2].tr.Send([]raft.Message{{Type: raft.MsgVote, From: 2, To: 1, Term: 6}})
	if m := recvMsg(t, c[1]); m.Type != raft.MsgVote || len(m.Context) != 0 {
		t.Fatalf("an ordinary vote carries a transfer mark: %+v", m)
	}
}

// D-32: a member that comes back with a new data-directory incarnation under
// its old ID is refused by the peers that knew it, both ways, and the driver
// is told.
func TestWipedPeerIsRefused(t *testing.T) {
	var (
		mu    sync.Mutex
		wiped = map[uint64][]uint64{}
	)
	inc := func(id uint64) uint64 { return 1000 + id }
	opt := func(cfg *transport.Config) {
		id := cfg.ID
		cfg.Incarnation = inc(id)
		cfg.OnPeerWiped = func(peer uint64) {
			mu.Lock()
			wiped[id] = append(wiped[id], peer)
			mu.Unlock()
		}
	}
	c := newCluster(t, 3, opt)
	// Everyone learns everyone's incarnation.
	for _, from := range []uint64{1, 2, 3} {
		for _, to := range []uint64{1, 2, 3} {
			if from != to {
				c[from].tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: from, To: to, Term: 1}})
				recvMsg(t, c[to])
			}
		}
	}
	// Node 3 comes back on the same address with a fresh data directory.
	addr := c[3].addr
	c[3].stop()
	book, err := transport.NewAddressBook(map[uint64]string{1: c[1].addr, 2: c[2].addr, 3: addr})
	if err != nil {
		t.Fatal(err)
	}
	n3 := startNode(t, 3, book, listen(t, addr), func(cfg *transport.Config) { cfg.Incarnation = 9999 })
	t.Cleanup(n3.stop)
	c[3] = n3
	// Its first request (a PreVote) is refused, and the peer reports it.
	conn, err := grpc.NewClient("passthrough:///"+c[1].addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "raftkv-incarnation", "9999")
	_, err = raftkvv1.NewRaftClient(conn).RequestVote(ctx, &raftkvv1.RequestVoteRequest{ClusterId: 7, Term: 2, CandidateId: 3, PreVote: true})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("wiped candidate: err = %v, want PermissionDenied", err)
	}
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return slices.Contains(wiped[1], 3)
	})
	if got := c[1].tr.WipedPeers(); !slices.Equal(got, []uint64{3}) {
		t.Fatalf("WipedPeers = %v", got)
	}
	// Node 2 finds out on its own when its stream to 3 reconnects; then it
	// sends 3 nothing.
	c[2].tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 2, To: 3, Term: 1}})
	waitFor(t, func() bool {
		c[2].tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 2, To: 3, Term: 1}})
		mu.Lock()
		defer mu.Unlock()
		return slices.Contains(wiped[2], 3)
	})
	for len(n3.inbox) > 0 {
		<-n3.inbox
	}
	c[2].tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, From: 2, To: 3, Term: 1}})
	select {
	case m := <-n3.inbox:
		t.Fatalf("a wiped member got %+v", m)
	case <-time.After(300 * time.Millisecond):
	}
}
