package transport_test

import (
	"context"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/transport"
)

// counters is a transport.Observer that counts events.
type counters struct {
	sent, failures, reconnects, dropped atomic.Int64
}

func (c *counters) MessageSent(uint64, raft.MessageType) { c.sent.Add(1) }
func (c *counters) SendFailure(uint64)                   { c.failures.Add(1) }
func (c *counters) Reconnect(uint64)                     { c.reconnects.Add(1) }
func (c *counters) Dropped(uint64)                       { c.dropped.Add(1) }

// tnode is one transport plus its gRPC server and an inbox of inbound messages.
type tnode struct {
	id          uint64
	tr          *transport.Transport
	srv         *grpc.Server
	addr        string
	inbox       chan raft.Message
	obs         *counters
	unreachable atomic.Int64
	cancel      context.CancelFunc
	serveDone   chan struct{}
}

type clusterOpt func(*transport.Config)

func listen(t *testing.T, addr string) net.Listener {
	t.Helper()
	var lis net.Listener
	var err error
	for range 50 { // the port may linger briefly after a restart
		if lis, err = net.Listen("tcp", addr); err == nil {
			return lis
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("listen %s: %v", addr, err)
	return nil
}

// newCluster starts n transports on real localhost gRPC servers.
func newCluster(t *testing.T, n int, opts ...clusterOpt) map[uint64]*tnode {
	t.Helper()
	lises := map[uint64]net.Listener{}
	peers := map[uint64]string{}
	for i := 1; i <= n; i++ {
		l := listen(t, "127.0.0.1:0")
		lises[uint64(i)] = l
		peers[uint64(i)] = l.Addr().String()
	}
	book, err := transport.NewAddressBook(peers)
	if err != nil {
		t.Fatal(err)
	}
	nodes := map[uint64]*tnode{}
	for id, l := range lises {
		nodes[id] = startNode(t, id, book, l, opts...)
	}
	t.Cleanup(func() {
		for _, nd := range nodes {
			nd.stop()
		}
	})
	return nodes
}

func startNode(t *testing.T, id uint64, book *transport.AddressBook, l net.Listener, opts ...clusterOpt) *tnode {
	t.Helper()
	nd := &tnode{id: id, inbox: make(chan raft.Message, 1024), obs: &counters{}, addr: l.Addr().String(), serveDone: make(chan struct{})}
	cfg := transport.Config{
		ID: id, ClusterID: 7, Peers: book, Observer: nd.obs,
		Handler: transport.HandlerFunc(func(ctx context.Context, m raft.Message) error {
			select {
			case nd.inbox <- m:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
		OnUnreachable: func(uint64) { nd.unreachable.Add(1) },
		BackoffMin:    10 * time.Millisecond,
		BackoffMax:    50 * time.Millisecond,
	}
	for _, o := range opts {
		o(&cfg)
	}
	tr, err := transport.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	nd.tr = tr
	nd.srv = tr.NewGRPCServer()
	go func() { _ = nd.srv.Serve(l); close(nd.serveDone) }()
	ctx, cancel := context.WithCancel(context.Background())
	nd.cancel = cancel
	tr.Start(ctx)
	return nd
}

func (nd *tnode) stop() {
	nd.srv.Stop()
	<-nd.serveDone
	nd.tr.Close()
	nd.cancel()
}

func recvMsg(t *testing.T, nd *tnode) raft.Message {
	t.Helper()
	select {
	case m := <-nd.inbox:
		return m
	case <-time.After(5 * time.Second):
		t.Fatalf("node %d: timed out waiting for a message", nd.id)
		return raft.Message{}
	}
}

func TestConversionsRoundTrip(t *testing.T) {
	ents := []raft.Entry{{Term: 2, Index: 5, Data: []byte("x")}, {Term: 2, Index: 6, Type: raft.EntryConfChange, Data: []byte("c")}}
	tests := []struct {
		name string
		m    raft.Message
	}{
		{"app", raft.Message{Type: raft.MsgApp, To: 2, From: 1, Term: 2, Index: 4, LogTerm: 1, Commit: 3, Entries: ents}},
		{"empty probe app", raft.Message{Type: raft.MsgApp, To: 2, From: 1, Term: 2, Index: 4, LogTerm: 1, Commit: 3}},
		{"heartbeat", raft.Message{Type: raft.MsgHeartbeat, To: 2, From: 1, Term: 2, Commit: 3, Context: []byte("ctx")}},
		{"app resp ok", raft.Message{Type: raft.MsgAppResp, To: 1, From: 2, Term: 2, Index: 6}},
		{"app resp reject", raft.Message{Type: raft.MsgAppResp, To: 1, From: 2, Term: 2, Index: 9, Reject: true, RejectHint: 4, LogTerm: 1}},
		{"heartbeat resp", raft.Message{Type: raft.MsgHeartbeatResp, To: 1, From: 2, Term: 2, Context: []byte("ctx")}},
		{"vote", raft.Message{Type: raft.MsgVote, To: 2, From: 1, Term: 3, Index: 6, LogTerm: 2}},
		{"prevote", raft.Message{Type: raft.MsgPreVote, To: 2, From: 1, Term: 4, Index: 6, LogTerm: 2}},
		{"vote resp granted", raft.Message{Type: raft.MsgVoteResp, To: 1, From: 2, Term: 3}},
		{"prevote resp rejected", raft.Message{Type: raft.MsgPreVoteResp, To: 1, From: 2, Term: 3, Reject: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got raft.Message
			switch {
			case transport.IsAppendRequest(tt.m.Type):
				got = transport.AppendRequestFromProto(transport.AppendRequestToProto(7, tt.m), tt.m.To)
			case transport.IsAppendResponse(tt.m.Type):
				got = transport.AppendResponseFromProto(transport.AppendResponseToProto(tt.m), tt.m.To)
			case transport.IsVoteRequest(tt.m.Type):
				got = transport.VoteRequestFromProto(transport.VoteRequestToProto(7, tt.m), tt.m.To)
			default:
				got = transport.VoteResponseFromProto(transport.VoteResponseToProto(tt.m), tt.m.To)
			}
			if !reflect.DeepEqual(got, tt.m) {
				t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, tt.m)
			}
		})
	}
}

// WBS 2.2.2: a leader's append reaches the follower over the Replicate stream
// and the follower's reply returns on the same stream.
func TestReplicateStreamBothDirections(t *testing.T) {
	c := newCluster(t, 3)
	app := raft.Message{Type: raft.MsgApp, To: 2, From: 1, Term: 1, Index: 0, Entries: []raft.Entry{{Term: 1, Index: 1, Data: []byte("a")}}}
	hb := raft.Message{Type: raft.MsgHeartbeat, To: 2, From: 1, Term: 1, Context: []byte("rc")}
	c[1].tr.Send([]raft.Message{app, hb})

	got := recvMsg(t, c[2])
	if !reflect.DeepEqual(got, app) {
		t.Fatalf("follower got %+v, want %+v", got, app)
	}
	if got := recvMsg(t, c[2]); got.Type != raft.MsgHeartbeat || string(got.Context) != "rc" {
		t.Fatalf("follower got %+v, want the heartbeat (order is preserved per stream)", got)
	}

	resp := raft.Message{Type: raft.MsgAppResp, To: 1, From: 2, Term: 1, Index: 1}
	c[2].tr.Send([]raft.Message{resp})
	if got := recvMsg(t, c[1]); !reflect.DeepEqual(got, resp) {
		t.Fatalf("leader got %+v, want %+v", got, resp)
	}
}

// WBS 2.2.5: RequestVote with the pre-vote flag, response routed back as a message.
func TestRequestVoteAndPreVote(t *testing.T) {
	c := newCluster(t, 3)
	for _, tc := range []struct {
		req  raft.MessageType
		resp raft.MessageType
	}{{raft.MsgPreVote, raft.MsgPreVoteResp}, {raft.MsgVote, raft.MsgVoteResp}} {
		c[1].tr.Send([]raft.Message{{Type: tc.req, To: 2, From: 1, Term: 5, Index: 9, LogTerm: 4}})
		req := recvMsg(t, c[2])
		if req.Type != tc.req || req.Term != 5 || req.Index != 9 || req.LogTerm != 4 || req.From != 1 {
			t.Fatalf("voter got %+v", req)
		}
		c[2].tr.Send([]raft.Message{{Type: tc.resp, To: 1, From: 2, Term: 5, Reject: tc.req == raft.MsgPreVote}})
		got := recvMsg(t, c[1])
		if got.Type != tc.resp || got.From != 2 || got.Term != 5 || got.Reject != (tc.req == raft.MsgPreVote) {
			t.Fatalf("candidate got %+v for %v", got, tc.req)
		}
	}
}

// WBS 2.2.1: the server rejects messages over the size limit.
func TestServerRejectsOversizeMessage(t *testing.T) {
	c := newCluster(t, 2, func(cfg *transport.Config) { cfg.MaxMsgBytes = 64 << 10 })
	conn, err := grpc.NewClient("passthrough:///"+c[2].addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := raftkvv1.NewRaftClient(conn).Replicate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	big := &raftkvv1.AppendEntriesRequest{ClusterId: 7, Term: 1, LeaderId: 1,
		Entries: []*raftkvv1.Entry{{Term: 1, Index: 1, Data: make([]byte, 128<<10)}}}
	_ = stream.Send(big)
	_, err = stream.Recv()
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Recv err = %v, want ResourceExhausted", err)
	}
	select {
	case m := <-c[2].inbox:
		t.Fatalf("oversize message reached the handler: %+v", m)
	default:
	}
}

// The transport refuses removed senders and wrong clusters (D-31).
func TestServerRefusesRemovedSenderAndWrongCluster(t *testing.T) {
	c := newCluster(t, 2)
	c[2].tr.RemovePeer(99)
	conn, err := grpc.NewClient("passthrough:///"+c[2].addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := raftkvv1.NewRaftClient(conn)
	for name, req := range map[string]*raftkvv1.RequestVoteRequest{
		"removed candidate": {ClusterId: 7, Term: 1, CandidateId: 99},
		"wrong cluster":     {ClusterId: 8, Term: 1, CandidateId: 1},
	} {
		if _, err := client.RequestVote(ctx, req); status.Code(err) != codes.PermissionDenied {
			t.Errorf("%s: err = %v, want PermissionDenied", name, err)
		}
	}
}

// WBS 2.2.3 / D-29: no more than MaxInflight unanswered appends per follower.
func TestInflightCap(t *testing.T) {
	const limit = 3
	c := newCluster(t, 2, func(cfg *transport.Config) { cfg.MaxInflight = limit })
	var msgs []raft.Message
	for i := uint64(1); i <= 10; i++ {
		msgs = append(msgs, raft.Message{Type: raft.MsgApp, To: 2, From: 1, Term: 1, Index: i - 1,
			Entries: []raft.Entry{{Term: 1, Index: i, Data: []byte("x")}}})
	}
	c[1].tr.Send(msgs) // node 2 never answers

	for i := 0; i < limit; i++ {
		recvMsg(t, c[2])
	}
	waitFor(t, func() bool { return c[1].obs.dropped.Load() == 10-limit })
	select {
	case m := <-c[2].inbox:
		t.Fatalf("a message beyond the cap was delivered: %+v", m)
	case <-time.After(100 * time.Millisecond):
	}

	// An answer frees a slot.
	c[2].tr.Send([]raft.Message{{Type: raft.MsgAppResp, To: 1, From: 2, Term: 1, Index: 1}})
	recvMsg(t, c[1])
	c[1].tr.Send(msgs[:1])
	recvMsg(t, c[2])
}

// WBS 2.2.4: a restarted peer is reconnected, and the failure is reported.
func TestReconnectAfterPeerRestart(t *testing.T) {
	lises := []net.Listener{listen(t, "127.0.0.1:0"), listen(t, "127.0.0.1:0")}
	book, err := transport.NewAddressBook(map[uint64]string{1: lises[0].Addr().String(), 2: lises[1].Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	n1 := startNode(t, 1, book, lises[0])
	n2 := startNode(t, 2, book, lises[1])
	defer n1.stop()

	send := func() {
		n1.tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, To: 2, From: 1, Term: 1}})
	}
	send()
	recvMsg(t, n2)

	addr := n2.addr
	n2.stop()
	waitFor(t, func() bool { send(); return n1.unreachable.Load() > 0 })

	n2 = startNode(t, 2, book, listen(t, addr))
	defer n2.stop()
	waitFor(t, func() bool {
		send()
		select {
		case <-n2.inbox:
			return true
		case <-time.After(50 * time.Millisecond):
			return false
		}
	})
	if n1.obs.reconnects.Load() == 0 || n1.obs.failures.Load() == 0 {
		t.Fatalf("expected reconnect and failure counts, got reconnects=%d failures=%d",
			n1.obs.reconnects.Load(), n1.obs.failures.Load())
	}
}

// WBS 2.2.6: the address book is static; names are re-resolved on each connect.
func TestAddressBookResolvesNamesOnEveryConnect(t *testing.T) {
	var lookups atomic.Int64
	var mu sync.Mutex
	answer := []string{"::1", "127.0.0.1"}
	book, err := transport.NewAddressBook(map[uint64]string{1: "node1.test:9000", 2: "127.0.0.1:9001"})
	if err != nil {
		t.Fatal(err)
	}
	book = book.WithLookup(func(context.Context, string) ([]string, error) {
		lookups.Add(1)
		mu.Lock()
		defer mu.Unlock()
		return answer, nil
	})
	if got, err := book.Resolve(context.Background(), 1); err != nil || got != "127.0.0.1:9000" {
		t.Fatalf("Resolve(1) = %q, %v; want IPv4 127.0.0.1:9000", got, err)
	}
	mu.Lock()
	answer = []string{"10.1.2.3"}
	mu.Unlock()
	if got, _ := book.Resolve(context.Background(), 1); got != "10.1.2.3:9000" {
		t.Fatalf("Resolve(1) after DNS change = %q, want 10.1.2.3:9000", got)
	}
	if got, _ := book.Resolve(context.Background(), 2); got != "127.0.0.1:9001" || lookups.Load() != 2 {
		t.Fatalf("IP literals must not hit DNS: got %q, lookups=%d", got, lookups.Load())
	}
	if _, err := book.Resolve(context.Background(), 3); err == nil {
		t.Fatal("unknown node must not resolve")
	}
	if _, err := transport.NewAddressBook(map[uint64]string{1: "no-port"}); err == nil {
		t.Fatal("address without a port must be rejected")
	}
	if !reflect.DeepEqual(book.IDs(), []uint64{1, 2}) {
		t.Fatalf("IDs = %v", book.IDs())
	}
}

// The transport dials whatever Resolve returns, so a DNS name that only maps to
// the real address after startup is still found (WBS 2.2.6).
func TestConnectsThroughResolvedName(t *testing.T) {
	l1, l2 := listen(t, "127.0.0.1:0"), listen(t, "127.0.0.1:0")
	_, port2, _ := net.SplitHostPort(l2.Addr().String())
	var up atomic.Bool
	book, err := transport.NewAddressBook(map[uint64]string{1: l1.Addr().String(), 2: net.JoinHostPort("node2.test", port2)})
	if err != nil {
		t.Fatal(err)
	}
	book = book.WithLookup(func(context.Context, string) ([]string, error) {
		if !up.Load() {
			return nil, &net.DNSError{Err: "no such host", Name: "node2.test", IsNotFound: true}
		}
		return []string{"127.0.0.1"}, nil
	})
	n1 := startNode(t, 1, book, l1)
	n2 := startNode(t, 2, book, l2)
	defer n1.stop()
	defer n2.stop()
	waitFor(t, func() bool { return n1.unreachable.Load() > 0 }) // name does not resolve yet
	up.Store(true)
	waitFor(t, func() bool {
		n1.tr.Send([]raft.Message{{Type: raft.MsgHeartbeat, To: 2, From: 1, Term: 1}})
		select {
		case <-n2.inbox:
			return true
		case <-time.After(50 * time.Millisecond):
			return false
		}
	})
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}
