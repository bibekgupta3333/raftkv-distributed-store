package transport_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/transport"
)

func TestReadIndexConversionsRoundTrip(t *testing.T) {
	req := raft.Message{Type: raft.MsgReadIndex, To: 1, From: 2, Context: []byte("r1")}
	if got := transport.ReadIndexRequestFromProto(transport.ReadIndexRequestToProto(7, req), 1); !reflect.DeepEqual(got, req) {
		t.Fatalf("request round trip: got %+v, want %+v", got, req)
	}
	resp := raft.Message{Type: raft.MsgReadIndexResp, To: 2, From: 1, Term: 3, Index: 42, Context: []byte("r1")}
	if got := transport.ReadIndexResponseFromProto(transport.ReadIndexResponseToProto(resp), 1, 2); !reflect.DeepEqual(got, resp) {
		t.Fatalf("response round trip: got %+v, want %+v", got, resp)
	}
}

// WBS 4.1.3 / D-36: a follower's MsgReadIndex reaches the leader over the
// ReadIndex RPC (term-less) and the leader's answer comes back to the follower
// as a MsgReadIndexResp.
func TestReadIndexRPC(t *testing.T) {
	c := newCluster(t, 2)
	c[2].tr.Send([]raft.Message{{Type: raft.MsgReadIndex, To: 1, From: 2, Context: []byte("r1")}})
	req := recvMsg(t, c[1])
	if req.Type != raft.MsgReadIndex || req.From != 2 || req.To != 1 || req.Term != 0 || string(req.Context) != "r1" {
		t.Fatalf("leader got %+v", req)
	}
	c[1].tr.Send([]raft.Message{{Type: raft.MsgReadIndexResp, To: 2, From: 1, Term: 4, Index: 17, Context: []byte("r1")}})
	got := recvMsg(t, c[2])
	want := raft.Message{Type: raft.MsgReadIndexResp, To: 2, From: 1, Term: 4, Index: 17, Context: []byte("r1")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("follower got %+v, want %+v", got, want)
	}
}

// A leader that rejects (not the leader) makes the follower see a rejected
// response at once.
func TestReadIndexRPCReject(t *testing.T) {
	c := newCluster(t, 2)
	c[2].tr.Send([]raft.Message{{Type: raft.MsgReadIndex, To: 1, From: 2, Context: []byte("r2")}})
	recvMsg(t, c[1])
	c[1].tr.Send([]raft.Message{{Type: raft.MsgReadIndexResp, To: 2, From: 1, Term: 4, Context: []byte("r2"), Reject: true}})
	got := recvMsg(t, c[2])
	if got.Type != raft.MsgReadIndexResp || !got.Reject || got.From != 1 || string(got.Context) != "r2" {
		t.Fatalf("follower got %+v, want a rejected response", got)
	}
}

// A leader that never answers (no quorum) times out; the follower then gets a
// rejected response instead of hanging.
func TestReadIndexRPCTimeout(t *testing.T) {
	c := newCluster(t, 2, func(cfg *transport.Config) { cfg.ReadIndexTimeout = 200 * time.Millisecond })
	start := time.Now()
	c[2].tr.Send([]raft.Message{{Type: raft.MsgReadIndex, To: 1, From: 2, Context: []byte("r3")}})
	recvMsg(t, c[1]) // and never answer
	got := recvMsg(t, c[2])
	if got.Type != raft.MsgReadIndexResp || !got.Reject || string(got.Context) != "r3" {
		t.Fatalf("follower got %+v, want a rejected response", got)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("rejection took %v", d)
	}
}
