package transport_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/transport"
)

// fakeSnaps sends `chunks` chunks and records what it receives.
type fakeSnaps struct {
	chunks   int
	leaderID uint64 // metadata leader id override (0: the real sender)
	refuse   error

	mu       sync.Mutex
	received []*raftkvv1.SnapshotChunk
	meta     *raftkvv1.SnapshotMetadata
	reports  chan [2]uint64 // {to, failed}
}

func newFakeSnaps(chunks int) *fakeSnaps {
	return &fakeSnaps{chunks: chunks, reports: make(chan [2]uint64, 4)}
}

func (f *fakeSnaps) SendSnapshot(_ context.Context, m raft.Message, send func(*raftkvv1.InstallSnapshotRequest) error) error {
	lead := m.From
	if f.leaderID != 0 {
		lead = f.leaderID
	}
	md := &raftkvv1.SnapshotMetadata{ClusterId: 7, LeaderId: lead, Term: m.Term,
		LastIncludedIndex: m.Snapshot.Index, LastIncludedTerm: m.Snapshot.Term}
	if err := send(&raftkvv1.InstallSnapshotRequest{Msg: &raftkvv1.InstallSnapshotRequest_Metadata{Metadata: md}}); err != nil {
		return err
	}
	for i := range f.chunks {
		c := &raftkvv1.SnapshotChunk{Offset: uint64(i), Data: []byte{byte(i)}, Last: i == f.chunks-1}
		if err := send(&raftkvv1.InstallSnapshotRequest{Msg: &raftkvv1.InstallSnapshotRequest_Chunk{Chunk: c}}); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeSnaps) ReceiveSnapshot(_ context.Context, meta *raftkvv1.SnapshotMetadata,
	recv func() (*raftkvv1.InstallSnapshotRequest, error)) (*raftkvv1.InstallSnapshotResponse, error) {
	f.mu.Lock()
	f.meta = meta
	f.mu.Unlock()
	for {
		req, err := recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		f.mu.Lock()
		f.received = append(f.received, req.GetChunk())
		f.mu.Unlock()
		if req.GetChunk().GetLast() {
			break
		}
	}
	if f.refuse != nil {
		return nil, f.refuse
	}
	return &raftkvv1.InstallSnapshotResponse{Term: meta.GetTerm(), Installed: true}, nil
}

func (f *fakeSnaps) ReportSnapshot(to uint64, failed bool) {
	v := uint64(0)
	if failed {
		v = 1
	}
	f.reports <- [2]uint64{to, v}
}

func (f *fakeSnaps) waitReport(t *testing.T) (uint64, bool) {
	t.Helper()
	select {
	case r := <-f.reports:
		return r[0], r[1] == 1
	case <-time.After(10 * time.Second):
		t.Fatal("no snapshot report")
		return 0, false
	}
}

func snapMsg() raft.Message {
	return raft.Message{Type: raft.MsgSnap, From: 1, To: 2, Term: 3, Snapshot: &raft.Snapshot{Index: 50, Term: 2}}
}

// WBS 5.4.1: a MsgSnap becomes an InstallSnapshot stream (metadata, then the
// chunks in order) and the leader's driver hears the outcome.
func TestInstallSnapshotStream(t *testing.T) {
	snaps := map[uint64]*fakeSnaps{1: newFakeSnaps(5), 2: newFakeSnaps(0)}
	c := newCluster(t, 2, func(cfg *transport.Config) { cfg.Snapshots = snaps[cfg.ID] })
	c[1].tr.Send([]raft.Message{snapMsg()})
	to, failed := snaps[1].waitReport(t)
	if to != 2 || failed {
		t.Fatalf("report to=%d failed=%v, want success for 2", to, failed)
	}
	r := snaps[2]
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.meta.GetLastIncludedIndex() != 50 || r.meta.GetLeaderId() != 1 || len(r.received) != 5 {
		t.Fatalf("receiver got meta %v and %d chunks", r.meta, len(r.received))
	}
	for i, ch := range r.received {
		if ch.GetOffset() != uint64(i) {
			t.Fatalf("chunk %d out of order: %v", i, ch)
		}
	}
}

// A receiver that refuses or fails the install makes the transfer fail, so
// the leader starts over (D-14).
func TestInstallSnapshotRefusedReportsFailure(t *testing.T) {
	recv := newFakeSnaps(0)
	recv.refuse = status.Error(codes.DataLoss, "bad chunk crc")
	snaps := map[uint64]*fakeSnaps{1: newFakeSnaps(3), 2: recv}
	c := newCluster(t, 2, func(cfg *transport.Config) { cfg.Snapshots = snaps[cfg.ID] })
	c[1].tr.Send([]raft.Message{snapMsg()})
	if to, failed := snaps[1].waitReport(t); to != 2 || !failed {
		t.Fatalf("report to=%d failed=%v, want failure", to, failed)
	}
}

// D-31: a snapshot whose metadata names an unknown leader is refused.
func TestInstallSnapshotUnknownLeaderRefused(t *testing.T) {
	send := newFakeSnaps(2)
	send.leaderID = 99
	snaps := map[uint64]*fakeSnaps{1: send, 2: newFakeSnaps(0)}
	c := newCluster(t, 2, func(cfg *transport.Config) { cfg.Snapshots = snaps[cfg.ID] })
	c[1].tr.Send([]raft.Message{snapMsg()})
	if _, failed := send.waitReport(t); !failed {
		t.Fatal("a snapshot from an unknown leader must be refused")
	}
	if snaps[2].meta != nil {
		t.Fatal("the handler must not see a refused stream")
	}
}

// A newer term in the answer is fed to the core so a stale leader steps down.
func TestInstallSnapshotHigherTermReachesCore(t *testing.T) {
	recv := &termSnaps{fakeSnaps: newFakeSnaps(0)}
	snaps := map[uint64]transport.SnapshotHandler{1: newFakeSnaps(1), 2: recv}
	c := newCluster(t, 2, func(cfg *transport.Config) { cfg.Snapshots = snaps[cfg.ID] })
	c[1].tr.Send([]raft.Message{snapMsg()})
	m := recvMsg(t, c[1])
	if m.Type != raft.MsgAppResp || m.Term != 9 || m.From != 2 {
		t.Fatalf("leader core got %+v, want a term-9 MsgAppResp from 2", m)
	}
}

type termSnaps struct{ *fakeSnaps }

func (s *termSnaps) ReceiveSnapshot(ctx context.Context, meta *raftkvv1.SnapshotMetadata,
	recv func() (*raftkvv1.InstallSnapshotRequest, error)) (*raftkvv1.InstallSnapshotResponse, error) {
	if _, err := s.fakeSnaps.ReceiveSnapshot(ctx, meta, recv); err != nil {
		return nil, err
	}
	return &raftkvv1.InstallSnapshotResponse{Term: 9}, nil
}
