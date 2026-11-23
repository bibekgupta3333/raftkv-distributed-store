package transport

import (
	"context"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// DefaultSnapshotTimeout bounds one InstallSnapshot transfer, install
// included.
const DefaultSnapshotTimeout = 10 * time.Minute

// SnapshotHandler moves snapshots for the driver (D-14, D-36). The transport
// owns the InstallSnapshot client stream and the RPC; the handler owns the
// snapshot files.
type SnapshotHandler interface {
	// SendSnapshot streams the snapshot a MsgSnap asks for: the metadata
	// first, then the chunks. It returns once everything is sent.
	SendSnapshot(ctx context.Context, m raft.Message, send func(*raftkvv1.InstallSnapshotRequest) error) error
	// ReceiveSnapshot handles an inbound transfer whose metadata (already
	// authenticated) is meta: it stages the chunks from recv, has the driver
	// install the snapshot, and returns the answer for the leader. An
	// error ends the RPC with that status.
	ReceiveSnapshot(ctx context.Context, meta *raftkvv1.SnapshotMetadata,
		recv func() (*raftkvv1.InstallSnapshotRequest, error)) (*raftkvv1.InstallSnapshotResponse, error)
	// ReportSnapshot is told how a transfer this node sent ended; failed
	// is true unless the follower answered installed=true (D-14).
	ReportSnapshot(to uint64, failed bool)
}

// sendSnapshot runs one InstallSnapshot transfer to this peer on its own
// goroutine, so it never blocks the Replicate stream, and reports the outcome.
// The core sends one MsgSnap per follower at a time.
func (p *peer) sendSnapshot(m raft.Message) {
	cfg := &p.t.cfg
	h := cfg.Snapshots
	if h == nil {
		return // no snapshot support: the follower stays in Snapshot state
	}
	p.t.mu.Lock()
	base := p.t.baseCtx
	if base == nil || p.t.closed {
		p.t.mu.Unlock()
		h.ReportSnapshot(p.id, true)
		return
	}
	p.t.wg.Add(1)
	p.t.mu.Unlock()
	go func() {
		defer p.t.wg.Done()
		ctx, cancel := context.WithTimeout(base, cfg.SnapshotTimeout)
		defer cancel()
		installed, err := p.installSnapshot(ctx, m)
		if err != nil {
			cfg.Observer.SendFailure(p.id)
			cfg.Logger.Warn("snapshot transfer failed", "component", "transport", "peer", p.id,
				"index", m.Snapshot.Index, "err", err)
		}
		h.ReportSnapshot(p.id, err != nil || !installed)
	}()
}

func (p *peer) installSnapshot(ctx context.Context, m raft.Message) (bool, error) {
	cfg := &p.t.cfg
	conn, err := p.getConn(ctx)
	if err != nil {
		return false, err
	}
	stream, err := raftkvv1.NewRaftClient(conn).InstallSnapshot(ctx)
	if err != nil {
		return false, err
	}
	if err := cfg.Snapshots.SendSnapshot(ctx, m, stream.Send); err != nil {
		_ = stream.CloseSend()
		return false, err
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return false, err
	}
	cfg.Observer.MessageSent(p.id, m.Type)
	if resp.GetTerm() > m.Term {
		// Let the core learn the newer term (and step down).
		_ = cfg.Handler.Step(ctx, raft.Message{Type: raft.MsgAppResp, From: p.id, To: cfg.ID, Term: resp.GetTerm()})
	}
	return resp.GetInstalled(), nil
}

// InstallSnapshot implements the receiving side of the snapshot stream (D-14,
// D-36): the first message must be the metadata, whose cluster and leader are
// checked like any peer message; the handler stages and installs the rest.
func (s *raftServer) InstallSnapshot(stream raftkvv1.Raft_InstallSnapshotServer) error {
	t := s.t
	h := t.cfg.Snapshots
	if h == nil {
		return status.Error(codes.Unimplemented, "snapshots are not supported by this node")
	}
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "empty snapshot stream")
		}
		return err
	}
	meta := first.GetMetadata()
	if meta == nil {
		return status.Error(codes.InvalidArgument, "the first InstallSnapshot message must be the metadata")
	}
	if err := t.checkSender(meta.GetClusterId(), meta.GetLeaderId()); err != nil {
		return status.Error(codes.PermissionDenied, err.Error())
	}
	resp, err := h.ReceiveSnapshot(stream.Context(), meta, stream.Recv)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Error(codes.Internal, err.Error())
	}
	resp.From = t.cfg.ID
	return stream.SendAndClose(resp)
}
