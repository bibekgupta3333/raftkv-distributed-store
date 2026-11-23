package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/snapshot"
)

// Snapshots in the driver (D-14, docs/system-design.md section 5,
// storage-design section 4). Everything that touches the state machine or the
// core runs on the loop goroutine: taking a snapshot between apply batches,
// trimming the WAL (D-30), and installing a received snapshot as part of
// persisting a Ready. The transport goroutines only stream files.

// ErrSnapshotInstalled fails a ProposeAndWait whose entry was covered by a
// snapshot this node installed: the entry may or may not have committed, and
// this node will never apply it itself.
var ErrSnapshotInstalled = errors.New("node: outcome unknown, a snapshot replaced the log")

// installReq hands a staged inbound snapshot to the loop.
type installReq struct {
	msg    raft.Message // the MsgSnap to step
	staged *snapshot.Staged
	done   chan installResult
}

type installResult struct {
	term      uint64
	installed bool
}

type snapReport struct {
	to     uint64
	failed bool
}

// SnapshotIndex is the index of the newest local snapshot (0 if none). Safe
// from any goroutine.
func (n *Node) SnapshotIndex() uint64 {
	s, _ := n.store.Snapshot()
	return s.Index
}

// SendSnapshot implements [transport.SnapshotHandler]: it streams the newest
// local snapshot at or above the one the core asked for.
func (n *Node) SendSnapshot(_ context.Context, m raft.Message, send func(*raftkvv1.InstallSnapshotRequest) error) error {
	if n.snaps == nil || m.Snapshot == nil {
		return errors.New("node: no snapshots")
	}
	meta, err := n.snaps.Send(m.Snapshot.Index, n.cfg.NodeID, m.Term, send)
	if err != nil {
		return err
	}
	n.log.Info("snapshot sent", "component", "snapshot", "peer", m.To,
		"index", meta.GetLastIncludedIndex(), "term", meta.GetLastIncludedTerm())
	return nil
}

// ReportSnapshot implements [transport.SnapshotHandler].
func (n *Node) ReportSnapshot(to uint64, failed bool) {
	n.metrics.SnapshotSent(failed)
	select {
	case n.snapReportCh <- snapReport{to: to, failed: failed}:
	case <-n.stopping:
	}
}

// ReceiveSnapshot implements [transport.SnapshotHandler]: it stages the
// transfer (verifying every chunk, D-14) and then lets the loop decide and
// install. The leader gets installed=true once the snapshot is installed and
// durable, or if this node already has everything it covers.
func (n *Node) ReceiveSnapshot(ctx context.Context, meta *raftkvv1.SnapshotMetadata,
	recv func() (*raftkvv1.InstallSnapshotRequest, error)) (*raftkvv1.InstallSnapshotResponse, error) {
	if n.snaps == nil {
		return nil, status.Error(codes.Unimplemented, "snapshots are disabled on this node")
	}
	staged, err := n.snaps.Receive(meta, recv)
	if err != nil {
		n.log.Warn("snapshot transfer aborted", "component", "snapshot", "from", meta.GetLeaderId(),
			"index", meta.GetLastIncludedIndex(), "err", err)
		switch {
		case errors.Is(err, snapshot.ErrChecksum):
			return nil, status.Error(codes.DataLoss, err.Error())
		case errors.Is(err, snapshot.ErrPosition), errors.Is(err, snapshot.ErrFormat):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if s, ok := status.FromError(err); ok {
			return nil, s.Err()
		}
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	req := &installReq{
		msg: raft.Message{
			Type: raft.MsgSnap, From: meta.GetLeaderId(), To: n.cfg.NodeID, Term: meta.GetTerm(),
			Snapshot: &raft.Snapshot{Index: meta.GetLastIncludedIndex(), Term: meta.GetLastIncludedTerm()},
		},
		staged: staged,
		done:   make(chan installResult, 1),
	}
	select {
	case n.installCh <- req:
	case <-ctx.Done():
		n.snaps.Discard(staged)
		return nil, status.FromContextError(ctx.Err()).Err()
	case <-n.stopping:
		n.snaps.Discard(staged)
		return nil, status.Error(codes.Unavailable, "node stopping")
	}
	// From here the loop owns the staged directory.
	select {
	case res := <-req.done:
		return &raftkvv1.InstallSnapshotResponse{Term: res.term, Installed: res.installed}, nil
	case <-n.stopping:
		return nil, status.Error(codes.Unavailable, "node stopping")
	}
}

// handleInstall steps the MsgSnap for a staged snapshot. The core either
// accepts it, and the Ready that follows installs it (installSnapshot), or
// ignores it; finishInstall answers the RPC after that Ready.
func (n *Node) handleInstall(req *installReq) {
	n.pendingInstall = req
	n.step(req.msg)
}

// installSnapshot installs rd.Snapshot from the staged transfer, before the
// Ready's entries and HardState are persisted (storage-design 4.4): swap the
// state machine (atomic sm/CURRENT rename), then write the WAL's reset marker;
// persist fsyncs it together with the HardState (commit = snapshot index).
func (n *Node) installSnapshot(s raft.Snapshot) error {
	req := n.pendingInstall
	if req == nil || req.msg.Snapshot == nil || *req.msg.Snapshot != s {
		return fmt.Errorf("node: the core asked to install snapshot %d/%d, which was not received", s.Index, s.Term)
	}
	start := time.Now()
	if err := n.snaps.Install(req.staged); err != nil {
		return fmt.Errorf("install snapshot %d: %w", s.Index, err)
	}
	if err := n.store.ApplySnapshot(s); err != nil {
		return fmt.Errorf("install snapshot %d: wal: %w", s.Index, err)
	}
	n.pendingInstall, n.installed = nil, req
	n.needSnapshot = true // a local copy to serve if this node becomes leader
	n.metrics.SnapshotInstalled(time.Since(start))
	n.log.Info("snapshot installed", "component", "snapshot", "index", s.Index, "term", s.Term,
		"from", req.msg.From, "took", time.Since(start))
	n.failWaitersUpTo(s.Index, ErrSnapshotInstalled)
	return nil
}

// finishInstall answers the InstallSnapshot RPC once the Ready that installed
// the snapshot (or the decision not to) has been fully handled.
func (n *Node) finishInstall() {
	st := n.rn.Status()
	if req := n.installed; req != nil {
		n.installed = nil
		n.notifyApplied()
		req.done <- installResult{term: st.Term, installed: true}
	}
	if req := n.pendingInstall; req != nil {
		// Not installed: stale (we already have it all), or from an old
		// term. Either way the staged copy is not needed.
		n.pendingInstall = nil
		n.snaps.Discard(req.staged)
		req.done <- installResult{term: st.Term, installed: st.Commit >= req.msg.Snapshot.Index}
	}
}

// maybeSnapshot takes a snapshot every SnapshotEntries applied entries (D-14)
// and then trims the WAL under D-30. It runs on the loop between Readys, so the
// checkpoint's index is exactly the applied index.
func (n *Node) maybeSnapshot() error {
	if n.snaps == nil {
		return nil
	}
	applied := n.applier.Applied()
	var last uint64
	if l := n.snaps.Latest(); l != nil {
		last = l.GetLastIncludedIndex()
	}
	if applied == 0 || applied <= last || (!n.needSnapshot && applied-last < n.cfg.SnapshotEntries) {
		return nil
	}
	start := time.Now()
	meta, err := n.snaps.Create()
	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}
	n.needSnapshot = false
	n.store.SetSnapshot(raft.Snapshot{Index: meta.GetLastIncludedIndex(), Term: meta.GetLastIncludedTerm()})
	var size uint64
	for _, f := range meta.GetFiles() {
		size += f.GetSize()
	}
	n.metrics.SnapshotCreated(meta.GetLastIncludedIndex(), size, time.Since(start))
	trimmed, err := n.snaps.TruncateWAL(n.store, n.cfg.SnapshotRetainEntries)
	if err != nil {
		return fmt.Errorf("trim wal: %w", err)
	}
	if first, err := n.store.FirstIndex(); err == nil {
		n.metrics.WALFirstIndex(first)
	}
	n.log.Info("snapshot created", "component", "snapshot", "index", meta.GetLastIncludedIndex(),
		"term", meta.GetLastIncludedTerm(), "bytes", size, "took", time.Since(start), "wal_trimmed_to", trimmed)
	return nil
}

// failWaitersUpTo fails ProposeAndWait callers whose entries are at or below
// index.
func (n *Node) failWaitersUpTo(index uint64, err error) {
	n.waitMu.Lock()
	defer n.waitMu.Unlock()
	for idx, w := range n.waiters {
		if idx <= index {
			w.done <- err
			delete(n.waiters, idx)
		}
	}
}
