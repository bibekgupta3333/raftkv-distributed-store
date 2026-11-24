package node

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/membership"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Membership administration (D-06, D-23, system-design 7).
//
// The node keeps the applied member table (IDs, names, addresses, roles) in
// memory, rebuilt at start from the state machine (or the bootstrap peers)
// and updated whenever a conf-change entry is applied. The transport follows
// it: a member's address is registered when it is added (WBS 6.4.3), and a
// removed member is cut off once its removal commits (D-31).

// Errors returned by the membership operations.
var (
	// ErrUnknownMember means the member ID is not in the configuration.
	ErrUnknownMember = errors.New("node: no such member")
	// ErrPeerURLInUse means another member already uses the peer URL.
	ErrPeerURLInUse = errors.New("node: peer URL already used by a member")
	// ErrLearnerNotCaughtUp means a learner is too far behind to promote
	// (system-design 7.2: within MaxPromoteLag entries of the leader).
	ErrLearnerNotCaughtUp = errors.New("node: learner has not caught up")
	// ErrWouldLoseQuorum means the remaining voters could not form a
	// quorum right now.
	ErrWouldLoseQuorum = errors.New("node: removal would leave no reachable quorum")
	// ErrTransferTimeout means no new leader appeared within the transfer
	// timeout; the old leader resumed (system-design 7.3).
	ErrTransferTimeout = errors.New("node: leadership transfer timed out")
	// ErrTargetIsLearner means a transfer target is a learner.
	ErrTargetIsLearner = errors.New("node: transfer target is a learner")
	// ErrRemoved means this node was removed from the cluster (D-31). A
	// removed node stops, and refuses to start again under its ID.
	ErrRemoved = errors.New("node: this node was removed from the cluster (D-31)")
)

// MaxPromoteLag is how far (in entries) a learner may lag the leader's last
// index and still be promoted (system-design 7.2).
const MaxPromoteLag = 1000

// MembershipReader is implemented by appliers that persist the member table
// (the Pebble applier, WBS 6.1.6).
type MembershipReader interface {
	Membership() (*raftkvv1.ClusterConfig, error)
}

// Members returns the applied member table. Safe from any goroutine.
func (n *Node) Members() *raftkvv1.ClusterConfig {
	if m := n.members.Load(); m != nil {
		return proto.CloneOf(m)
	}
	return &raftkvv1.ClusterConfig{}
}

// loadMembers sets the member table at start-up: the one the state machine
// recorded, else the bootstrap peers (none for a node that joined, D-32).
func (n *Node) loadMembers(joined bool) error {
	var table *raftkvv1.ClusterConfig
	if r, ok := n.applier.(MembershipReader); ok {
		t, err := r.Membership()
		if err != nil {
			return err
		}
		table = t
	}
	if table == nil {
		table = &raftkvv1.ClusterConfig{}
		if !joined {
			table = membership.BootstrapIDs(n.cfg.PeerIDs())
		}
	}
	n.members.Store(membership.FillAddrs(table, n.cfg.PeerAddrs()))
	return nil
}

// bootstrapIDs are the voters the core starts from before any conf change.
func (n *Node) bootstrapIDs(joined bool) []uint64 {
	if joined {
		return nil // D-32: a joining node learns its configuration from the leader
	}
	return n.cfg.PeerIDs()
}

// syncTransport registers every member's address with the transport and cuts
// off removed members (D-31).
func (n *Node) syncTransport(table *raftkvv1.ClusterConfig) {
	for _, m := range table.GetMembers() {
		if m.GetId() != n.cfg.NodeID && m.GetPeerUrl() != "" {
			if err := n.tr.AddPeer(m.GetId(), m.GetPeerUrl()); err != nil {
				n.log.Warn("bad member address", "component", "membership", "member", m.GetId(), "err", err)
			}
		}
	}
	for _, id := range table.GetRemoved() {
		n.tr.RemovePeer(id)
	}
}

// applyMembership updates the member table after the applier applied ents
// (loop goroutine).
func (n *Node) applyMembership(ents []raft.Entry) {
	for _, e := range ents {
		if e.Type != raft.EntryConfChange {
			continue
		}
		next, cc, err := membership.FromEntry(e, n.members.Load())
		if err == nil {
			next = membership.FillAddrs(next, n.cfg.PeerAddrs())
		}
		if err != nil {
			// The applier accepted it, so this cannot happen; keep the
			// old table rather than stop.
			n.log.Error("cannot decode an applied conf change", "component", "membership", "index", e.Index, "err", err)
			continue
		}
		n.members.Store(next)
		n.syncTransport(next)
		n.log.Info("membership changed", "component", "membership", "index", e.Index,
			"change", cc.Type.String(), "member", cc.NodeID, "voters", len(membership.ConfState(next).Voters))
		if cc.Type == raft.ConfChangeRemoveNode && cc.NodeID == n.cfg.NodeID {
			n.removedAt = time.Now() // system-design 7.4: stop shortly after
			n.log.Warn("this node was removed from the cluster; stopping", "component", "membership")
		}
	}
}

// reloadMembers re-reads the table after a snapshot install replaced the
// state machine.
func (n *Node) reloadMembers() {
	r, ok := n.applier.(MembershipReader)
	if !ok {
		return
	}
	t, err := r.Membership()
	if err != nil || t == nil {
		return
	}
	t = membership.FillAddrs(t, n.cfg.PeerAddrs())
	n.members.Store(t)
	n.syncTransport(t)
}

// ---- proposals (WBS 6.4) ----

type confReq struct {
	kind   raft.ConfChangeType
	id     uint64           // promote, remove
	member *raftkvv1.Member // add: name, peer and client URL
	force  bool             // remove: skip the quorum check
	resp   chan confResult
}

type confResult struct {
	index  uint64
	w      *waiter
	member *raftkvv1.Member
	done   bool // nothing to wait for (an idempotent no-op)
	err    error
}

// randomID draws a fresh random 64-bit member ID that was never used (D-38).
func randomID(table *raftkvv1.ClusterConfig, self uint64) uint64 {
	for {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			panic(fmt.Sprintf("node: crypto/rand: %v", err))
		}
		id := binary.BigEndian.Uint64(b[:])
		if id == 0 || id == self || membership.Find(table, id) != nil {
			continue
		}
		used := false
		for _, r := range table.GetRemoved() {
			used = used || r == id
		}
		if !used {
			return id
		}
	}
}

// AddLearner adds a non-voting member at peerURL and waits until the change is
// applied here (WBS 6.4.3). The leader assigns a random ID (D-38). Adding the
// same peer URL again returns the existing learner.
func (n *Node) AddLearner(ctx context.Context, m *raftkvv1.Member) (*raftkvv1.Member, error) {
	res, err := n.confChange(ctx, &confReq{kind: raft.ConfChangeAddLearner, member: m})
	if err != nil {
		return nil, err
	}
	return res.member, nil
}

// PromoteLearner turns a caught-up learner into a voter (WBS 6.4.4).
func (n *Node) PromoteLearner(ctx context.Context, id uint64) error {
	_, err := n.confChange(ctx, &confReq{kind: raft.ConfChangePromoteLearner, id: id})
	return err
}

// RemoveMember removes a voter or a learner (WBS 6.4.5). Removing the leader
// itself is allowed: it steps down once the removal commits and hands over
// with TimeoutNow (WBS 6.1.5). force skips the reachable-quorum check.
func (n *Node) RemoveMember(ctx context.Context, id uint64, force bool) error {
	_, err := n.confChange(ctx, &confReq{kind: raft.ConfChangeRemoveNode, id: id, force: force})
	return err
}

func (n *Node) confChange(ctx context.Context, req *confReq) (confResult, error) {
	req.resp = make(chan confResult, 1)
	select {
	case n.confCh <- req:
	case <-ctx.Done():
		return confResult{}, ctx.Err()
	case <-n.stopping:
		return confResult{}, ErrStopped
	}
	var res confResult
	select {
	case res = <-req.resp:
	case <-ctx.Done():
		return confResult{}, ctx.Err()
	case <-n.stopping:
		return confResult{}, ErrStopped
	}
	if res.err != nil || res.done {
		return res, res.err
	}
	select {
	case err := <-res.w.done:
		return res, err
	case <-ctx.Done():
		n.dropWaiter(res.index, res.w)
		return confResult{}, ctx.Err()
	}
}

// handleConfChange validates and proposes a change on the loop goroutine.
func (n *Node) handleConfChange(req *confReq) {
	res := n.proposeConfChange(req)
	if res.err == nil && !res.done {
		res.w = &waiter{term: n.rn.Status().Term, done: make(chan error, 1)}
		n.waitMu.Lock()
		n.waiters[res.index] = res.w
		n.waitMu.Unlock()
	}
	req.resp <- res
}

func (n *Node) proposeConfChange(req *confReq) confResult {
	st := n.rn.Status()
	if st.State != raft.StateLeader {
		if st.Lead == raft.None {
			return confResult{err: raft.ErrProposalDropped}
		}
		return confResult{err: &raft.NotLeaderError{Leader: st.Lead}}
	}
	table := n.members.Load()
	cs := st.Config
	cc := raft.ConfChange{Type: req.kind, NodeID: req.id}
	var added *raftkvv1.Member
	switch req.kind {
	case raft.ConfChangeAddLearner:
		url := req.member.GetPeerUrl()
		if existing := membership.FindPeerURL(table, url); existing != nil {
			if cs.IsLearner(existing.GetId()) {
				return confResult{member: proto.CloneOf(existing), done: true} // idempotent
			}
			return confResult{err: fmt.Errorf("%w: %s is member %d", ErrPeerURLInUse, url, existing.GetId())}
		}
		added = proto.CloneOf(req.member)
		added.Id = randomID(table, n.cfg.NodeID) // D-38
		added.Role = raftkvv1.MemberRole_LEARNER
		if added.GetClientUrl() == "" {
			added.ClientUrl = url
		}
		cc.NodeID = added.GetId()
	case raft.ConfChangePromoteLearner:
		switch {
		case cs.IsVoter(req.id):
			return confResult{done: true} // idempotent
		case !cs.IsLearner(req.id):
			return confResult{err: fmt.Errorf("%w: %d", ErrUnknownMember, req.id)}
		}
		if pr, ok := st.Progress[req.id]; !ok || pr.Match+MaxPromoteLag < st.LastIndex {
			return confResult{err: fmt.Errorf("%w: learner %d has %d of %d entries", ErrLearnerNotCaughtUp,
				req.id, pr.Match, st.LastIndex)}
		}
	case raft.ConfChangeRemoveNode:
		if !cs.IsMember(req.id) {
			return confResult{err: fmt.Errorf("%w: %d", ErrUnknownMember, req.id)}
		}
		if cs.IsVoter(req.id) && !req.force && !n.quorumAfterRemoval(st, req.id) {
			return confResult{err: ErrWouldLoseQuorum}
		}
	}
	next, err := cs.Apply(cc)
	if err != nil {
		return confResult{err: err}
	}
	ctxBytes, err := membership.EncodeContext(membership.Next(table, next, added))
	if err != nil {
		return confResult{err: err}
	}
	cc.Context = ctxBytes
	index, _, err := n.rn.ProposeConfChange(cc)
	if err != nil {
		return confResult{err: err}
	}
	return confResult{index: index, member: added}
}

// quorumAfterRemoval reports whether the voters left after removing id include
// a quorum the leader has heard from within the last election timeout (WBS
// 6.4.5, WOULD_LOSE_QUORUM).
func (n *Node) quorumAfterRemoval(st raft.Status, id uint64) bool {
	window := time.Duration(n.cfg.ElectionTick) * n.cfg.TickInterval
	var left, live int
	for _, v := range st.Config.Voters {
		if v == id {
			continue
		}
		left++
		if v == n.cfg.NodeID || time.Since(n.lastHeard[v]) < window {
			live++
		}
	}
	return left > 0 && live >= left/2+1
}

// ---- leadership transfer (WBS 6.2) ----

type transferReq struct {
	target uint64
	resp   chan error
}

// TransferLeadership moves leadership to voter target (0: the most
// up-to-date voter) and waits until a new leader is known or the transfer
// times out (system-design 7.3). It returns the new leader and its term.
func (n *Node) TransferLeadership(ctx context.Context, target uint64) (leader, term uint64, err error) {
	st := n.Status()
	if st.State == raft.StateLeader && (target == n.cfg.NodeID || (target == 0 && len(st.Config.Voters) <= 1)) {
		return n.cfg.NodeID, st.Term, nil
	}
	req := &transferReq{target: target, resp: make(chan error, 1)}
	select {
	case n.transferCh <- req:
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	case <-n.stopping:
		return 0, 0, ErrStopped
	}
	select {
	case err := <-req.resp:
		if err != nil {
			return 0, 0, err
		}
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
	// Wait for the outcome: someone else leads, or the core gave up.
	deadline := time.Now().Add(n.transferTimeout())
	for {
		st := n.Status()
		if st.Lead != raft.None && st.Lead != n.cfg.NodeID {
			return st.Lead, st.Term, nil
		}
		if st.State == raft.StateLeader && st.LeadTransferee == raft.None || time.Now().After(deadline) {
			return 0, 0, ErrTransferTimeout
		}
		select {
		case <-time.After(n.cfg.TickInterval):
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		case <-n.stopping:
			return 0, 0, ErrStopped
		}
	}
}

// transferTimeout is one maximum election timeout plus a little slack.
func (n *Node) transferTimeout() time.Duration {
	return time.Duration(2*n.cfg.ElectionTick+2) * n.cfg.TickInterval
}

// handleTransfer starts a transfer on the loop goroutine. D-25: the lease is
// dropped first, so no lease read is served while another node may win.
func (n *Node) handleTransfer(req *transferReq) {
	st := n.rn.Status()
	if st.State == raft.StateLeader && req.target != raft.None {
		if st.Config.IsLearner(req.target) {
			req.resp <- fmt.Errorf("%w: %d", ErrTargetIsLearner, req.target)
			return
		}
		if !st.Config.IsVoter(req.target) {
			req.resp <- fmt.Errorf("%w: %d", ErrUnknownMember, req.target)
			return
		}
	}
	n.revokeLease()
	req.resp <- n.rn.TransferLeadership(req.target)
}

// revokeLease drops the read lease (D-25): on a transfer and on any change of
// the voter set.
func (n *Node) revokeLease() {
	if n.lease == nil {
		return
	}
	n.leaseMu.Lock()
	n.lease.l.Revoke()
	n.leaseMu.Unlock()
	n.lease.renewing = false
}

// onPeerWiped is the transport's D-32 report: member id came back with an
// empty data directory and is refused.
func (n *Node) onPeerWiped(id uint64) {
	n.log.Error("member restarted with an empty data directory and is refused (D-32); remove it and add it back as a new learner",
		"component", "membership", "member", id)
}
