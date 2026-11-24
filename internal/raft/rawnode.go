package raft

import "fmt"

// Status is a read-only snapshot of a node, for observers and invariant
// checkers (the simulator, D-18). It is a copy; mutating it has no effect.
type Status struct {
	ID        uint64
	Term      uint64
	Vote      uint64
	Lead      uint64
	State     StateType
	Commit    uint64
	Applied   uint64
	LastIndex uint64
	// Config is the configuration in effect (D-31: the newest conf-change
	// entry in the log, committed or not) and ConfIndex that entry's index
	// (0: the bootstrap or snapshot configuration).
	Config    ConfState
	ConfIndex uint64
	// LeadTransferee is the target of a leadership transfer in progress on
	// this leader, or None.
	LeadTransferee uint64
	// Progress is the replication state per voter and learner (and the
	// leader itself); it is non-nil only on the leader.
	Progress map[uint64]Progress
}

// RawNode is the public face of the Raft core (D-01). It wraps the state
// machine with the Ready/Advance contract described in the package docs.
//
// A RawNode is not safe for concurrent use: exactly one goroutine (the driver,
// D-35) may call it. Mutating methods panic if called while a Ready is
// outstanding (between Ready and Advance); that is a driver bug.
type RawNode struct {
	raft        *raft
	prevSoft    SoftState
	prevHard    HardState
	outstanding bool
}

// NewRawNode validates cfg and returns a node in the follower state with the
// HardState and log found in cfg.Storage.
func NewRawNode(cfg Config) (*RawNode, error) {
	r, err := newRaft(cfg)
	if err != nil {
		return nil, err
	}
	return &RawNode{raft: r, prevSoft: r.softState(), prevHard: r.hardState()}, nil
}

func (n *RawNode) mustBeIdle(op string) {
	if n.outstanding {
		panic(fmt.Sprintf("raft: %s called while a Ready is outstanding; call Advance first", op))
	}
}

// Tick advances logical time by one tick (D-24). Followers and candidates may
// start an election; leaders heartbeat and run CheckQuorum.
func (n *RawNode) Tick() {
	n.mustBeIdle("Tick")
	n.raft.tick()
}

// Campaign makes the node start an election immediately (with a PreVote round
// when Config.PreVote is set) instead of waiting for the election timeout.
// Useful for bootstrapping and deterministic tests.
func (n *RawNode) Campaign() {
	n.mustBeIdle("Campaign")
	n.raft.campaign(n.raft.cfg.PreVote, false)
}

// Propose appends data to the log if this node is the leader and returns the
// index and term the entry will have if it commits. The entry is committed
// only once it appears in Ready.CommittedEntries; callers should match on
// (index, term) because a leader change can replace the entry.
//
// Errors:
//   - [ErrProposalDropped]: no known leader, or the uncommitted-size limit
//     (D-29) was hit. Retry later.
//   - *[NotLeaderError] (errors.Is ErrNotLeader): this node follows Leader.
//     Forwarding arrives in M4 (D-28).
func (n *RawNode) Propose(data []byte) (index, term uint64, err error) {
	n.mustBeIdle("Propose")
	return n.raft.propose(data)
}

// ProposeConfChange proposes a single-server membership change (D-06) and
// returns the index and term its entry will have if it commits. The new
// configuration takes effect on this leader at once, and on every other node
// when it appends the entry (D-31); callers learn that it is final once the
// entry appears in Ready.CommittedEntries.
//
// Errors, besides those of [RawNode.Propose]:
//   - [ErrConfChangePending]: the previous change is not committed yet.
//   - [ErrConfChangeTooEarly]: this leader has not committed in its term.
//   - [ErrTransferInProgress]: a leadership transfer is under way.
//   - errors wrapping [ErrInvalidConfChange]: cc does not fit the
//     configuration (unknown node, last voter, a removed ID, ...).
func (n *RawNode) ProposeConfChange(cc ConfChange) (index, term uint64, err error) {
	n.mustBeIdle("ProposeConfChange")
	return n.raft.proposeConfChange(cc)
}

// TransferLeadership starts handing leadership to voter to (None: the most
// up-to-date voter) (system-design 7.3). The leader stops taking proposals,
// brings the target's log up to date and then sends it MsgTimeoutNow; the
// target campaigns at once. If no new leader appears within one election
// timeout the transfer is abandoned and proposals resume. It returns
// *[NotLeaderError] (or [ErrProposalDropped]) on a non-leader and an error
// wrapping [ErrTransferTarget] if to is not a voter. Transferring to this
// node is a no-op.
func (n *RawNode) TransferLeadership(to uint64) error {
	n.mustBeIdle("TransferLeadership")
	return n.raft.transferLeadership(to)
}

// ConfStateAt returns the configuration in effect at log index i, for a
// snapshot taken there (WBS 6.1.6). ok is false if i is below the oldest
// configuration the node knows (its last snapshot).
func (n *RawNode) ConfStateAt(i uint64) (cs ConfState, ok bool) { return n.raft.confStateAt(i) }

// ReadIndex requests a linearizable read (D-05). ctx identifies the request
// and must be unique among requests in flight on this node. On the leader the
// request is confirmed by a heartbeat quorum (concurrent requests share one
// round); on a follower it is forwarded to the leader. The answer arrives
// later as a [ReadState] in [Ready.ReadStates] with the same RequestCtx; it may
// never arrive (a lost message), so callers need a deadline. A rejected
// ReadState means retry.
//
// It returns [ErrReadIndexDropped] when no leader is known.
func (n *RawNode) ReadIndex(ctx []byte) error {
	n.mustBeIdle("ReadIndex")
	return n.raft.readIndex(ctx)
}

// Step feeds one inbound peer message to the state machine. It returns
// [ErrUnknownMessage] for an invalid message type. Messages from removed
// members (D-31), and messages addressed to another node, are ignored.
func (n *RawNode) Step(m Message) error {
	n.mustBeIdle("Step")
	if !m.Type.valid() {
		return ErrUnknownMessage
	}
	if m.To != n.raft.id {
		return nil
	}
	n.raft.step(m)
	return nil
}

// ReportUnreachable tells the core that the transport failed to reach node id
// (D-12). A leader drops that follower from Replicate back to Probe, so it
// stops streaming optimistically into a dead link and probes with one append
// per round trip until the follower answers again. It is a no-op on
// non-leaders and for unknown nodes.
func (n *RawNode) ReportUnreachable(id uint64) {
	n.mustBeIdle("ReportUnreachable")
	n.raft.reportUnreachable(id)
}

// ReportSnapshot tells the core how a snapshot transfer to node id that a
// MsgSnap asked for ended (D-14). failed means the transfer broke or the
// follower refused it; there is no resume, so the next attempt starts over. On
// success or failure the leader returns that follower to Probe and resumes
// replication once it hears from it. It is a no-op on non-leaders and when no
// snapshot is pending for id (the follower's acknowledgement may already have
// moved it on).
func (n *RawNode) ReportSnapshot(id uint64, failed bool) {
	n.mustBeIdle("ReportSnapshot")
	n.raft.reportSnapshot(id, failed)
}

// HasReady reports whether Ready would return something to handle. It is
// cheap and may be called while a Ready is outstanding.
func (n *RawNode) HasReady() bool {
	r := n.raft
	return len(r.msgs) > 0 ||
		len(r.readStates) > 0 ||
		r.log.hasPendingSnapshot() ||
		len(r.log.unstable.entries) > 0 ||
		r.log.hasNextCommittedEnts() ||
		r.softState() != n.prevSoft ||
		r.hardState() != n.prevHard
}

// Ready returns the pending output and marks it outstanding until Advance.
// Call it only when HasReady is true. See [Ready] for the handling order.
func (n *RawNode) Ready() Ready {
	n.mustBeIdle("Ready")
	r := n.raft
	rd := Ready{
		Entries:          r.log.unstableEntries(),
		CommittedEntries: r.log.nextCommittedEnts(),
		Messages:         r.msgs,
		ReadStates:       r.readStates,
	}
	r.msgs, r.readStates = nil, nil
	if s := r.log.unstable.snapshot; s != nil {
		snap := *s
		rd.Snapshot = &snap
	}
	if ss := r.softState(); ss != n.prevSoft {
		rd.SoftState = &ss
	}
	if hs := r.hardState(); hs != n.prevHard {
		rd.HardState = hs
		rd.MustSync = hs.Term != n.prevHard.Term || hs.Vote != n.prevHard.Vote
	}
	rd.MustSync = rd.MustSync || len(rd.Entries) > 0 || rd.Snapshot != nil
	n.outstanding = true
	return rd
}

// Advance tells the core that rd was fully handled: entries and HardState are
// durable, messages were handed to the transport, committed entries were
// applied (or queued for the applier in order). It may make a new Ready
// available (for example a new commit index).
func (n *RawNode) Advance(rd Ready) {
	if !n.outstanding {
		panic("raft: Advance called without an outstanding Ready")
	}
	n.outstanding = false
	if !rd.HardState.IsEmpty() {
		n.prevHard = rd.HardState
	}
	if rd.SoftState != nil {
		n.prevSoft = *rd.SoftState
	}
	n.raft.advance(rd)
}

// Status returns a snapshot of the node's state.
func (n *RawNode) Status() Status {
	r := n.raft
	s := Status{
		ID: r.id, Term: r.term, Vote: r.vote, Lead: r.lead, State: r.state,
		Commit: r.log.committed, Applied: r.log.applied, LastIndex: r.log.lastIndex(),
		Config: r.conf.Clone(), ConfIndex: r.pendingConfIndex(), LeadTransferee: r.leadTransferee,
	}
	if r.state == StateLeader {
		s.Progress = make(map[uint64]Progress, len(r.prs))
		for id, pr := range r.prs {
			s.Progress[id] = Progress{
				Match: pr.Match, Next: pr.Next, State: pr.State,
				RecentActive: pr.RecentActive, PendingSnapshot: pr.PendingSnapshot,
			}
		}
	}
	return s
}
