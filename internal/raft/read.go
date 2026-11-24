package raft

import (
	"encoding/binary"
	"slices"
)

// ReadIndex (D-05, system-design 3.1): a linearizable read without writing to
// the log. The leader
//
//  1. waits until it has committed an entry in its own term (its no-op, T6);
//     before that its commit index may lag entries an earlier leader
//     committed,
//  2. records readIndex = commitIndex,
//  3. sends a heartbeat round carrying a fresh context, and
//  4. once a quorum (itself included) has acked that context, knows it was
//     still the leader when it took readIndex: a newer leader would need a
//     quorum at a higher term, and a node at a higher term never acks.
//
// The caller then serves the read once appliedIndex >= readIndex (WBS 4.2.1).
//
// Batching (WBS 4.1.2): at most one round is in flight. Requests that arrive
// while it is in flight cannot join it (its readIndex was taken before they
// started, so it could miss a write they must see); they wait together and
// share the next round, which starts as soon as the current one completes.
// Heartbeat ticks resend the in-flight round's context, so a lost heartbeat
// costs at most one tick.
//
// ReadIndex messages are term-less: MsgReadIndex is sent with Term 0 and
// neither message changes a receiver's term or role. The quorum round, not the
// term of the messages, is what makes the answer safe, and a follower may use
// an answer even if its own term moved on meanwhile: the answer was correct
// when the leader confirmed it, which is after the request was made.

// ReadState is the answer to one ReadIndex request, surfaced in
// [Ready.ReadStates]. The driver serves the read identified by RequestCtx once
// its applied index reaches Index.
type ReadState struct {
	// Index is the read index: every write acknowledged before the request
	// was made is at or below it. Zero when Rejected.
	Index uint64
	// RequestCtx is the context passed to [RawNode.ReadIndex].
	RequestCtx []byte
	// Rejected means the request failed: the node that ran it lost (or
	// never had) leadership, or the transport could not reach it. Nothing
	// may be served for it; the caller should retry.
	Rejected bool
}

// readRequest is one queued ReadIndex request on the leader. from is the
// leader's own ID for a local request, or the follower that forwarded it.
type readRequest struct {
	ctx  []byte
	from uint64
}

// readRound is one heartbeat round confirming leadership for a batch.
type readRound struct {
	id    uint64 // carried as the heartbeat Context
	index uint64 // commit index when the round started
	acks  map[uint64]bool
	reqs  []readRequest
}

// readOnly is the leader's ReadIndex state. It lives in memory only and is
// cleared on any role change (system-design section 0).
type readOnly struct {
	waiting []readRequest // not yet in a round
	round   *readRound    // in flight, at most one
	seq     uint64        // last round id; never reset, so ids stay unique
}

func roundCtx(id uint64) []byte { return binary.BigEndian.AppendUint64(nil, id) }

func parseRoundCtx(b []byte) (uint64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(b), true
}

// committedInCurrentTerm reports whether the leader has committed an entry of
// its own term (T6). Terms only grow along the log, so it is enough to look
// at the commit index.
func (r *raft) committedInCurrentTerm() bool {
	t, err := r.log.term(r.log.committed)
	return err == nil && t == r.term
}

// readIndex handles a local request (RawNode.ReadIndex).
func (r *raft) readIndex(ctx []byte) error {
	switch {
	case r.state == StateLeader:
		r.queueRead(readRequest{ctx: ctx, from: r.id})
		return nil
	case r.lead != None:
		// D-05, D-36: a follower asks the leader and serves locally.
		r.send(Message{Type: MsgReadIndex, To: r.lead, Context: ctx}) // Term 0: term-less
		return nil
	default:
		return ErrReadIndexDropped
	}
}

// handleReadIndex is the leader side of a forwarded request. A non-leader
// rejects it so the follower can retry elsewhere instead of timing out.
func (r *raft) handleReadIndex(m Message) {
	if r.state != StateLeader {
		r.send(Message{Type: MsgReadIndexResp, To: m.From, Term: r.term, Context: m.Context, Reject: true})
		return
	}
	r.queueRead(readRequest{ctx: m.Context, from: m.From})
}

// handleReadIndexResp is the follower side: the leader's answer becomes a
// ReadState whatever this node's current term or role.
func (r *raft) handleReadIndexResp(m Message) {
	rs := ReadState{RequestCtx: m.Context, Rejected: m.Reject}
	if !m.Reject {
		rs.Index = m.Index
	}
	r.readStates = append(r.readStates, rs)
}

func (r *raft) queueRead(req readRequest) {
	r.ro.waiting = append(r.ro.waiting, req)
	r.maybeStartReadRound()
}

// maybeStartReadRound starts a round for every waiting request if none is in
// flight and the leader has committed in its term. It is called whenever one
// of those conditions may have changed.
func (r *raft) maybeStartReadRound() {
	if r.state != StateLeader || r.ro.round != nil || len(r.ro.waiting) == 0 || !r.committedInCurrentTerm() {
		return
	}
	r.ro.seq++
	rd := &readRound{
		id: r.ro.seq, index: r.log.committed,
		acks: map[uint64]bool{r.id: true}, reqs: r.ro.waiting,
	}
	r.ro.waiting = nil
	r.ro.round = rd
	if !r.maybeFinishReadRound() {
		r.bcastHeartbeat()
	}
}

// ackReadRound records a heartbeat ack carrying ctx from a follower.
func (r *raft) ackReadRound(from uint64, ctx []byte) {
	id, ok := parseRoundCtx(ctx)
	if !ok || r.ro.round == nil || r.ro.round.id != id {
		return // an older round, already finished
	}
	r.ro.round.acks[from] = true
	r.maybeFinishReadRound()
}

// maybeFinishReadRound answers the in-flight round once a quorum acked it and
// starts the next one. It reports whether the round finished.
func (r *raft) maybeFinishReadRound() bool {
	rd := r.ro.round
	if rd == nil || !r.voterQuorum(rd.acks) {
		return false
	}
	r.ro.round = nil
	for _, req := range rd.reqs {
		if req.from == r.id {
			r.readStates = append(r.readStates, ReadState{Index: rd.index, RequestCtx: req.ctx})
			continue
		}
		r.send(Message{Type: MsgReadIndexResp, To: req.from, Term: r.term, Index: rd.index, Context: req.ctx})
	}
	r.maybeStartReadRound()
	return true
}

// voterQuorum reports whether the voters in acks form a majority of the
// voters. Learners never count (D-06), nor does a leader whose own removal is
// pending.
func (r *raft) voterQuorum(acks map[uint64]bool) bool {
	n := 0
	for _, id := range r.conf.Voters {
		if acks[id] {
			n++
		}
	}
	return n >= quorumSize(len(r.conf.Voters))
}

// readRoundCtx is the context heartbeats carry: the in-flight round's.
func (r *raft) readRoundCtx() []byte {
	if r.ro.round == nil {
		return nil
	}
	return roundCtx(r.ro.round.id)
}

// abortReads rejects every queued and in-flight request when this node stops
// leading, so callers fail fast instead of waiting for a deadline. It runs
// before the term changes, so rejections carry the term the requests saw.
func (r *raft) abortReads() {
	reqs := slices.Clone(r.ro.waiting)
	if r.ro.round != nil {
		reqs = append(reqs, r.ro.round.reqs...)
	}
	r.ro.waiting, r.ro.round = nil, nil
	for _, req := range reqs {
		if req.from == r.id {
			r.readStates = append(r.readStates, ReadState{RequestCtx: req.ctx, Rejected: true})
			continue
		}
		r.send(Message{Type: MsgReadIndexResp, To: req.from, Term: r.term, Context: req.ctx, Reject: true})
	}
}
