package raft

import (
	"fmt"
	"slices"
)

// raft is the consensus state machine. It is unexported: callers drive it
// through RawNode. It is single-threaded by contract (D-35) and pure (D-01):
// it appends to msgs and the log, and never performs I/O.
type raft struct {
	id  uint64
	cfg Config

	term uint64
	vote uint64
	// D-01: state, lead are the SoftState.
	state StateType
	lead  uint64

	log *raftLog

	peers []uint64 // sorted voters, includes id; fixed order => deterministic
	prs   map[uint64]*Progress
	votes map[uint64]bool

	msgs []Message

	electionElapsed           int
	heartbeatElapsed          int
	randomizedElectionTimeout int

	// D-29: payload bytes appended by this leader (current term) and not yet
	// committed.
	uncommittedSize uint64
}

func newRaft(cfg Config) (*raft, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	hs, err := cfg.Storage.InitialState()
	if err != nil {
		return nil, fmt.Errorf("raft: reading initial state: %w", err)
	}
	log, err := newLog(cfg.Storage)
	if err != nil {
		return nil, fmt.Errorf("raft: opening log: %w", err)
	}
	if hs.Commit != 0 {
		if hs.Commit < log.committed || hs.Commit > log.lastIndex() {
			return nil, fmt.Errorf("raft: HardState.Commit %d outside log [%d, %d]",
				hs.Commit, log.committed, log.lastIndex())
		}
		log.committed = hs.Commit
	}
	if cfg.Applied != 0 {
		if cfg.Applied < log.applied || cfg.Applied > log.committed {
			return nil, fmt.Errorf("raft: Applied %d outside [%d, committed %d]",
				cfg.Applied, log.applied, log.committed)
		}
		log.applied = cfg.Applied
	}

	r := &raft{
		id:    cfg.ID,
		cfg:   cfg,
		log:   log,
		peers: slices.Sorted(slices.Values(cfg.Peers)),
		prs:   make(map[uint64]*Progress, len(cfg.Peers)),
	}
	r.becomeFollower(hs.Term, None)
	r.vote = hs.Vote // becomeFollower clears the vote only when the term changes
	return r, nil
}

func (r *raft) hardState() HardState {
	return HardState{Term: r.term, Vote: r.vote, Commit: r.log.committed}
}

func (r *raft) softState() SoftState { return SoftState{Lead: r.lead, State: r.state} }

// ---- role transitions ----

func (r *raft) reset(term uint64) {
	if r.term != term {
		r.term = term
		r.vote = None
	}
	r.lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
	r.votes = make(map[uint64]bool)
	r.uncommittedSize = 0
	last := r.log.lastIndex()
	for _, id := range r.peers {
		pr := newProgress(last+1, r.cfg.MaxInflight)
		if id == r.id {
			// The leader counts itself only up to what is durable (D-03,
			// system-design 2.4), so Match is the stable index, not last.
			pr.Match = r.log.stableIndex()
			pr.State = ProgressReplicate
		}
		r.prs[id] = pr
	}
}

// resetRandomizedElectionTimeout draws from [ElectionTick, 2*ElectionTick)
// (D-08) using the injected source (D-24).
func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.cfg.ElectionTick + r.cfg.Rand.IntN(r.cfg.ElectionTick)
}

func (r *raft) becomeFollower(term, lead uint64) {
	r.reset(term)
	r.state = StateFollower
	r.lead = lead
}

func (r *raft) becomePreCandidate() {
	// D-08: PreVote must not change term or vote.
	r.votes = make(map[uint64]bool)
	r.state = StatePreCandidate
	r.lead = None
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
}

func (r *raft) becomeCandidate() {
	r.reset(r.term + 1)
	r.vote = r.id
	r.state = StateCandidate
}

func (r *raft) becomeLeader() {
	r.reset(r.term)
	r.state = StateLeader
	r.lead = r.id
	// T6: a no-op in the new term lets older entries commit (Figure 8) and
	// is empty so it never trips the D-29 limit.
	if !r.appendEntries(Entry{}) {
		panic("raft: empty no-op entry rejected")
	}
}

// ---- ticking ----

func (r *raft) tick() {
	if r.state == StateLeader {
		r.tickHeartbeat()
		return
	}
	r.tickElection()
}

func (r *raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.campaign(r.cfg.PreVote)
	}
}

func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.electionElapsed >= r.cfg.ElectionTick {
		r.electionElapsed = 0
		// D-08 CheckQuorum: a leader without recent contact with a majority
		// steps down so it cannot linger as a stale leader.
		if r.cfg.CheckQuorum && !r.checkQuorumActive() {
			r.becomeFollower(r.term, None)
			return
		}
	}
	if r.heartbeatElapsed >= r.cfg.HeartbeatTick {
		r.heartbeatElapsed = 0
		r.bcastHeartbeat()
	}
}

// checkQuorumActive counts voters heard from since the last check (self always
// counts) and clears the flags for the next window.
func (r *raft) checkQuorumActive() bool {
	active := 0
	for _, id := range r.peers {
		pr := r.prs[id]
		if id == r.id || pr.RecentActive {
			active++
		}
		if id != r.id {
			pr.RecentActive = false
		}
	}
	return active >= quorumSize(len(r.peers))
}

// ---- elections ----

// campaign starts a PreVote round (preVote) or a real election.
func (r *raft) campaign(preVote bool) {
	var (
		typ  MessageType
		term uint64
	)
	if preVote {
		r.becomePreCandidate()
		typ, term = MsgPreVote, r.term+1
	} else {
		r.becomeCandidate()
		typ, term = MsgVote, r.term
	}
	r.votes[r.id] = true
	if r.checkVoteResult() {
		return
	}
	for _, id := range r.peers {
		if id == r.id {
			continue
		}
		r.send(Message{
			Type: typ, To: id, Term: term,
			Index: r.log.lastIndex(), LogTerm: r.log.lastTerm(),
		})
	}
}

// checkVoteResult acts on a decided election and reports whether it was
// decided.
func (r *raft) checkVoteResult() bool {
	granted, rejected := tallyVotes(r.peers, r.votes)
	q := quorumSize(len(r.peers))
	switch {
	case granted >= q:
		if r.state == StatePreCandidate {
			r.campaign(false) // D-08: only now does the term bump
		} else {
			r.becomeLeader()
			r.bcastAppend()
		}
		return true
	case len(r.peers)-rejected < q:
		r.becomeFollower(r.term, None)
		return true
	}
	return false
}

// ---- message plumbing ----

func (r *raft) send(m Message) {
	m.From = r.id
	r.msgs = append(r.msgs, m)
}

func (r *raft) newMsg(typ MessageType, to uint64) Message {
	return Message{Type: typ, To: to, Term: r.term}
}

func (r *raft) bcastAppend() {
	for _, id := range r.peers {
		if id != r.id {
			r.sendAppend(id, true)
		}
	}
}

func (r *raft) bcastHeartbeat() {
	for _, id := range r.peers {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

func (r *raft) sendHeartbeat(to uint64) {
	m := r.newMsg(MsgHeartbeat, to)
	// Never tell a follower to commit beyond what we know it has.
	m.Commit = min(r.prs[to].Match, r.log.committed)
	r.send(m)
}

// sendAppend sends entries from Progress.Next to follower to (AppendEntries
// with prevIndex/prevTerm = Next-1, WBS 1.3.1). It reports whether a message
// was sent.
func (r *raft) sendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.prs[to]
	if pr.isPaused() {
		return false
	}
	prevIndex := pr.Next - 1
	prevTerm, err := r.log.term(prevIndex)
	var ents []Entry
	if err == nil {
		ents, err = r.log.entries(pr.Next, r.cfg.MaxMsgBytes)
	}
	if err != nil {
		// The needed prefix is compacted. Sending a snapshot arrives with
		// M5 (D-14); until then the follower simply cannot be served.
		return false
	}
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}
	m := r.newMsg(MsgApp, to)
	m.Index, m.LogTerm, m.Entries, m.Commit = prevIndex, prevTerm, ents, r.log.committed
	if pr.State == ProgressReplicate {
		if n := len(ents); n > 0 {
			last := ents[n-1].Index
			pr.Next = last + 1 // optimistic
			pr.inflights.add(last)
		}
	} else {
		pr.probeSent = true
	}
	r.send(m)
	return true
}

// ---- proposals ----

// propose appends data as a new entry if this node is leader.
func (r *raft) propose(data []byte) (index, term uint64, err error) {
	if r.state != StateLeader {
		if r.lead == None {
			return 0, 0, ErrProposalDropped
		}
		return 0, 0, &NotLeaderError{Leader: r.lead}
	}
	if !r.appendEntries(Entry{Data: data}) {
		return 0, 0, ErrProposalDropped // D-29: uncommitted limit
	}
	r.bcastAppend()
	return r.log.lastIndex(), r.term, nil
}

// appendEntries stamps term/index on ents and appends them as leader. It
// returns false, appending nothing, when the D-29 uncommitted limit would be
// exceeded. A single entry is always admitted when nothing is outstanding so
// one over-sized command cannot wedge the leader.
func (r *raft) appendEntries(ents ...Entry) bool {
	var size uint64
	for _, e := range ents {
		size += e.Size()
	}
	if r.cfg.MaxUncommittedBytes > 0 && r.uncommittedSize > 0 && size > 0 &&
		r.uncommittedSize+size > r.cfg.MaxUncommittedBytes {
		return false
	}
	last := r.log.lastIndex()
	for i := range ents {
		ents[i].Term = r.term
		ents[i].Index = last + 1 + uint64(i)
	}
	r.log.append(ents...)
	r.uncommittedSize += size
	return true
}

// ---- Step ----

func (r *raft) step(m Message) {
	if _, member := r.prs[m.From]; !member {
		return // D-31 spirit: unknown senders are ignored (static membership)
	}
	switch {
	case m.Term > r.term:
		if (m.Type == MsgVote || m.Type == MsgPreVote) &&
			r.cfg.CheckQuorum && r.lead != None && r.electionElapsed < r.cfg.ElectionTick {
			// T5: we heard from a live leader within the minimum election
			// timeout, so ignore this (pre-)vote and do not bump our term.
			return
		}
		switch {
		case m.Type == MsgPreVote:
			// T4: a PreVote never changes our term.
		case m.Type == MsgPreVoteResp && !m.Reject:
			// A grant carries the prospective term; we adopt it only when
			// the pre-candidate converts to a real candidate.
		case m.Type == MsgApp || m.Type == MsgHeartbeat:
			r.becomeFollower(m.Term, m.From) // T1
		default:
			r.becomeFollower(m.Term, None) // T1
		}
	case m.Term < r.term:
		// T2: drop, but let a stale leader (or pre-candidate) learn our term.
		switch {
		case (r.cfg.CheckQuorum || r.cfg.PreVote) && (m.Type == MsgApp || m.Type == MsgHeartbeat):
			r.send(r.newMsg(MsgAppResp, m.From))
		case m.Type == MsgPreVote:
			resp := r.newMsg(MsgPreVoteResp, m.From)
			resp.Reject = true
			r.send(resp)
		}
		return
	}

	switch m.Type {
	case MsgVote, MsgPreVote:
		r.handleVoteRequest(m)
		return
	}
	switch r.state {
	case StateLeader:
		r.stepLeader(m)
	case StateCandidate, StatePreCandidate:
		r.stepCandidate(m)
	case StateFollower:
		r.stepFollower(m)
	}
}

// handleVoteRequest implements T3 (votes) and T4 (PreVote).
func (r *raft) handleVoteRequest(m Message) {
	respType := MsgVoteResp
	if m.Type == MsgPreVote {
		respType = MsgPreVoteResp
	}
	canVote := r.vote == m.From ||
		(r.vote == None && r.lead == None) ||
		(m.Type == MsgPreVote && m.Term > r.term)
	if canVote && r.log.isUpToDate(m.Index, m.LogTerm) {
		// D-01: the vote is part of HardState; the driver persists it before
		// sending this message.
		resp := r.newMsg(respType, m.From)
		resp.Term = m.Term
		r.send(resp)
		if m.Type == MsgVote {
			r.electionElapsed = 0
			r.vote = m.From
		}
		return
	}
	resp := r.newMsg(respType, m.From)
	resp.Reject = true
	r.send(resp)
}

func (r *raft) stepLeader(m Message) {
	pr := r.prs[m.From]
	switch m.Type {
	case MsgAppResp:
		pr.RecentActive = true
		if m.Reject {
			r.handleAppendReject(pr, m)
			return
		}
		if !pr.maybeUpdate(m.Index) {
			return
		}
		if pr.State == ProgressProbe {
			pr.becomeReplicate()
		} else {
			pr.inflights.freeLE(m.Index)
		}
		if r.maybeCommit() {
			r.bcastAppend() // propagate the new commit index
		} else {
			for r.sendAppend(m.From, false) {
			}
		}
	case MsgHeartbeatResp:
		pr.RecentActive = true
		pr.probeSent = false
		if pr.State == ProgressReplicate && pr.inflights.full() {
			pr.inflights.freeFirstOne()
		}
		if pr.Match < r.log.lastIndex() {
			r.sendAppend(m.From, true)
		}
	}
}

// handleAppendReject implements fast backtracking (WBS 1.3.3): the follower's
// hint (index, term) lets the leader skip every entry of a conflicting term in
// one step instead of one index per round trip.
func (r *raft) handleAppendReject(pr *Progress, m Message) {
	next := m.RejectHint
	if m.LogTerm > 0 {
		next, _ = r.log.findConflictByTerm(m.RejectHint, m.LogTerm)
	}
	if !pr.maybeDecrTo(m.Index, next) {
		return // stale rejection
	}
	if pr.State == ProgressReplicate {
		pr.becomeProbe()
	}
	r.sendAppend(m.From, true)
}

// maybeCommit applies the commit rule (system-design 2.4): the quorum index
// of match values, committed only if that entry has the current term.
func (r *raft) maybeCommit() bool {
	matches := make([]uint64, 0, len(r.peers))
	for _, id := range r.peers {
		matches = append(matches, r.prs[id].Match)
	}
	return r.log.maybeCommit(quorumIndex(matches), r.term)
}

func (r *raft) stepCandidate(m Message) {
	want := MsgVoteResp
	if r.state == StatePreCandidate {
		want = MsgPreVoteResp
	}
	switch m.Type {
	case MsgApp, MsgHeartbeat:
		// A leader exists at our term (or higher, handled in step).
		r.becomeFollower(r.term, m.From)
		r.stepFollower(m)
	case want:
		r.votes[m.From] = !m.Reject
		r.checkVoteResult()
	}
}

func (r *raft) stepFollower(m Message) {
	switch m.Type {
	case MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppendEntries(m)
	case MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.log.commitTo(m.Commit)
		resp := r.newMsg(MsgHeartbeatResp, m.From)
		resp.Context = m.Context
		r.send(resp)
	}
}

// handleAppendEntries is the follower side of AppendEntries (system-design 2.2).
func (r *raft) handleAppendEntries(m Message) {
	resp := r.newMsg(MsgAppResp, m.From)
	if m.Index < r.log.committed {
		resp.Index = r.log.committed // already have it; ack our commit point
		r.send(resp)
		return
	}
	if lastNew, ok := r.log.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		resp.Index = lastNew
		r.send(resp)
		return
	}
	// Reject with a hint: the last index in our log whose term is <= the
	// leader's prevTerm, bounded by our last index.
	hintIndex := min(m.Index, r.log.lastIndex())
	hintIndex, hintTerm := r.log.findConflictByTerm(hintIndex, m.LogTerm)
	resp.Reject = true
	resp.Index = m.Index
	resp.RejectHint = hintIndex
	resp.LogTerm = hintTerm
	r.send(resp)
}

// advance is called after the driver fully handled rd.
func (r *raft) advance(rd Ready) {
	if n := len(rd.Entries); n > 0 {
		last := rd.Entries[n-1]
		r.log.stableTo(last.Index, last.Term)
		if r.state == StateLeader {
			// The leader counts its own entries only once durable.
			if r.prs[r.id].maybeUpdate(last.Index) && r.maybeCommit() {
				r.bcastAppend()
			}
		}
	}
	if n := len(rd.CommittedEntries); n > 0 {
		r.log.appliedTo(rd.CommittedEntries[n-1].Index)
		if r.state == StateLeader {
			// D-29: committed current-term entries no longer count as
			// uncommitted. Older-term entries were never counted.
			var freed uint64
			for _, e := range rd.CommittedEntries {
				if e.Term == r.term {
					freed += e.Size()
				}
			}
			r.uncommittedSize -= min(freed, r.uncommittedSize)
		}
	}
}
