package raft

import (
	"bytes"
	"errors"
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

	// Membership (confchange.go, D-06, D-31). conf is the configuration in
	// effect: the newest entry of confs, else baseConf. baseConf is valid at
	// baseIndex (a snapshot's index, or 0 for the bootstrap voters); confs
	// are the conf-change entries in the log above it, oldest first, so a
	// truncated entry can be rolled back.
	baseConf  ConfState
	baseIndex uint64
	confs     []confAt
	conf      ConfState
	members   []uint64 // conf.Members(), cached; fixed order => deterministic

	prs   map[uint64]*Progress
	votes map[uint64]bool

	msgs []Message

	electionElapsed           int
	heartbeatElapsed          int
	randomizedElectionTimeout int

	// Leadership transfer (system-design 7.3): the target, and the ticks
	// since the transfer started. Proposals pause while it is set.
	leadTransferee  uint64
	transferElapsed int

	// D-29: payload bytes appended by this leader (current term) and not yet
	// committed.
	uncommittedSize uint64

	// D-05: ReadIndex state (leader only) and answers for the next Ready.
	ro         readOnly
	readStates []ReadState
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
		id:  cfg.ID,
		cfg: cfg,
		log: log,
		prs: make(map[uint64]*Progress),
	}
	if err := r.loadConf(); err != nil {
		return nil, err
	}
	r.becomeFollower(hs.Term, None)
	r.vote = hs.Vote // becomeFollower clears the vote only when the term changes
	return r, nil
}

// loadConf rebuilds the configuration at start-up (WBS 6.1.6): the newest
// snapshot's configuration (or the bootstrap voters), then every conf-change
// entry in the log after it, appended or not (D-31: effective on append).
func (r *raft) loadConf() error {
	r.baseConf = ConfState{Voters: slices.Sorted(slices.Values(r.cfg.Peers))}
	from := r.log.firstIndex()
	if snap, err := r.cfg.Storage.Snapshot(); err == nil && snap.Index > 0 && snap.ConfState != nil {
		r.baseConf, r.baseIndex = snap.ConfState.Clone(), snap.Index
		from = max(from, snap.Index+1)
	}
	if last := r.log.lastIndex(); from <= last {
		ents, err := r.log.slice(from, last+1, noLimit)
		if err != nil {
			return fmt.Errorf("raft: reading log for the configuration: %w", err)
		}
		for _, e := range ents {
			if e.Type != EntryConfChange {
				continue
			}
			_, cs, err := DecodeConfChange(e.Data)
			if err != nil {
				return fmt.Errorf("raft: conf-change entry %d: %w", e.Index, err)
			}
			r.confs = append(r.confs, confAt{index: e.Index, cs: cs})
		}
	}
	r.setConf(r.currentConf())
	return nil
}

func (r *raft) currentConf() ConfState {
	if n := len(r.confs); n > 0 {
		return r.confs[n-1].cs
	}
	return r.baseConf
}

// pendingConfIndex is the index of the newest conf-change entry in the log, or
// 0. The change is pending while it is above the commit index.
func (r *raft) pendingConfIndex() uint64 {
	if n := len(r.confs); n > 0 {
		return r.confs[n-1].index
	}
	return 0
}

// confStateAt returns the configuration in effect at log index i, if i is not
// below the base configuration's index.
func (r *raft) confStateAt(i uint64) (ConfState, bool) {
	if i < r.baseIndex {
		return ConfState{}, false
	}
	cs := r.baseConf
	for _, c := range r.confs {
		if c.index > i {
			break
		}
		cs = c.cs
	}
	return cs.Clone(), true
}

// isTombstoned reports whether id's removal is committed here (D-31). The
// committed configuration is used, not the one in effect: a leader that
// appended its own removal must still reach the followers to commit it.
func (r *raft) isTombstoned(id uint64) bool {
	cs := &r.baseConf
	for i := range r.confs {
		if r.confs[i].index > r.log.committed {
			break
		}
		cs = &r.confs[i].cs
	}
	return cs.IsRemoved(id)
}

// onAppend keeps the configuration in step with the log (D-31): ents were just
// appended and replaced everything from ents[0].Index on, so conf-change
// entries at or above it are rolled back and the new ones take effect.
func (r *raft) onAppend(ents []Entry) {
	if len(ents) == 0 {
		return
	}
	first, changed := ents[0].Index, false
	for n := len(r.confs); n > 0 && r.confs[n-1].index >= first; n = len(r.confs) {
		r.confs, changed = r.confs[:n-1], true
	}
	for _, e := range ents {
		if e.Type != EntryConfChange {
			continue
		}
		_, cs, err := DecodeConfChange(e.Data)
		if err != nil {
			panic(fmt.Sprintf("raft: conf-change entry %d: %v", e.Index, err))
		}
		r.confs, changed = append(r.confs, confAt{index: e.Index, cs: cs}), true
	}
	if changed {
		r.setConf(r.currentConf())
	}
}

// setConf switches to configuration cs. A leader starts tracking new members
// and forgets removed ones; the quorum may have changed, so it re-checks its
// commit index. A candidate that is no longer a voter gives up.
func (r *raft) setConf(cs ConfState) {
	r.conf = cs.Clone()
	r.members = cs.Members()
	switch r.state {
	case StateLeader:
		last := r.log.lastIndex()
		for _, id := range r.members {
			if r.prs[id] == nil {
				r.prs[id] = newProgress(last+1, r.cfg.MaxInflight)
			}
		}
		for id := range r.prs {
			if id != r.id && !cs.IsMember(id) {
				delete(r.prs, id)
			}
		}
		if r.leadTransferee != None && !cs.IsVoter(r.leadTransferee) {
			r.abortTransfer()
		}
		if r.maybeCommit() {
			r.bcastAppend()
			r.maybeStartReadRound()
		}
		r.maybeFinishReadRound()
	case StateCandidate, StatePreCandidate:
		if !cs.IsVoter(r.id) {
			r.becomeFollower(r.term, None)
		}
	}
}

// promotable reports whether this node may campaign: only voters do (D-06).
func (r *raft) promotable() bool { return r.conf.IsVoter(r.id) }

func (r *raft) hardState() HardState {
	return HardState{Term: r.term, Vote: r.vote, Commit: r.log.committed}
}

func (r *raft) softState() SoftState { return SoftState{Lead: r.lead, State: r.state} }

// ---- role transitions ----

func (r *raft) reset(term uint64) {
	if r.state == StateLeader {
		r.abortReads() // before the term changes
	}
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
	r.leadTransferee, r.transferElapsed = None, 0
	last := r.log.lastIndex()
	r.prs = make(map[uint64]*Progress, len(r.members)+1)
	for _, id := range r.members {
		r.prs[id] = newProgress(last+1, r.cfg.MaxInflight)
	}
	// The node always tracks itself, even while it is not (or no longer) a
	// member: a leader whose own removal is pending still appends.
	self := newProgress(last+1, r.cfg.MaxInflight)
	// The leader counts itself only up to what is durable (D-03,
	// system-design 2.4), so Match is the stable index, not last.
	self.Match = r.log.stableIndex()
	self.State = ProgressReplicate
	r.prs[r.id] = self
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
	// D-06: learners and nodes outside the configuration never campaign.
	if r.promotable() && r.electionElapsed >= r.randomizedElectionTimeout {
		r.campaign(r.cfg.PreVote, false)
	}
}

func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.leadTransferee != None {
		// system-design 7.3: no new leader within one election timeout
		// means the transfer failed; resume normal service.
		r.transferElapsed++
		if r.transferElapsed >= r.cfg.ElectionTick {
			r.abortTransfer()
		}
	}
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
// counts if it is a voter) and clears the flags for the next window.
func (r *raft) checkQuorumActive() bool {
	active := 0
	for _, id := range r.conf.Voters {
		if pr := r.prs[id]; id == r.id || (pr != nil && pr.RecentActive) {
			active++
		}
	}
	for id, pr := range r.prs {
		if id != r.id {
			pr.RecentActive = false
		}
	}
	return active >= quorumSize(len(r.conf.Voters))
}

// ---- elections ----

// campaign starts a PreVote round (preVote) or a real election. transfer marks
// an election ordered by MsgTimeoutNow: it never uses PreVote and its votes
// carry TransferContext so voters skip T5.
func (r *raft) campaign(preVote, transfer bool) {
	if !r.promotable() {
		return // D-06: only voters campaign
	}
	var (
		typ  MessageType
		term uint64
	)
	if preVote && !transfer {
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
	for _, id := range r.conf.Voters {
		if id == r.id {
			continue
		}
		m := Message{
			Type: typ, To: id, Term: term,
			Index: r.log.lastIndex(), LogTerm: r.log.lastTerm(),
		}
		if transfer {
			m.Context = TransferContext
		}
		r.send(m)
	}
}

// checkVoteResult acts on a decided election and reports whether it was
// decided. Only voters' answers count (D-06).
func (r *raft) checkVoteResult() bool {
	granted, rejected := tallyVotes(r.conf.Voters, r.votes)
	q := quorumSize(len(r.conf.Voters))
	switch {
	case granted >= q:
		if r.state == StatePreCandidate {
			r.campaign(false, false) // D-08: only now does the term bump
		} else {
			r.becomeLeader()
			r.bcastAppend()
		}
		return true
	case len(r.conf.Voters)-rejected < q:
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

// bcastAppend replicates to every voter and learner.
func (r *raft) bcastAppend() {
	for _, id := range r.members {
		if id != r.id {
			r.sendAppend(id, true)
		}
	}
}

func (r *raft) bcastHeartbeat() {
	for _, id := range r.members {
		if id != r.id {
			r.sendHeartbeat(id)
		}
	}
}

func (r *raft) sendHeartbeat(to uint64) {
	pr := r.prs[to]
	if pr == nil {
		return
	}
	m := r.newMsg(MsgHeartbeat, to)
	// Never tell a follower to commit beyond what we know it has.
	m.Commit = min(pr.Match, r.log.committed)
	m.Context = r.readRoundCtx() // D-05: confirms the in-flight ReadIndex round
	r.send(m)
}

// sendAppend sends entries from Progress.Next to follower to (AppendEntries
// with prevIndex/prevTerm = Next-1, WBS 1.3.1). It reports whether a message
// was sent.
func (r *raft) sendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.prs[to]
	if pr == nil || pr.isPaused() {
		return false
	}
	prevIndex := pr.Next - 1
	prevTerm, err := r.log.term(prevIndex)
	var ents []Entry
	if err == nil {
		ents, err = r.log.entries(pr.Next, r.cfg.MaxMsgBytes)
	}
	if err != nil {
		if errors.Is(err, ErrCompacted) {
			// The follower needs entries we compacted away (D-14).
			return r.sendSnapshot(to, pr)
		}
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

// sendSnapshot asks the driver to send the newest local snapshot to a follower
// whose next index was compacted away (D-14) and pauses appends to it until
// the transfer is reported (ReportSnapshot) or acknowledged. It reports whether
// a message was sent.
func (r *raft) sendSnapshot(to uint64, pr *Progress) bool {
	snap, err := r.log.snapshot()
	if err != nil {
		if errors.Is(err, ErrSnapshotTemporarilyUnavailable) {
			return false
		}
		panic(fmt.Sprintf("raft: reading snapshot: %v", err))
	}
	if snap.IsEmpty() || snap.Index+1 < r.log.firstIndex() {
		// The snapshot does not cover the compacted prefix: the driver
		// broke the D-30 rule. Nothing can be sent; the driver must create
		// a newer snapshot.
		return false
	}
	m := r.newMsg(MsgSnap, to)
	m.Snapshot = &snap
	pr.becomeSnapshot(snap.Index)
	r.send(m)
	return true
}

// reportSnapshot implements RawNode.ReportSnapshot.
func (r *raft) reportSnapshot(id uint64, failed bool) {
	if r.state != StateLeader {
		return
	}
	pr, ok := r.prs[id]
	if !ok || id == r.id || pr.State != ProgressSnapshot {
		return
	}
	if failed {
		pr.PendingSnapshot = 0 // D-14: no resume; the next attempt starts over
	}
	pr.becomeProbe()
	// Wait for the next heartbeat response before probing, so a follower
	// that is still installing is not flooded.
	pr.probeSent = true
}

// ---- proposals ----

// checkProposable returns why this node cannot take a proposal now, or nil.
func (r *raft) checkProposable() error {
	switch {
	case r.state != StateLeader && r.lead == None:
		return ErrProposalDropped
	case r.state != StateLeader:
		return &NotLeaderError{Leader: r.lead}
	case r.leadTransferee != None:
		return ErrTransferInProgress // system-design 7.3: proposals pause
	}
	return nil
}

// propose appends data as a new entry if this node is leader.
func (r *raft) propose(data []byte) (index, term uint64, err error) {
	if err := r.checkProposable(); err != nil {
		return 0, 0, err
	}
	if !r.appendEntries(Entry{Data: data}) {
		return 0, 0, ErrProposalDropped // D-29: uncommitted limit
	}
	r.bcastAppend()
	return r.log.lastIndex(), r.term, nil
}

// proposeConfChange appends a single-server membership change (D-06) if the
// rules of system-design 7.1 allow it. The new configuration takes effect on
// the leader at once (D-31).
func (r *raft) proposeConfChange(cc ConfChange) (index, term uint64, err error) {
	if err := r.checkProposable(); err != nil {
		return 0, 0, err
	}
	if !r.cfg.Unsafe.AllowEarlyConfChange && !r.committedInCurrentTerm() {
		return 0, 0, ErrConfChangeTooEarly
	}
	if !r.cfg.Unsafe.AllowConcurrentConfChanges && r.pendingConfIndex() > r.log.committed {
		return 0, 0, ErrConfChangePending
	}
	next, err := r.conf.Apply(cc)
	if err != nil {
		return 0, 0, err
	}
	if !r.appendEntries(Entry{Type: EntryConfChange, Data: EncodeConfChange(cc, next)}) {
		return 0, 0, ErrProposalDropped
	}
	if r.state == StateLeader { // still: applying the change never deposes us
		r.bcastAppend()
	}
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
	r.onAppend(ents) // D-31: a conf change takes effect on append
	return true
}

// ---- leadership transfer (system-design 7.3) ----

// transferLeadership implements RawNode.TransferLeadership.
func (r *raft) transferLeadership(to uint64) error {
	if r.state != StateLeader {
		if r.lead == None {
			return ErrProposalDropped
		}
		return &NotLeaderError{Leader: r.lead}
	}
	if to == None {
		if to = r.mostUpToDateVoter(); to == None {
			return fmt.Errorf("%w: no other voter", ErrTransferTarget)
		}
	}
	if to == r.id {
		return nil
	}
	if !r.conf.IsVoter(to) {
		return fmt.Errorf("%w: %d", ErrTransferTarget, to)
	}
	if r.leadTransferee == to {
		return nil
	}
	r.leadTransferee, r.transferElapsed = to, 0
	if pr := r.prs[to]; pr.Match == r.log.lastIndex() {
		r.sendTimeoutNow(to)
	} else {
		r.sendAppend(to, true) // catch the target up first
	}
	return nil
}

func (r *raft) abortTransfer() { r.leadTransferee, r.transferElapsed = None, 0 }

func (r *raft) sendTimeoutNow(to uint64) { r.send(r.newMsg(MsgTimeoutNow, to)) }

// mostUpToDateVoter returns the voter other than this node with the highest
// match index (lowest ID on ties), or None.
func (r *raft) mostUpToDateVoter() uint64 {
	best, bestMatch := None, uint64(0)
	for _, id := range r.conf.Voters {
		pr := r.prs[id]
		if id == r.id || pr == nil {
			continue
		}
		if best == None || pr.Match > bestMatch {
			best, bestMatch = id, pr.Match
		}
	}
	return best
}

// maybeStepDownAfterRemoval makes a leader whose own removal has committed
// hand over and step down (WBS 6.1.5): it is no longer part of the cluster.
// It orders the most up-to-date voter to campaign at once if that voter has
// the whole log, so the cluster does not wait an election timeout.
func (r *raft) maybeStepDownAfterRemoval() {
	if r.state != StateLeader || r.conf.IsVoter(r.id) || r.pendingConfIndex() > r.log.committed {
		return
	}
	if to := r.mostUpToDateVoter(); to != None && r.prs[to].Match == r.log.lastIndex() {
		r.sendTimeoutNow(to)
	}
	r.becomeFollower(r.term, None)
}

// ---- Step ----

func isTransferVote(m Message) bool {
	return m.Type == MsgVote && bytes.Equal(m.Context, TransferContext)
}

func (r *raft) step(m Message) {
	if m.From == None || (r.isTombstoned(m.From) && m.Type != MsgTimeoutNow) {
		// D-31: messages from removed members are ignored. A leader that
		// removed itself hands over with MsgTimeoutNow right after its
		// removal commits; that order is still honoured.
		return
	}
	// D-05: ReadIndex messages are term-less (see read.go).
	switch m.Type {
	case MsgReadIndex:
		r.handleReadIndex(m)
		return
	case MsgReadIndexResp:
		r.handleReadIndexResp(m)
		return
	}
	switch {
	case m.Term > r.term:
		if (m.Type == MsgVote || m.Type == MsgPreVote) && !isTransferVote(m) &&
			r.cfg.CheckQuorum && r.lead != None && r.electionElapsed < r.cfg.ElectionTick {
			// T5: we heard from a live leader within the minimum election
			// timeout, so ignore this (pre-)vote and do not bump our term.
			// A transfer vote is exempt: the leader asked for it.
			return
		}
		switch {
		case m.Type == MsgPreVote:
			// T4: a PreVote never changes our term.
		case m.Type == MsgPreVoteResp && !m.Reject:
			// A grant carries the prospective term; we adopt it only when
			// the pre-candidate converts to a real candidate.
		case m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap || m.Type == MsgTimeoutNow:
			r.becomeFollower(m.Term, m.From) // T1
		default:
			r.becomeFollower(m.Term, None) // T1
		}
	case m.Term < r.term:
		// T2: drop, but let a stale leader (or pre-candidate) learn our term.
		switch {
		case (r.cfg.CheckQuorum || r.cfg.PreVote) && (m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap):
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
	r.maybeStepDownAfterRemoval()
}

// handleVoteRequest implements T3 (votes) and T4 (PreVote). Learners answer
// too: they may already be voters in the candidate's configuration, and only
// the candidate decides whose answers count.
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
	if pr == nil {
		return // not a member (any more)
	}
	switch m.Type {
	case MsgAppResp:
		pr.RecentActive = true
		if m.Reject {
			r.handleAppendReject(pr, m)
			return
		}
		oldPaused := pr.isPaused()
		if !pr.maybeUpdate(m.Index) {
			return
		}
		switch pr.State {
		case ProgressProbe:
			pr.becomeReplicate()
		case ProgressSnapshot:
			// The follower installed the snapshot (or already had what it
			// covers). Once the log can serve it again, replicate (D-14).
			if pr.Match+1 >= r.log.firstIndex() {
				pr.becomeProbe()
				pr.becomeReplicate()
			}
		default:
			pr.inflights.freeLE(m.Index)
		}
		if r.maybeCommit() {
			r.bcastAppend() // propagate the new commit index
			r.maybeStartReadRound()
		} else {
			if oldPaused {
				// The follower was waiting on this reply; an empty append
				// also tells it the current commit index.
				r.sendAppend(m.From, true)
			}
			for r.sendAppend(m.From, false) {
			}
		}
		if m.From == r.leadTransferee && pr.Match == r.log.lastIndex() {
			r.sendTimeoutNow(m.From) // system-design 7.3 step 4
		}
	case MsgHeartbeatResp:
		pr.RecentActive = true
		pr.probeSent = false
		if len(m.Context) > 0 {
			r.ackReadRound(m.From, m.Context)
		}
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
	if pr.State != ProgressSnapshot && pr.Match > 0 && m.Index == pr.Match && m.RejectHint < pr.Match {
		// The follower rejects an append right at the index it already
		// acknowledged (responses arrive in order, so this is not an old
		// rejection): it lost entries it had made durable, i.e. its disk
		// was wiped. Forget its match so it is no longer counted (lowering
		// Match never uncommits anything) and probe it from scratch. D-32
		// forbids such a node to rejoin under its old ID: the transport
		// spots its new data-directory incarnation and the node refuses it
		// before it gets this far (internal/transport, internal/node).
		pr.Match = 0
		pr.becomeProbe()
		pr.Next = max(next+1, 1)
		r.sendAppend(m.From, true)
		return
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
// of the voters' match values (learners never count, D-06), committed only if
// that entry has the current term.
func (r *raft) maybeCommit() bool {
	if len(r.conf.Voters) == 0 {
		return false
	}
	matches := make([]uint64, 0, len(r.conf.Voters))
	for _, id := range r.conf.Voters {
		var match uint64
		if pr := r.prs[id]; pr != nil {
			match = pr.Match
		}
		matches = append(matches, match)
	}
	return r.log.maybeCommit(quorumIndex(matches), r.term)
}

func (r *raft) stepCandidate(m Message) {
	want := MsgVoteResp
	if r.state == StatePreCandidate {
		want = MsgPreVoteResp
	}
	switch m.Type {
	case MsgApp, MsgHeartbeat, MsgSnap:
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
		if m.Commit <= r.log.lastIndex() {
			r.log.commitTo(m.Commit)
		}
		// Otherwise the leader believes we hold entries we do not (our disk
		// was wiped); keep our commit until appends repair the log.
		resp := r.newMsg(MsgHeartbeatResp, m.From)
		resp.Context = m.Context
		r.send(resp)
	case MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case MsgTimeoutNow:
		// system-design 7.3 step 5: campaign at once, without PreVote.
		// A learner (or a node outside the configuration) cannot.
		r.campaign(false, true)
	}
}

// handleSnapshot is the follower side of InstallSnapshot (D-14,
// system-design 5.2). It answers with a MsgAppResp carrying the follower's new
// match index: the snapshot index if it was accepted, else the commit index.
func (r *raft) handleSnapshot(m Message) {
	if m.Snapshot == nil {
		return
	}
	resp := r.newMsg(MsgAppResp, m.From)
	if r.restore(*m.Snapshot) {
		resp.Index = r.log.lastIndex()
	} else {
		resp.Index = r.log.committed
	}
	r.send(resp)
}

// restore decides whether to install s. A snapshot at or below the commit
// index is stale and ignored: installing it would roll the state machine
// back. If the log already holds the snapshot's last entry (same index and
// term), the log matches up to there (Log Matching), so it is kept and only
// the commit index moves (Raft section 7). Otherwise the whole log is
// replaced and the driver installs the snapshot from the next Ready; the
// configuration becomes the snapshot's (WBS 6.1.6).
func (r *raft) restore(s Snapshot) bool {
	if s.Index <= r.log.committed {
		return false
	}
	if r.log.matchTerm(s.Index, s.Term) {
		r.log.commitTo(s.Index)
		return false
	}
	r.log.installSnapshot(s)
	if s.ConfState != nil {
		r.baseConf = s.ConfState.Clone()
	} else {
		r.baseConf = r.currentConf() // a pre-M6 snapshot: keep what we have
	}
	r.baseIndex, r.confs = s.Index, nil
	r.setConf(r.baseConf)
	return true
}

// handleAppendEntries is the follower side of AppendEntries (system-design 2.2).
func (r *raft) handleAppendEntries(m Message) {
	resp := r.newMsg(MsgAppResp, m.From)
	if m.Index < r.log.committed {
		resp.Index = r.log.committed // already have it; ack our commit point
		r.send(resp)
		return
	}
	conflict := r.log.findConflict(m.Entries)
	if lastNew, ok := r.log.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		if conflict != 0 {
			// D-31: the configuration follows the log, including any
			// truncated suffix.
			r.onAppend(m.Entries[conflict-(m.Index+1):])
		}
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
	if rd.Snapshot != nil {
		r.log.stableSnapTo(rd.Snapshot.Index)
	}
	if n := len(rd.Entries); n > 0 {
		last := rd.Entries[n-1]
		r.log.stableTo(last.Index, last.Term)
		if r.state == StateLeader {
			// The leader counts its own entries only once durable.
			if r.prs[r.id].maybeUpdate(last.Index) && r.maybeCommit() {
				r.bcastAppend()
				r.maybeStartReadRound()
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
	r.maybeStepDownAfterRemoval()
}

// reportUnreachable implements RawNode.ReportUnreachable.
func (r *raft) reportUnreachable(id uint64) {
	if r.state != StateLeader {
		return
	}
	if pr, ok := r.prs[id]; ok && id != r.id && pr.State == ProgressReplicate {
		pr.becomeProbe()
	}
}
