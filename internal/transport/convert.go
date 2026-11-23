package transport

import (
	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// This file maps the core's plain Go messages to the protobuf wire types and
// back (D-12, D-36). The mapping is lossless for every field the core uses:
//
//	MsgApp            <-> AppendEntriesRequest (heartbeat=false)
//	MsgHeartbeat      <-> AppendEntriesRequest (heartbeat=true)
//	MsgAppResp        <-> AppendEntriesResponse (heartbeat=false)
//	MsgHeartbeatResp  <-> AppendEntriesResponse (heartbeat=true)
//	MsgVote/MsgPreVote         <-> RequestVoteRequest (pre_vote flag, D-36)
//	MsgVoteResp/MsgPreVoteResp <-> RequestVoteResponse (pre_vote flag)
//	MsgReadIndex               <-> ReadIndexRequest (D-05, term-less)
//	MsgReadIndexResp           <-> ReadIndexResponse, or an RPC error if rejected
//
// The destination (Message.To) is not on the wire: it is the node that
// receives the stream or RPC, and the receiver fills it in.

// EntryToProto converts a log entry. The core's leader no-op is an
// EntryNormal with no data; it travels as ENTRY_NORMAL and comes back the same.
func EntryToProto(e raft.Entry) *raftkvv1.Entry {
	t := raftkvv1.EntryType_ENTRY_NORMAL
	if e.Type == raft.EntryConfChange {
		t = raftkvv1.EntryType_ENTRY_CONF_CHANGE
	}
	return &raftkvv1.Entry{Term: e.Term, Index: e.Index, Type: t, Data: e.Data}
}

// EntryFromProto converts a wire entry. ENTRY_NOOP is an EntryNormal.
func EntryFromProto(e *raftkvv1.Entry) raft.Entry {
	t := raft.EntryNormal
	if e.GetType() == raftkvv1.EntryType_ENTRY_CONF_CHANGE {
		t = raft.EntryConfChange
	}
	return raft.Entry{Term: e.GetTerm(), Index: e.GetIndex(), Type: t, Data: e.GetData()}
}

// IsAppendRequest reports whether m travels as an AppendEntriesRequest on the
// Replicate stream (leader to follower).
func IsAppendRequest(t raft.MessageType) bool { return t == raft.MsgApp || t == raft.MsgHeartbeat }

// IsAppendResponse reports whether m travels as an AppendEntriesResponse on
// the Replicate stream (follower to leader).
func IsAppendResponse(t raft.MessageType) bool {
	return t == raft.MsgAppResp || t == raft.MsgHeartbeatResp
}

// IsVoteRequest reports whether m is carried by the RequestVote RPC.
func IsVoteRequest(t raft.MessageType) bool { return t == raft.MsgVote || t == raft.MsgPreVote }

// IsVoteResponse reports whether m answers a RequestVote RPC.
func IsVoteResponse(t raft.MessageType) bool {
	return t == raft.MsgVoteResp || t == raft.MsgPreVoteResp
}

// AppendRequestToProto converts a MsgApp or MsgHeartbeat.
func AppendRequestToProto(clusterID uint64, m raft.Message) *raftkvv1.AppendEntriesRequest {
	req := &raftkvv1.AppendEntriesRequest{
		ClusterId:    clusterID,
		Term:         m.Term,
		LeaderId:     m.From,
		PrevLogIndex: m.Index,
		PrevLogTerm:  m.LogTerm,
		LeaderCommit: m.Commit,
		ReadContext:  m.Context,
		Heartbeat:    m.Type == raft.MsgHeartbeat,
	}
	if n := len(m.Entries); n > 0 {
		req.Entries = make([]*raftkvv1.Entry, n)
		for i, e := range m.Entries {
			req.Entries[i] = EntryToProto(e)
		}
	}
	return req
}

// AppendRequestFromProto converts a wire request received by node `to`.
func AppendRequestFromProto(req *raftkvv1.AppendEntriesRequest, to uint64) raft.Message {
	m := raft.Message{
		Type:    raft.MsgApp,
		To:      to,
		From:    req.GetLeaderId(),
		Term:    req.GetTerm(),
		Index:   req.GetPrevLogIndex(),
		LogTerm: req.GetPrevLogTerm(),
		Commit:  req.GetLeaderCommit(),
		Context: req.GetReadContext(),
	}
	if req.GetHeartbeat() {
		m.Type = raft.MsgHeartbeat
	}
	if n := len(req.GetEntries()); n > 0 {
		m.Entries = make([]raft.Entry, n)
		for i, e := range req.GetEntries() {
			m.Entries[i] = EntryFromProto(e)
		}
	}
	return m
}

// AppendResponseToProto converts a MsgAppResp or MsgHeartbeatResp. On a reject
// match_index carries the rejected prev index (see raft.proto).
func AppendResponseToProto(m raft.Message) *raftkvv1.AppendEntriesResponse {
	return &raftkvv1.AppendEntriesResponse{
		Term:          m.Term,
		From:          m.From,
		Success:       !m.Reject,
		MatchIndex:    m.Index,
		ConflictIndex: m.RejectHint,
		ConflictTerm:  m.LogTerm,
		ReadContext:   m.Context,
		Heartbeat:     m.Type == raft.MsgHeartbeatResp,
	}
}

// AppendResponseFromProto converts a wire response received by node `to`.
func AppendResponseFromProto(r *raftkvv1.AppendEntriesResponse, to uint64) raft.Message {
	m := raft.Message{
		Type:       raft.MsgAppResp,
		To:         to,
		From:       r.GetFrom(),
		Term:       r.GetTerm(),
		Reject:     !r.GetSuccess(),
		Index:      r.GetMatchIndex(),
		RejectHint: r.GetConflictIndex(),
		LogTerm:    r.GetConflictTerm(),
		Context:    r.GetReadContext(),
	}
	if r.GetHeartbeat() {
		m.Type = raft.MsgHeartbeatResp
	}
	return m
}

// VoteRequestToProto converts a MsgVote or MsgPreVote (D-36).
func VoteRequestToProto(clusterID uint64, m raft.Message) *raftkvv1.RequestVoteRequest {
	return &raftkvv1.RequestVoteRequest{
		ClusterId:    clusterID,
		Term:         m.Term,
		CandidateId:  m.From,
		LastLogIndex: m.Index,
		LastLogTerm:  m.LogTerm,
		PreVote:      m.Type == raft.MsgPreVote,
	}
}

// VoteRequestFromProto converts a wire vote request received by node `to`.
func VoteRequestFromProto(r *raftkvv1.RequestVoteRequest, to uint64) raft.Message {
	typ := raft.MsgVote
	if r.GetPreVote() {
		typ = raft.MsgPreVote
	}
	return raft.Message{
		Type:    typ,
		To:      to,
		From:    r.GetCandidateId(),
		Term:    r.GetTerm(),
		Index:   r.GetLastLogIndex(),
		LogTerm: r.GetLastLogTerm(),
	}
}

// VoteResponseToProto converts a MsgVoteResp or MsgPreVoteResp.
func VoteResponseToProto(m raft.Message) *raftkvv1.RequestVoteResponse {
	return &raftkvv1.RequestVoteResponse{
		Term:        m.Term,
		VoteGranted: !m.Reject,
		PreVote:     m.Type == raft.MsgPreVoteResp,
		From:        m.From,
	}
}

// VoteResponseFromProto converts a wire vote response received by node `to`.
func VoteResponseFromProto(r *raftkvv1.RequestVoteResponse, to uint64) raft.Message {
	typ := raft.MsgVoteResp
	if r.GetPreVote() {
		typ = raft.MsgPreVoteResp
	}
	return raft.Message{Type: typ, To: to, From: r.GetFrom(), Term: r.GetTerm(), Reject: !r.GetVoteGranted()}
}

// IsReadIndexRequest reports whether m is carried by the ReadIndex RPC
// (follower to leader, D-36).
func IsReadIndexRequest(t raft.MessageType) bool { return t == raft.MsgReadIndex }

// IsReadIndexResponse reports whether m answers a ReadIndex RPC.
func IsReadIndexResponse(t raft.MessageType) bool { return t == raft.MsgReadIndexResp }

// ReadIndexRequestToProto converts a MsgReadIndex. The message is term-less.
func ReadIndexRequestToProto(clusterID uint64, m raft.Message) *raftkvv1.ReadIndexRequest {
	return &raftkvv1.ReadIndexRequest{ClusterId: clusterID, From: m.From, RequestCtx: m.Context}
}

// ReadIndexRequestFromProto converts a wire request received by node `to`.
func ReadIndexRequestFromProto(r *raftkvv1.ReadIndexRequest, to uint64) raft.Message {
	return raft.Message{Type: raft.MsgReadIndex, To: to, From: r.GetFrom(), Context: r.GetRequestCtx()}
}

// ReadIndexResponseToProto converts a successful MsgReadIndexResp. A rejected
// one has no wire form: the RPC fails with a status instead.
func ReadIndexResponseToProto(m raft.Message) *raftkvv1.ReadIndexResponse {
	return &raftkvv1.ReadIndexResponse{Term: m.Term, ReadIndex: m.Index, RequestCtx: m.Context}
}

// ReadIndexResponseFromProto converts a wire response from leader, received by
// node `to`.
func ReadIndexResponseFromProto(r *raftkvv1.ReadIndexResponse, leader, to uint64) raft.Message {
	return raft.Message{Type: raft.MsgReadIndexResp, To: to, From: leader, Term: r.GetTerm(),
		Index: r.GetReadIndex(), Context: r.GetRequestCtx()}
}
