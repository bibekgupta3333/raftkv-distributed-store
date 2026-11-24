package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Admin is the membership side of a node (D-06, D-23). *node.Node satisfies
// it.
type Admin interface {
	StatusMember
	PeerAddr(id uint64) string
	Members() *raftkvv1.ClusterConfig
	AddLearner(ctx context.Context, m *raftkvv1.Member) (*raftkvv1.Member, error)
	PromoteLearner(ctx context.Context, id uint64) error
	RemoveMember(ctx context.Context, id uint64, force bool) error
	TransferLeadership(ctx context.Context, target uint64) (leader, term uint64, err error)
	LinearizableRead(ctx context.Context, read func() error) error
}

// Reasons of the Cluster admin service (api-design 4, 9).
const (
	ReasonConfChangeInProgress = "CONF_CHANGE_IN_PROGRESS"
	ReasonAlreadyExists        = "ALREADY_EXISTS"
	ReasonNotFound             = "NOT_FOUND"
	ReasonLearnerNotCaughtUp   = "LEARNER_NOT_CAUGHT_UP"
	ReasonWouldLoseQuorum      = "WOULD_LOSE_QUORUM"
	ReasonTransferTimeout      = "TRANSFER_TIMEOUT"
	ReasonTargetIsLearner      = "TARGET_IS_LEARNER"
)

// admin returns the membership side of the member, or an UNIMPLEMENTED error
// for a member that has none (tests with a status-only fake).
func (s *Cluster) admin() (Admin, error) {
	a, ok := s.member.(Admin)
	if !ok {
		return nil, newError(codes.Unimplemented, ReasonInternal, Definite, "membership administration is not available", nil)
	}
	return a, nil
}

func (s *Cluster) header(a Admin) *raftkvv1.ResponseHeader {
	st := a.Status()
	return &raftkvv1.ResponseHeader{ClusterId: a.ClusterID(), MemberId: a.ID(), RaftTerm: st.Term,
		Revision: s.reader.Applied(), LeaderId: st.Lead}
}

func (s *Cluster) hint(a Admin, leader uint64) *raftkvv1.LeaderHint {
	h := &raftkvv1.LeaderHint{LeaderId: leader, Term: a.Status().Term}
	if leader != 0 {
		h.LeaderClientUrl = a.PeerAddr(leader)
	}
	return h
}

// mapAdminError labels a membership error (api-design 9). Conf-change
// conflicts are DEFINITE: nothing was proposed.
func (s *Cluster) mapAdminError(a Admin, err error) error {
	var nl *raft.NotLeaderError
	switch {
	case errors.As(err, &nl):
		return newError(codes.Unavailable, ReasonNotLeader, Definite, "not the leader", s.hint(a, nl.Leader))
	case errors.Is(err, raft.ErrTransferInProgress):
		return newError(codes.Unavailable, ReasonNotLeader, Definite, "leadership transfer in progress", s.hint(a, 0))
	case errors.Is(err, raft.ErrProposalDropped):
		return newError(codes.Unavailable, ReasonNoLeader, Definite, "no leader", nil)
	case errors.Is(err, raft.ErrConfChangePending), errors.Is(err, raft.ErrConfChangeTooEarly):
		return newError(codes.FailedPrecondition, ReasonConfChangeInProgress, Definite, err.Error(), nil)
	case errors.Is(err, node.ErrLearnerNotCaughtUp):
		return newError(codes.FailedPrecondition, ReasonLearnerNotCaughtUp, Definite, err.Error(), nil)
	case errors.Is(err, node.ErrWouldLoseQuorum):
		return newError(codes.FailedPrecondition, ReasonWouldLoseQuorum, Definite, err.Error(), nil)
	case errors.Is(err, node.ErrTargetIsLearner):
		return newError(codes.FailedPrecondition, ReasonTargetIsLearner, Definite, err.Error(), nil)
	case errors.Is(err, node.ErrUnknownMember), errors.Is(err, raft.ErrTransferTarget):
		return newError(codes.NotFound, ReasonNotFound, Definite, err.Error(), nil)
	case errors.Is(err, node.ErrPeerURLInUse):
		return newError(codes.AlreadyExists, ReasonAlreadyExists, Definite, err.Error(), nil)
	case errors.Is(err, raft.ErrInvalidConfChange):
		return newError(codes.FailedPrecondition, ReasonMalformed, Definite, err.Error(), nil)
	case errors.Is(err, node.ErrTransferTimeout):
		return newError(codes.DeadlineExceeded, ReasonTransferTimeout, Definite, err.Error(), nil)
	}
	return mapWaitError(err) // proposed: the outcome is unknown
}

// route runs a mutating admin call here if this member leads, and otherwise
// forwards it once to the leader, like a KV write (D-28, api-design 4).
func route[R headed](ctx context.Context, s *Cluster, a Admin, hdr *raftkvv1.RequestHeader,
	local func(context.Context) (R, error),
	remote func(context.Context, raftkvv1.ClusterClient, *raftkvv1.RequestHeader) (R, error),
) (R, error) {
	var zero R
	if id := hdr.GetClusterId(); id != 0 && id != a.ClusterID() {
		return zero, newError(codes.FailedPrecondition, ReasonClusterMismatch, "", "cluster ID mismatch", nil)
	}
	st := a.Status()
	switch {
	case st.State == raft.StateLeader:
		return local(ctx)
	case st.Lead == 0:
		return zero, newError(codes.Unavailable, ReasonNoLeader, Definite, "no known leader", nil)
	case hdr.GetForwarded() || hdr.GetWriteRouting() == raftkvv1.WriteRouting_REDIRECT:
		return zero, newError(codes.Unavailable, ReasonNotLeader, Definite, "not the leader", s.hint(a, st.Lead))
	}
	conn, err := s.fwd.conn(ctx, a.PeerAddr(st.Lead))
	if err != nil {
		return zero, newError(codes.Unavailable, ReasonNoLeader, Definite, "leader unreachable: "+err.Error(), nil)
	}
	fh := proto.CloneOf(hdr)
	if fh == nil {
		fh = &raftkvv1.RequestHeader{}
	}
	fh.Forwarded, fh.WriteRouting = true, raftkvv1.WriteRouting_FORWARD
	resp, err := remote(ctx, raftkvv1.NewClusterClient(conn), fh)
	if err != nil {
		return zero, relayForwardError(ctx, err)
	}
	if h := resp.GetHeader(); h != nil {
		h.MemberId = a.ID()
	}
	return resp, nil
}

// AddLearner implements raftkvv1.ClusterServer (WBS 6.4.3).
func (s *Cluster) AddLearner(ctx context.Context, req *raftkvv1.AddLearnerRequest) (*raftkvv1.AddLearnerResponse, error) {
	a, err := s.admin()
	if err != nil {
		return nil, err
	}
	if req.GetPeerUrl() == "" {
		return nil, newError(codes.InvalidArgument, ReasonMalformed, Definite, "peer_url is required", nil)
	}
	return route(ctx, s, a, req.GetHeader(),
		func(ctx context.Context) (*raftkvv1.AddLearnerResponse, error) {
			m, err := a.AddLearner(ctx, &raftkvv1.Member{Name: req.GetName(), PeerUrl: req.GetPeerUrl(), ClientUrl: req.GetClientUrl()})
			if err != nil {
				return nil, s.mapAdminError(a, err)
			}
			return &raftkvv1.AddLearnerResponse{Header: s.header(a), Member: m, Members: a.Members().GetMembers()}, nil
		},
		func(ctx context.Context, c raftkvv1.ClusterClient, h *raftkvv1.RequestHeader) (*raftkvv1.AddLearnerResponse, error) {
			r := proto.CloneOf(req)
			r.Header = h
			return c.AddLearner(ctx, r)
		})
}

// PromoteLearner implements raftkvv1.ClusterServer (WBS 6.4.4).
func (s *Cluster) PromoteLearner(ctx context.Context, req *raftkvv1.PromoteLearnerRequest) (*raftkvv1.PromoteLearnerResponse, error) {
	a, err := s.admin()
	if err != nil {
		return nil, err
	}
	return route(ctx, s, a, req.GetHeader(),
		func(ctx context.Context) (*raftkvv1.PromoteLearnerResponse, error) {
			if err := a.PromoteLearner(ctx, req.GetMemberId()); err != nil {
				return nil, s.mapAdminError(a, err)
			}
			return &raftkvv1.PromoteLearnerResponse{Header: s.header(a), Members: a.Members().GetMembers()}, nil
		},
		func(ctx context.Context, c raftkvv1.ClusterClient, h *raftkvv1.RequestHeader) (*raftkvv1.PromoteLearnerResponse, error) {
			r := proto.CloneOf(req)
			r.Header = h
			return c.PromoteLearner(ctx, r)
		})
}

// RemoveMember implements raftkvv1.ClusterServer (WBS 6.4.5). Removing the
// leader is allowed: it hands over once its removal commits (WBS 6.1.5).
func (s *Cluster) RemoveMember(ctx context.Context, req *raftkvv1.RemoveMemberRequest) (*raftkvv1.RemoveMemberResponse, error) {
	a, err := s.admin()
	if err != nil {
		return nil, err
	}
	return route(ctx, s, a, req.GetHeader(),
		func(ctx context.Context) (*raftkvv1.RemoveMemberResponse, error) {
			if err := a.RemoveMember(ctx, req.GetMemberId(), req.GetForce()); err != nil {
				return nil, s.mapAdminError(a, err)
			}
			return &raftkvv1.RemoveMemberResponse{Header: s.header(a), Members: a.Members().GetMembers()}, nil
		},
		func(ctx context.Context, c raftkvv1.ClusterClient, h *raftkvv1.RequestHeader) (*raftkvv1.RemoveMemberResponse, error) {
			r := proto.CloneOf(req)
			r.Header = h
			return c.RemoveMember(ctx, r)
		})
}

// TransferLeadership implements raftkvv1.ClusterServer (WBS 6.2.1, 6.4.5).
func (s *Cluster) TransferLeadership(ctx context.Context, req *raftkvv1.TransferLeadershipRequest) (*raftkvv1.TransferLeadershipResponse, error) {
	a, err := s.admin()
	if err != nil {
		return nil, err
	}
	if t := req.GetTargetMemberId(); t != 0 && t == a.Status().Lead {
		st := a.Status()
		return &raftkvv1.TransferLeadershipResponse{Header: s.header(a), NewLeaderId: t, NewTerm: st.Term}, nil // already leads
	}
	return route(ctx, s, a, req.GetHeader(),
		func(ctx context.Context) (*raftkvv1.TransferLeadershipResponse, error) {
			leader, term, err := a.TransferLeadership(ctx, req.GetTargetMemberId())
			if err != nil {
				return nil, s.mapAdminError(a, err)
			}
			return &raftkvv1.TransferLeadershipResponse{Header: s.header(a), NewLeaderId: leader, NewTerm: term}, nil
		},
		func(ctx context.Context, c raftkvv1.ClusterClient, h *raftkvv1.RequestHeader) (*raftkvv1.TransferLeadershipResponse, error) {
			r := proto.CloneOf(req)
			r.Header = h
			return c.TransferLeadership(ctx, r)
		})
}

// ListMembers implements raftkvv1.ClusterServer (WBS 6.4.5): the applied
// member table, after a linearizable read barrier unless STALE was asked for.
func (s *Cluster) ListMembers(ctx context.Context, req *raftkvv1.ListMembersRequest) (*raftkvv1.ListMembersResponse, error) {
	a, err := s.admin()
	if err != nil {
		return nil, err
	}
	if id := req.GetHeader().GetClusterId(); id != 0 && id != a.ClusterID() {
		return nil, newError(codes.FailedPrecondition, ReasonClusterMismatch, "", "cluster ID mismatch", nil)
	}
	var table *raftkvv1.ClusterConfig
	read := func() error { table = a.Members(); return nil }
	if req.GetConsistency() == raftkvv1.Consistency_STALE {
		_ = read()
	} else if err := a.LinearizableRead(ctx, read); err != nil {
		return nil, mapReadError(err)
	}
	return &raftkvv1.ListMembersResponse{Header: s.header(a), Members: table.GetMembers(), ConfigIndex: table.GetConfigIndex()}, nil
}
