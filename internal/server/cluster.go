package server

import (
	"context"
	"slices"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// StatusMember is the part of a node the Cluster service needs. *node.Node
// satisfies it.
type StatusMember interface {
	Status() raft.Status
	ID() uint64
	ClusterID() uint64
	FirstIndex() (uint64, error)
	// SnapshotIndex is the newest local snapshot's index (0 if none).
	SnapshotIndex() uint64
}

// Cluster implements raftkvv1.ClusterServer. Only Status is served; the
// membership RPCs (AddLearner, PromoteLearner, RemoveMember,
// TransferLeadership, ListMembers) answer UNIMPLEMENTED until M6.
type Cluster struct {
	raftkvv1.UnimplementedClusterServer
	member StatusMember
	reader Reader
}

// NewCluster returns the Cluster service.
func NewCluster(member StatusMember, reader Reader) *Cluster {
	return &Cluster{member: member, reader: reader}
}

// Register adds the service to a gRPC server.
func (s *Cluster) Register(r grpc.ServiceRegistrar) { raftkvv1.RegisterClusterServer(r, s) }

// Status implements raftkvv1.ClusterServer. It reports this member's local view
// only and is never forwarded (api-design.md section 3): it feeds the
// Kubernetes probes (D-46) and the observer UI (D-48), both of which must see
// each node as it is, including a partitioned or lagging one.
func (s *Cluster) Status(_ context.Context, req *raftkvv1.StatusRequest) (*raftkvv1.StatusResponse, error) {
	if id := req.GetHeader().GetClusterId(); id != 0 && id != s.member.ClusterID() {
		return nil, newError(codes.FailedPrecondition, ReasonClusterMismatch, "", "cluster ID mismatch", nil)
	}
	st := s.member.Status()
	applied := s.reader.Applied()
	resp := &raftkvv1.StatusResponse{
		Header: &raftkvv1.ResponseHeader{
			ClusterId: s.member.ClusterID(), MemberId: s.member.ID(),
			RaftTerm: st.Term, Revision: applied, LeaderId: st.Lead,
		},
		State:         stateToProto(st.State),
		LeaderId:      st.Lead,
		Term:          st.Term,
		CommitIndex:   st.Commit,
		AppliedIndex:  applied,
		LastLogIndex:  st.LastIndex,
		SnapshotIndex: s.member.SnapshotIndex(), // D-14
	}
	if first, err := s.member.FirstIndex(); err == nil {
		resp.FirstLogIndex = first
	}
	if st.State == raft.StateLeader {
		ids := make([]uint64, 0, len(st.Progress))
		for id := range st.Progress {
			if id != s.member.ID() {
				ids = append(ids, id)
			}
		}
		slices.Sort(ids)
		for _, id := range ids {
			pr := st.Progress[id]
			resp.Followers = append(resp.Followers, &raftkvv1.FollowerProgress{
				MemberId: id, MatchIndex: pr.Match, NextIndex: pr.Next, RecentActive: pr.RecentActive,
			})
		}
	}
	return resp, nil
}

func stateToProto(s raft.StateType) raftkvv1.RaftState {
	switch s {
	case raft.StateFollower:
		return raftkvv1.RaftState_FOLLOWER
	case raft.StatePreCandidate:
		return raftkvv1.RaftState_PRE_CANDIDATE
	case raft.StateCandidate:
		return raftkvv1.RaftState_CANDIDATE
	case raft.StateLeader:
		return raftkvv1.RaftState_LEADER
	default:
		return raftkvv1.RaftState_RAFT_STATE_UNSPECIFIED
	}
}
