// Package server implements the client-facing KV gRPC service (api-design.md
// sections 2 and 6). In M3 every operation goes through the Raft log: writes
// and linearizable reads are proposed, and the handler waits for the command to
// be applied on this node (WBS 3.2.3, 3.2.4). A non-leader answers NOT_LEADER
// with a LeaderHint (REDIRECT behaviour; forwarding, D-28, comes with M4).
package server

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/command"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
)

// Member is the part of a node the KV service needs. *node.Node satisfies it.
type Member interface {
	Propose(ctx context.Context, data []byte) (index, term uint64, err error)
	Status() raft.Status
	ID() uint64
	ClusterID() uint64
	PeerAddr(id uint64) string
}

// Reader reads committed local state. *storage.Store satisfies it.
type Reader interface {
	Get(key []byte) (storage.Value, bool, error)
	Applied() uint64
}

// KV implements raftkvv1.KVServer for Put, Delete and Get. The other RPCs
// (sessions, CAS, Range) answer UNIMPLEMENTED until M4.
type KV struct {
	raftkvv1.UnimplementedKVServer
	member  Member
	reader  Reader
	tracker *Tracker
	now     func() time.Time
}

// New returns the KV service. tracker must be the same one registered as the
// applier's sink and the node's observer.
func New(member Member, reader Reader, tracker *Tracker) *KV {
	return &KV{member: member, reader: reader, tracker: tracker, now: time.Now}
}

// Register adds the service to a gRPC server.
func (s *KV) Register(r grpc.ServiceRegistrar) { raftkvv1.RegisterKVServer(r, s) }

func (s *KV) header() *raftkvv1.ResponseHeader {
	st := s.member.Status()
	return &raftkvv1.ResponseHeader{
		ClusterId: s.member.ClusterID(), MemberId: s.member.ID(),
		RaftTerm: st.Term, Revision: s.reader.Applied(), LeaderId: st.Lead,
	}
}

func (s *KV) hint(leader uint64) *raftkvv1.LeaderHint {
	st := s.member.Status()
	h := &raftkvv1.LeaderHint{LeaderId: leader, Term: st.Term}
	if leader != 0 {
		h.LeaderClientUrl = s.member.PeerAddr(leader) // client API shares the peer listener in M3
	}
	return h
}

func (s *KV) checkCluster(h *raftkvv1.RequestHeader) error {
	if id := h.GetClusterId(); id != 0 && id != s.member.ClusterID() {
		return newError(codes.FailedPrecondition, ReasonClusterMismatch, Definite, "cluster ID mismatch", nil)
	}
	return nil
}

// run proposes cmd and waits until this node has applied it. Order matters:
// the waiter is registered before the proposal so an apply that races Propose's
// return still finds it (WBS 3.2.2).
func (s *KV) run(ctx context.Context, cmd *raftkvv1.Command) (storage.Result, error) {
	data, err := command.Marshal(cmd)
	if err != nil {
		return storage.Result{}, mapValidation(err)
	}
	// Cheap pre-check so a follower answers without queueing a proposal.
	if st := s.member.Status(); st.State != raft.StateLeader && st.Lead != 0 {
		return storage.Result{}, s.mapProposeError(&raft.NotLeaderError{Leader: st.Lead})
	}
	w, err := s.tracker.Register(cmd.GetRequestId())
	if err != nil {
		return storage.Result{}, s.mapProposeError(err)
	}
	_, term, err := s.member.Propose(ctx, data)
	if err != nil {
		s.tracker.Cancel(w)
		return storage.Result{}, s.mapProposeError(err)
	}
	s.tracker.Bind(w, term)
	res, err := w.Wait(ctx)
	if err != nil {
		s.tracker.Cancel(w)
		return storage.Result{}, mapWaitError(err)
	}
	return res, nil
}

func toKeyValue(key []byte, v storage.Value) *raftkvv1.KeyValue {
	return &raftkvv1.KeyValue{Key: key, Value: v.Data,
		CreateRevision: v.CreateRevision, ModRevision: v.ModRevision, Version: v.Version}
}

// Put implements raftkvv1.KVServer (WBS 3.2.3).
func (s *KV) Put(ctx context.Context, req *raftkvv1.PutRequest) (*raftkvv1.PutResponse, error) {
	if err := s.checkCluster(req.GetHeader()); err != nil {
		return nil, err
	}
	cmd, err := command.Put(command.NewID(), s.now().UnixMilli(), req.GetKey(), req.GetValue()) // D-27: leader stamps time
	if err != nil {
		return nil, mapValidation(err)
	}
	cmd.GetPut().ReturnPrevious = req.GetReturnPrevious()
	res, err := s.run(ctx, cmd)
	if err != nil {
		return nil, err
	}
	resp := &raftkvv1.PutResponse{Header: s.header(), Revision: res.Index}
	if req.GetReturnPrevious() && res.Found {
		resp.Previous = toKeyValue(req.GetKey(), res.Previous)
	}
	return resp, nil
}

// Delete implements raftkvv1.KVServer (WBS 3.2.3).
func (s *KV) Delete(ctx context.Context, req *raftkvv1.DeleteRequest) (*raftkvv1.DeleteResponse, error) {
	if err := s.checkCluster(req.GetHeader()); err != nil {
		return nil, err
	}
	cmd, err := command.Delete(command.NewID(), s.now().UnixMilli(), req.GetKey())
	if err != nil {
		return nil, mapValidation(err)
	}
	cmd.GetDelete().ReturnPrevious = req.GetReturnPrevious()
	res, err := s.run(ctx, cmd)
	if err != nil {
		return nil, err
	}
	resp := &raftkvv1.DeleteResponse{Header: s.header(), Revision: res.Index, Deleted: res.Found}
	if req.GetReturnPrevious() && res.Found {
		resp.Previous = toKeyValue(req.GetKey(), res.Previous)
	}
	return resp, nil
}

// Get implements raftkvv1.KVServer. LINEARIZABLE reads go through the log in M3
// (WBS 3.2.4); ReadIndex replaces that in M4 (D-05). STALE reads are served
// from local state.
func (s *KV) Get(ctx context.Context, req *raftkvv1.GetRequest) (*raftkvv1.GetResponse, error) {
	if err := s.checkCluster(req.GetHeader()); err != nil {
		return nil, err
	}
	if req.GetConsistency() == raftkvv1.Consistency_STALE {
		return s.staleGet(ctx, req)
	}
	if req.GetMinRevision() != 0 {
		return nil, newError(codes.InvalidArgument, ReasonMalformed, "", "min_revision requires STALE consistency", nil)
	}
	cmd, err := command.Get(command.NewID(), s.now().UnixMilli(), req.GetKey())
	if err != nil {
		return nil, mapValidation(err)
	}
	res, err := s.run(ctx, cmd)
	if err != nil {
		return nil, err
	}
	resp := &raftkvv1.GetResponse{Header: s.header(), Found: res.Found}
	if res.Found {
		resp.Kv = toKeyValue(req.GetKey(), res.Value)
	}
	return resp, nil
}

func (s *KV) staleGet(ctx context.Context, req *raftkvv1.GetRequest) (*raftkvv1.GetResponse, error) {
	if len(req.GetKey()) == 0 {
		return nil, mapValidation(command.ErrEmptyKey)
	}
	for s.reader.Applied() < req.GetMinRevision() {
		select {
		case <-ctx.Done():
			return nil, newError(codes.DeadlineExceeded, ReasonRevisionMissing, "", "revision not reached", nil)
		case <-time.After(5 * time.Millisecond):
		}
	}
	v, found, err := s.reader.Get(req.GetKey())
	if err != nil {
		return nil, newError(codes.DataLoss, "STORAGE_CORRUPTION", "", err.Error(), nil)
	}
	resp := &raftkvv1.GetResponse{Header: s.header(), Found: found}
	if found {
		resp.Kv = toKeyValue(req.GetKey(), v)
	}
	return resp, nil
}
