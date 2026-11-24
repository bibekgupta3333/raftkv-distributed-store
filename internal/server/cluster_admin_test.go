package server_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/kvtest"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// Cluster admin service integration tests (D-23, WBS 6.4.3-6.4.6, 6.2).

func reason(err error) string {
	st, _ := status.FromError(err)
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}

func wantAdminErr(t *testing.T, err error, code codes.Code, why string) {
	t.Helper()
	if status.Code(err) != code || reason(err) != why {
		t.Fatalf("err = %v (reason %q), want %v / %s", err, reason(err), code, why)
	}
}

func roles(members []*raftkvv1.Member) map[uint64]raftkvv1.MemberRole {
	out := map[uint64]raftkvv1.MemberRole{}
	for _, m := range members {
		out[m.GetId()] = m.GetRole()
	}
	return out
}

func followerOf(c *kvtest.Cluster, leader uint64) uint64 {
	for _, id := range c.IDs() {
		if id != leader {
			return id
		}
	}
	return 0
}

// WBS 6.4.3-6.4.5: through a follower (which forwards to the leader): add a
// learner (the transport learns its address at run time), see it catch up,
// promote it, list, transfer leadership to it, then remove it.
func TestClusterAdminLifecycle(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	leader := c.Leader(15 * time.Second)
	cc := clusterClient(t, c.Addr(followerOf(c, leader)))
	ctx := ctx5(t)
	putN(t, newClient(t, c), "pre", 20)

	addr := c.Reserve()
	add, err := cc.AddLearner(ctx, &raftkvv1.AddLearnerRequest{Name: "n4", PeerUrl: addr})
	if err != nil {
		t.Fatal(err)
	}
	id := add.GetMember().GetId()
	if id == 0 || slices.Contains([]uint64{1, 2, 3}, id) || add.GetMember().GetRole() != raftkvv1.MemberRole_LEARNER {
		t.Fatalf("AddLearner = %+v", add.GetMember())
	}
	if r := roles(add.GetMembers()); r[id] != raftkvv1.MemberRole_LEARNER || len(r) != 4 {
		t.Fatalf("members after AddLearner: %v", r)
	}
	// Idempotent: the same peer URL returns the same learner.
	again, err := cc.AddLearner(ctx, &raftkvv1.AddLearnerRequest{Name: "n4", PeerUrl: addr})
	if err != nil || again.GetMember().GetId() != id {
		t.Fatalf("second AddLearner: %+v, %v", again.GetMember(), err)
	}
	c.StartJoined(id, addr)
	waitFor(t, 15*time.Second, "the learner to catch up", func() bool {
		n := c.Node(id)
		return n != nil && n.Store.Applied() >= c.Node(leader).Store.Applied()
	})
	if _, err := cc.PromoteLearner(ctx, &raftkvv1.PromoteLearnerRequest{MemberId: id}); err != nil {
		t.Fatal(err)
	}
	list, err := cc.ListMembers(ctx, &raftkvv1.ListMembersRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if r := roles(list.GetMembers()); r[id] != raftkvv1.MemberRole_VOTER || len(r) != 4 || list.GetConfigIndex() == 0 {
		t.Fatalf("ListMembers = %v (config index %d)", r, list.GetConfigIndex())
	}
	// The new voter takes over.
	tr, err := cc.TransferLeadership(ctx, &raftkvv1.TransferLeadershipRequest{TargetMemberId: id})
	if err != nil {
		t.Fatal(err)
	}
	if tr.GetNewLeaderId() != id {
		t.Fatalf("TransferLeadership = %+v, want leader %d", tr, id)
	}
	if got := c.Leader(5 * time.Second); got != id {
		t.Fatalf("leader = %d, want %d", got, id)
	}
	putN(t, newClient(t, c), "after-transfer", 10)
	// Remove the old leader; its ID is gone for good (D-31, D-38).
	if _, err := cc.RemoveMember(ctx, &raftkvv1.RemoveMemberRequest{MemberId: leader}); err != nil {
		t.Fatal(err)
	}
	c.Forget(leader)
	list, err = clusterClient(t, c.Addr(id)).ListMembers(ctx, &raftkvv1.ListMembersRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if r := roles(list.GetMembers()); len(r) != 3 || r[leader] != 0 {
		t.Fatalf("members after RemoveMember: %v", r)
	}
	putN(t, newClient(t, c), "after-remove", 10)
	requireSameState(t, c)
}

// WBS 6.4.4: promoting a learner far behind the leader is refused with
// LEARNER_NOT_CAUGHT_UP; once it caught up it is accepted.
func TestPromoteRefusedWhileLagging(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	c.Leader(15 * time.Second)
	cl := newClient(t, c)
	addr := c.Reserve()
	cc := clusterClient(t, c.Addr(c.Leader(15*time.Second)))
	ctx := ctx5(t)
	add, err := cc.AddLearner(ctx, &raftkvv1.AddLearnerRequest{PeerUrl: addr})
	if err != nil {
		t.Fatal(err)
	}
	id := add.GetMember().GetId()
	// The learner is not running: write more than MaxPromoteLag entries.
	var wg sync.WaitGroup
	for w := range 16 {
		wg.Go(func() {
			wctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			for i := range node.MaxPromoteLag/16 + 10 {
				if _, err := cl.Put(wctx, fmt.Appendf(nil, "k%d", w), fmt.Appendf(nil, "%d", i)); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	_, err = cc.PromoteLearner(ctx, &raftkvv1.PromoteLearnerRequest{MemberId: id})
	wantAdminErr(t, err, codes.FailedPrecondition, "LEARNER_NOT_CAUGHT_UP")
	c.StartJoined(id, addr)
	waitFor(t, 30*time.Second, "the promotion", func() bool {
		_, err := cc.PromoteLearner(ctx, &raftkvv1.PromoteLearnerRequest{MemberId: id})
		return err == nil
	})
}

// WBS 6.4.6 (M6 exit criterion 5): a second change while one is pending is
// rejected with FAILED_PRECONDITION / CONF_CHANGE_IN_PROGRESS.
func TestSecondConfChangeRejectedWhilePending(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{Tick: 50 * time.Millisecond})
	leader := c.Leader(15 * time.Second)
	cc := clusterClient(t, c.Addr(leader))
	// Cut the leader off: its change can be appended but not committed.
	c.Partition([]uint64{leader})
	first := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_, err := cc.AddLearner(ctx, &raftkvv1.AddLearnerRequest{PeerUrl: "127.0.0.1:1"})
		first <- err
	}()
	var err error
	waitFor(t, 5*time.Second, "the first change to be pending", func() bool {
		st := c.Node(leader).Status()
		return st.ConfIndex > st.Commit
	})
	_, err = cc.AddLearner(ctx5(t), &raftkvv1.AddLearnerRequest{PeerUrl: "127.0.0.1:2"})
	wantAdminErr(t, err, codes.FailedPrecondition, "CONF_CHANGE_IN_PROGRESS")
	if err := <-first; status.Code(err) == codes.OK {
		t.Fatal("the first change committed without a quorum")
	}
	c.Heal()
}

// WBS 6.4.5: transfer and remove refusals.
func TestClusterAdminRefusals(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	leader := c.Leader(15 * time.Second)
	cc := clusterClient(t, c.Addr(leader))
	ctx := ctx5(t)
	_, err := cc.TransferLeadership(ctx, &raftkvv1.TransferLeadershipRequest{TargetMemberId: 999})
	wantAdminErr(t, err, codes.NotFound, "MEMBER_NOT_FOUND")
	_, err = cc.RemoveMember(ctx, &raftkvv1.RemoveMemberRequest{MemberId: 999})
	wantAdminErr(t, err, codes.NotFound, "MEMBER_NOT_FOUND")
	_, err = cc.PromoteLearner(ctx, &raftkvv1.PromoteLearnerRequest{MemberId: 999})
	wantAdminErr(t, err, codes.NotFound, "MEMBER_NOT_FOUND")
	_, err = cc.AddLearner(ctx, &raftkvv1.AddLearnerRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("AddLearner without a peer URL: %v", err)
	}
	// A voter's peer URL cannot be added as a learner.
	_, err = cc.AddLearner(ctx, &raftkvv1.AddLearnerRequest{PeerUrl: c.Node(leader).PeerAddr(followerOf(c, leader))})
	wantAdminErr(t, err, codes.AlreadyExists, "MEMBER_EXISTS")
	// A learner cannot lead.
	add, err := cc.AddLearner(ctx, &raftkvv1.AddLearnerRequest{PeerUrl: "127.0.0.1:9"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = cc.TransferLeadership(ctx, &raftkvv1.TransferLeadershipRequest{TargetMemberId: add.GetMember().GetId()})
	wantAdminErr(t, err, codes.FailedPrecondition, "TARGET_IS_LEARNER")
	// With one voter stopped, removing another would leave 1 of 2 live.
	f := followerOf(c, leader)
	c.Stop(f)
	var other uint64
	for _, id := range c.IDs() {
		if id != leader && id != f {
			other = id
		}
	}
	time.Sleep(300 * time.Millisecond) // > one election timeout: the leader has not heard from it since
	_, err = cc.RemoveMember(ctx, &raftkvv1.RemoveMemberRequest{MemberId: other})
	wantAdminErr(t, err, codes.FailedPrecondition, "WOULD_LOSE_QUORUM")
	c.Start(f)
}

// WBS 6.2.3 (in process): a leader that shuts down gracefully hands
// leadership over, so a new leader exists long before an election timeout.
func TestGracefulShutdownHandsLeadershipOver(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{Tick: 50 * time.Millisecond}) // elections: 500-1000 ms
	leader := c.Leader(15 * time.Second)
	term := c.Node(leader).Status().Term
	c.Stop(leader)
	start := time.Now()
	var others []uint64
	for _, id := range c.IDs() {
		if id != leader {
			others = append(others, id)
		}
	}
	var newLeader uint64
	for time.Since(start) < 5*time.Second && newLeader == 0 {
		for _, id := range others {
			if st := c.Node(id).Status(); st.State == raft.StateLeader {
				newLeader = id
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	took := time.Since(start)
	if newLeader == 0 {
		t.Fatal("no new leader")
	}
	if took > 300*time.Millisecond {
		t.Fatalf("a new leader took %v after the old one stopped; a handover takes a round trip", took)
	}
	if st := c.Node(newLeader).Status(); st.Term != term+1 {
		t.Fatalf("new term %d, want %d (one transfer election)", st.Term, term+1)
	}
	c.Start(leader)
}
