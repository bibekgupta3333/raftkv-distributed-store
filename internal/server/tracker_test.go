package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
)

func leaderStatus(term uint64) raft.Status {
	return raft.Status{State: raft.StateLeader, Term: term}
}

func follower(term uint64) raft.Status { return raft.Status{State: raft.StateFollower, Term: term} }

func mustRegister(t *testing.T, tr *Tracker, id uint64) *Waiter {
	t.Helper()
	w, err := tr.Register(id)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func waitFor(t *testing.T, w *Waiter) (storage.Result, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return w.Wait(ctx)
}

// WBS 3.2.2: a waiter is resolved by its request ID when the command applies.
func TestTrackerResolvesOnApply(t *testing.T) {
	tr := NewTracker()
	tr.OnStatus(leaderStatus(3))
	w := mustRegister(t, tr, 42)
	tr.Bind(w, 3)
	tr.Applied(7, storage.Result{Index: 1}) // someone else's command: ignored
	tr.Applied(42, storage.Result{Index: 9})
	res, err := waitFor(t, w)
	if err != nil || res.Index != 9 {
		t.Fatalf("res = %+v, err = %v", res, err)
	}
	if tr.Pending() != 0 {
		t.Fatalf("pending = %d", tr.Pending())
	}
}

// An apply that beats Bind (the entry applied before the handler resumed) is
// still delivered, because Register comes before Propose.
func TestTrackerApplyBeforeBind(t *testing.T) {
	tr := NewTracker()
	tr.OnStatus(leaderStatus(3))
	w := mustRegister(t, tr, 1)
	tr.Applied(1, storage.Result{Index: 5})
	tr.Bind(w, 3)
	if res, err := waitFor(t, w); err != nil || res.Index != 5 {
		t.Fatalf("res = %+v, err = %v", res, err)
	}
}

// A leader change fails bound waiters with a retryable error; unbound ones
// (not yet proposed) are left alone.
func TestTrackerFailsOnLeaderChange(t *testing.T) {
	tr := NewTracker()
	tr.OnStatus(leaderStatus(3))
	bound := mustRegister(t, tr, 1)
	tr.Bind(bound, 3)
	unbound := mustRegister(t, tr, 2)

	tr.OnStatus(follower(4))
	if _, err := waitFor(t, bound); !errors.Is(err, ErrLeaderChanged) {
		t.Fatalf("bound waiter err = %v, want ErrLeaderChanged", err)
	}
	if tr.Pending() != 1 {
		t.Fatalf("pending = %d, want the unbound waiter only", tr.Pending())
	}
	tr.Cancel(unbound)
	if tr.Pending() != 0 {
		t.Fatal("Cancel did not remove the waiter")
	}
}

// Losing and regaining leadership is a different term: still a failure.
func TestTrackerFailsOnTermChangeWhileStillLeader(t *testing.T) {
	tr := NewTracker()
	tr.OnStatus(leaderStatus(3))
	w := mustRegister(t, tr, 1)
	tr.Bind(w, 3)
	tr.OnStatus(follower(4))
	tr.OnStatus(leaderStatus(5))
	if _, err := waitFor(t, w); !errors.Is(err, ErrLeaderChanged) {
		t.Fatalf("err = %v", err)
	}
}

// Bind after the node already lost leadership fails immediately.
func TestTrackerBindAfterLeadershipLost(t *testing.T) {
	tr := NewTracker()
	tr.OnStatus(leaderStatus(3))
	w := mustRegister(t, tr, 1)
	tr.OnStatus(follower(4))
	tr.Bind(w, 3)
	if _, err := waitFor(t, w); !errors.Is(err, ErrLeaderChanged) {
		t.Fatalf("err = %v", err)
	}
}

func TestTrackerStopAndContext(t *testing.T) {
	tr := NewTracker()
	w := mustRegister(t, tr, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := w.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the caller's deadline", err)
	}
	tr.OnStop()
	if _, err := waitFor(t, w); !errors.Is(err, ErrTrackerClosed) {
		t.Fatalf("err = %v, want ErrTrackerClosed", err)
	}
	if _, err := tr.Register(2); !errors.Is(err, ErrTrackerClosed) {
		t.Fatalf("Register after stop = %v", err)
	}
}
