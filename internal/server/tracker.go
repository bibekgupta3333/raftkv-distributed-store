package server

import (
	"context"
	"errors"
	"sync"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
)

// Errors a [Waiter] can fail with.
var (
	// ErrLeaderChanged means leadership was lost (or the term changed) after
	// the command was proposed. The entry may still commit under the new
	// leader, so the outcome is INDETERMINATE and the error is retryable
	// (WBS 3.2.2; api-design 6.1 OUTCOME_UNKNOWN).
	ErrLeaderChanged = errors.New("server: leadership changed before the command was applied")
	// ErrTrackerClosed means the node is shutting down.
	ErrTrackerClosed = errors.New("server: node is shutting down")
)

// Tracker maps a command's request ID to the goroutine waiting for its result.
// A waiter is resolved when the command is applied ([Tracker.Applied], called by
// the applier after the batch commits) and failed with [ErrLeaderChanged] when
// the node stops leading the term the command was proposed in
// ([Tracker.OnStatus], called by the driver loop). It implements both
// storage.ResultSink and node.Observer.
//
// Waiters are keyed by request ID, not by log index: if the entry is replaced
// by another leader's entry the ID never shows up, and if it commits anyway the
// result still reaches the waiter if it is still registered.
type Tracker struct {
	mu      sync.Mutex
	waiters map[uint64]*Waiter

	known      bool   // a status has been observed
	leaderTerm uint64 // term we lead, 0 if we are not leader
	closed     bool
}

// NewTracker returns an empty Tracker.
func NewTracker() *Tracker { return &Tracker{waiters: make(map[uint64]*Waiter)} }

// Waiter is one in-flight proposal.
type Waiter struct {
	id   uint64
	term uint64 // 0 until bound to an accepted proposal
	done chan outcome
}

type outcome struct {
	res storage.Result
	err error
}

// Register adds a waiter for id. Call it before proposing: the entry can be
// applied before Propose returns to the caller.
func (t *Tracker) Register(id uint64) (*Waiter, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrTrackerClosed
	}
	w := &Waiter{id: id, done: make(chan outcome, 1)}
	t.waiters[id] = w
	return w, nil
}

// Bind records the term the proposal was accepted in. If the node already lost
// that leadership the waiter fails at once.
func (t *Tracker) Bind(w *Waiter, term uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, live := t.waiters[w.id]; !live || t.waiters[w.id] != w {
		return // already resolved
	}
	w.term = term
	if t.known && t.leaderTerm != term {
		t.failLocked(w, ErrLeaderChanged)
	}
}

// Cancel removes a waiter whose proposal was rejected (nothing was appended).
func (t *Tracker) Cancel(w *Waiter) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.waiters[w.id] == w {
		delete(t.waiters, w.id)
	}
}

// Wait blocks for the result, the caller's context, or a failure.
func (w *Waiter) Wait(ctx context.Context) (storage.Result, error) {
	select {
	case o := <-w.done:
		return o.res, o.err
	case <-ctx.Done():
		return storage.Result{}, ctx.Err()
	}
}

// Applied implements storage.ResultSink. Unknown IDs (entries proposed by
// other nodes, or by us before a restart) are ignored.
func (t *Tracker) Applied(id uint64, r storage.Result) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if w, ok := t.waiters[id]; ok {
		delete(t.waiters, id)
		w.done <- outcome{res: r}
	}
}

// OnStatus implements node.Observer: when we stop leading the term a waiter was
// bound to, fail it with a retryable error.
func (t *Tracker) OnStatus(st raft.Status) {
	var lead uint64
	if st.State == raft.StateLeader {
		lead = st.Term
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.known && lead == t.leaderTerm {
		return
	}
	t.known, t.leaderTerm = true, lead
	for _, w := range t.waiters {
		if w.term != 0 && w.term != lead {
			t.failLocked(w, ErrLeaderChanged)
		}
	}
}

// OnStop implements node.Observer: fail everything.
func (t *Tracker) OnStop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	for _, w := range t.waiters {
		t.failLocked(w, ErrTrackerClosed)
	}
}

// Pending is the number of in-flight waiters (tests).
func (t *Tracker) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.waiters)
}

func (t *Tracker) failLocked(w *Waiter, err error) {
	delete(t.waiters, w.id)
	w.done <- outcome{err: err}
}
