package client

import (
	"context"
	"errors"
	"sync"
	"time"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

// sessionWindow mirrors the server's limit on outstanding sequences
// (sequence - acked_sequence, api-design 3.2).
const sessionWindow = 64

// sessionState is the client half of exactly-once writes (D-09, api-design 5):
// one session per Client, one sequence per logical write, and acked = the
// highest S such that every sequence <= S has finished (with any outcome:
// the client never resends a finished sequence, so the server may drop its
// cached reply).
type sessionState struct {
	regMu sync.Mutex // serializes registration

	mu       sync.Mutex
	clientID uint64
	next     uint64          // last sequence handed out
	acked    uint64          // every sequence <= acked has finished
	finished map[uint64]bool // finished sequences above acked
}

func (s *sessionState) current() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clientID
}

func (s *sessionState) reset(clientID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clientID, s.next, s.acked, s.finished = clientID, 0, 0, map[uint64]bool{}
}

// expire forgets clientID (if still current) so the next write registers anew.
func (s *sessionState) expire(clientID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clientID == clientID {
		s.clientID = 0
	}
}

// SessionID returns the current session's client ID (0 if none yet).
func (c *Client) SessionID() uint64 { return c.sess.current() }

// RegisterSession creates a new session on the cluster and returns its client
// ID (D-26). It is safe to retry: an unused session simply expires. Writes
// register automatically; call this only to manage sessions by hand.
func (c *Client) RegisterSession(ctx context.Context) (uint64, error) {
	var resp *raftkvv1.RegisterSessionResponse
	err := c.call(ctx, true, true, func(ctx context.Context, kv raftkvv1.KVClient) (*raftkvv1.ResponseHeader, error) {
		var err error
		resp, err = kv.RegisterSession(ctx, &raftkvv1.RegisterSessionRequest{Header: c.header()})
		return resp.GetHeader(), err
	})
	if err != nil {
		return 0, err
	}
	return resp.GetClientId(), nil
}

// beginWrite returns the session fields for a new logical write, registering
// a session first if there is none, and a func to call when the write is done.
func (c *Client) beginWrite(ctx context.Context) (*raftkvv1.WriteSession, func(), error) {
	c.sess.regMu.Lock()
	if c.sess.current() == 0 {
		id, err := c.RegisterSession(ctx)
		if err != nil {
			c.sess.regMu.Unlock()
			var ce *Error
			if errors.As(err, &ce) {
				ce.Outcome = OutcomeDefinite // the write itself was never sent
			}
			return nil, nil, err
		}
		c.sess.reset(id)
	}
	c.sess.regMu.Unlock()

	s := &c.sess
	for {
		s.mu.Lock()
		if s.next+1-s.acked <= sessionWindow {
			break
		}
		s.mu.Unlock()
		// Too many writes in flight on this session: wait for one to finish.
		select {
		case <-ctx.Done():
			return nil, nil, &Error{Code: codeOf(ctx), Outcome: OutcomeDefinite, Message: "session window full: " + ctx.Err().Error()}
		case <-time.After(time.Millisecond):
		}
	}
	s.next++
	ws := &raftkvv1.WriteSession{ClientId: s.clientID, Sequence: s.next, AckedSequence: s.acked}
	s.mu.Unlock()
	done := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.clientID != ws.GetClientId() {
			return
		}
		s.finished[ws.GetSequence()] = true
		for s.finished[s.acked+1] {
			delete(s.finished, s.acked+1)
			s.acked++
		}
	}
	return ws, done, nil
}

// write runs one logical write. With a session, every retry reuses the same
// (client_id, sequence), so the write applies at most once (D-09). If the
// session expired and nothing of this write can have applied, it is retried
// on a fresh session; otherwise the error is reported as indeterminate.
func (c *Client) write(ctx context.Context, f func(context.Context, raftkvv1.KVClient, *raftkvv1.WriteSession) (*raftkvv1.ResponseHeader, error)) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.cfg.DefaultTimeout)
		defer cancel()
	}
	if c.cfg.DisableSessions {
		return c.call(ctx, true, false, func(ctx context.Context, kv raftkvv1.KVClient) (*raftkvv1.ResponseHeader, error) {
			return f(ctx, kv, nil)
		})
	}
	for {
		ws, done, err := c.beginWrite(ctx)
		if err != nil {
			return err
		}
		err = c.call(ctx, true, true, func(ctx context.Context, kv raftkvv1.KVClient) (*raftkvv1.ResponseHeader, error) {
			return f(ctx, kv, ws)
		})
		done()
		var ce *Error
		if errors.As(err, &ce) && ce.Reason == reasonSessionExpired {
			c.sess.expire(ws.GetClientId())
			if !ce.Indeterminate() && ctx.Err() == nil {
				continue // provably not applied: retry on a new session
			}
		}
		return err
	}
}
