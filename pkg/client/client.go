// Package client is the RaftKV Go client (api-design.md sections 6.2 and 7).
//
// It connects to every endpoint, caches the leader, follows NOT_LEADER hints,
// and retries with jittered exponential backoff inside the caller's deadline.
// Without sessions (M4, D-09) a write whose outcome is unknown (timeout,
// connection reset, leadership lost after proposing) is NOT retried by default,
// because a retry could apply it twice; the caller gets an indeterminate [Error].
package client

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

// Config configures a [Client].
type Config struct {
	// Endpoints are the host:port addresses of the members.
	Endpoints []string
	// ClusterID, if non-zero, is sent so a server of another cluster refuses.
	ClusterID uint64
	// DialOptions replace the default (plaintext) transport credentials.
	DialOptions []grpc.DialOption
	// DefaultTimeout bounds a call whose context has no deadline (10 s).
	DefaultTimeout time.Duration
	// AttemptTimeout bounds one RPC attempt (2 s) so a hung member (for
	// example a partitioned old leader) does not eat the whole budget.
	AttemptTimeout time.Duration
	// ConnectTimeout is how long to wait for a connection before trying the
	// next endpoint (300 ms). A request is never sent on an unready connection,
	// so this failure is always DEFINITE.
	ConnectTimeout time.Duration
	// MaxRedirects bounds consecutive hint redirects before backing off (3).
	MaxRedirects int
	// RetryIndeterminate retries writes whose outcome is unknown. Only safe for
	// idempotent use until sessions exist (M4); default false.
	RetryIndeterminate bool
	// Backoff returns the sleep before retry number attempt (0-based). The
	// default is rand(0, min(1 s, 25 ms * 2^attempt)) (api-design 6.2).
	Backoff func(attempt int) time.Duration
}

func (c *Config) setDefaults() {
	if c.DefaultTimeout <= 0 {
		c.DefaultTimeout = 10 * time.Second
	}
	if c.AttemptTimeout <= 0 {
		c.AttemptTimeout = 2 * time.Second
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = 300 * time.Millisecond
	}
	if c.MaxRedirects <= 0 {
		c.MaxRedirects = 3
	}
	if c.Backoff == nil {
		c.Backoff = defaultBackoff
	}
	if len(c.DialOptions) == 0 {
		c.DialOptions = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
}

func defaultBackoff(attempt int) time.Duration {
	limit := time.Second
	if attempt < 6 {
		limit = min(limit, 25*time.Millisecond<<attempt)
	}
	return rand.N(limit + 1)
}

type endpoint struct {
	addr     string
	conn     *grpc.ClientConn
	kv       raftkvv1.KVClient
	memberID atomic.Uint64 // learned from response headers
}

// Client talks to a RaftKV cluster. It is safe for concurrent use.
type Client struct {
	cfg Config
	eps []*endpoint

	mu     sync.Mutex
	leader int // index of the cached leader endpoint, -1 if unknown
	next   int // round-robin start when there is no cached leader
}

// New connects (lazily, all endpoints) and returns a Client.
func New(cfg Config) (*Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("client: no endpoints")
	}
	cfg.setDefaults()
	c := &Client{cfg: cfg, leader: -1}
	for _, addr := range cfg.Endpoints {
		conn, err := grpc.NewClient(addr, cfg.DialOptions...)
		if err != nil {
			_ = c.Close()
			return nil, err
		}
		conn.Connect()
		c.eps = append(c.eps, &endpoint{addr: addr, conn: conn, kv: raftkvv1.NewKVClient(conn)})
	}
	return c, nil
}

// Close closes every connection.
func (c *Client) Close() error {
	var err error
	for _, ep := range c.eps {
		err = errors.Join(err, ep.conn.Close())
	}
	return err
}

// Leader returns the cached leader's address ("" if unknown).
func (c *Client) Leader() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader < 0 {
		return ""
	}
	return c.eps[c.leader].addr
}

func (c *Client) start() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader >= 0 {
		return c.leader
	}
	i := c.next % len(c.eps)
	c.next++
	return i
}

func (c *Client) setLeader(i int) {
	c.mu.Lock()
	c.leader = i
	c.mu.Unlock()
}

func (c *Client) forget(i int) {
	c.mu.Lock()
	if c.leader == i {
		c.leader = -1
	}
	c.mu.Unlock()
}

// resolve maps a hint to an endpoint index: by member ID learned from earlier
// responses, then by address. -1 means unknown.
func (c *Client) resolve(h *raftkvv1.LeaderHint) int {
	if h == nil || h.GetLeaderId() == 0 {
		return -1
	}
	for i, ep := range c.eps {
		if ep.memberID.Load() == h.GetLeaderId() {
			return i
		}
	}
	for i, ep := range c.eps {
		if ep.addr == h.GetLeaderClientUrl() {
			return i
		}
	}
	return -1
}

// ready waits for ep's connection without sending anything. A connection that
// is not up cannot have received the request, so skipping it is always safe.
func (ep *endpoint) ready(ctx context.Context, timeout time.Duration) bool {
	state := ep.conn.GetState()
	if state == connectivity.Ready {
		return true
	}
	switch state {
	case connectivity.TransientFailure:
		ep.conn.ResetConnectBackoff()
	case connectivity.Idle:
		ep.conn.Connect()
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for state != connectivity.Ready {
		if !ep.conn.WaitForStateChange(wctx, state) {
			return false
		}
		state = ep.conn.GetState()
		if state == connectivity.Shutdown {
			return false
		}
	}
	return true
}

// call runs one logical request with leader following and retries. f performs
// one RPC and returns the responding member's ID (0 if unknown).
func (c *Client) call(ctx context.Context, write bool, f func(ctx context.Context, kv raftkvv1.KVClient) (member uint64, err error)) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.cfg.DefaultTimeout)
		defer cancel()
	}
	cur := c.start()
	var last *Error
	redirects, attempt := 0, 0
	backoff := func() bool {
		d := c.cfg.Backoff(attempt)
		attempt++
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
			return true
		case <-ctx.Done():
			return false
		}
	}
	finish := func(e *Error) error {
		e.Attempts = attempt + 1
		return e
	}
	for tries := 1; ; tries++ {
		if ctx.Err() != nil {
			return finish(c.expired(ctx, last))
		}
		ep := c.eps[cur]
		if !ep.ready(ctx, c.cfg.ConnectTimeout) {
			c.forget(cur)
			last = &Error{Code: codes.Unavailable, Outcome: OutcomeDefinite, Message: "endpoint " + ep.addr + " is not reachable"}
			cur = (cur + 1) % len(c.eps)
			if tries%len(c.eps) == 0 && !backoff() { // a full lap without a live member
				return finish(c.expired(ctx, last))
			}
			continue
		}
		actx, cancel := context.WithTimeout(ctx, c.cfg.AttemptTimeout)
		member, err := f(actx, ep.kv)
		cancel()
		if member != 0 {
			ep.memberID.Store(member)
		}
		if err == nil {
			if write {
				// Only a write proves leadership: any member serves
				// reads (D-05), so a read must not pin a follower.
				c.setLeader(cur)
			}
			return nil
		}
		last = fromRPC(err)
		switch {
		case last.Reason == reasonNotLeader && last.Hint != nil && c.resolve(last.Hint) >= 0 && redirects < c.cfg.MaxRedirects:
			c.forget(cur)
			cur = c.resolve(last.Hint)
			redirects++
			continue // follow the hint at once, no backoff
		case last.Reason == reasonNotLeader, last.Reason == reasonNoLeader, last.Reason == reasonNoQuorum,
			last.Reason == reasonLeaderNotReady, last.Reason == reasonNotProposed:
			c.forget(cur)
			redirects = 0
			cur = (cur + 1) % len(c.eps)
		case last.Reason == reasonOverloaded:
			// same member, just slower
		case last.Code == codes.InvalidArgument, last.Code == codes.FailedPrecondition,
			last.Code == codes.NotFound, last.Code == codes.AlreadyExists, last.Code == codes.Unimplemented:
			return finish(last)
		default:
			// OUTCOME_UNKNOWN, or no status at all (reset, attempt deadline).
			// A write may have been applied: never resend it unless asked.
			c.forget(cur)
			if write && last.Indeterminate() && !c.cfg.RetryIndeterminate {
				return finish(last)
			}
			cur = (cur + 1) % len(c.eps)
		}
		if !backoff() {
			return finish(c.expired(ctx, last))
		}
	}
}

// expired builds the error for an exhausted deadline, keeping the last
// server-side error if there was one.
func (c *Client) expired(ctx context.Context, last *Error) *Error {
	if last != nil {
		return last
	}
	code := codes.DeadlineExceeded
	if errors.Is(ctx.Err(), context.Canceled) {
		code = codes.Canceled
	}
	return &Error{Code: code, Outcome: OutcomeDefinite, Message: ctx.Err().Error()} // nothing was sent
}

func (c *Client) header() *raftkvv1.RequestHeader {
	return &raftkvv1.RequestHeader{ClusterId: c.cfg.ClusterID, WriteRouting: raftkvv1.WriteRouting_REDIRECT}
}

// KeyValue is a stored value with its revisions.
type KeyValue struct {
	Key, Value                           []byte
	CreateRevision, ModRevision, Version uint64
}

func fromKV(kv *raftkvv1.KeyValue) *KeyValue {
	if kv == nil {
		return nil
	}
	return &KeyValue{Key: kv.GetKey(), Value: kv.GetValue(),
		CreateRevision: kv.GetCreateRevision(), ModRevision: kv.GetModRevision(), Version: kv.GetVersion()}
}

// PutResult is the outcome of a successful Put.
type PutResult struct {
	Revision uint64
	Previous *KeyValue // set only with [WithPrevious] and an existing key
}

// DeleteResult is the outcome of a successful Delete.
type DeleteResult struct {
	Revision uint64
	Deleted  bool // false if the key did not exist
	Previous *KeyValue
}

// GetResult is the outcome of a successful Get.
type GetResult struct {
	Found bool
	KV    *KeyValue
	// Revision is the serving member's applied index.
	Revision uint64
}

type opts struct {
	previous    bool
	stale       bool
	minRevision uint64
}

// Option tunes one request.
type Option func(*opts)

// WithPrevious asks Put or Delete to return the previous value.
func WithPrevious() Option { return func(o *opts) { o.previous = true } }

// WithStale makes Get a STALE read from one member's local state (D-05); it
// may be old and is not linearizable. minRevision, if non-zero, waits until
// the member has applied that index.
func WithStale(minRevision uint64) Option {
	return func(o *opts) { o.stale, o.minRevision = true, minRevision }
}

func collect(os []Option) opts {
	var o opts
	for _, f := range os {
		f(&o)
	}
	return o
}

// Put sets key to value.
func (c *Client) Put(ctx context.Context, key, value []byte, os ...Option) (*PutResult, error) {
	o := collect(os)
	var resp *raftkvv1.PutResponse
	err := c.call(ctx, true, func(ctx context.Context, kv raftkvv1.KVClient) (uint64, error) {
		var err error
		resp, err = kv.Put(ctx, &raftkvv1.PutRequest{Header: c.header(), Key: key, Value: value, ReturnPrevious: o.previous})
		return resp.GetHeader().GetMemberId(), err
	})
	if err != nil {
		return nil, err
	}
	return &PutResult{Revision: resp.GetRevision(), Previous: fromKV(resp.GetPrevious())}, nil
}

// Delete removes key. Deleting a missing key succeeds with Deleted == false.
func (c *Client) Delete(ctx context.Context, key []byte, os ...Option) (*DeleteResult, error) {
	o := collect(os)
	var resp *raftkvv1.DeleteResponse
	err := c.call(ctx, true, func(ctx context.Context, kv raftkvv1.KVClient) (uint64, error) {
		var err error
		resp, err = kv.Delete(ctx, &raftkvv1.DeleteRequest{Header: c.header(), Key: key, ReturnPrevious: o.previous})
		return resp.GetHeader().GetMemberId(), err
	})
	if err != nil {
		return nil, err
	}
	return &DeleteResult{Revision: resp.GetRevision(), Deleted: resp.GetDeleted(), Previous: fromKV(resp.GetPrevious())}, nil
}

// Get reads key. By default the read is linearizable (ReadIndex, D-05) and may
// be served by any member, leader or follower; [WithStale] trades that for a
// purely local read. The result's Revision is the serving member's applied
// index.
func (c *Client) Get(ctx context.Context, key []byte, os ...Option) (*GetResult, error) {
	o := collect(os)
	req := &raftkvv1.GetRequest{Header: c.header(), Key: key, Consistency: raftkvv1.Consistency_LINEARIZABLE}
	if o.stale {
		req.Consistency, req.MinRevision = raftkvv1.Consistency_STALE, o.minRevision
	}
	var resp *raftkvv1.GetResponse
	err := c.call(ctx, false, func(ctx context.Context, kv raftkvv1.KVClient) (uint64, error) {
		var err error
		resp, err = kv.Get(ctx, req)
		return resp.GetHeader().GetMemberId(), err
	})
	if err != nil {
		return nil, err
	}
	return &GetResult{Found: resp.GetFound(), KV: fromKV(resp.GetKv()), Revision: resp.GetHeader().GetRevision()}, nil
}
