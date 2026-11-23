package client

import (
	"context"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

// Compare is the condition of a CompareAndSwap (api-design 3.1).
type Compare struct {
	kind  int
	value []byte
	rev   uint64
}

// ValueEquals matches when the key exists with exactly value (an empty value
// means "exists with an empty value", not "absent").
func ValueEquals(value []byte) Compare { return Compare{kind: 1, value: value} }

// ModRevisionEquals matches when the key exists and was last modified at rev.
func ModRevisionEquals(rev uint64) Compare { return Compare{kind: 2, rev: rev} }

// Absent matches when the key does not exist (create-if-absent).
func Absent() Compare { return Compare{kind: 3} }

// Action is what a successful CompareAndSwap does.
type Action struct {
	del   bool
	value []byte
}

// SetValue sets the key to value.
func SetValue(value []byte) Action { return Action{value: value} }

// DeleteKey removes the key.
func DeleteKey() Action { return Action{del: true} }

// CASResult is the outcome of a CompareAndSwap. A failed compare is not an
// error: Succeeded is false and Current holds the conflicting state.
type CASResult struct {
	Succeeded bool
	// Revision is the index of the entry that evaluated the CAS.
	Revision uint64
	// Found and Current: the key after a successful swap, or the value that
	// made the compare fail.
	Found   bool
	Current *KeyValue
}

// CompareAndSwap atomically checks cmp against key and, if it holds, applies
// act. It is evaluated at apply time, serialized with every other write.
func (c *Client) CompareAndSwap(ctx context.Context, key []byte, cmp Compare, act Action) (*CASResult, error) {
	req := &raftkvv1.CompareAndSwapRequest{Key: key}
	switch cmp.kind {
	case 1:
		req.Expected = &raftkvv1.CompareAndSwapRequest_ExpectedValue{ExpectedValue: cmp.value}
	case 2:
		req.Expected = &raftkvv1.CompareAndSwapRequest_ExpectedModRevision{ExpectedModRevision: cmp.rev}
	case 3:
		req.Expected = &raftkvv1.CompareAndSwapRequest_ExpectAbsent{ExpectAbsent: true}
	}
	if act.del {
		req.Action = &raftkvv1.CompareAndSwapRequest_Delete{Delete: true}
	} else {
		req.Action = &raftkvv1.CompareAndSwapRequest_NewValue{NewValue: act.value}
	}
	var resp *raftkvv1.CompareAndSwapResponse
	err := c.write(ctx, func(ctx context.Context, kv raftkvv1.KVClient, ws *raftkvv1.WriteSession) (*raftkvv1.ResponseHeader, error) {
		r := &raftkvv1.CompareAndSwapRequest{Header: c.header(), Key: req.Key, Expected: req.Expected, Action: req.Action, Session: ws}
		var err error
		resp, err = kv.CompareAndSwap(ctx, r)
		return resp.GetHeader(), err
	})
	if err != nil {
		return nil, err
	}
	return &CASResult{Succeeded: resp.GetSucceeded(), Revision: resp.GetRevision(),
		Found: resp.GetFound(), Current: fromKV(resp.GetCurrent())}, nil
}

// RangeRequest selects one page of a scan. Use either Prefix or Start/End
// ([Start, End); empty End means to the last key).
type RangeRequest struct {
	Start, End, Prefix []byte
	// Limit is the page size, 1..1000; 0 means 1000 (D-11).
	Limit    int
	KeysOnly bool
	Reverse  bool
	// Stale serves the page from one member's local state; MinRevision then
	// waits until that member has applied the index.
	Stale       bool
	MinRevision uint64
	// PageToken continues a scan from RangePage.NextPageToken.
	PageToken []byte
}

// RangePage is one page of a scan. Each page is linearizable on its own (or
// stale, if asked); pages of one scan are not a single snapshot (D-34).
type RangePage struct {
	KVs           []*KeyValue
	More          bool
	NextPageToken []byte
	// Revision is the serving member's applied index.
	Revision uint64
}

// Range reads one page.
func (c *Client) Range(ctx context.Context, rr RangeRequest) (*RangePage, error) {
	req := &raftkvv1.RangeRequest{Header: c.header(), StartKey: rr.Start, EndKey: rr.End, Prefix: rr.Prefix,
		Limit: uint32(max(rr.Limit, 0)), KeysOnly: rr.KeysOnly, Reverse: rr.Reverse, PageToken: rr.PageToken} //nolint:gosec // bounded by the server
	if rr.Stale {
		req.Consistency, req.MinRevision = raftkvv1.Consistency_STALE, rr.MinRevision
	}
	var resp *raftkvv1.RangeResponse
	err := c.call(ctx, false, false, func(ctx context.Context, kv raftkvv1.KVClient) (*raftkvv1.ResponseHeader, error) {
		var err error
		resp, err = kv.Range(ctx, req)
		return resp.GetHeader(), err
	})
	if err != nil {
		return nil, err
	}
	page := &RangePage{More: resp.GetMore(), NextPageToken: resp.GetNextPageToken(), Revision: resp.GetHeader().GetRevision()}
	for _, kv := range resp.GetKvs() {
		page.KVs = append(page.KVs, fromKV(kv))
	}
	return page, nil
}

// RangeAll reads every page of rr in order and calls fn for each one, until
// the scan ends or fn returns false.
func (c *Client) RangeAll(ctx context.Context, rr RangeRequest, fn func(*RangePage) bool) error {
	for {
		page, err := c.Range(ctx, rr)
		if err != nil {
			return err
		}
		if !fn(page) || !page.More {
			return nil
		}
		rr.PageToken = page.NextPageToken
	}
}
