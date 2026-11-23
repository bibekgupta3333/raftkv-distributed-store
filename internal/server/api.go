package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/command"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
)

// RegisterSession implements raftkvv1.KVServer (WBS 4.3.1, D-26): a replicated
// entry whose log index becomes the client ID. It is routed like a write.
func (s *KV) RegisterSession(ctx context.Context, req *raftkvv1.RegisterSessionRequest) (*raftkvv1.RegisterSessionResponse, error) {
	if err := s.checkCluster(req.GetHeader()); err != nil {
		return nil, err
	}
	return routeWrite(ctx, s, req.GetHeader(), func(ctx context.Context) (*raftkvv1.RegisterSessionResponse, error) {
		res, err := s.run(ctx, command.RegisterSession(command.NewID(), s.stamp()))
		if err != nil {
			return nil, err
		}
		return &raftkvv1.RegisterSessionResponse{Header: s.header(), ClientId: res.ClientID}, nil
	}, func(ctx context.Context, kv raftkvv1.KVClient, h *raftkvv1.RequestHeader) (*raftkvv1.RegisterSessionResponse, error) {
		return kv.RegisterSession(ctx, &raftkvv1.RegisterSessionRequest{Header: h})
	})
}

// casOp converts the request's oneofs into the log operation.
func casOp(req *raftkvv1.CompareAndSwapRequest) *raftkvv1.CasOp {
	op := &raftkvv1.CasOp{Key: req.GetKey()}
	switch e := req.GetExpected().(type) {
	case *raftkvv1.CompareAndSwapRequest_ExpectedValue:
		op.Expected = &raftkvv1.CasOp_ExpectedValue{ExpectedValue: e.ExpectedValue}
	case *raftkvv1.CompareAndSwapRequest_ExpectedModRevision:
		op.Expected = &raftkvv1.CasOp_ExpectedModRevision{ExpectedModRevision: e.ExpectedModRevision}
	case *raftkvv1.CompareAndSwapRequest_ExpectAbsent:
		op.Expected = &raftkvv1.CasOp_ExpectAbsent{ExpectAbsent: e.ExpectAbsent}
	}
	switch a := req.GetAction().(type) {
	case *raftkvv1.CompareAndSwapRequest_NewValue:
		op.Action = &raftkvv1.CasOp_NewValue{NewValue: a.NewValue}
	case *raftkvv1.CompareAndSwapRequest_Delete:
		op.Action = &raftkvv1.CasOp_Delete{Delete: a.Delete}
	}
	return op
}

// CompareAndSwap implements raftkvv1.KVServer (WBS 4.4.2, api-design 3.1). The
// compare runs at apply time, serialized with every write. A failed compare is
// OK with succeeded=false and the current state of the key.
func (s *KV) CompareAndSwap(ctx context.Context, req *raftkvv1.CompareAndSwapRequest) (*raftkvv1.CompareAndSwapResponse, error) {
	if err := s.checkCluster(req.GetHeader()); err != nil {
		return nil, err
	}
	op := casOp(req)
	if err := command.ValidateCas(op); err != nil {
		return nil, mapValidation(err)
	}
	if err := command.ValidateSession(req.GetSession()); err != nil {
		return nil, mapValidation(err)
	}
	return routeWrite(ctx, s, req.GetHeader(), func(ctx context.Context) (*raftkvv1.CompareAndSwapResponse, error) {
		cmd, err := command.Cas(command.NewID(), s.stamp(), op)
		if err != nil {
			return nil, mapValidation(err)
		}
		cmd.Session = req.GetSession()
		res, err := s.run(ctx, cmd)
		if err != nil {
			return nil, err
		}
		resp := &raftkvv1.CompareAndSwapResponse{Header: s.header(), Succeeded: res.Succeeded,
			Revision: res.Index, Found: res.Found}
		if res.Found {
			resp.Current = toKeyValue(req.GetKey(), res.Value)
		}
		return resp, nil
	}, func(ctx context.Context, kv raftkvv1.KVClient, h *raftkvv1.RequestHeader) (*raftkvv1.CompareAndSwapResponse, error) {
		r := proto.CloneOf(req)
		r.Header = h
		return kv.CompareAndSwap(ctx, r)
	})
}

// Range implements raftkvv1.KVServer (WBS 4.4.3, D-34). Each page is read with
// one iterator after its own read point (ReadIndex on any member, like Get),
// so a page is linearizable; a multi-page scan is not one snapshot.
func (s *KV) Range(ctx context.Context, req *raftkvv1.RangeRequest) (*raftkvv1.RangeResponse, error) {
	if err := s.checkCluster(req.GetHeader()); err != nil {
		return nil, err
	}
	opts, err := rangeOptions(req)
	if err != nil {
		return nil, err
	}
	stale := req.GetConsistency() == raftkvv1.Consistency_STALE
	if !stale && req.GetMinRevision() != 0 {
		return nil, newError(codes.InvalidArgument, ReasonMalformed, "", "min_revision requires STALE consistency", nil)
	}
	var (
		rev  uint64
		kvs  []storage.KV
		more bool
	)
	// The page is read by one iterator inside the read callback, so it is a
	// snapshot at or after the read point: each page is linearizable (D-34).
	read := func() error {
		rev = s.reader.Applied()
		var err error
		if kvs, more, err = s.reader.Range(opts); err != nil {
			return &storageErr{err}
		}
		return nil
	}
	ctx, cancel := replyBefore(ctx)
	defer cancel()
	if stale {
		if err := s.member.WaitApplied(ctx, req.GetMinRevision()); err != nil {
			return nil, mapStaleWaitError(err)
		}
		if err := read(); err != nil {
			return nil, mapReadError(err)
		}
	} else if err := s.linearizableRead(ctx, read); err != nil { // same ReadIndex path as Get (D-05)
		return nil, mapReadError(err)
	}
	resp := &raftkvv1.RangeResponse{Header: s.header(), More: more}
	resp.Header.Revision = rev
	for _, kv := range kvs {
		resp.Kvs = append(resp.Kvs, toKeyValue(kv.Key, kv.Value))
	}
	if more {
		last := kvs[len(kvs)-1].Key
		resume := last // reverse: the next page ends (exclusive) at the last key
		if !req.GetReverse() {
			resume = append(append([]byte(nil), last...), 0) // smallest key after last
		}
		resp.NextPageToken = encodePageToken(req, resume)
	}
	return resp, nil
}

// rangeOptions validates a RangeRequest (D-11) and resolves prefix, bounds and
// page token into one page's storage options.
func rangeOptions(req *raftkvv1.RangeRequest) (storage.RangeOptions, error) {
	malformed := func(msg string) error { return newError(codes.InvalidArgument, ReasonMalformed, "", msg, nil) }
	for _, k := range [][]byte{req.GetStartKey(), req.GetEndKey(), req.GetPrefix()} {
		if len(k) > command.MaxKeySize {
			return storage.RangeOptions{}, newError(codes.InvalidArgument, ReasonKeyTooLarge, "", "range bound longer than 4096 bytes", nil)
		}
	}
	if req.GetLimit() > storage.MaxRangeLimit {
		return storage.RangeOptions{}, newError(codes.InvalidArgument, ReasonLimitTooLarge, "", "limit above 1000", nil)
	}
	o := storage.RangeOptions{Start: req.GetStartKey(), End: req.GetEndKey(), Limit: int(req.GetLimit()),
		KeysOnly: req.GetKeysOnly(), Reverse: req.GetReverse()}
	if o.Limit == 0 {
		o.Limit = storage.MaxRangeLimit
	}
	if len(req.GetPrefix()) > 0 {
		if len(o.Start) > 0 || len(o.End) > 0 {
			return storage.RangeOptions{}, malformed("prefix and start/end keys are exclusive")
		}
		o.Start, o.End = req.GetPrefix(), prefixEnd(req.GetPrefix())
	}
	if tok := req.GetPageToken(); len(tok) > 0 {
		resume, ok := decodePageToken(req, tok)
		if !ok {
			return storage.RangeOptions{}, newError(codes.InvalidArgument, ReasonBadPageToken, "", "page token does not match this range", nil)
		}
		if o.Reverse {
			o.End = resume
		} else {
			o.Start = resume
		}
	}
	return o, nil
}

// prefixEnd is the smallest key greater than every key with prefix p, or nil
// (to the end) if p is all 0xff bytes.
func prefixEnd(p []byte) []byte {
	end := bytes.Clone(p)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// A page token is: version (1 byte) | CRC-32C of the original range
// parameters (4 bytes LE) | resume key. The checksum ties the token to the
// range it came from (api-design 3.1), so a token reused with other bounds is
// INVALID_PAGE_TOKEN rather than a silently wrong scan.
const pageTokenVersion = 1

func rangeChecksum(req *raftkvv1.RangeRequest) uint32 {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	var n [4]byte
	for _, b := range [][]byte{req.GetStartKey(), req.GetEndKey(), req.GetPrefix()} {
		binary.LittleEndian.PutUint32(n[:], uint32(len(b))) //nolint:gosec // keys are <= 4096 bytes
		h.Write(n[:])
		h.Write(b)
	}
	var flags byte
	if req.GetKeysOnly() {
		flags |= 1
	}
	if req.GetReverse() {
		flags |= 2
	}
	h.Write([]byte{flags})
	return h.Sum32()
}

func encodePageToken(req *raftkvv1.RangeRequest, resume []byte) []byte {
	tok := make([]byte, 5, 5+len(resume))
	tok[0] = pageTokenVersion
	binary.LittleEndian.PutUint32(tok[1:], rangeChecksum(req))
	return append(tok, resume...)
}

func decodePageToken(req *raftkvv1.RangeRequest, tok []byte) ([]byte, bool) {
	if len(tok) < 6 || tok[0] != pageTokenVersion || binary.LittleEndian.Uint32(tok[1:]) != rangeChecksum(req) {
		return nil, false
	}
	resume := tok[5:]
	if len(resume) > command.MaxKeySize+1 {
		return nil, false
	}
	// The resume key must lie inside the original range.
	start, end := req.GetStartKey(), req.GetEndKey()
	if p := req.GetPrefix(); len(p) > 0 {
		start, end = p, prefixEnd(p)
	}
	if bytes.Compare(resume, start) < 0 || (len(end) > 0 && bytes.Compare(resume, end) > 0) {
		return nil, false
	}
	return resume, true
}
