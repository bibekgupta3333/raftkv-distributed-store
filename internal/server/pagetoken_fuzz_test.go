package server

import (
	"bytes"
	"testing"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

// WBS 7.5.1: a Range page token comes back from the client as opaque bytes.
// rangeOptions must never panic on any request, an accepted token must be
// exactly what encodePageToken would produce for that range and resume key,
// and the resumed page must stay inside the original range (api-design 3.1).
func FuzzRangePageToken(f *testing.F) {
	type seed struct {
		start, end, prefix []byte
		flags              byte
		resume             []byte
	}
	for _, s := range []seed{
		{start: []byte("a"), end: []byte("z"), resume: []byte("m\x00")},
		{prefix: []byte("p/"), resume: []byte("p/7\x00")},
		{prefix: []byte("p/"), flags: 2, resume: []byte("p/7")},
		{start: []byte("a"), flags: 3, resume: []byte("b")},
		{prefix: []byte{0xff, 0xff}, resume: []byte{0xff, 0xff, 0x01}},
	} {
		req := fuzzRangeRequest(s.start, s.end, s.prefix, s.flags, nil)
		f.Add(s.start, s.end, s.prefix, s.flags, encodePageToken(req, s.resume))
	}
	f.Add([]byte(nil), []byte(nil), []byte("p/"), byte(0), []byte("garbage"))
	f.Fuzz(func(t *testing.T, start, end, prefix []byte, flags byte, tok []byte) {
		req := fuzzRangeRequest(start, end, prefix, flags, tok)
		o, err := rangeOptions(req)
		if err != nil || len(tok) == 0 {
			return
		}
		resume, ok := decodePageToken(req, tok)
		if !ok {
			t.Fatal("rangeOptions accepted a token decodePageToken rejects")
		}
		if !bytes.Equal(encodePageToken(req, resume), tok) {
			t.Fatalf("token %x does not round-trip (resume %x)", tok, resume)
		}
		lo, hi := start, end
		if len(prefix) > 0 {
			lo, hi = prefix, prefixEnd(prefix)
		}
		if bytes.Compare(o.Start, lo) < 0 || (len(hi) > 0 && (len(o.End) == 0 || bytes.Compare(o.End, hi) > 0)) {
			t.Fatalf("resumed range [%x, %x) escapes [%x, %x)", o.Start, o.End, lo, hi)
		}
	})
}

func fuzzRangeRequest(start, end, prefix []byte, flags byte, tok []byte) *raftkvv1.RangeRequest {
	return &raftkvv1.RangeRequest{StartKey: start, EndKey: end, Prefix: prefix,
		KeysOnly: flags&1 != 0, Reverse: flags&2 != 0, PageToken: tok}
}
