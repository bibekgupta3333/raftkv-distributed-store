package command

import (
	"testing"

	"google.golang.org/protobuf/proto"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

// WBS 7.5.1: the command envelope is decoded from every committed log entry
// (WAL on disk, AppendEntries from the network). Unmarshal must never panic,
// the validators must not panic on whatever it returns, and an accepted
// envelope must round-trip through Marshal unchanged.
func FuzzUnmarshal(f *testing.F) {
	put, _ := Put(11, 1234, []byte("k"), []byte("v"))
	del, _ := Delete(12, 1235, []byte("k"))
	get, _ := Get(13, 1236, []byte("k"))
	cas, _ := Cas(14, 1237, &raftkvv1.CasOp{
		Key:      []byte("k"),
		Expected: &raftkvv1.CasOp_ExpectedModRevision{ExpectedModRevision: 3},
		Action:   &raftkvv1.CasOp_NewValue{NewValue: []byte("w")},
	})
	for _, c := range []*raftkvv1.Command{put, del, get, cas, RegisterSession(15, 1238)} {
		b, err := proto.Marshal(c)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := Unmarshal(b)
		if err != nil {
			return
		}
		kind := KindOf(c)
		_ = kind.String()
		if op := c.GetCas(); op != nil {
			_ = ValidateCas(op)
		}
		_ = ValidateKey(c.GetPut().GetKey())
		enc, err := Marshal(c)
		if kind == KindUnknown {
			if err == nil {
				t.Fatal("a command without an operation marshalled")
			}
			return
		}
		if err != nil {
			t.Fatalf("decoded %v kind %v but it does not marshal: %v", c, kind, err)
		}
		back, err := Unmarshal(enc)
		if err != nil {
			t.Fatalf("re-encoded command no longer decodes: %v", err)
		}
		if !proto.Equal(back, c) || KindOf(back) != kind {
			t.Fatalf("round trip changed the command: %v -> %v", c, back)
		}
	})
}
