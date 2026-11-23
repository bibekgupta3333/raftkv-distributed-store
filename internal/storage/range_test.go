package storage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/command"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

func keysOf(kvs []KV) string {
	var b bytes.Buffer
	for i, kv := range kvs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.Write(kv.Key)
	}
	return b.String()
}

// WBS 4.4.3 at the store: bounds, reverse, limit, keys-only, the 4 MiB page
// cut, and that sessions and metadata never leak into a scan.
func TestRange(t *testing.T) {
	s := openMem(t, vfs.NewMem())
	defer func() { _ = s.Close() }()
	a := NewApplier(s, nil)
	var ents []raft.Entry
	for i, k := range []string{"a", "b", "c", "d", "e"} {
		ents = append(ents, entryOf(uint64(i+1), put(0, k, "val-"+k)))
	}
	ents = append(ents, entryOf(6, command.RegisterSession(0, 1))) // an 's' key
	applyAll(t, a, ents, 3)

	for _, tc := range []struct {
		o    RangeOptions
		want string
		more bool
	}{
		{RangeOptions{}, "a,b,c,d,e", false},
		{RangeOptions{Start: []byte("b"), End: []byte("d")}, "b,c", false},
		{RangeOptions{Start: []byte("b"), Limit: 2}, "b,c", true},
		{RangeOptions{Reverse: true, Limit: 2}, "e,d", true},
		{RangeOptions{Reverse: true, End: []byte("c")}, "b,a", false},
		{RangeOptions{Start: []byte("x")}, "", false},
		{RangeOptions{Limit: 5}, "a,b,c,d,e", false},
		{RangeOptions{Limit: 4}, "a,b,c,d", true},
	} {
		kvs, more, err := s.Range(tc.o)
		if err != nil || keysOf(kvs) != tc.want || more != tc.more {
			t.Errorf("Range(%+v) = %q more=%v err=%v, want %q more=%v", tc.o, keysOf(kvs), more, err, tc.want, tc.more)
		}
	}
	kvs, _, _ := s.Range(RangeOptions{KeysOnly: true, Limit: 1})
	if len(kvs) != 1 || kvs[0].Value.Data != nil || kvs[0].Value.ModRevision != 1 {
		t.Fatalf("keys only = %+v", kvs)
	}
	// Byte cut: 3 values of 1.5 MiB cannot fit in 4 MiB; the page holds two.
	// A single value larger than the budget is still returned alone.
	big := bytes.Repeat([]byte("x"), 1_500_000)
	for i := range 3 {
		if err := a.Apply([]raft.Entry{entryOf(uint64(7+i), put(0, fmt.Sprint("big", i), string(big[:1_000_000])))}); err != nil {
			t.Fatal(err)
		}
	}
	kvs, more, err := s.Range(RangeOptions{Start: []byte("big"), End: []byte("bih"), MaxBytes: 2_500_000})
	if err != nil || len(kvs) != 2 || !more {
		t.Fatalf("byte-limited page: %d keys more=%v err=%v, want 2 and more", len(kvs), more, err)
	}
	kvs, more, _ = s.Range(RangeOptions{Start: []byte("big"), End: []byte("bih"), MaxBytes: 10})
	if len(kvs) != 1 || !more {
		t.Fatalf("a page always has at least one pair: %d keys", len(kvs))
	}
}

// WBS 4.4.2 at the state machine: every compare kind, both actions, and a
// failed compare that still advances appliedIndex but changes nothing.
func TestCompareAndSwapApply(t *testing.T) {
	s := openMem(t, vfs.NewMem())
	defer func() { _ = s.Close() }()
	rec := newRecorder()
	a := NewApplier(s, rec)
	var idx uint64
	cas := func(op *raftkvv1.CasOp) Result {
		t.Helper()
		idx++
		cmd, err := command.Cas(0, 0, op)
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Apply([]raft.Entry{entryOf(idx, cmd)}); err != nil {
			t.Fatal(err)
		}
		return rec.res[idx]
	}
	k := []byte("k")
	newV := func(v string) *raftkvv1.CasOp_NewValue { return &raftkvv1.CasOp_NewValue{NewValue: []byte(v)} }

	if r := cas(&raftkvv1.CasOp{Key: k, Expected: &raftkvv1.CasOp_ExpectAbsent{ExpectAbsent: true}, Action: newV("v1")}); !r.Succeeded || !r.Found || string(r.Value.Data) != "v1" {
		t.Fatalf("create-if-absent = %+v", r)
	}
	r := cas(&raftkvv1.CasOp{Key: k, Expected: &raftkvv1.CasOp_ExpectAbsent{ExpectAbsent: true}, Action: newV("v2")})
	if r.Succeeded || !r.Found || string(r.Value.Data) != "v1" || s.Applied() != 2 {
		t.Fatalf("second create-if-absent = %+v (must fail, report v1, still advance applied)", r)
	}
	if r := cas(&raftkvv1.CasOp{Key: k, Expected: &raftkvv1.CasOp_ExpectedValue{ExpectedValue: []byte("v1")}, Action: newV("v3")}); !r.Succeeded || r.Value.ModRevision != 3 || r.Value.Version != 2 {
		t.Fatalf("value compare = %+v", r)
	}
	if r := cas(&raftkvv1.CasOp{Key: k, Expected: &raftkvv1.CasOp_ExpectedModRevision{ExpectedModRevision: 1}, Action: newV("x")}); r.Succeeded || r.Value.ModRevision != 3 {
		t.Fatalf("stale revision compare = %+v", r)
	}
	if r := cas(&raftkvv1.CasOp{Key: k, Expected: &raftkvv1.CasOp_ExpectedModRevision{ExpectedModRevision: 3}, Action: &raftkvv1.CasOp_Delete{Delete: true}}); !r.Succeeded || r.Found {
		t.Fatalf("revision compare + delete = %+v", r)
	}
	if r := cas(&raftkvv1.CasOp{Key: k, Expected: &raftkvv1.CasOp_ExpectedValue{ExpectedValue: []byte{}}, Action: newV("y")}); r.Succeeded || r.Found {
		t.Fatalf("empty expected value on a missing key = %+v (empty value != absent)", r)
	}
	if _, found, _ := s.Get(k); found {
		t.Fatal("key exists after delete and a failed compare")
	}
}
