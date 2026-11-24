package storage

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/command"
)

// WBS 7.5.1: fuzz targets for the fixed-layout records the state machine reads
// back from Pebble (storage-design 3.1). Each must never panic and must
// re-encode what it accepts to the same bytes.

func FuzzDecodeValue(f *testing.F) {
	f.Add(Value{CreateRevision: 3, ModRevision: 9, Version: 2, Data: []byte("v")}.encode())
	f.Add(Value{CreateRevision: 1, ModRevision: 1, Version: 1}.encode())
	f.Add([]byte("short"))
	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := decodeValue(b)
		if err != nil {
			return
		}
		if !bytes.Equal(v.encode(), b) {
			t.Fatalf("value %x re-encodes as %x", b, v.encode())
		}
	})
}

func FuzzDecodeSession(f *testing.F) {
	f.Add(session{lastSeq: 7, acked: 5, lastActive: 1_700_000_000_000}.encode())
	f.Add(make([]byte, 23))
	f.Fuzz(func(t *testing.T, b []byte) {
		s, err := decodeSession(b)
		if err != nil {
			return
		}
		if !bytes.Equal(s.encode(), b) {
			t.Fatalf("session %x re-encodes as %x", b, s.encode())
		}
	})
}

// FuzzDecodeReply: a cached reply. encodeReply writes "no value" for a value
// with mod revision 0 (every stored value has one >= 1), so the property is
// that decoding is stable: decode(encode(decode(b))) == decode(b).
func FuzzDecodeReply(f *testing.F) {
	v := Value{CreateRevision: 2, ModRevision: 4, Version: 2, Data: []byte("new")}
	p := Value{CreateRevision: 2, ModRevision: 2, Version: 1, Data: []byte("old")}
	f.Add(encodeReply(Result{Kind: command.KindPut, Found: true, Index: 4, Value: v, Previous: p}))
	f.Add(encodeReply(Result{Kind: command.KindCas, Succeeded: false, Index: 5, Value: v}))
	f.Add(encodeReply(Result{Kind: command.KindDelete, Index: 6}))
	f.Add([]byte{1, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := decodeReply(b)
		if err != nil {
			return
		}
		back, err := decodeReply(encodeReply(r))
		if err != nil {
			t.Fatalf("re-encoded reply no longer decodes: %v", err)
		}
		norm := func(v Value) Value {
			if v.ModRevision == 0 {
				return Value{}
			}
			return v
		}
		r.Value, r.Previous = norm(r.Value), norm(r.Previous)
		if !reflect.DeepEqual(back, r) {
			t.Fatalf("round trip changed the reply: %+v -> %+v", r, back)
		}
	})
}
