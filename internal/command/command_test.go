package command

import (
	"bytes"
	"errors"
	"testing"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

// WBS 3.2.1: every command kind survives marshal/unmarshal with its request ID,
// leader time and payload intact.
func TestEnvelopeRoundTrip(t *testing.T) {
	put, _ := Put(11, 1234, []byte("k"), []byte("v"))
	del, _ := Delete(12, 1235, []byte("k"))
	get, _ := Get(13, 1236, []byte("k"))
	for _, tc := range []struct {
		cmd  *raftkvv1.Command
		kind Kind
		id   uint64
		ms   int64
	}{{put, KindPut, 11, 1234}, {del, KindDelete, 12, 1235}, {get, KindGet, 13, 1236}} {
		data, err := Marshal(tc.cmd)
		if err != nil {
			t.Fatal(err)
		}
		back, err := Unmarshal(data)
		if err != nil {
			t.Fatal(err)
		}
		if KindOf(back) != tc.kind || back.GetRequestId() != tc.id || back.GetLeaderTimeUnixMs() != tc.ms {
			t.Fatalf("%v: round trip lost fields: %v", tc.kind, back)
		}
	}
	back, _ := Unmarshal(mustMarshal(t, put))
	if string(back.GetPut().GetKey()) != "k" || string(back.GetPut().GetValue()) != "v" {
		t.Fatalf("put payload = %v", back.GetPut())
	}
	if _, err := Marshal(&raftkvv1.Command{}); !errors.Is(err, ErrNoOp) {
		t.Fatalf("an empty command must not marshal: %v", err)
	}
	if _, err := Unmarshal([]byte{0xff, 0xff, 0xff}); err == nil {
		t.Fatal("garbage must not decode")
	}
}

func mustMarshal(t *testing.T, c *raftkvv1.Command) []byte {
	t.Helper()
	data, err := Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestValidation(t *testing.T) {
	if _, err := Put(1, 0, nil, nil); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key = %v", err)
	}
	if _, err := Delete(1, 0, bytes.Repeat([]byte("k"), MaxKeySize+1)); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("huge key = %v", err)
	}
	if _, err := Put(1, 0, []byte("k"), make([]byte, MaxValueSize+1)); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("huge value = %v", err)
	}
	if _, err := Put(1, 0, bytes.Repeat([]byte("k"), MaxKeySize), make([]byte, MaxValueSize)); err != nil {
		t.Fatalf("limits are inclusive: %v", err)
	}
	if a, b := NewID(), NewID(); a == 0 || a == b {
		t.Fatal("request IDs must be non-zero and (practically) unique")
	}
}
