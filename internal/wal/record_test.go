package wal

import (
	"bytes"
	"testing"
)

func TestRecordRoundTrip(t *testing.T) {
	for _, payload := range [][]byte{nil, {1}, bytes.Repeat([]byte{7}, 8), bytes.Repeat([]byte{9}, 1000)} {
		in := Record{Type: RecordEntry, Term: 3, Index: 42, Payload: payload}
		b := in.Encode(nil)
		if len(b)%recordAlign != 0 || len(b) != in.EncodedLen() {
			t.Fatalf("bad encoded length %d", len(b))
		}
		out, n, err := DecodeRecord(b)
		if err != nil || n != len(b) {
			t.Fatalf("decode: n=%d err=%v", n, err)
		}
		if out.Type != in.Type || out.Term != in.Term || out.Index != in.Index || !bytes.Equal(out.Payload, in.Payload) {
			t.Fatalf("mismatch: %+v vs %+v", out, in)
		}
	}
}

func TestRecordDetectsEveryBitFlip(t *testing.T) {
	b := Record{Type: RecordEntry, Term: 1, Index: 2, Payload: []byte("hello")}.Encode(nil)
	for i := range b {
		for bit := 0; bit < 8; bit++ {
			c := append([]byte(nil), b...)
			c[i] ^= 1 << bit
			if _, _, err := DecodeRecord(c); err == nil {
				t.Fatalf("flip byte %d bit %d not detected", i, bit)
			}
		}
	}
}

func TestRecordRejectsTruncationAndOversize(t *testing.T) {
	b := Record{Type: RecordHardState, Payload: make([]byte, 8)}.Encode(nil)
	for n := 0; n < len(b); n++ {
		if _, _, err := DecodeRecord(b[:n]); err == nil {
			t.Fatalf("truncated to %d accepted", n)
		}
	}
	huge := make([]byte, recordHeaderSize)
	huge[3] = 0xff // length far above MaxPayload
	if _, _, err := DecodeRecord(huge); err == nil {
		t.Fatal("oversize accepted")
	}
}

// FuzzDecodeRecord checks the decoder never panics and that whatever it
// accepts re-encodes to exactly the bytes it consumed (no silent leniency).
func FuzzDecodeRecord(f *testing.F) {
	f.Add(Record{Type: RecordEntry, Term: 1, Index: 1, Payload: []byte("x")}.Encode(nil))
	f.Add(Record{Type: RecordHardState, Term: 5, Index: 4, Payload: make([]byte, 8)}.Encode(nil))
	f.Add([]byte{})
	f.Add(make([]byte, 64))
	f.Fuzz(func(t *testing.T, b []byte) {
		rec, n, err := DecodeRecord(b)
		if err != nil {
			return
		}
		if n > len(b) || n%recordAlign != 0 {
			t.Fatalf("bad consumed length %d", n)
		}
		if !bytes.Equal(rec.Encode(nil), b[:n]) {
			t.Fatal("re-encode differs from consumed bytes")
		}
	})
}
