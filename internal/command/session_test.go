package command

import (
	"bytes"
	"errors"
	"testing"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

func TestValidateCas(t *testing.T) {
	k := []byte("k")
	absent := &raftkvv1.CasOp_ExpectAbsent{ExpectAbsent: true}
	set := &raftkvv1.CasOp_NewValue{NewValue: []byte("v")}
	for name, tc := range map[string]struct {
		op   *raftkvv1.CasOp
		want error
	}{
		"ok":               {&raftkvv1.CasOp{Key: k, Expected: absent, Action: set}, nil},
		"no compare":       {&raftkvv1.CasOp{Key: k, Action: set}, ErrMalformed},
		"no action":        {&raftkvv1.CasOp{Key: k, Expected: absent}, ErrMalformed},
		"revision 0":       {&raftkvv1.CasOp{Key: k, Expected: &raftkvv1.CasOp_ExpectedModRevision{}, Action: set}, ErrMalformed},
		"absent false":     {&raftkvv1.CasOp{Key: k, Expected: &raftkvv1.CasOp_ExpectAbsent{}, Action: set}, ErrMalformed},
		"delete false":     {&raftkvv1.CasOp{Key: k, Expected: absent, Action: &raftkvv1.CasOp_Delete{}}, ErrMalformed},
		"empty key":        {&raftkvv1.CasOp{Expected: absent, Action: set}, ErrEmptyKey},
		"key at limit":     {&raftkvv1.CasOp{Key: bytes.Repeat(k, MaxKeySize), Expected: absent, Action: set}, nil},
		"key over limit":   {&raftkvv1.CasOp{Key: bytes.Repeat(k, MaxKeySize+1), Expected: absent, Action: set}, ErrKeyTooLarge},
		"value at limit":   {&raftkvv1.CasOp{Key: k, Expected: absent, Action: &raftkvv1.CasOp_NewValue{NewValue: make([]byte, MaxValueSize)}}, nil},
		"value over limit": {&raftkvv1.CasOp{Key: k, Expected: absent, Action: &raftkvv1.CasOp_NewValue{NewValue: make([]byte, MaxValueSize+1)}}, ErrValueTooLarge},
		"expected over":    {&raftkvv1.CasOp{Key: k, Expected: &raftkvv1.CasOp_ExpectedValue{ExpectedValue: make([]byte, MaxValueSize+1)}, Action: set}, ErrValueTooLarge},
	} {
		if err := ValidateCas(tc.op); !errors.Is(err, tc.want) || (tc.want == nil && err != nil) {
			t.Errorf("%s: err = %v, want %v", name, err, tc.want)
		}
	}
}

func TestValidateSession(t *testing.T) {
	for name, tc := range map[string]struct {
		ws   *raftkvv1.WriteSession
		want error
	}{
		"none":          {nil, nil},
		"client 0":      {&raftkvv1.WriteSession{Sequence: 9}, nil},
		"seq 0":         {&raftkvv1.WriteSession{ClientId: 1}, ErrMalformed},
		"ok":            {&raftkvv1.WriteSession{ClientId: 1, Sequence: 1}, nil},
		"acked >= seq":  {&raftkvv1.WriteSession{ClientId: 1, Sequence: 3, AckedSequence: 3}, ErrSequenceStale},
		"window edge":   {&raftkvv1.WriteSession{ClientId: 1, Sequence: 65, AckedSequence: 1}, nil},
		"window over":   {&raftkvv1.WriteSession{ClientId: 1, Sequence: 66, AckedSequence: 1}, ErrSessionWindow},
		"window from 0": {&raftkvv1.WriteSession{ClientId: 1, Sequence: 65}, ErrSessionWindow},
	} {
		if err := ValidateSession(tc.ws); !errors.Is(err, tc.want) || (tc.want == nil && err != nil) {
			t.Errorf("%s: err = %v, want %v", name, err, tc.want)
		}
	}
	for _, c := range []*raftkvv1.Command{RegisterSession(1, 2)} {
		back, err := Unmarshal(mustMarshal(t, c))
		if err != nil || KindOf(back) != KindOf(c) {
			t.Fatalf("round trip of %v: %v", KindOf(c), err)
		}
	}
}
