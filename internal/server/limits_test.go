package server_test

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/kvtest"
)

// WBS 4.5.1 (D-11): every limit has a boundary test: at the limit passes, one
// over is rejected with INVALID_ARGUMENT before anything is proposed (DEFINITE,
// and the applied index does not move).
func TestLimitBoundaries(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	lead := c.Leader(15 * time.Second)
	kv := rawClient(t, c.Addr(lead))
	ctx := ctx5(t)
	keyAt, keyOver := bytes.Repeat([]byte("k"), 4096), bytes.Repeat([]byte("k"), 4097)
	valAt, valOver := make([]byte, 1<<20), make([]byte, 1<<20+1)
	absent := &raftkvv1.CompareAndSwapRequest_ExpectAbsent{ExpectAbsent: true}

	// At the limit.
	if _, err := kv.Put(ctx, &raftkvv1.PutRequest{Key: keyAt, Value: valAt}); err != nil {
		t.Fatalf("Put with a 4096-byte key and a 1 MiB value: %v", err)
	}
	if g, err := kv.Get(ctx, &raftkvv1.GetRequest{Key: keyAt}); err != nil || len(g.GetKv().GetValue()) != 1<<20 {
		t.Fatalf("Get at the limits: %v", err)
	}
	if _, err := kv.CompareAndSwap(ctx, &raftkvv1.CompareAndSwapRequest{Key: append(bytes.Clone(keyAt[:4095]), 'z'),
		Expected: absent, Action: &raftkvv1.CompareAndSwapRequest_NewValue{NewValue: valAt}}); err != nil {
		t.Fatalf("CAS at the limits: %v", err)
	}
	if _, err := kv.Delete(ctx, &raftkvv1.DeleteRequest{Key: keyAt}); err != nil {
		t.Fatalf("Delete at the key limit: %v", err)
	}

	// One over: rejected before propose.
	before := c.Node(lead).Store.Applied()
	over := []struct {
		name   string
		call   func() error
		reason string
	}{
		{"put key", func() error { _, err := kv.Put(ctx, &raftkvv1.PutRequest{Key: keyOver}); return err }, "KEY_TOO_LARGE"},
		{"put value", func() error {
			_, err := kv.Put(ctx, &raftkvv1.PutRequest{Key: []byte("k"), Value: valOver})
			return err
		}, "VALUE_TOO_LARGE"},
		{"delete key", func() error { _, err := kv.Delete(ctx, &raftkvv1.DeleteRequest{Key: keyOver}); return err }, "KEY_TOO_LARGE"},
		{"cas key", func() error {
			_, err := kv.CompareAndSwap(ctx, &raftkvv1.CompareAndSwapRequest{Key: keyOver, Expected: absent,
				Action: &raftkvv1.CompareAndSwapRequest_Delete{Delete: true}})
			return err
		}, "KEY_TOO_LARGE"},
		{"cas value", func() error {
			_, err := kv.CompareAndSwap(ctx, &raftkvv1.CompareAndSwapRequest{Key: []byte("k"), Expected: absent,
				Action: &raftkvv1.CompareAndSwapRequest_NewValue{NewValue: valOver}})
			return err
		}, "VALUE_TOO_LARGE"},
		{"get key", func() error { _, err := kv.Get(ctx, &raftkvv1.GetRequest{Key: keyOver}); return err }, "KEY_TOO_LARGE"},
		{"range bound", func() error { _, err := kv.Range(ctx, &raftkvv1.RangeRequest{StartKey: keyOver}); return err }, "KEY_TOO_LARGE"},
		{"range limit", func() error { _, err := kv.Range(ctx, &raftkvv1.RangeRequest{Limit: 1001}); return err }, "LIMIT_TOO_LARGE"},
	}
	for _, tc := range over {
		t.Run(tc.name, func(t *testing.T) {
			outcome := "DEFINITE"
			if strings.HasPrefix(tc.name, "range") { // reads carry no outcome
				outcome = ""
			}
			wantErr(t, tc.call(), codes.InvalidArgument, tc.reason, outcome)
		})
	}
	if after := c.Node(lead).Store.Applied(); after != before {
		t.Fatalf("rejected requests moved the applied index %d -> %d", before, after)
	}

	// Range page: 1000 keys pass in one page; 1001 keys need two pages.
	var wg sync.WaitGroup
	errs := make(chan error, 1001)
	for w := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := w; i < 1001; i += 16 {
				_, err := kv.Put(ctx, &raftkvv1.PutRequest{Key: []byte(fmt.Sprintf("r/%04d", i))})
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	p, err := kv.Range(ctx, &raftkvv1.RangeRequest{Prefix: []byte("r/"), Limit: 1000})
	if err != nil || len(p.GetKvs()) != 1000 || !p.GetMore() {
		t.Fatalf("limit 1000: %d keys more=%v err=%v", len(p.GetKvs()), p.GetMore(), err)
	}
	p, err = kv.Range(ctx, &raftkvv1.RangeRequest{Prefix: []byte("r/")}) // 0 = 1000
	if err != nil || len(p.GetKvs()) != 1000 {
		t.Fatalf("default limit: %d keys err=%v", len(p.GetKvs()), err)
	}

	// Range bytes: four 1 MiB values reach 4 MiB with their keys, so the page
	// is cut after three (at most 4 MiB, at least one pair).
	for i := range 4 {
		if _, err := kv.Put(ctx, &raftkvv1.PutRequest{Key: []byte(fmt.Sprint("big/", i)), Value: valAt}); err != nil {
			t.Fatal(err)
		}
	}
	p, err = kv.Range(ctx, &raftkvv1.RangeRequest{Prefix: []byte("big/")})
	if err != nil || len(p.GetKvs()) != 3 || !p.GetMore() {
		t.Fatalf("4 MiB page: %d keys more=%v err=%v, want 3 and more", len(p.GetKvs()), p.GetMore(), err)
	}
	p, err = kv.Range(ctx, &raftkvv1.RangeRequest{Prefix: []byte("big/"), PageToken: p.GetNextPageToken()})
	if err != nil || len(p.GetKvs()) != 1 || p.GetMore() {
		t.Fatalf("second page: %d keys more=%v err=%v", len(p.GetKvs()), p.GetMore(), err)
	}
}
