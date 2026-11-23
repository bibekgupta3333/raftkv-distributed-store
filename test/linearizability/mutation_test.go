package linearizability

import (
	"context"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/kvtest"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/server"
	"github.com/bibekgupta3333/raftkv-distributed-store/pkg/client"
)

// testing.md 3.4, WBS 4.6.1: "the checker must catch a deliberately broken
// build". A follower is cut off from the rest; the majority overwrites a key
// and a group of keys (put, CAS, delete); then Get and Range pages are sent to
// the isolated follower, and everything is checked with the KV model.
//
//   - Normal build: the follower's ReadIndex cannot reach the leader, so its
//     reads fail or time out (indeterminate reads are dropped) or, after the
//     heal, return the new values. The history must be linearizable.
//   - -tags raftkv_mutation: the follower serves "linearizable" reads from its
//     local, stale state (server.MutationLocalReads). The checker MUST report
//     a violation, for the Get and for the Range page on their own; otherwise
//     the checker is not able to catch the bug and the e2e runs prove nothing.
//
// Run the broken variant with:
//
//	go test -tags raftkv_mutation -run TestCheckerCatchesBrokenReadPath ./test/linearizability/
func TestCheckerCatchesBrokenReadPath(t *testing.T) {
	if testing.Short() {
		t.Skip("in-process cluster with a partition")
	}
	c := kvtest.New(t, 5, kvtest.Options{})
	lead := c.Leader(15 * time.Second)
	var follower uint64
	var rest []uint64
	for _, id := range c.IDs() {
		if id != lead && follower == 0 {
			follower = id
		} else {
			rest = append(rest, id)
		}
	}
	newClient := func(eps ...string) *client.Client {
		cl, err := client.New(client.Config{Endpoints: eps, AttemptTimeout: 500 * time.Millisecond,
			ConnectTimeout: 200 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cl.Close() })
		return cl
	}
	var majorityEps []string
	for _, id := range rest {
		majorityEps = append(majorityEps, c.Addr(id))
	}
	writer := newClient(majorityEps...)
	reader := newClient(c.Addr(follower))
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	type history struct {
		name string
		rec  *Recorder
	}
	recGet, recRange := NewRecorder(), NewRecorder()

	// Before the partition: k=v1 and the group g/ = {k0: a, k1: b}, all
	// applied on the follower (checked with a stale read there).
	p := recGet.Invoke(1, FPut, "k", "v1")
	_, err := writer.Put(ctx, []byte("k"), []byte("v1"))
	must(err)
	p.OK(false, "")
	for _, kv := range [][2]string{{"g/k0", "a"}, {"g/k1", "b"}} {
		p := recRange.Invoke(1, FPut, kv[0], kv[1])
		_, err := writer.Put(ctx, []byte(kv[0]), []byte(kv[1]))
		must(err)
		p.OK(false, "")
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		page, err := reader.Range(ctx, client.RangeRequest{Prefix: []byte("g/"), Stale: true})
		g, gerr := reader.Get(ctx, []byte("k"), client.WithStale(0))
		if err == nil && gerr == nil && len(page.KVs) == 2 && g.Found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("follower never applied the initial writes")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Cut the follower off; the majority moves on.
	c.Partition([]uint64{follower}, rest)
	t.Logf("isolated follower %d (leader %d)", follower, lead)
	p = recGet.Invoke(1, FPut, "k", "v2")
	_, err = writer.Put(ctx, []byte("k"), []byte("v2"))
	must(err)
	p.OK(false, "")
	cp := recRange.InvokeCAS(1, CASCall{Key: "g/k0", Cmp: CmpValue, Expect: "a", Value: "a2"})
	res, err := writer.CompareAndSwap(ctx, []byte("g/k0"), client.ValueEquals([]byte("a")), client.SetValue([]byte("a2")))
	must(err)
	cp.CASOK(res.Succeeded, res.Found, string(res.Current.Value))
	dp := recRange.Invoke(1, FDelete, "g/k1", "")
	dres, err := writer.Delete(ctx, []byte("g/k1"))
	must(err)
	dp.DeleteOK(dres.Deleted)

	// Linearizable reads on the isolated follower, started after the writes
	// above were acknowledged.
	readCtx := func() (context.Context, context.CancelFunc) { return context.WithTimeout(ctx, 1500*time.Millisecond) }
	rctx, cancel := readCtx()
	gp := recGet.Invoke(2, FGet, "k", "")
	if g, err := reader.Get(rctx, []byte("k")); err != nil {
		failed(gp, err, false)
	} else if g.Found {
		gp.OK(true, string(g.KV.Value))
	} else {
		gp.OK(false, "")
	}
	rp := recRange.InvokeRange(2, "g/", "g0")
	cancel()
	rctx, cancel = readCtx()
	if page, err := reader.Range(rctx, client.RangeRequest{Prefix: []byte("g/")}); err != nil {
		failed(rp, err, false)
	} else {
		var kvs []KVPair
		for _, kv := range page.KVs {
			kvs = append(kvs, KVPair{Key: string(kv.Key), Value: string(kv.Value)})
		}
		rp.RangeOK(kvs, page.More)
	}
	cancel()
	c.Heal()

	for _, h := range []history{{"get", recGet}, {"range", recRange}} {
		rep := CheckRecorder(h.rec, 10*time.Second)
		t.Logf("%s history: %s, verdict %s", h.name, rep.Stats, rep.Verdict)
		switch {
		case server.MutationLocalReads && rep.Verdict != Violation:
			t.Errorf("%s: the deliberately broken build (stale local reads) was NOT caught: verdict %s", h.name, rep.Verdict)
		case !server.MutationLocalReads && !rep.OK():
			t.Errorf("%s: normal build is not linearizable: %s", h.name, rep.Verdict)
		}
	}
}
