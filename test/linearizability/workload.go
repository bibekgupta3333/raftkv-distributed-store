package linearizability

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/pkg/client"
)

// KV is the client surface the workload drives. *client.Client satisfies it.
type KV interface {
	Put(ctx context.Context, key, value []byte, opts ...client.Option) (*client.PutResult, error)
	Get(ctx context.Context, key []byte, opts ...client.Option) (*client.GetResult, error)
	Delete(ctx context.Context, key []byte, opts ...client.Option) (*client.DeleteResult, error)
	CompareAndSwap(ctx context.Context, key []byte, cmp client.Compare, act client.Action) (*client.CASResult, error)
	Range(ctx context.Context, rr client.RangeRequest) (*client.RangePage, error)
}

// WorkloadConfig shapes a run.
type WorkloadConfig struct {
	Workers int           // concurrent clients (default 6)
	Groups  int           // key groups (default 2)
	Keys    int           // keys per group, at most 10 (default 3)
	OpTime  time.Duration // per-operation deadline (default 4 s)
	Seed    uint64
}

func (c *WorkloadConfig) defaults() {
	if c.Workers <= 0 {
		c.Workers = 6
	}
	if c.Groups <= 0 {
		c.Groups = 2
	}
	if c.Keys <= 0 || c.Keys > 10 {
		c.Keys = 3
	}
	if c.OpTime <= 0 {
		c.OpTime = 4 * time.Second
	}
}

// Workload runs concurrent clients against a KV and records every operation:
// put, get, delete, CompareAndSwap (value, revision and absent compares; set
// and delete actions) and paged Range scans.
//
// Each worker is a "proc" that runs one operation at a time; after an
// indeterminate write the worker continues under a fresh proc number, because
// that proc is no longer sequential (testing.md 3.2, standard Jepsen practice).
// Every written value is unique (v-<proc>-<op>), so a read pins down exactly
// which write it saw.
//
// Keys are "e<epoch>/g<group>/k<n>". A range scan stays inside one group, so
// the model's partitions (testing.md 3.3) are one group each. [Workload.NextEpoch]
// moves every worker to fresh keys: operations of different epochs never share
// a partition, which bounds the checker's work on long runs the way cutting
// the history at quiet points would (testing.md 3.4).
type Workload struct {
	rec *Recorder
	cfg WorkloadConfig

	nextProc atomic.Int64
	ops      atomic.Int64
	epoch    atomic.Int64
}

// NewWorkload returns a workload that records into rec.
func NewWorkload(rec *Recorder, cfg WorkloadConfig) *Workload {
	cfg.defaults()
	return &Workload{rec: rec, cfg: cfg}
}

// Ops is how many operations have been issued.
func (w *Workload) Ops() int64 { return w.ops.Load() }

// NextEpoch switches every worker to a fresh set of keys and returns the new
// epoch number.
func (w *Workload) NextEpoch() int64 { return w.epoch.Add(1) }

// Run starts the workers (each with its own KV from mk) and blocks until ctx is
// done and every in-flight operation has finished.
func (w *Workload) Run(ctx context.Context, mk func(worker int) KV) {
	w.nextProc.Store(int64(w.cfg.Workers))
	var wg sync.WaitGroup
	for i := range w.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.worker(ctx, i, mk(i))
		}()
	}
	wg.Wait()
}

// observed is a worker's last successful view of a key, used to build CAS
// compares that sometimes match.
type observed struct {
	found bool
	value string
	rev   uint64
}

func (w *Workload) worker(ctx context.Context, id int, kv KV) {
	rng := rand.New(rand.NewPCG(w.cfg.Seed, uint64(id)+1))
	proc := id
	seen := map[string]observed{}
	note := func(key string, found bool, k *client.KeyValue) {
		if found && k != nil {
			seen[key] = observed{found: true, value: string(k.Value), rev: k.ModRevision}
		} else {
			seen[key] = observed{}
		}
	}
	for ctx.Err() == nil {
		epoch := w.epoch.Load()
		group := rng.IntN(w.cfg.Groups)
		prefix := fmt.Sprintf("e%d/g%d/", epoch, group)
		key := fmt.Sprintf("%sk%d", prefix, rng.IntN(w.cfg.Keys))
		opID := w.ops.Add(1)
		val := fmt.Sprintf("v-%d-%d", proc, opID)
		octx, cancel := context.WithTimeout(context.Background(), w.cfg.OpTime) // finish started ops even after ctx ends
		var retire bool
		switch r := rng.IntN(100); {
		case r < 25:
			p := w.rec.Invoke(proc, FPut, key, val)
			if _, err := kv.Put(octx, []byte(key), []byte(val)); err != nil {
				retire = failed(p, err, true)
			} else {
				p.OK(false, "")
			}
		case r < 32:
			p := w.rec.Invoke(proc, FDelete, key, "")
			if res, err := kv.Delete(octx, []byte(key)); err != nil {
				retire = failed(p, err, true)
			} else {
				p.DeleteOK(res.Deleted)
				note(key, false, nil)
			}
		case r < 52:
			retire = w.cas(octx, rng, proc, kv, key, val, seen[key], note)
		case r < 82:
			p := w.rec.Invoke(proc, FGet, key, "")
			if res, err := kv.Get(octx, []byte(key)); err != nil {
				failed(p, err, false)
			} else if res.Found {
				p.OK(true, string(res.KV.Value))
				note(key, true, res.KV)
			} else {
				p.OK(false, "")
				note(key, false, nil)
			}
		default:
			w.scan(rng, proc, kv, prefix, note)
		}
		cancel()
		if retire {
			proc = int(w.nextProc.Add(1))
		}
		time.Sleep(time.Duration(rng.IntN(5)) * time.Millisecond)
	}
}

// cas runs one CompareAndSwap. It reports whether the proc must be retired.
func (w *Workload) cas(ctx context.Context, rng *rand.Rand, proc int, kv KV, key, val string, o observed,
	note func(string, bool, *client.KeyValue)) bool {
	c := CASCall{Key: key, Value: val, Cmp: CmpAbsent, Del: rng.IntN(7) == 0}
	cmp := client.Absent()
	if o.found {
		switch r := rng.IntN(10); {
		case r < 5:
			c.Cmp, c.Expect, cmp = CmpValue, o.value, client.ValueEquals([]byte(o.value))
		case r < 8 && o.rev != 0:
			c.Cmp, c.Expect, c.Rev, cmp = CmpRev, o.value, o.rev, client.ModRevisionEquals(o.rev)
		}
	}
	act := client.SetValue([]byte(val))
	if c.Del {
		c.Value, act = "", client.DeleteKey()
	}
	p := w.rec.InvokeCAS(proc, c)
	res, err := kv.CompareAndSwap(ctx, []byte(key), cmp, act)
	if err != nil {
		return failed(p, err, true)
	}
	cur := ""
	if res.Found && res.Current != nil {
		cur = string(res.Current.Value)
	}
	p.CASOK(res.Succeeded, res.Found, cur)
	note(key, res.Found, res.Current)
	return false
}

// scan reads one group with a paged Range: either the whole group (prefix) or
// a [start, end) slice of it, with a small page size so scans take several
// pages. Each page is recorded as its own operation over the span it covers
// (D-34: each page is linearizable, a scan is not one snapshot).
func (w *Workload) scan(rng *rand.Rand, proc int, kv KV, prefix string, note func(string, bool, *client.KeyValue)) {
	keyAt := func(i int) string { return fmt.Sprintf("%sk%d", prefix, i) }
	rr := client.RangeRequest{Limit: 1 + rng.IntN(w.cfg.Keys+1)}
	start, end := prefix, prefixEnd(prefix)
	if rng.IntN(5) < 3 {
		rr.Prefix = []byte(prefix)
	} else {
		i := rng.IntN(w.cfg.Keys)
		j := i + 1 + rng.IntN(w.cfg.Keys-i)
		start = keyAt(i)
		if j < w.cfg.Keys {
			end = keyAt(j)
		}
		rr.Start, rr.End = []byte(start), []byte(end)
	}
	for range w.cfg.Keys + 2 {
		ctx, cancel := context.WithTimeout(context.Background(), w.cfg.OpTime)
		p := w.rec.InvokeRange(proc, start, end)
		page, err := kv.Range(ctx, rr)
		cancel()
		if err != nil {
			failed(p, err, false)
			return
		}
		kvs := make([]KVPair, 0, len(page.KVs))
		for _, k := range page.KVs {
			kvs = append(kvs, KVPair{Key: string(k.Key), Value: string(k.Value)})
			note(string(k.Key), true, k)
		}
		p.RangeOK(kvs, page.More)
		if !page.More || len(kvs) == 0 {
			return
		}
		start = kvs[len(kvs)-1].Key + "\x00"
		rr.PageToken = page.NextPageToken
	}
}

// PinnedReads sends writes through KV and reads (Get and Range pages) through
// Reads, typically a client with a single endpoint, so linearizable reads are
// served by that member whether it is the leader or a follower (D-05).
type PinnedReads struct {
	KV
	Reads KV
}

// Get reads through Reads.
func (p PinnedReads) Get(ctx context.Context, key []byte, opts ...client.Option) (*client.GetResult, error) {
	return p.Reads.Get(ctx, key, opts...)
}

// Range reads a page through Reads.
func (p PinnedReads) Range(ctx context.Context, rr client.RangeRequest) (*client.RangePage, error) {
	return p.Reads.Range(ctx, rr)
}

// prefixEnd is the smallest key after every key with prefix p (p is ASCII and
// never all 0xff in this workload).
func prefixEnd(p string) string {
	b := []byte(p)
	b[len(b)-1]++
	return string(b)
}

// failed records a failed operation (testing.md 3.2) and reports whether the
// proc must be retired: a write whose outcome is unknown may still take effect,
// so its proc is no longer sequential. An unknown read is simply dropped.
func failed(p *Pending, err error, write bool) (retire bool) {
	var ce *client.Error
	if errors.As(err, &ce) && !ce.Indeterminate() {
		p.Fail() // provably never proposed
		return false
	}
	p.Info()
	return write
}
