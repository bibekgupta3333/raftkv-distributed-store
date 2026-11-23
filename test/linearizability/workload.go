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
}

// WorkloadConfig shapes a run.
type WorkloadConfig struct {
	Workers int           // concurrent clients (default 6)
	Keys    int           // size of the key space (default 4)
	OpTime  time.Duration // per-operation deadline (default 4 s)
	Seed    uint64
}

func (c *WorkloadConfig) defaults() {
	if c.Workers <= 0 {
		c.Workers = 6
	}
	if c.Keys <= 0 {
		c.Keys = 4
	}
	if c.OpTime <= 0 {
		c.OpTime = 4 * time.Second
	}
}

// Workload runs concurrent clients against a KV and records every operation.
// Each worker is a "proc" that runs one operation at a time; after an
// indeterminate write the worker continues under a fresh proc number, because
// that proc is no longer sequential (testing.md 3.2, standard Jepsen practice).
// Every written value is unique (v-<proc>-<op>), so a read pins down exactly
// which write it saw.
type Workload struct {
	rec *Recorder
	cfg WorkloadConfig

	nextProc atomic.Int64
	ops      atomic.Int64
}

// NewWorkload returns a workload that records into rec.
func NewWorkload(rec *Recorder, cfg WorkloadConfig) *Workload {
	cfg.defaults()
	return &Workload{rec: rec, cfg: cfg}
}

// Ops is how many operations have been issued.
func (w *Workload) Ops() int64 { return w.ops.Load() }

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

func (w *Workload) worker(ctx context.Context, id int, kv KV) {
	rng := rand.New(rand.NewPCG(w.cfg.Seed, uint64(id)+1))
	proc := id
	for ctx.Err() == nil {
		key := fmt.Sprintf("key-%d", rng.IntN(w.cfg.Keys))
		opID := w.ops.Add(1)
		octx, cancel := context.WithTimeout(context.Background(), w.cfg.OpTime) // finish started ops even after ctx ends
		var retire bool
		switch r := rng.IntN(10); {
		case r < 4:
			val := fmt.Sprintf("v-%d-%d", proc, opID)
			p := w.rec.Invoke(proc, FPut, key, val)
			_, err := kv.Put(octx, []byte(key), []byte(val))
			retire = settle(p, err, nil)
		case r < 5:
			p := w.rec.Invoke(proc, FDelete, key, "")
			_, err := kv.Delete(octx, []byte(key))
			retire = settle(p, err, nil)
		default:
			p := w.rec.Invoke(proc, FGet, key, "")
			res, err := kv.Get(octx, []byte(key))
			retire = settle(p, err, res)
		}
		cancel()
		if retire {
			proc = int(w.nextProc.Add(1))
		}
		time.Sleep(time.Duration(rng.IntN(5)) * time.Millisecond)
	}
}

// settle records the outcome (testing.md 3.2) and reports whether the proc
// must be retired.
func settle(p *Pending, err error, get *client.GetResult) (retire bool) {
	if err == nil {
		if get != nil {
			if get.Found {
				p.OK(true, string(get.KV.Value))
			} else {
				p.OK(false, "")
			}
		} else {
			p.OK(false, "")
		}
		return false
	}
	var ce *client.Error
	if errors.As(err, &ce) && !ce.Indeterminate() {
		p.Fail() // provably never proposed
		return false
	}
	p.Info()
	return true
}
