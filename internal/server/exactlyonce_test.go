package server_test

import (
	"context"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/kvtest"
	"github.com/bibekgupta3333/raftkv-distributed-store/pkg/client"
)

// lossy is a client interceptor that sends every write 1-3 times and drops
// the reply of some of them (the caller sees an unlabelled UNAVAILABLE, an
// INDETERMINATE outcome), so the client must retry writes that already
// applied.
type lossy struct {
	mu      sync.Mutex
	rng     *rand.Rand
	sends   atomic.Int64
	dropped atomic.Int64
}

func (l *lossy) intercept(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if !strings.HasSuffix(method, "/CompareAndSwap") && !strings.HasSuffix(method, "/Put") {
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	l.mu.Lock()
	n, drop := 1+l.rng.IntN(3), l.rng.IntN(10) < 4
	l.mu.Unlock()
	var err error
	for range n { // duplicates on the wire
		l.sends.Add(1)
		err = invoker(ctx, method, req, reply, cc, opts...)
	}
	if err == nil && drop {
		l.dropped.Add(1)
		return status.Error(codes.Unavailable, "test: reply dropped")
	}
	return err
}

// incrementAll runs workers that each increment a shared counter n times with
// a read-modify-write CAS loop, and returns how many increments the clients
// saw succeed. A CAS that applied but whose reply was lost looks like a
// failure to a client without sessions, so it increments again: the counter
// then exceeds the successes. With sessions the retry returns the cached
// success and the counter matches exactly.
func incrementAll(t *testing.T, c *kvtest.Cluster, key string, workers, n int, l *lossy, sessions bool) (successes int64) {
	t.Helper()
	var wg sync.WaitGroup
	var ok atomic.Int64
	for w := range workers {
		cl, err := client.New(client.Config{Endpoints: c.Endpoints(), DefaultTimeout: 20 * time.Second,
			DisableSessions: !sessions, RetryIndeterminate: !sessions,
			DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithUnaryInterceptor(l.intercept)}})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = cl.Close() }()
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for range n {
				for attempt := 0; ; attempt++ {
					if attempt > 200 {
						t.Errorf("worker %d: no progress", w)
						return
					}
					g, err := cl.Get(ctx, []byte(key))
					if err != nil {
						t.Errorf("Get: %v", err)
						return
					}
					cmp, cur := client.Absent(), 0
					if g.Found {
						cmp = client.ValueEquals(g.KV.Value)
						cur, _ = strconv.Atoi(string(g.KV.Value))
					}
					r, err := cl.CompareAndSwap(ctx, []byte(key), cmp, client.SetValue([]byte(strconv.Itoa(cur+1))))
					if err != nil {
						t.Errorf("CAS: %v", err)
						return
					}
					if r.Succeeded {
						ok.Add(1)
						break
					}
				}
			}
		}()
	}
	wg.Wait()
	return ok.Load()
}

func counter(t *testing.T, c *kvtest.Cluster, key string) int {
	t.Helper()
	cl := newClient(t, c)
	g, err := cl.Get(ctx5(t), []byte(key))
	if err != nil || !g.Found {
		t.Fatalf("read counter: %+v %v", g, err)
	}
	v, _ := strconv.Atoi(string(g.KV.Value))
	return v
}

// WBS 4.3.5 (D-09, M4 exit criterion 4): every write is sent 1-3 times and
// many replies are dropped, so the client retries writes that already applied;
// with sessions each (client_id, seq) applies exactly once, so the counter
// model shows 0 duplicates. A run without sessions over the same faults is
// logged for contrast.
func TestExactlyOnceUnderRetries(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	c.Leader(15 * time.Second)
	const workers, n = 4, 8

	l := &lossy{rng: rand.New(rand.NewPCG(1, 2))}
	got := incrementAll(t, c, "ctr", workers, n, l, true)
	final := counter(t, c, "ctr")
	t.Logf("sessions: %d increments, counter %d, %d sends, %d replies dropped", got, final, l.sends.Load(), l.dropped.Load())
	if got != workers*n || final != workers*n {
		t.Fatalf("counter = %d after %d successful increments (want %d): %d duplicate applies",
			final, got, workers*n, final-int(got))
	}
	if l.dropped.Load() == 0 || l.sends.Load() <= int64(workers*n) {
		t.Fatal("no retries were provoked; the test proves nothing")
	}

	nl := &lossy{rng: rand.New(rand.NewPCG(1, 2))}
	ngot := incrementAll(t, c, "ctr-nosession", 2, 4, nl, false)
	t.Logf("without sessions: %d increments, counter %d (duplicates %d)", ngot, counter(t, c, "ctr-nosession"),
		counter(t, c, "ctr-nosession")-int(ngot))

	// The replicas agree on everything, sessions included.
	hashes := c.StateHashes(10 * time.Second)
	var first [32]byte
	for id, h := range hashes {
		if first == ([32]byte{}) {
			first = h
		} else if h != first {
			t.Fatalf("node %d state differs", id)
		}
	}
}
