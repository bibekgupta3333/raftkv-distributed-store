package storage

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/command"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// flagSessionSeeds sizes the session property test (WBS 4.3.6). The M4 exit
// criterion is 10,000:
//
//	go test ./internal/storage/ -run TestSessionTableDeterministic -session-seeds=10000
var flagSessionSeeds = flag.Int("session-seeds", 1000, "seeds for TestSessionTableDeterministic")

// recorder is a ResultSink that keeps every result by request ID.
type recorder struct {
	mu  sync.Mutex
	res map[uint64]Result
	ids []uint64
}

func newRecorder() *recorder { return &recorder{res: map[uint64]Result{}} }

func (r *recorder) Applied(id uint64, res Result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.res[id] = res
	r.ids = append(r.ids, id)
}

const minute = int64(60_000)

func entryOf(index uint64, cmd *raftkvv1.Command) raft.Entry {
	cmd.RequestId = index
	data, err := command.Marshal(cmd)
	if err != nil {
		panic(err)
	}
	return raft.Entry{Index: index, Term: 1, Type: raft.EntryNormal, Data: data}
}

func withSession(cmd *raftkvv1.Command, cid, seq, acked uint64) *raftkvv1.Command {
	cmd.Session = &raftkvv1.WriteSession{ClientId: cid, Sequence: seq, AckedSequence: acked}
	return cmd
}

func put(t int64, k, v string) *raftkvv1.Command {
	c, err := command.Put(0, t, []byte(k), []byte(v))
	if err != nil {
		panic(err)
	}
	return c
}

// WBS 4.3.1 / 4.3.2 at the state machine: registration assigns the entry
// index; a retried (client_id, seq) returns the cached reply without
// re-executing; stale and out-of-window sequences and unknown sessions are
// refused without touching data; everything commits in the apply batch.
func TestSessionDedupOnApply(t *testing.T) {
	s := openMem(t, vfs.NewMem())
	defer func() { _ = s.Close() }()
	rec := newRecorder()
	a := NewApplier(s, rec)
	t0 := int64(1_700_000_000_000)
	apply := func(e raft.Entry) Result {
		t.Helper()
		if err := a.Apply([]raft.Entry{e}); err != nil {
			t.Fatal(err)
		}
		return rec.res[e.Index]
	}

	reg := apply(entryOf(1, command.RegisterSession(0, t0)))
	if reg.ClientID != 1 {
		t.Fatalf("client ID = %d, want the entry index 1", reg.ClientID)
	}
	if ok, _ := s.HasSession(1); !ok {
		t.Fatal("session not stored")
	}
	first := apply(entryOf(2, withSession(put(t0, "k", "v1"), 1, 1, 0)))
	if first.Duplicate || first.Err != nil || first.Index != 2 {
		t.Fatalf("first write = %+v", first)
	}
	// The same seq again (a retry after a lost reply), even with another value:
	// cached reply, nothing re-executed.
	dup := apply(entryOf(3, withSession(put(t0, "k", "OTHER"), 1, 1, 0)))
	if !dup.Duplicate || dup.Index != 2 {
		t.Fatalf("duplicate = %+v, want the cached reply of index 2", dup)
	}
	if v, _, _ := s.Get([]byte("k")); string(v.Data) != "v1" || v.Version != 1 {
		t.Fatalf("value = %q v%d; the duplicate was re-executed", v.Data, v.Version)
	}
	// Out-of-order seq inside the window runs once.
	if r := apply(entryOf(4, withSession(put(t0, "k", "v3"), 1, 3, 0))); r.Err != nil || r.Duplicate {
		t.Fatalf("seq 3 = %+v", r)
	}
	if r := apply(entryOf(5, withSession(put(t0, "k", "v2"), 1, 2, 0))); r.Err != nil || r.Duplicate {
		t.Fatalf("seq 2 (late) = %+v", r)
	}
	// acked=3 drops replies 1..3; a resend of 2 is now SEQUENCE_STALE.
	if r := apply(entryOf(6, withSession(put(t0, "k", "v4"), 1, 4, 3))); r.Err != nil {
		t.Fatalf("seq 4 = %+v", r)
	}
	if r := apply(entryOf(7, withSession(put(t0, "k", "x"), 1, 2, 3))); !errors.Is(r.Err, command.ErrSequenceStale) {
		t.Fatalf("stale seq = %+v", r)
	}
	if r := apply(entryOf(8, withSession(put(t0, "k", "x"), 1, 4+command.SessionWindow, 3))); !errors.Is(r.Err, command.ErrSessionWindow) {
		t.Fatalf("window = %+v", r)
	}
	if r := apply(entryOf(9, withSession(put(t0, "k", "x"), 77, 1, 0))); !errors.Is(r.Err, command.ErrSessionExpired) {
		t.Fatalf("unknown session = %+v", r)
	}
	if v, _, _ := s.Get([]byte("k")); string(v.Data) != "v4" {
		t.Fatalf("value = %q; a refused write changed data", v.Data)
	}
	// Cached replies keep what the response needs: CAS current value, Delete's
	// previous value when asked.
	cas, _ := command.Cas(0, t0, &raftkvv1.CasOp{Key: []byte("k"),
		Expected: &raftkvv1.CasOp_ExpectedValue{ExpectedValue: []byte("nope")},
		Action:   &raftkvv1.CasOp_NewValue{NewValue: []byte("z")}})
	c1 := apply(entryOf(10, withSession(cas, 1, 5, 4)))
	c2 := apply(entryOf(11, withSession(roundTrip(cas), 1, 5, 4)))
	if c1.Succeeded || !c2.Duplicate || c2.Succeeded || string(c2.Value.Data) != "v4" || !c2.Found || c2.Index != 10 {
		t.Fatalf("cas %+v, duplicate %+v", c1, c2)
	}
	del, _ := command.Delete(0, t0, []byte("k"))
	del.GetDelete().ReturnPrevious = true
	d1 := apply(entryOf(12, withSession(del, 1, 6, 5)))
	d2 := apply(entryOf(13, withSession(roundTrip(del), 1, 6, 5)))
	if !d2.Duplicate || !d2.Found || string(d2.Previous.Data) != string(d1.Previous.Data) {
		t.Fatalf("delete %+v, duplicate %+v", d1, d2)
	}
	if s.Applied() != 13 {
		t.Fatalf("applied = %d", s.Applied())
	}
}

func roundTrip(c *raftkvv1.Command) *raftkvv1.Command {
	b, _ := command.Marshal(c)
	out, _ := command.Unmarshal(b)
	return out
}

// WBS 4.3.3 at the state machine: sessions expire after 10 minutes of
// leader-stamped time, the clock never goes backwards when a later leader's
// clock is behind, and activity keeps a session alive.
func TestSessionExpiryUsesStampedTime(t *testing.T) {
	s := openMem(t, vfs.NewMem())
	defer func() { _ = s.Close() }()
	rec := newRecorder()
	a := NewApplier(s, rec)
	t0 := int64(1_700_000_000_000)
	var idx uint64
	apply := func(cmd *raftkvv1.Command) Result {
		t.Helper()
		idx++
		if err := a.Apply([]raft.Entry{entryOf(idx, cmd)}); err != nil {
			t.Fatal(err)
		}
		return rec.res[idx]
	}
	idle := apply(command.RegisterSession(0, t0)).ClientID
	busy := apply(command.RegisterSession(0, t0)).ClientID
	seq := uint64(0)
	for m := int64(3); m <= 12; m += 3 { // busy writes every 3 minutes
		seq++
		if r := apply(withSession(put(t0+m*minute, "k", "v"), busy, seq, seq-1)); r.Err != nil {
			t.Fatalf("busy write at +%dm: %v", m, r.Err)
		}
	}
	if ok, _ := s.HasSession(idle); ok {
		t.Fatal("idle session survived 12 minutes")
	}
	if ok, _ := s.HasSession(busy); !ok {
		t.Fatal("active session expired")
	}
	if r := apply(withSession(put(t0+12*minute, "k", "v"), idle, 1, 0)); !errors.Is(r.Err, command.ErrSessionExpired) {
		t.Fatalf("write on the expired session = %+v", r)
	}
	// A new leader whose clock is 20 minutes behind cannot rewind session time.
	before := s.SessionClock()
	apply(put(t0-8*minute, "x", "y"))
	if s.SessionClock() != before {
		t.Fatalf("session clock went backwards: %d -> %d", before, s.SessionClock())
	}
	// Exactly at the TTL the session lives; one millisecond later it is gone.
	last := int64(s.SessionClock())
	apply(put(last+command.SessionTTL.Milliseconds(), "x", "y"))
	if ok, _ := s.HasSession(busy); !ok {
		t.Fatal("session expired at exactly the TTL")
	}
	apply(put(last+command.SessionTTL.Milliseconds()+1, "x", "y"))
	if ok, _ := s.HasSession(busy); ok {
		t.Fatal("session outlived the TTL")
	}
	if n, _, _ := s.SessionHash(); n != 0 {
		t.Fatalf("%d sessions left", n)
	}
}

// genSessionLog builds a random command log that registers sessions, writes
// through them with retries, duplicates, stale and out-of-window sequences,
// unknown sessions, CAS and no-session writes, under several "leaders" whose
// clocks disagree by minutes, with idle gaps that cross the 10-minute TTL.
func genSessionLog(seed uint64, n int) []raft.Entry {
	r := rand.New(rand.NewPCG(seed, 0x5e55))
	base := int64(1_700_000_000_000)
	skew := int64(0)
	type cl struct{ id, next, acked uint64 }
	var clients []*cl
	var out []raft.Entry
	for i := 1; i <= n; i++ {
		idx := uint64(i)
		switch {
		case r.IntN(40) == 0: // leader change: new skew, a no-op entry
			skew = r.Int64N(4*minute) - 2*minute
			out = append(out, raft.Entry{Index: idx, Term: 1, Type: raft.EntryNormal})
			continue
		case r.IntN(80) == 0: // idle gap, often longer than the TTL
			base += r.Int64N(14 * minute)
		default:
			base += r.Int64N(5_000)
		}
		now := base + skew
		key := fmt.Sprintf("k%d", r.IntN(6))
		var cmd *raftkvv1.Command
		switch op := r.IntN(20); {
		case op < 3 || len(clients) == 0:
			cmd = command.RegisterSession(0, now)
			clients = append(clients, &cl{id: idx})
		case op < 4:
			cmd = put(now, key, fmt.Sprint("ns", i)) // no session
		case op < 5:
			cmd = withSession(put(now, key, "ghost"), uint64(n+r.IntN(5)+1), 1, 0) // unknown session
		default:
			c := clients[r.IntN(len(clients))]
			var seq uint64
			switch k := r.IntN(10); {
			case k < 2 && c.next > 0: // retry of a recent sequence
				seq = c.next - uint64(r.IntN(int(min(c.next, 3))))
			case k < 3: // far ahead: may exceed the window
				seq = c.next + uint64(r.IntN(2*command.SessionWindow))
			default:
				c.next++
				seq = c.next
			}
			if r.IntN(3) == 0 && c.acked+1 < c.next {
				c.acked += uint64(r.IntN(int(c.next - c.acked)))
			}
			if r.IntN(4) == 0 {
				cmd, _ = command.Cas(0, now, &raftkvv1.CasOp{Key: []byte(key),
					Expected: &raftkvv1.CasOp_ExpectAbsent{ExpectAbsent: true},
					Action:   &raftkvv1.CasOp_NewValue{NewValue: []byte(fmt.Sprint("c", i))}})
			} else if r.IntN(5) == 0 {
				cmd, _ = command.Delete(0, now, []byte(key))
			} else {
				cmd = put(now, key, fmt.Sprint("v", i))
			}
			withSession(cmd, c.id, seq, c.acked)
		}
		out = append(out, entryOf(idx, cmd))
	}
	return out
}

type sessionRun struct {
	sessHash, stateHash [32]byte
	sessions            int
	results             []Result
}

// applyReplica applies log in random batches, optionally closing and
// reopening the store (a clean restart) at a random point.
func applyReplica(t *testing.T, log []raft.Entry, r *rand.Rand, maxBatch int, restart bool) sessionRun {
	t.Helper()
	fs := vfs.NewMem()
	s := openMem(t, fs)
	rec := newRecorder()
	a := NewApplier(s, rec)
	restartAt := -1
	if restart {
		restartAt = r.IntN(len(log))
	}
	for pos := 0; pos < len(log); {
		if restartAt >= 0 && pos >= restartAt {
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openMem(t, fs)
			a = NewApplier(s, rec)
			restartAt = -1
		}
		n := min(1+r.IntN(maxBatch), len(log)-pos)
		if err := a.Apply(log[pos : pos+n]); err != nil {
			t.Fatal(err)
		}
		pos += n
	}
	var out sessionRun
	var err error
	if out.sessions, out.sessHash, err = s.SessionHash(); err != nil {
		t.Fatal(err)
	}
	if _, out.stateHash, err = s.StateHash(); err != nil {
		t.Fatal(err)
	}
	for _, id := range rec.ids {
		out.results = append(out.results, rec.res[id])
	}
	_ = s.Close()
	return out
}

// WBS 4.3.6: session expiry and dedup are deterministic. For every seed, three
// replicas apply the same log crossing the session TTL many times, with
// different batch boundaries and one clean restart, and must end with the
// identical session table hash, state hash and per-entry results; and no
// (client_id, sequence) ever executes twice.
//
// This is a property test over the apply path rather than a simulator run:
// the simulator (WBS 1.x) already proves every replica applies the same
// committed log, and apply is a pure function of that log, so replicas that
// agree here agree in any cluster schedule. -session-seeds=10000 is the exit
// criterion.
func TestSessionTableDeterministic(t *testing.T) {
	seeds := *flagSessionSeeds
	if testing.Short() {
		seeds = min(seeds, 50)
	}
	expiredSeen := 0
	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		log := genSessionLog(seed, 250)
		r := rand.New(rand.NewPCG(seed, 7))
		one := applyReplica(t, log, r, 1, false)
		many := applyReplica(t, log, r, 40, false)
		restarted := applyReplica(t, log, r, 15, true)
		for name, other := range map[string]sessionRun{"batched": many, "restarted": restarted} {
			if other.sessHash != one.sessHash || other.stateHash != one.stateHash || other.sessions != one.sessions {
				t.Fatalf("seed %d: %s replica diverged: sessions %d vs %d, session hash %x vs %x",
					seed, name, other.sessions, one.sessions, other.sessHash[:6], one.sessHash[:6])
			}
			if len(other.results) != len(one.results) {
				t.Fatalf("seed %d: %s replica produced %d results, want %d", seed, name, len(other.results), len(one.results))
			}
			for i := range one.results {
				a, b := one.results[i], other.results[i]
				if a.Index != b.Index || a.Duplicate != b.Duplicate || !errors.Is(b.Err, a.Err) ||
					a.Succeeded != b.Succeeded || a.ClientID != b.ClientID || !bytes.Equal(a.Value.Data, b.Value.Data) {
					t.Fatalf("seed %d: %s replica result %d differs: %+v vs %+v", seed, name, i, b, a)
				}
			}
		}
		// Exactly-once: each (client, seq) executes at most once.
		executed := map[[2]uint64]uint64{}
		for _, res := range one.results {
			if res.Duplicate {
				continue
			}
			c, _ := command.Unmarshal(log[res.Index-1].Data)
			ws := c.GetSession()
			if errors.Is(res.Err, command.ErrSessionExpired) && ws.GetClientId() <= uint64(len(log)) {
				expiredSeen++ // a real session that expired (not a made-up ID)
			}
			if ws.GetClientId() == 0 || res.Err != nil {
				continue
			}
			k := [2]uint64{ws.GetClientId(), ws.GetSequence()}
			if prev, ok := executed[k]; ok {
				t.Fatalf("seed %d: (client %d, seq %d) executed at %d and again at %d", seed, k[0], k[1], prev, res.Index)
			}
			executed[k] = res.Index
		}
	}
	if expiredSeen == 0 {
		t.Fatal("no write ever hit an expired session: the test does not cross the TTL")
	}
	t.Logf("%d seeds, %d writes refused on expired sessions", seeds, expiredSeen)
}
