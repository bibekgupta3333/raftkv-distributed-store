package storage

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/command"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// genEntry is the deterministic workload every crash test applies: entry i is
// a function of i alone, so any prefix can be rebuilt as a reference.
func genEntry(i uint64) raft.Entry {
	r := rand.New(rand.NewPCG(i, 0x5eed))
	key := []byte(fmt.Sprintf("key-%02d", r.IntN(40)))
	e := raft.Entry{Index: i, Term: 1 + i/1000, Type: raft.EntryNormal}
	switch r.IntN(10) {
	case 0: // the leader's empty no-op
		return e
	case 1, 2:
		cmd, _ := command.Delete(i, 0, key)
		e.Data, _ = command.Marshal(cmd)
	case 3:
		cmd, _ := command.Get(i, 0, key)
		e.Data, _ = command.Marshal(cmd)
	default:
		val := make([]byte, r.IntN(64))
		for j := range val {
			val[j] = byte(r.IntN(256))
		}
		cmd, _ := command.Put(i, 0, key, val)
		e.Data, _ = command.Marshal(cmd)
	}
	return e
}

func genRange(lo, hi uint64) []raft.Entry {
	var out []raft.Entry
	for i := lo; i <= hi; i++ {
		out = append(out, genEntry(i))
	}
	return out
}

func openMem(t testing.TB, fs vfs.FS) *Store {
	t.Helper()
	s, err := Open(Options{Dir: "db", FS: fs})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

// applyAll feeds entries to a in batches of up to batch entries.
func applyAll(t testing.TB, a *Applier, entries []raft.Entry, batch int) {
	t.Helper()
	for len(entries) > 0 {
		n := min(batch, len(entries))
		if err := a.Apply(entries[:n]); err != nil {
			t.Fatalf("Apply: %v", err)
		}
		entries = entries[n:]
	}
}

// referenceHash applies entries 1..upTo to a fresh in-memory store and returns
// its state hash: the reference model the crash suites compare against.
func referenceHash(t testing.TB, upTo uint64) [32]byte {
	t.Helper()
	s := openMem(t, vfs.NewMem())
	defer func() { _ = s.Close() }()
	applyAll(t, NewApplier(s, nil), genRange(1, upTo), 16)
	_, h, err := s.StateHash()
	if err != nil {
		t.Fatal(err)
	}
	return h
}
