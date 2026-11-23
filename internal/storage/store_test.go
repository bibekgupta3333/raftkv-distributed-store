package storage

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"google.golang.org/protobuf/proto"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/command"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
)

// WBS 3.1.1: opens with the WAL off and closes cleanly; reopening keeps state.
func TestOpenCloseReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if s.Applied() != 0 {
		t.Fatal("fresh store must start at applied 0")
	}
	applyAll(t, NewApplier(s, nil), genRange(1, 50), 7)
	_, h1, _ := s.StateHash()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	if s2.Applied() != 50 || s2.DurableApplied() != 50 {
		t.Fatalf("reopened applied=%d durable=%d, want 50/50 (graceful Close flushes)", s2.Applied(), s2.DurableApplied())
	}
	if _, h2, _ := s2.StateHash(); h1 != h2 {
		t.Fatal("state changed across a clean restart")
	}
}

// The Pebble WAL must really be off (D-02, D-03).
func TestPebbleWALDisabled(t *testing.T) {
	// A store that never flushed loses its applies on a crash even if every
	// byte reached the file system: there is no Pebble WAL to replay.
	fs := vfs.NewCrashableMem()
	c := openMem(t, fs)
	applyAll(t, NewApplier(c, nil), genRange(1, 20), 5)
	clone := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 100, RNG: rand.New(rand.NewPCG(1, 1))})
	_ = c.db.Close()
	r := openMem(t, clone)
	defer func() { _ = r.Close() }()
	if r.Applied() != 0 {
		t.Fatalf("applied %d survived a crash with no flush; the Pebble WAL is not off", r.Applied())
	}
}

// WBS 3.1.2: schema version is written and enforced; key namespaces are disjoint.
func TestSchemaVersionAndKeyEncoding(t *testing.T) {
	fs := vfs.NewMem()
	s := openMem(t, fs)
	b := s.db.NewBatch()
	_ = b.Set(keySchemaVersion, putU64(99), nil)
	if err := s.db.Apply(b, pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	if err := s.db.Flush(); err != nil {
		t.Fatal(err)
	}
	_ = s.db.Close()
	if _, err := Open(Options{Dir: "db", FS: fs}); !errors.Is(err, ErrSchemaVersion) {
		t.Fatalf("Open with schema 99 = %v, want ErrSchemaVersion", err)
	}

	prefixes := map[byte]string{}
	for name, k := range map[string][]byte{
		"meta": keyAppliedIndex, "session": SessionKey(7), "expiry": ExpiryKey(1, 7), "user": UserKey([]byte("x")),
	} {
		if old, dup := prefixes[k[0]]; dup {
			t.Fatalf("%s and %s share prefix %q", name, old, k[0])
		}
		prefixes[k[0]] = name
	}
	if got, ok := ClientKey(UserKey([]byte("hello"))); !ok || string(got) != "hello" {
		t.Fatalf("ClientKey round trip = %q, %v", got, ok)
	}
	if _, ok := ClientKey(keyAppliedIndex); ok {
		t.Fatal("a metadata key must not decode as a client key")
	}
	if bytes.Compare(SessionKey(1), SessionKey(256)) >= 0 {
		t.Fatal("session keys must sort by client ID (big-endian)")
	}
	v := Value{CreateRevision: 3, ModRevision: 9, Version: 2, Data: []byte("data")}
	got, err := decodeValue(v.encode())
	if err != nil || got.CreateRevision != 3 || got.ModRevision != 9 || got.Version != 2 || string(got.Data) != "data" {
		t.Fatalf("value round trip = %+v, %v", got, err)
	}
	if _, err := decodeValue([]byte{1, 2}); err == nil {
		t.Fatal("short value must not decode")
	}
}

// WBS 3.1.3 / D-04: the batch carries appliedIndex; revisions follow the log index.
func TestApplyBatchIsAtomicWithAppliedIndex(t *testing.T) {
	s := openMem(t, vfs.NewMem())
	defer func() { _ = s.Close() }()
	a := NewApplier(s, nil)

	put := func(i uint64, k, v string) raft.Entry {
		c, _ := command.Put(i, 0, []byte(k), []byte(v))
		d, _ := command.Marshal(c)
		return raft.Entry{Index: i, Term: 2, Type: raft.EntryNormal, Data: d}
	}
	// no-op, put, overwrite of the same key within one batch
	if err := a.Apply([]raft.Entry{{Index: 1, Term: 2}, put(2, "k", "v1"), put(3, "k", "v2")}); err != nil {
		t.Fatal(err)
	}
	if s.Applied() != 3 || s.AppliedTerm() != 2 {
		t.Fatalf("applied = %d term %d, want 3/2", s.Applied(), s.AppliedTerm())
	}
	v, ok, err := s.Get([]byte("k"))
	if err != nil || !ok || string(v.Data) != "v2" || v.CreateRevision != 2 || v.ModRevision != 3 || v.Version != 2 {
		t.Fatalf("k = %+v ok=%v err=%v; want create 2 mod 3 version 2 (read-your-writes inside the batch)", v, ok, err)
	}

	// A failing entry aborts the whole batch: nothing and no appliedIndex.
	bad := raft.Entry{Index: 5, Term: 2, Type: raft.EntryNormal, Data: []byte{0xff, 0xff}}
	if err := a.Apply([]raft.Entry{put(4, "x", "y"), bad}); err == nil {
		t.Fatal("garbage command must fail the batch")
	}
	if s.Applied() != 3 {
		t.Fatalf("applied advanced to %d past a failed batch", s.Applied())
	}
	if _, ok, _ := s.Get([]byte("x")); ok {
		t.Fatal("a failed batch leaked data")
	}

	// Gaps and repeats are rejected (no double apply, no gap).
	if err := a.Apply([]raft.Entry{put(5, "x", "y")}); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("gap = %v, want ErrOutOfOrder", err)
	}
	if err := a.Apply([]raft.Entry{put(3, "x", "y")}); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("repeat = %v, want ErrOutOfOrder", err)
	}
}

type cmdT = *raftkvv1.Command

type sinkFunc func(uint64, Result)

func (f sinkFunc) Applied(id uint64, r Result) { f(id, r) }

func TestApplierResults(t *testing.T) {
	s := openMem(t, vfs.NewMem())
	defer func() { _ = s.Close() }()
	got := map[uint64]Result{}
	a := NewApplier(s, sinkFunc(func(id uint64, r Result) { got[id] = r }))
	mk := func(i uint64, c func() (cmdT, error)) raft.Entry {
		cmd, err := c()
		if err != nil {
			t.Fatal(err)
		}
		d, _ := command.Marshal(cmd)
		return raft.Entry{Index: i, Term: 1, Type: raft.EntryNormal, Data: d}
	}
	entries := []raft.Entry{
		mk(1, func() (cmdT, error) { return command.Get(11, 0, []byte("k")) }),
		mk(2, func() (cmdT, error) { return command.Put(12, 0, []byte("k"), []byte("v")) }),
		mk(3, func() (cmdT, error) { return command.Get(13, 0, []byte("k")) }),
		mk(4, func() (cmdT, error) { return command.Delete(14, 0, []byte("k")) }),
		mk(5, func() (cmdT, error) { return command.Delete(15, 0, []byte("k")) }),
	}
	if err := a.Apply(entries); err != nil {
		t.Fatal(err)
	}
	if got[11].Found || !got[13].Found || string(got[13].Value.Data) != "v" {
		t.Fatalf("get results wrong: %+v %+v", got[11], got[13])
	}
	if got[12].Found || got[12].Value.ModRevision != 2 {
		t.Fatalf("put result = %+v", got[12])
	}
	if !got[14].Found || got[15].Found {
		t.Fatalf("delete results = %+v %+v", got[14], got[15])
	}
	// Get-through-the-log changes no state.
	if _, ok, _ := s.Get([]byte("k")); ok {
		t.Fatal("key should be deleted")
	}
}

// CheckProposal accepts exactly what Apply can apply, so the debug Propose RPC
// can refuse a payload that would otherwise crash every replica (D-33).
func TestCheckProposal(t *testing.T) {
	s := openMem(t, vfs.NewMem())
	defer func() { _ = s.Close() }()
	a := NewApplier(s, nil)
	enc := func(c cmdT, err error) []byte {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		d, err := proto.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	for name, d := range map[string][]byte{
		"put":    enc(command.Put(1, 0, []byte("k"), []byte("v"))),
		"delete": enc(command.Delete(1, 0, []byte("k"))),
		"get":    enc(command.Get(1, 0, []byte("k"))),
	} {
		if err := a.CheckProposal(d); err != nil {
			t.Errorf("%s: CheckProposal = %v, want nil", name, err)
		}
	}
	bad := map[string][]byte{
		"raw bytes": []byte("smoke"),
		"no op":     enc(&raftkvv1.Command{RequestId: 1}, nil),
		"empty key": enc(&raftkvv1.Command{Op: &raftkvv1.Command_Put{Put: &raftkvv1.PutOp{Value: []byte("v")}}}, nil),
	}
	for name, d := range bad {
		if err := a.CheckProposal(d); err == nil {
			t.Errorf("%s: CheckProposal = nil, want an error", name)
		}
		// Each would indeed have been fatal in Apply.
		if err := a.Apply([]raft.Entry{{Index: s.Applied() + 1, Term: 1, Type: raft.EntryNormal, Data: d}}); err == nil {
			t.Errorf("%s: Apply accepted a payload CheckProposal rejects", name)
		}
	}
}

// WBS 3.1.4: the durable applied index advances only after a flush, and the
// flush listener sees it.
func TestDurableAppliedAdvancesOnlyAfterFlush(t *testing.T) {
	flushed := make(chan struct{}, 8)
	s, err := Open(Options{Dir: "db", FS: vfs.NewMem(), OnFlush: func() { flushed <- struct{}{} }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	base := s.FlushCount()
	applyAll(t, NewApplier(s, nil), genRange(1, 30), 4)
	if s.Applied() != 30 || s.DurableApplied() != 0 {
		t.Fatalf("applied=%d durable=%d; durable must lag until a flush", s.Applied(), s.DurableApplied())
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	if s.DurableApplied() != 30 {
		t.Fatalf("durable = %d after Flush, want 30", s.DurableApplied())
	}
	select {
	case <-flushed:
	case <-time.After(5 * time.Second):
		t.Fatal("flush listener never fired")
	}
	if s.FlushCount() <= base {
		t.Fatal("FlushCount did not advance")
	}
	applyAll(t, NewApplier(s, nil), genRange(31, 40), 4)
	if s.DurableApplied() != 30 {
		t.Fatalf("durable = %d, want 30 until the next flush", s.DurableApplied())
	}
}

type fakeLog struct{ compacted []uint64 }

func (f *fakeLog) CompactTo(i uint64) error { f.compacted = append(f.compacted, i); return nil }

// WBS 3.1.5 / D-30: truncation never passes the durable applied index, and it
// flushes first.
func TestTruncationNeverPassesDurableApplied(t *testing.T) {
	for _, tc := range []struct{ snap, durable, retain, want uint64 }{
		{100, 50, 0, 50}, {50, 100, 0, 50}, {100, 100, 10, 90}, {100, 5, 10, 0}, {0, 100, 0, 0},
	} {
		if got := TruncationBound(tc.snap, tc.durable, tc.retain); got != tc.want {
			t.Errorf("TruncationBound(%d,%d,%d) = %d, want %d", tc.snap, tc.durable, tc.retain, got, tc.want)
		}
	}

	s := openMem(t, vfs.NewMem())
	defer func() { _ = s.Close() }()
	applyAll(t, NewApplier(s, nil), genRange(1, 60), 6)
	if s.DurableApplied() != 0 {
		t.Fatal("precondition: nothing flushed yet")
	}
	log := &fakeLog{}
	// Snapshot index far ahead of anything durable: TruncateWAL flushes first,
	// then is limited by the (now current) durable index, not the snapshot's.
	to, err := s.TruncateWAL(log, 1000, 0)
	if err != nil {
		t.Fatal(err)
	}
	if to != 60 || len(log.compacted) != 1 || log.compacted[0] > s.DurableApplied() {
		t.Fatalf("trimmed to %d (%v), durable %d", to, log.compacted, s.DurableApplied())
	}
	// With unflushed applies after that, a snapshot ahead of durable still can't pass it.
	applyAll(t, NewApplier(s, nil), genRange(61, 80), 6)
	if got := TruncationBound(1000, s.DurableApplied(), 0); got > 60 {
		t.Fatalf("bound %d passes durable %d with unflushed memtables", got, s.DurableApplied())
	}
}

func TestReadOnlyStoreRejectsWrites(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	applyAll(t, NewApplier(s, nil), genRange(1, 5), 5)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	ro, err := Open(Options{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	if ro.Applied() != 5 {
		t.Fatalf("applied = %d", ro.Applied())
	}
	if _, err := ro.NewApplyBatch(); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("NewApplyBatch = %v", err)
	}
}
