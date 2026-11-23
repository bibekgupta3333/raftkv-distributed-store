package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// Errors returned by the store.
var (
	// ErrSchemaVersion means the database was written by an incompatible schema.
	ErrSchemaVersion = errors.New("storage: unsupported schema version")
	// ErrOutOfOrder means a batch did not carry the next log index. Apply is
	// strictly contiguous (D-04): a gap or repeat is a driver bug.
	ErrOutOfOrder = errors.New("storage: apply batch is not the next log index")
	// ErrReadOnly is returned by writes on a store opened read-only.
	ErrReadOnly = errors.New("storage: store is read-only")
)

// Options configure [Open].
type Options struct {
	// Dir is the Pebble directory.
	Dir string
	// FS replaces the real file system (crash tests use a crashable in-memory
	// one). nil means the OS.
	FS vfs.FS
	// ReadOnly opens an existing database without writing (debug tools).
	ReadOnly bool
	// OnFlush, if set, is called after every Pebble memtable flush completes.
	// It runs on a Pebble goroutine and must not block.
	OnFlush func()
}

// Store is the Pebble-backed state machine storage (D-02). Pebble's own WAL is
// off (D-03); the Raft WAL is the only log. Every apply batch carries the
// appliedIndex it covers (D-04), and Pebble is durable only up to its last
// memtable flush, so the WAL must never be trimmed past [Store.DurableApplied]
// (D-30, see [Store.TruncateWAL]).
//
// One goroutine (the node's apply path) commits batches; any goroutine may
// call the read methods and [Store.Flush].
type Store struct {
	db       *pebble.DB
	readOnly bool

	applied     atomic.Uint64 // last committed batch's index
	appliedTerm atomic.Uint64
	durable     atomic.Uint64 // applied index known to be flushed (D-30)
	flushes     atomic.Uint64 // completed Pebble flushes seen by the listener
}

// quietLogger drops Pebble's informational log; fatal errors still panic.
type quietLogger struct{}

func (quietLogger) Infof(string, ...any)  {}
func (quietLogger) Errorf(string, ...any) {}
func (quietLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("pebble fatal: "+format, args...))
}

// Open opens (or creates) the state machine at opts.Dir. On a fresh database it
// writes the schema version; on an existing one it checks it. The applied index
// it recovers is also the durable one: Pebble reopens at its last flush, so
// everything it holds is on disk (storage-design 2.4).
func Open(opts Options) (*Store, error) {
	s := &Store{readOnly: opts.ReadOnly}
	popts := &pebble.Options{
		DisableWAL: true, // D-02, D-03
		FS:         opts.FS,
		ReadOnly:   opts.ReadOnly,
		Logger:     quietLogger{},
		EventListener: &pebble.EventListener{
			FlushEnd: func(pebble.FlushInfo) {
				s.flushes.Add(1)
				if opts.OnFlush != nil {
					opts.OnFlush()
				}
			},
		},
	}
	db, err := pebble.Open(opts.Dir, popts)
	if err != nil {
		return nil, fmt.Errorf("storage: open pebble: %w", err)
	}
	s.db = db
	if err := s.recover(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) recover() error {
	ver, found, err := s.getMeta(keySchemaVersion)
	if err != nil {
		return err
	}
	switch {
	case !found:
		if s.readOnly {
			return errors.New("storage: no schema version (not an initialised state machine)")
		}
		b := s.db.NewBatch()
		defer func() { _ = b.Close() }()
		if err := b.Set(keySchemaVersion, putU64(SchemaVersion), nil); err != nil {
			return err
		}
		if err := s.db.Apply(b, pebble.NoSync); err != nil {
			return err
		}
		return s.db.Flush() // the schema key must survive a crash before the first Flush
	case ver != SchemaVersion:
		return fmt.Errorf("%w: have %d, want %d", ErrSchemaVersion, ver, SchemaVersion)
	}
	a, _, err := s.getMeta(keyAppliedIndex)
	if err != nil {
		return err
	}
	t, _, err := s.getMeta(keyAppliedTerm)
	if err != nil {
		return err
	}
	s.applied.Store(a)
	s.appliedTerm.Store(t)
	s.durable.Store(a) // D-30: only startup and our own Flush raise it
	return nil
}

func (s *Store) getMeta(key []byte) (uint64, bool, error) {
	v, c, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer func() { _ = c.Close() }()
	n, err := getU64(v)
	return n, true, err
}

// Applied is the index of the last committed apply batch (0 if none).
func (s *Store) Applied() uint64 { return s.applied.Load() }

// AppliedTerm is the term of the entry at [Store.Applied].
func (s *Store) AppliedTerm() uint64 { return s.appliedTerm.Load() }

// DurableApplied is the applied index known to be flushed to disk. D-30: the
// WAL may only be trimmed up to this index (and the snapshot index).
func (s *Store) DurableApplied() uint64 { return s.durable.Load() }

// FlushCount is how many Pebble flushes the flush listener has observed.
func (s *Store) FlushCount() uint64 { return s.flushes.Load() }

// Flush makes everything applied so far durable and then raises
// DurableApplied. The index is read before the flush starts: any batch with a
// lower or equal index was already in a memtable, so the flush covers it, and
// batches that land during the flush are simply not counted (storage-design
// 2.4). Pebble's background flushes are deliberately not counted, because a
// flush event does not say which indexes it covered.
func (s *Store) Flush() error {
	if s.readOnly {
		return ErrReadOnly
	}
	a := s.applied.Load()
	if err := s.db.Flush(); err != nil {
		return fmt.Errorf("storage: flush: %w", err)
	}
	for {
		d := s.durable.Load()
		if d >= a || s.durable.CompareAndSwap(d, a) {
			return nil
		}
	}
}

// Close flushes (so a graceful shutdown loses nothing) and closes Pebble.
func (s *Store) Close() error {
	var ferr error
	if !s.readOnly {
		ferr = s.Flush()
	}
	return errors.Join(ferr, s.db.Close())
}

// Get reads a user key from committed state.
func (s *Store) Get(key []byte) (Value, bool, error) { return getValue(s.db, key) }

type reader interface {
	Get(key []byte) ([]byte, io.Closer, error)
}

func getValue(r reader, key []byte) (Value, bool, error) {
	v, c, err := r.Get(UserKey(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return Value{}, false, nil
	}
	if err != nil {
		return Value{}, false, err
	}
	defer func() { _ = c.Close() }()
	val, err := decodeValue(v)
	return val, err == nil, err
}

// StateHash hashes the whole replicated state: every key and value except the
// per-node applied metadata. Replicas that applied the same entries produce the
// same hash, which is what the crash suite and `raftkvctl debug state-hash`
// compare (WBS 3.1.7, 3.1.8). It returns the applied index it was taken at. It
// is meant for quiescent stores (a stopped node or a test).
func (s *Store) StateHash() (applied uint64, sum [sha256.Size]byte, err error) {
	applied = s.Applied()
	it, err := s.db.NewIter(nil)
	if err != nil {
		return 0, sum, err
	}
	h := sha256.New()
	var lenBuf [8]byte
	write := func(b []byte) {
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(b)))
		h.Write(lenBuf[:])
		h.Write(b)
	}
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if bytes.Equal(k, keyAppliedIndex) || bytes.Equal(k, keyAppliedTerm) {
			continue
		}
		write(k)
		write(it.Value())
	}
	err = errors.Join(it.Error(), it.Close())
	copy(sum[:], h.Sum(nil))
	return applied, sum, err
}

// ApplyBatch accumulates the effects of one committed range of entries. It is
// indexed, so later entries in the same batch see earlier ones (read-your-writes,
// storage-design 3.2). [ApplyBatch.Commit] writes data and appliedIndex in one
// Pebble batch (D-04).
type ApplyBatch struct {
	s    *Store
	b    *pebble.Batch
	last uint64 // last index staged
	term uint64
}

// NewApplyBatch starts a batch that continues from the current applied index.
func (s *Store) NewApplyBatch() (*ApplyBatch, error) {
	if s.readOnly {
		return nil, ErrReadOnly
	}
	return &ApplyBatch{s: s, b: s.db.NewIndexedBatch(), last: s.Applied()}, nil
}

// Next declares that the entry (index, term) is being applied into this batch.
// Indexes must be contiguous (D-04: no double apply, no gap).
func (a *ApplyBatch) Next(index, term uint64) error {
	if index != a.last+1 {
		return fmt.Errorf("%w: have %d, got %d", ErrOutOfOrder, a.last, index)
	}
	a.last, a.term = index, term
	return nil
}

// Get reads a user key including this batch's own earlier writes.
func (a *ApplyBatch) Get(key []byte) (Value, bool, error) { return getValue(a.b, key) }

// Put stages key=data as the entry declared by the last [ApplyBatch.Next] and
// returns the previous value if there was one. Revisions come from the log
// index, so they are identical on every replica.
func (a *ApplyBatch) Put(key, data []byte) (cur, prev Value, hadPrev bool, err error) {
	prev, hadPrev, err = a.Get(key)
	if err != nil {
		return Value{}, Value{}, false, err
	}
	cur = Value{CreateRevision: a.last, ModRevision: a.last, Version: 1, Data: data}
	if hadPrev {
		cur.CreateRevision, cur.Version = prev.CreateRevision, prev.Version+1
	}
	return cur, prev, hadPrev, a.b.Set(UserKey(key), cur.encode(), nil)
}

// Delete stages the removal of key and returns the value it had.
func (a *ApplyBatch) Delete(key []byte) (prev Value, existed bool, err error) {
	prev, existed, err = a.Get(key)
	if err != nil || !existed {
		return Value{}, false, err
	}
	return prev, true, a.b.Delete(UserKey(key), nil)
}

// Commit writes the staged changes together with appliedIndex and appliedTerm
// in one Pebble batch without a sync (D-04; Pebble's WAL is off, D-03), then
// publishes the new applied index. A failure here must crash the node
// (D-33): a partial batch is never acknowledged.
func (a *ApplyBatch) Commit() error {
	defer func() { _ = a.b.Close() }()
	if err := a.b.Set(keyAppliedIndex, putU64(a.last), nil); err != nil {
		return err
	}
	if err := a.b.Set(keyAppliedTerm, putU64(a.term), nil); err != nil {
		return err
	}
	if err := a.s.db.Apply(a.b, pebble.NoSync); err != nil {
		return fmt.Errorf("storage: commit batch ending at %d: %w", a.last, err)
	}
	a.s.appliedTerm.Store(a.term)
	a.s.applied.Store(a.last)
	return nil
}

// Abort discards the batch.
func (a *ApplyBatch) Abort() { _ = a.b.Close() }
