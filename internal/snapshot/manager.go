package snapshot

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
)

const (
	tmpPrefix   = "tmp-"
	incomingDir = "incoming"
	// DefaultKeep is how many complete snapshots are retained (storage-design
	// 4.5).
	DefaultKeep = 2
)

// ErrNoSnapshot means no local snapshot is recent enough.
var ErrNoSnapshot = errors.New("snapshot: no suitable local snapshot")

// Config configures a [Manager].
type Config struct {
	// Dir is the snapshot directory (<data>/snap); StateDir is the state
	// machine directory (<data>/sm). They must be on one file system
	// (hard links and renames).
	Dir, StateDir string
	// Store is the live state machine.
	Store     *storage.Store
	ClusterID uint64
	// Members is the membership recorded in every snapshot (static until M6).
	Members []*raftkvv1.Member
	// Keep is how many complete snapshots to retain (DefaultKeep if 0).
	Keep int
	// Hook, if set, is called after each install step with its name; an
	// error stops the install right there, as a crash would (tests only).
	Hook func(step string) error
}

// Manager owns a node's snapshots: creation, retention, transfer and install.
// Create, Install and Discard run on the node's driver goroutine; Send and
// Receive run on transport goroutines.
type Manager struct {
	cfg Config

	mu    sync.Mutex
	snaps []*Meta // complete snapshots, oldest first
	refs  map[uint64]int
}

// Open prepares the snapshot directory (storage-design 4.5): it deletes
// in-progress creations (snap/tmp-*) and transfers (snap/incoming/*) left by a
// crash and loads the complete snapshots. A directory whose metadata does not
// verify is deleted; it was never complete. One whose metadata carries an
// unknown format_version is an error wrapping ErrFormat: the node refuses to
// start (WBS 7.5.2).
func Open(cfg Config) (*Manager, error) {
	if cfg.Keep <= 0 {
		cfg.Keep = DefaultKeep
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	m := &Manager{cfg: cfg, refs: map[uint64]int{}}
	entries, err := os.ReadDir(cfg.Dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		path := filepath.Join(cfg.Dir, e.Name())
		if strings.HasPrefix(e.Name(), tmpPrefix) || e.Name() == incomingDir {
			if err := os.RemoveAll(path); err != nil {
				return nil, err
			}
			continue
		}
		if _, _, ok := parseName(e.Name()); !ok || !e.IsDir() {
			continue
		}
		meta, err := ReadMeta(path)
		if errors.Is(err, ErrFormat) {
			// A complete snapshot in a format this build cannot read
			// (written by a newer release, WBS 7.5.2): refuse to start
			// rather than delete it as if it were a crash leftover.
			return nil, err
		}
		if err != nil || dirName(meta) != e.Name() {
			if err := os.RemoveAll(path); err != nil {
				return nil, err
			}
			continue
		}
		m.snaps = append(m.snaps, meta)
	}
	slices.SortFunc(m.snaps, func(a, b *Meta) int { return compareIndex(a.GetLastIncludedIndex(), b.GetLastIncludedIndex()) })
	if err := storage.SyncDir(cfg.Dir); err != nil {
		return nil, err
	}
	return m, nil
}

func compareIndex(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func dirName(meta *Meta) string {
	return fmt.Sprintf("%016x-%016x", meta.GetLastIncludedIndex(), meta.GetLastIncludedTerm())
}

func parseName(name string) (index, term uint64, ok bool) {
	a, b, found := strings.Cut(name, "-")
	if !found || len(a) != 16 || len(b) != 16 {
		return 0, 0, false
	}
	i, err1 := strconv.ParseUint(a, 16, 64)
	t, err2 := strconv.ParseUint(b, 16, 64)
	return i, t, err1 == nil && err2 == nil
}

// Latest returns the newest complete snapshot's metadata, or nil.
func (m *Manager) Latest() *Meta {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.snaps) == 0 {
		return nil
	}
	return m.snaps[len(m.snaps)-1]
}

// List returns the complete snapshots, oldest first.
func (m *Manager) List() []*Meta {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.snaps)
}

// Dir returns the directory of a complete snapshot.
func (m *Manager) Dir(meta *Meta) string { return filepath.Join(m.cfg.Dir, dirName(meta)) }

// Create snapshots the state machine at its applied index (storage-design
// 4.1). It must run on the apply goroutine between apply batches, so the
// checkpoint's appliedIndex is exactly the applied index. Steps: checkpoint
// (flush + hard links) into snap/tmp-*, read the applied index and term back
// from the checkpoint itself, CRC every file, write and fsync snapshot.meta,
// fsync the directory, rename it into place and fsync snap/. A crash at any
// point leaves only a tmp-* directory, deleted on the next start.
func (m *Manager) Create() (*Meta, error) {
	st := m.cfg.Store
	applied := st.Applied()
	if latest := m.Latest(); latest != nil && latest.GetLastIncludedIndex() >= applied {
		return latest, nil
	}
	tmp := filepath.Join(m.cfg.Dir, fmt.Sprintf("%s%016x", tmpPrefix, applied))
	if err := os.RemoveAll(tmp); err != nil {
		return nil, err
	}
	index, term, err := st.Checkpoint(tmp)
	if err != nil {
		_ = os.RemoveAll(tmp)
		return nil, err
	}
	meta, err := m.seal(tmp, index, term)
	if err != nil {
		_ = os.RemoveAll(tmp)
		return nil, err
	}
	final := m.Dir(meta)
	if err := os.Rename(tmp, final); err != nil {
		_ = os.RemoveAll(tmp)
		return nil, err
	}
	if err := storage.SyncDir(m.cfg.Dir); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.snaps = append(m.snaps, meta)
	m.mu.Unlock()
	return meta, m.gc()
}

// seal checks that the checkpoint in dir holds (index, term) as its applied
// position, then writes its metadata.
func (m *Manager) seal(dir string, index, term uint64) (*Meta, error) {
	ro, err := storage.Open(storage.Options{Dir: dir, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	ai, at := ro.Applied(), ro.AppliedTerm()
	if err := ro.Close(); err != nil {
		return nil, err
	}
	if ai != index || at != term {
		return nil, fmt.Errorf("snapshot: checkpoint holds applied %d/%d, want %d/%d", ai, at, index, term)
	}
	meta := &Meta{
		ClusterId:         m.cfg.ClusterID,
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
		Config:            &raftkvv1.ClusterConfig{Members: m.cfg.Members},
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.Type().IsRegular() || e.Name() == MetaFile || e.Name() == "LOCK" {
			continue
		}
		path := filepath.Join(dir, e.Name())
		size, crc, err := fileCRC(path)
		if err != nil {
			return nil, err
		}
		if err := syncFile(path); err != nil {
			return nil, err
		}
		meta.Files = append(meta.Files, &raftkvv1.SnapshotFile{Name: e.Name(), Size: uint64(size), Crc32C: crc})
	}
	Seal(meta)
	if err := WriteMeta(dir, meta); err != nil {
		return nil, err
	}
	if err := storage.SyncDir(dir); err != nil {
		return nil, err
	}
	return meta, nil
}

// gc keeps the Keep newest snapshots and any older one an outgoing transfer
// still reads (storage-design 4.5).
func (m *Manager) gc() error {
	m.mu.Lock()
	var drop []*Meta
	var keep []*Meta
	for i, s := range m.snaps {
		if i < len(m.snaps)-m.cfg.Keep && m.refs[s.GetLastIncludedIndex()] == 0 {
			drop = append(drop, s)
		} else {
			keep = append(keep, s)
		}
	}
	m.snaps = keep
	m.mu.Unlock()
	for _, s := range drop {
		if err := os.RemoveAll(m.Dir(s)); err != nil {
			return err
		}
	}
	if len(drop) > 0 {
		return storage.SyncDir(m.cfg.Dir)
	}
	return nil
}

// acquire returns the newest snapshot with index >= minIndex and pins it
// against retention until release is called.
func (m *Manager) acquire(minIndex uint64) (*Meta, func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n := len(m.snaps); n > 0 && m.snaps[n-1].GetLastIncludedIndex() >= minIndex {
		s := m.snaps[n-1]
		idx := s.GetLastIncludedIndex()
		m.refs[idx]++
		return s, func() {
			m.mu.Lock()
			m.refs[idx]--
			if m.refs[idx] == 0 {
				delete(m.refs, idx)
			}
			m.mu.Unlock()
			_ = m.gc()
		}, nil
	}
	return nil, nil, fmt.Errorf("%w: need index >= %d", ErrNoSnapshot, minIndex)
}

// TruncateWAL trims log under D-30: Pebble is flushed first, then the log is
// trimmed to min(newest snapshot, durable applied) - retain. It returns the
// index trimmed to (0 if none).
func (m *Manager) TruncateWAL(log storage.LogCompactor, retain uint64) (uint64, error) {
	latest := m.Latest()
	if latest == nil {
		return 0, nil
	}
	return m.cfg.Store.TruncateWAL(log, latest.GetLastIncludedIndex(), retain)
}
