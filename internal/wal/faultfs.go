package wal

import (
	"errors"
	"sync"
)

// ErrInjected is returned by operations that FaultFS was told to fail.
var ErrInjected = errors.New("wal: injected fault")

// FaultFS wraps an FS and injects faults for tests (D-18): failing the n-th
// write / fsync / directory fsync, and truncating or corrupting a file at an
// offset to simulate crashes and bit rot.
type FaultFS struct {
	FS

	mu          sync.Mutex
	writes      int
	syncs       int
	dirSyncs    int
	failWriteAt int // absolute call number; 0 = never
	failSyncAt  int
	failDirAt   int
}

// NewFaultFS wraps inner.
func NewFaultFS(inner FS) *FaultFS { return &FaultFS{FS: inner} }

// FailWriteOn makes the n-th file Write from now fail (n >= 1).
func (f *FaultFS) FailWriteOn(n int) { f.mu.Lock(); f.failWriteAt = f.writes + n; f.mu.Unlock() }

// FailSyncOn makes the n-th file Sync from now fail (n >= 1).
func (f *FaultFS) FailSyncOn(n int) { f.mu.Lock(); f.failSyncAt = f.syncs + n; f.mu.Unlock() }

// FailSyncDirOn makes the n-th SyncDir from now fail (n >= 1).
func (f *FaultFS) FailSyncDirOn(n int) { f.mu.Lock(); f.failDirAt = f.dirSyncs + n; f.mu.Unlock() }

// Writes returns the number of file Write calls so far. Tests use the call
// counters to prove a failed call was not retried.
func (f *FaultFS) Writes() int { f.mu.Lock(); defer f.mu.Unlock(); return f.writes }

// Syncs returns the number of file Sync calls so far.
func (f *FaultFS) Syncs() int { f.mu.Lock(); defer f.mu.Unlock(); return f.syncs }

// DirSyncs returns the number of SyncDir calls so far.
func (f *FaultFS) DirSyncs() int { f.mu.Lock(); defer f.mu.Unlock(); return f.dirSyncs }

// TruncateFile cuts the file to size bytes (a torn write / lost tail).
func (f *FaultFS) TruncateFile(name string, size int64) error {
	file, err := f.FS.OpenAppend(name)
	if err != nil {
		return err
	}
	if err := file.Truncate(size); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// CorruptByte XORs the byte at off with mask (mask must be non-zero).
func (f *FaultFS) CorruptByte(name string, off int64, mask byte) error {
	data, err := f.ReadFile(name)
	if err != nil {
		return err
	}
	if off < 0 || off >= int64(len(data)) || mask == 0 {
		return errors.New("wal: bad corrupt request")
	}
	data[off] ^= mask
	file, err := f.FS.OpenAppend(name)
	if err != nil {
		return err
	}
	if err := file.Truncate(0); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// Create implements FS.
func (f *FaultFS) Create(name string) (File, error) {
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &faultFile{File: file, fs: f}, nil
}

// OpenAppend implements FS.
func (f *FaultFS) OpenAppend(name string) (File, error) {
	file, err := f.FS.OpenAppend(name)
	if err != nil {
		return nil, err
	}
	return &faultFile{File: file, fs: f}, nil
}

// SyncDir implements FS.
func (f *FaultFS) SyncDir(dir string) error {
	f.mu.Lock()
	f.dirSyncs++
	fail := f.failDirAt != 0 && f.dirSyncs == f.failDirAt
	f.mu.Unlock()
	if fail {
		return ErrInjected
	}
	return f.FS.SyncDir(dir)
}

type faultFile struct {
	File
	fs *FaultFS
}

func (ff *faultFile) Write(p []byte) (int, error) {
	ff.fs.mu.Lock()
	ff.fs.writes++
	fail := ff.fs.failWriteAt != 0 && ff.fs.writes == ff.fs.failWriteAt
	ff.fs.mu.Unlock()
	if fail {
		return 0, ErrInjected
	}
	return ff.File.Write(p)
}

func (ff *faultFile) Sync() error {
	ff.fs.mu.Lock()
	ff.fs.syncs++
	fail := ff.fs.failSyncAt != 0 && ff.fs.syncs == ff.fs.failSyncAt
	ff.fs.mu.Unlock()
	if fail {
		return ErrInjected
	}
	return ff.File.Sync()
}
