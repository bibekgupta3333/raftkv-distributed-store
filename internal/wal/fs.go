package wal

import (
	"os"
	"path/filepath"
	"sort"
)

// File is the subset of file operations the WAL needs. Every file is opened in
// append mode, so Write always lands at the current end (also after Truncate).
type File interface {
	Write(p []byte) (int, error)
	Sync() error
	Truncate(size int64) error
	Close() error
}

// FS abstracts the directory and file operations of the WAL so tests can inject
// faults (see FaultFS). Names are full paths.
type FS interface {
	MkdirAll(dir string) error
	// Create makes a new file and fails if it already exists.
	Create(name string) (File, error)
	OpenAppend(name string) (File, error)
	ReadFile(name string) ([]byte, error)
	// List returns the sorted base names in dir.
	List(dir string) ([]string, error)
	Remove(name string) error
	// SyncDir fsyncs a directory so created/removed entries are durable.
	SyncDir(dir string) error
}

// OSFS is the real file system. os.File.Sync uses F_FULLFSYNC on macOS.
type OSFS struct{}

// MkdirAll implements FS.
func (OSFS) MkdirAll(dir string) error { return os.MkdirAll(dir, 0o755) }

// Create implements FS.
func (OSFS) Create(name string) (File, error) {
	return os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o644)
}

// OpenAppend implements FS.
func (OSFS) OpenAppend(name string) (File, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_APPEND, 0o644)
}

// ReadFile implements FS.
func (OSFS) ReadFile(name string) ([]byte, error) { return os.ReadFile(name) }

// List implements FS.
func (OSFS) List(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names, nil
}

// Remove implements FS.
func (OSFS) Remove(name string) error { return os.Remove(name) }

// SyncDir implements FS.
func (OSFS) SyncDir(dir string) error {
	d, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
