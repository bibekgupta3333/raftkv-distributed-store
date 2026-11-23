package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// IdentityFile is the name of the identity record inside the data directory.
const IdentityFile = "IDENTITY"

// WALDir is the WAL's subdirectory of the data directory (storage-design 1).
const WALDir = "wal"

// StateDir is the state machine's (Pebble) subdirectory of the data directory.
const StateDir = "sm"

// SnapDir is the snapshots' subdirectory of the data directory (D-14).
const SnapDir = "snap"

// ErrIdentityMismatch means the data directory belongs to a different node or
// cluster than the configuration says.
var ErrIdentityMismatch = errors.New("node: data directory identity mismatch")

// Identity is who a data directory belongs to. It is written once, on first
// start, and checked on every later start (WBS 2.3.4). Starting under another
// ID could make the node vote twice in one term with a log it no longer
// matches (D-32), so a mismatch is a hard refusal, never a repair.
type Identity struct {
	NodeID    uint64 `json:"node_id"`
	ClusterID uint64 `json:"cluster_id"`
}

// ReadIdentity loads the identity of dir. A missing file returns (zero, false, nil).
func ReadIdentity(dir string) (id Identity, ok bool, err error) {
	data, err := os.ReadFile(filepath.Join(dir, IdentityFile))
	if errors.Is(err, os.ErrNotExist) {
		return Identity{}, false, nil
	}
	if err != nil {
		return Identity{}, false, err
	}
	if err := json.Unmarshal(data, &id); err != nil {
		return Identity{}, false, fmt.Errorf("node: corrupt %s in %s: %w", IdentityFile, dir, err)
	}
	return id, true, nil
}

// EnsureIdentity creates dir and writes want if the directory is fresh, or
// verifies it matches. A directory that already holds WAL data but no identity
// file is refused: its owner is unknown (D-32).
func EnsureIdentity(dir string, want Identity) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	got, ok, err := ReadIdentity(dir)
	if err != nil {
		return err
	}
	if ok {
		if got != want {
			return fmt.Errorf("%w: %s belongs to node %d of cluster %d, but the config says node %d of cluster %d",
				ErrIdentityMismatch, dir, got.NodeID, got.ClusterID, want.NodeID, want.ClusterID)
		}
		return nil
	}
	if ents, err := os.ReadDir(filepath.Join(dir, WALDir)); err == nil && len(ents) > 0 {
		return fmt.Errorf("%w: %s has WAL data but no %s file", ErrIdentityMismatch, dir, IdentityFile)
	}
	return writeIdentity(dir, want)
}

// writeIdentity writes the file atomically: temp file, fsync, rename, fsync dir.
func writeIdentity(dir string, id Identity) error {
	data, err := json.Marshal(id)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, IdentityFile+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, IdentityFile)); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
