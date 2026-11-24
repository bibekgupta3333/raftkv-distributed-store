package node

import (
	"crypto/rand"
	"encoding/binary"
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

// ErrStateLost means the data directory still names its node but lost the
// node's state (its WAL is gone). The node may have voted or acknowledged
// entries it no longer remembers, so it must not run under that ID again
// (D-32): remove it and add the machine back as a new learner.
var ErrStateLost = errors.New("node: data directory lost its state (D-32)")

// Identity is who a data directory belongs to. It is written once, on first
// start, and checked on every later start (WBS 2.3.4). Starting under another
// ID could make the node vote twice in one term with a log it no longer
// matches (D-32), so a mismatch is a hard refusal, never a repair.
type Identity struct {
	NodeID    uint64 `json:"node_id"`
	ClusterID uint64 `json:"cluster_id"`
	// Incarnation is a random number drawn when the directory is created.
	// Peers remember it: a member that comes back under its old ID with a
	// new incarnation lost its disk and is refused (D-32).
	Incarnation uint64 `json:"incarnation,omitempty"`
	// Joined records that the node joined an existing cluster as a learner
	// (-join, D-32): it has no bootstrap voters and learns the
	// configuration from the leader.
	Joined bool `json:"joined,omitempty"`
}

func newIncarnation() uint64 {
	for {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			panic(fmt.Sprintf("node: crypto/rand: %v", err))
		}
		if v := binary.BigEndian.Uint64(b[:]); v != 0 {
			return v
		}
	}
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

// EnsureIdentity creates dir and records want (with a fresh incarnation) if the
// directory is new, or verifies that it belongs to want's node and cluster, and
// returns the recorded identity. It refuses (D-32):
//   - a directory that holds WAL data but no identity file: its owner is
//     unknown;
//   - a directory whose identity file survived but whose WAL is gone: the
//     node lost its votes and acknowledged entries ([ErrStateLost]).
//
// The WAL directory is created before the identity file is written, so a
// crash between the two never looks like lost state.
func EnsureIdentity(dir string, want Identity) (Identity, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Identity{}, err
	}
	got, ok, err := ReadIdentity(dir)
	if err != nil {
		return Identity{}, err
	}
	walDir := filepath.Join(dir, WALDir)
	if ok {
		if got.NodeID != want.NodeID || got.ClusterID != want.ClusterID {
			return Identity{}, fmt.Errorf("%w: %s belongs to node %d of cluster %d, but the config says node %d of cluster %d",
				ErrIdentityMismatch, dir, got.NodeID, got.ClusterID, want.NodeID, want.ClusterID)
		}
		if _, err := os.Stat(walDir); errors.Is(err, os.ErrNotExist) {
			return Identity{}, fmt.Errorf("%w: %s names node %d but has no WAL; remove the member and add this machine back as a new learner",
				ErrStateLost, dir, got.NodeID)
		}
		if got.Incarnation == 0 { // written before M6
			got.Incarnation = newIncarnation()
			if err := writeIdentity(dir, got); err != nil {
				return Identity{}, err
			}
		}
		return got, nil
	}
	if ents, err := os.ReadDir(walDir); err == nil && len(ents) > 0 {
		return Identity{}, fmt.Errorf("%w: %s has WAL data but no %s file", ErrIdentityMismatch, dir, IdentityFile)
	}
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		return Identity{}, err
	}
	want.Incarnation = newIncarnation()
	return want, writeIdentity(dir, want)
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
