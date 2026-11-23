package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// State directory layout (storage-design 7): the live Pebble database is one
// subdirectory of sm/, named by the CURRENT pointer file. Installing a snapshot
// moves a new database next to it and atomically swaps CURRENT (D-14,
// storage-design 4.4), so a crash leaves either the old database or the new
// one live, never a mix.
//
//	sm/
//	  CURRENT                  # "db-<index:016x>\n", the live database
//	  db-0000000000000000/     # the database a fresh node starts with
//	  db-0000000000186a00/     # a database installed from a snapshot
const (
	// CurrentFile names the live database inside the state directory.
	CurrentFile = "CURRENT"
	currentTmp  = CurrentFile + ".tmp"
	dbPrefix    = "db-"
)

// ErrLegacyStateDir means the state directory holds a database in the
// pre-M5 flat layout (Pebble files directly in sm/), which this version cannot
// open. Wipe the member's data and let it catch up from the cluster.
var ErrLegacyStateDir = errors.New("storage: state directory has the pre-M5 layout (no CURRENT)")

// DBDirName is the name of the database directory for a snapshot at index.
func DBDirName(index uint64) string { return fmt.Sprintf("%s%016x", dbPrefix, index) }

// PrepareStateDir readies smDir at start-up and returns the path of the live
// database (storage-design 5 step 2): it creates the layout on a fresh
// directory, removes a leftover CURRENT.tmp and every db-* directory CURRENT
// does not name (a snapshot install that crashed before its commit point, or
// the old database of one that crashed after it).
func PrepareStateDir(smDir string) (string, error) {
	if err := os.MkdirAll(smDir, 0o755); err != nil {
		return "", err
	}
	if err := os.Remove(filepath.Join(smDir, currentTmp)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	live, err := ReadCurrent(smDir)
	if errors.Is(err, os.ErrNotExist) {
		names, lerr := os.ReadDir(smDir)
		if lerr != nil {
			return "", lerr
		}
		for _, e := range names {
			if !strings.HasPrefix(e.Name(), dbPrefix) {
				return "", fmt.Errorf("%w: %s holds %s", ErrLegacyStateDir, smDir, e.Name())
			}
		}
		live = DBDirName(0)
		if err := os.MkdirAll(filepath.Join(smDir, live), 0o755); err != nil {
			return "", err
		}
		if err := SwapCurrent(smDir, live); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	}
	entries, err := os.ReadDir(smDir)
	if err != nil {
		return "", err
	}
	removed := false
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), dbPrefix) && e.Name() != live {
			if err := os.RemoveAll(filepath.Join(smDir, e.Name())); err != nil {
				return "", err
			}
			removed = true
		}
	}
	if removed {
		if err := SyncDir(smDir); err != nil {
			return "", err
		}
	}
	return filepath.Join(smDir, live), nil
}

// ReadCurrent returns the database directory name CURRENT points at. A missing
// CURRENT returns an error wrapping os.ErrNotExist.
func ReadCurrent(smDir string) (string, error) {
	b, err := os.ReadFile(filepath.Join(smDir, CurrentFile))
	if err != nil {
		return "", err
	}
	name := strings.TrimSpace(string(b))
	if !strings.HasPrefix(name, dbPrefix) || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("storage: %s/%s names %q, not a database directory", smDir, CurrentFile, name)
	}
	return name, nil
}

// SwapCurrent atomically points CURRENT at the database directory name:
// write CURRENT.tmp, fsync it, rename it over CURRENT, fsync the directory
// (storage-design 4.4 step 4). The rename is the commit point.
func SwapCurrent(smDir, name string) error {
	tmp := filepath.Join(smDir, currentTmp)
	if err := WriteFileSync(tmp, []byte(name+"\n")); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(smDir, CurrentFile)); err != nil {
		return err
	}
	return SyncDir(smDir)
}

// WriteFileSync writes data to path and fsyncs it.
func WriteFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// SyncDir fsyncs a directory so renames, creates and deletes in it are
// durable (D-33).
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
