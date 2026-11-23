package snapshot

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
)

// Install step names, passed to Config.Hook after each step (crash tests).
const (
	StepRenamed  = "renamed"  // staged dir moved to sm/db-<index>, sm/ fsynced
	StepSwapped  = "swapped"  // sm/CURRENT points at it: the commit point
	StepReopened = "reopened" // the store serves the new database
	StepCleaned  = "cleaned"  // the old database directory is gone
)

// Install replaces the live state machine with a staged snapshot
// (storage-design 4.4 steps 3-5 and 8). It must run on the driver goroutine
// with nothing else applying:
//
//  1. rename the staged directory to sm/db-<index> and fsync sm/;
//  2. swap sm/CURRENT to it (write CURRENT.tmp, fsync, rename, fsync sm/):
//     the commit point;
//  3. close the old database and open the new one, checking that its
//     applied index and term are the snapshot's;
//  4. delete the old database directory.
//
// A crash before step 2 leaves the old database live (the orphan sm/db-* is
// deleted on start); after it, the new one, whose appliedIndex is ahead of the
// WAL: recovery writes the missing WAL marker (storage-design 5 step 6). The
// caller writes the WAL marker and HardState (walstore.ApplySnapshot) after
// Install returns. Errors are fatal to the node (D-33).
func (m *Manager) Install(s *Staged) error {
	sm := m.cfg.StateDir
	name := storage.DBDirName(s.Meta.GetLastIncludedIndex())
	newDir := filepath.Join(sm, name)
	oldDir := m.cfg.Store.Dir()
	if filepath.Clean(oldDir) == filepath.Clean(newDir) {
		return fmt.Errorf("snapshot: install of %d over the live database of the same index", s.Meta.GetLastIncludedIndex())
	}
	if err := os.RemoveAll(newDir); err != nil {
		return err
	}
	if err := os.Rename(s.Dir, newDir); err != nil {
		return err
	}
	if err := storage.SyncDir(sm); err != nil {
		return err
	}
	if err := m.hook(StepRenamed); err != nil {
		return err
	}
	if err := storage.SwapCurrent(sm, name); err != nil {
		return err
	}
	if err := m.hook(StepSwapped); err != nil {
		return err
	}
	if err := m.cfg.Store.Reopen(newDir); err != nil {
		return err
	}
	st := m.cfg.Store
	if st.Applied() != s.Meta.GetLastIncludedIndex() || st.AppliedTerm() != s.Meta.GetLastIncludedTerm() {
		return fmt.Errorf("snapshot: installed database holds applied %d/%d, metadata says %d/%d",
			st.Applied(), st.AppliedTerm(), s.Meta.GetLastIncludedIndex(), s.Meta.GetLastIncludedTerm())
	}
	if err := m.hook(StepReopened); err != nil {
		return err
	}
	if err := os.RemoveAll(oldDir); err != nil {
		return err
	}
	if err := storage.SyncDir(sm); err != nil {
		return err
	}
	return m.hook(StepCleaned)
}

func (m *Manager) hook(step string) error {
	if m.cfg.Hook == nil {
		return nil
	}
	return m.cfg.Hook(step)
}
