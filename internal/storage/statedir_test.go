package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareStateDirFreshAndOrphans(t *testing.T) {
	sm := filepath.Join(t.TempDir(), "sm")
	live, err := PrepareStateDir(sm)
	if err != nil {
		t.Fatal(err)
	}
	if live != filepath.Join(sm, DBDirName(0)) {
		t.Fatalf("live = %s", live)
	}
	if name, err := ReadCurrent(sm); err != nil || name != DBDirName(0) {
		t.Fatalf("CURRENT = %q, %v", name, err)
	}
	// An install that crashed before its commit point left an orphan db and
	// a CURRENT.tmp.
	if err := os.MkdirAll(filepath.Join(sm, DBDirName(77)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sm, currentTmp), []byte(DBDirName(77)), 0o644); err != nil {
		t.Fatal(err)
	}
	live2, err := PrepareStateDir(sm)
	if err != nil || live2 != live {
		t.Fatalf("live = %s, %v", live2, err)
	}
	es, _ := os.ReadDir(sm)
	if len(es) != 2 {
		t.Fatalf("sm/ holds %d entries, want CURRENT and the live db", len(es))
	}
	// After a swap the old db is the orphan.
	if err := os.MkdirAll(filepath.Join(sm, DBDirName(90)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := SwapCurrent(sm, DBDirName(90)); err != nil {
		t.Fatal(err)
	}
	live3, err := PrepareStateDir(sm)
	if err != nil || live3 != filepath.Join(sm, DBDirName(90)) {
		t.Fatalf("live = %s, %v", live3, err)
	}
	if _, err := os.Stat(live); !os.IsNotExist(err) {
		t.Fatalf("old db survived: %v", err)
	}
}

func TestPrepareStateDirRefusesLegacyLayout(t *testing.T) {
	sm := filepath.Join(t.TempDir(), "sm")
	s, err := Open(Options{Dir: sm}) // pre-M5: Pebble directly in sm/
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareStateDir(sm); !errors.Is(err, ErrLegacyStateDir) {
		t.Fatalf("err = %v, want ErrLegacyStateDir", err)
	}
}

func TestReopenSwapsDatabase(t *testing.T) {
	base := t.TempDir()
	a, err := Open(Options{Dir: filepath.Join(base, "a")})
	if err != nil {
		t.Fatal(err)
	}
	applyAll(t, NewApplier(a, nil), genRange(1, 30), 7)
	if _, _, err := a.Checkpoint(filepath.Join(base, "cp")); err != nil {
		t.Fatal(err)
	}
	_, want, _ := a.StateHash()
	applyAll(t, NewApplier(a, nil), genRange(31, 40), 7)
	if err := a.Reopen(filepath.Join(base, "cp")); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	applied, got, err := a.StateHash()
	if err != nil || applied != 30 || got != want || a.DurableApplied() != 30 {
		t.Fatalf("after Reopen applied %d durable %d equal %v err %v", applied, a.DurableApplied(), got == want, err)
	}
	// Applying continues from the checkpoint's index.
	applyAll(t, NewApplier(a, nil), genRange(31, 32), 7)
	if a.Applied() != 32 || a.Dir() != filepath.Join(base, "cp") {
		t.Fatalf("applied %d dir %s", a.Applied(), a.Dir())
	}
}
