package wal

import (
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// op is one record written by the random workload.
type op struct {
	isHS bool
	hs   HardState
	e    Entry
}

// model replays the first k ops independently of the WAL implementation.
func model(ops []op) ([]Entry, HardState) {
	var entries []Entry
	var hs HardState
	for _, o := range ops {
		if o.isHS {
			hs = o.hs
			continue
		}
		for len(entries) > 0 && entries[len(entries)-1].Index >= o.e.Index {
			entries = entries[:len(entries)-1]
		}
		entries = append(entries, o.e)
	}
	return entries, hs
}

func randomWorkload(r *rand.Rand) []op {
	var ops []op
	var last uint64
	n := 5 + r.Intn(25)
	for len(ops) < n {
		if r.Intn(4) == 0 {
			// Commit stays at 1 so later conflicts (index >= 2) never touch it.
			ops = append(ops, op{isHS: true, hs: HardState{Term: uint64(r.Intn(5) + 1), Vote: uint64(r.Intn(4)), Commit: 1}})
			continue
		}
		idx := last + 1
		if last >= 3 && r.Intn(4) == 0 {
			idx = 2 + uint64(r.Intn(int(last)-1)) // conflicting rewrite
		}
		data := make([]byte, r.Intn(40))
		r.Read(data)
		ops = append(ops, op{e: Entry{Index: idx, Term: uint64(r.Intn(5) + 1), Type: uint32(r.Intn(3)), Data: data}})
		last = idx
	}
	return ops
}

func writeOps(t *testing.T, dir string, ops []op) {
	t.Helper()
	w, _ := mustOpen(t, dir, Options{})
	for _, o := range ops {
		if o.isHS {
			mustNil(t, w.SetHardState(o.hs))
		} else {
			mustNil(t, w.AppendEntries([]Entry{o.e}))
		}
	}
	mustNil(t, w.Sync())
	mustNil(t, w.Close())
}

// recordOffsets returns the start offset of each record in a single segment.
func recordOffsets(t *testing.T, data []byte) []int {
	t.Helper()
	var offs []int
	for off := segmentHeaderSize; off < len(data); {
		_, n, err := DecodeRecord(data[off:])
		mustNil(t, err)
		offs = append(offs, off)
		off += n
	}
	return offs
}

func equalState(gotE []Entry, gotHS HardState, wantE []Entry, wantHS HardState) bool {
	if len(gotE) != len(wantE) {
		return false
	}
	for i := range gotE {
		if gotE[i].Index != wantE[i].Index || gotE[i].Term != wantE[i].Term || gotE[i].Type != wantE[i].Type ||
			string(gotE[i].Data) != string(wantE[i].Data) {
			return false
		}
	}
	return gotHS == wantHS
}

// TestCrashMatrix damages every byte offset of the last two records, both by
// truncation (crash) and by corruption (bit rot), over many random logs. The
// WAL must either refuse to start with ErrCorrupt or recover exactly a prefix
// of the written records; it must never return altered data (D-33, D-18).
func TestCrashMatrix(t *testing.T) {
	logs := 20
	if testing.Short() {
		logs = 8
	}
	r := rand.New(rand.NewSource(1))
	cases, refused := 0, 0
	for l := 0; l < logs; l++ {
		ops := randomWorkload(r)
		master := t.TempDir()
		writeOps(t, master, ops)
		segName := segFiles(t, master)[0]
		orig, err := os.ReadFile(filepath.Join(master, segName))
		mustNil(t, err)
		offs := recordOffsets(t, orig)
		if len(offs) != len(ops) {
			t.Fatalf("records %d != ops %d", len(offs), len(ops))
		}
		ends := append(append([]int(nil), offs[1:]...), len(orig))
		secondLast := offs[len(offs)-2]

		for pos := secondLast; pos < len(orig); pos++ {
			// k = number of records fully intact before the damage at pos.
			k := 0
			for k < len(offs) && ends[k] <= pos {
				k++
			}
			for _, mode := range []string{"truncate", "corrupt"} {
				dir := t.TempDir()
				path := filepath.Join(dir, segName)
				mustNil(t, os.WriteFile(path, orig, 0o644))
				ffs := NewFaultFS(OSFS{})
				if mode == "truncate" {
					mustNil(t, ffs.TruncateFile(path, int64(pos)))
				} else {
					mustNil(t, ffs.CorruptByte(path, int64(pos), byte(1+r.Intn(255))))
				}
				cases++
				w, rec, err := Open(dir, Options{})
				if err != nil {
					if !errors.Is(err, ErrCorrupt) {
						t.Fatalf("log %d %s@%d: unexpected error %v", l, mode, pos, err)
					}
					if mode == "truncate" {
						t.Fatalf("log %d: pure truncation must be repairable: %v", l, err)
					}
					if k == len(offs)-1 {
						t.Fatalf("log %d: damage confined to the last record must be repairable: %v", l, err)
					}
					refused++
					continue
				}
				// Recovered state must equal replaying exactly k records.
				wantE, wantHS := model(ops[:k])
				if !equalState(rec.Entries, rec.HardState, wantE, wantHS) {
					t.Fatalf("log %d %s@%d: silent divergence: got %d entries hs=%+v, want %d entries hs=%+v (k=%d)",
						l, mode, pos, len(rec.Entries), rec.HardState, len(wantE), wantHS, k)
				}
				mustNil(t, w.Close())
				// Repaired log must reopen identically with nothing left to cut.
				w2, rec2, err := Open(dir, Options{})
				if err != nil || rec2.TruncatedBytes != 0 || !reflect.DeepEqual(rec2.Entries, rec.Entries) {
					t.Fatalf("log %d %s@%d: reopen after repair: err=%v", l, mode, pos, err)
				}
				mustNil(t, w2.Close())
			}
		}
	}
	if cases == 0 || refused == 0 {
		t.Fatalf("matrix did not exercise both outcomes: cases=%d refused=%d", cases, refused)
	}
	t.Logf("crash matrix: %d cases, %d refused to start, 0 silent corruptions", cases, refused)
}
