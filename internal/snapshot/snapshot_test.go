package snapshot

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/command"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/storage"
)

// node is a test data directory: the state machine plus a snapshot manager.
type node struct {
	t     *testing.T
	data  string
	store *storage.Store
	mgr   *Manager
	next  uint64 // next log index to apply
}

func openNode(t *testing.T, data string, hook func(string) error) *node {
	t.Helper()
	sm := filepath.Join(data, "sm")
	live, err := storage.PrepareStateDir(sm)
	if err != nil {
		t.Fatal(err)
	}
	st, err := storage.Open(storage.Options{Dir: live})
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := Open(Config{
		Dir: filepath.Join(data, "snap"), StateDir: sm, Store: st, ClusterID: 7,
		Members: []*raftkvv1.Member{{Id: 1, Role: raftkvv1.MemberRole_VOTER}}, Hook: hook,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &node{t: t, data: data, store: st, mgr: mgr, next: st.Applied() + 1}
}

func (n *node) close() {
	if err := n.store.Close(); err != nil {
		n.t.Fatal(err)
	}
}

// put applies count puts with values of size bytes, in batches of 10.
func (n *node) put(count, size int) {
	n.t.Helper()
	a := storage.NewApplier(n.store, nil)
	var batch []raft.Entry
	for range count {
		val := make([]byte, size)
		r := rand.New(rand.NewPCG(n.next, 1)) // incompressible, so SSTs are large
		for i := range val {
			val[i] = byte(r.Uint32())
		}
		cmd, err := command.Put(n.next, 0, fmt.Appendf(nil, "k%05d", n.next%500), val)
		if err != nil {
			n.t.Fatal(err)
		}
		copy(cmd.GetPut().Value, fmt.Appendf(nil, "v%d", n.next))
		data, _ := command.Marshal(cmd)
		batch = append(batch, raft.Entry{Index: n.next, Term: 1 + n.next/100, Data: data})
		n.next++
		if len(batch) == 10 {
			if err := a.Apply(batch); err != nil {
				n.t.Fatal(err)
			}
			batch = nil
		}
	}
	if len(batch) > 0 {
		if err := a.Apply(batch); err != nil {
			n.t.Fatal(err)
		}
	}
}

func (n *node) hash() (uint64, [sha256.Size]byte) {
	n.t.Helper()
	a, h, err := n.store.StateHash()
	if err != nil {
		n.t.Fatal(err)
	}
	return a, h
}

// WBS 5.2.1: metadata round trip and tamper detection.
func TestMetaRoundTrip(t *testing.T) {
	m := &Meta{
		ClusterId: 7, LastIncludedIndex: 100, LastIncludedTerm: 3,
		Config: &raftkvv1.ClusterConfig{Members: []*raftkvv1.Member{{Id: 1}, {Id: 2}}},
		Files:  []*raftkvv1.SnapshotFile{{Name: "000001.sst", Size: 10, Crc32C: 0xabc}},
	}
	Seal(m)
	if m.GetFormatVersion() != FormatVersion || m.GetChecksum() == 0 {
		t.Fatalf("Seal did not stamp version/checksum: %+v", m)
	}
	dir := t.TempDir()
	if err := WriteMeta(dir, m); err != nil {
		t.Fatal(err)
	}
	got, err := ReadMeta(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, m) {
		t.Fatalf("round trip: got %v, want %v", got, m)
	}
	// Per-transfer fields are not covered.
	wire, _ := proto.Clone(m).(*Meta)
	wire.LeaderId, wire.Term = 9, 9
	if err := Verify(wire); err != nil {
		t.Fatalf("leader_id/term changed the checksum: %v", err)
	}
	for name, tamper := range map[string]func(*Meta){
		"index":   func(m *Meta) { m.LastIncludedIndex++ },
		"term":    func(m *Meta) { m.LastIncludedTerm++ },
		"members": func(m *Meta) { m.Config.Members[0].Id = 5 },
		"filecrc": func(m *Meta) { m.Files[0].Crc32C++ },
	} {
		bad, _ := proto.Clone(m).(*Meta)
		tamper(bad)
		if err := Verify(bad); !errors.Is(err, ErrChecksum) {
			t.Errorf("tampered %s: err = %v, want ErrChecksum", name, err)
		}
	}
	bad, _ := proto.Clone(m).(*Meta)
	bad.FormatVersion = 2
	if err := Verify(bad); !errors.Is(err, ErrFormat) {
		t.Errorf("format 2: err = %v", err)
	}
	bad, _ = proto.Clone(m).(*Meta)
	bad.Files[0].Name = "../x"
	Seal(bad)
	if err := Verify(bad); err == nil {
		t.Error("a path in a file name must be rejected")
	}
}

// WBS 5.2.2: the checkpoint's appliedIndex equals the applied index it was
// taken at, read back from the checkpoint itself.
func TestCreateCheckpointMatchesApplied(t *testing.T) {
	n := openNode(t, t.TempDir(), nil)
	defer n.close()
	n.put(57, 32)
	meta, err := n.mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	if meta.GetLastIncludedIndex() != n.store.Applied() || meta.GetLastIncludedTerm() != n.store.AppliedTerm() {
		t.Fatalf("snapshot %d/%d, applied %d/%d", meta.GetLastIncludedIndex(), meta.GetLastIncludedTerm(), n.store.Applied(), n.store.AppliedTerm())
	}
	// Open the snapshot directory itself: same applied position, same state.
	ro, err := storage.Open(storage.Options{Dir: n.mgr.Dir(meta), ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ro.Close() }()
	if ro.Applied() != 57 {
		t.Fatalf("checkpoint appliedIndex = %d, want 57", ro.Applied())
	}
	_, want := n.hash()
	_, got, err := ro.StateHash()
	if err != nil || got != want {
		t.Fatalf("checkpoint state differs from the live state (err %v)", err)
	}
	if err := Verify(meta); err != nil {
		t.Fatal(err)
	}
	// Every listed file exists with its recorded size and CRC.
	for _, f := range meta.GetFiles() {
		size, crc, err := fileCRC(filepath.Join(n.mgr.Dir(meta), f.GetName()))
		if err != nil || uint64(size) != f.GetSize() || crc != f.GetCrc32C() {
			t.Fatalf("file %s: size %d crc %08x err %v, meta %+v", f.GetName(), size, crc, err, f)
		}
	}
	// Taking another snapshot without new applies returns the same one.
	again, err := n.mgr.Create()
	if err != nil || again.GetLastIncludedIndex() != 57 {
		t.Fatalf("Create without new applies = %v, %v", again, err)
	}
}

// WBS 5.2.3: keep the last 2, clean up temp dirs on start.
func TestRetentionAndStartupCleanup(t *testing.T) {
	data := t.TempDir()
	n := openNode(t, data, nil)
	for range 4 {
		n.put(5, 8)
		if _, err := n.mgr.Create(); err != nil {
			t.Fatal(err)
		}
	}
	list := n.mgr.List()
	if len(list) != 2 || list[0].GetLastIncludedIndex() != 15 || list[1].GetLastIncludedIndex() != 20 {
		t.Fatalf("kept %d snapshots: %v", len(list), indexes(list))
	}
	if names := dirNames(t, filepath.Join(data, "snap")); len(names) != 2 {
		t.Fatalf("snap/ holds %v, want the 2 newest", names)
	}
	// A pinned snapshot (an outgoing transfer) survives retention.
	pinned, release, err := n.mgr.acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	n.put(5, 8)
	if _, err := n.mgr.Create(); err != nil {
		t.Fatal(err)
	}
	n.put(5, 8)
	if _, err := n.mgr.Create(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(n.mgr.Dir(pinned)); err != nil {
		t.Fatalf("pinned snapshot deleted: %v", err)
	}
	release()
	if _, err := os.Stat(n.mgr.Dir(pinned)); !os.IsNotExist(err) {
		t.Fatalf("released snapshot kept: %v", err)
	}
	n.close()

	// Leftovers of crashes: a half-made snapshot, a half-received transfer,
	// a complete-looking dir whose metadata is missing.
	snap := filepath.Join(data, "snap")
	for _, d := range []string{"tmp-0000000000000099", "incoming/0000000000000099-0000000000000001", "00000000000000ff-0000000000000001"} {
		if err := os.MkdirAll(filepath.Join(snap, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	n = openNode(t, data, nil)
	defer n.close()
	got := dirNames(t, snap)
	if len(got) != 2 || strings.HasPrefix(got[0], "tmp-") {
		t.Fatalf("after restart snap/ holds %v", got)
	}
	if l := n.mgr.Latest(); l == nil || l.GetLastIncludedIndex() != 30 {
		t.Fatalf("Latest after restart = %v", l)
	}
}

func indexes(ms []*Meta) []uint64 {
	var out []uint64
	for _, m := range ms {
		out = append(out, m.GetLastIncludedIndex())
	}
	return out
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	es, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range es {
		out = append(out, e.Name())
	}
	return out
}

// pipe collects what Send produces so Receive can consume it.
type pipe struct {
	msgs []*raftkvv1.InstallSnapshotRequest
}

func (p *pipe) send(m *raftkvv1.InstallSnapshotRequest) error {
	p.msgs = append(p.msgs, proto.Clone(m).(*raftkvv1.InstallSnapshotRequest))
	return nil
}

func (p *pipe) recv(from int) (*Meta, RecvFunc) {
	i := from + 1
	return p.msgs[from].GetMetadata(), func() (*raftkvv1.InstallSnapshotRequest, error) {
		if i >= len(p.msgs) {
			return nil, io.EOF
		}
		i++
		return p.msgs[i-1], nil
	}
}

// leaderWithSnapshot builds a node with enough data for multi-chunk files.
func leaderWithSnapshot(t *testing.T) (*node, *Meta) {
	t.Helper()
	n := openNode(t, t.TempDir(), nil)
	n.put(400, 8<<10) // ~3 MiB of values, so SSTs span several 1 MiB chunks
	meta, err := n.mgr.Create()
	if err != nil {
		t.Fatal(err)
	}
	return n, meta
}

// WBS 5.4.1-5.4.3: chunked transfer, staging, install; the follower ends with
// the leader's state.
func TestSendReceiveInstall(t *testing.T) {
	leader, meta := leaderWithSnapshot(t)
	defer leader.close()
	_, wantHash := leader.hash()

	var p pipe
	sent, err := leader.mgr.Send(0, 1, 5, p.send)
	if err != nil {
		t.Fatal(err)
	}
	if sent.GetLastIncludedIndex() != meta.GetLastIncludedIndex() {
		t.Fatalf("sent %d, want %d", sent.GetLastIncludedIndex(), meta.GetLastIncludedIndex())
	}
	if md := p.msgs[0].GetMetadata(); md == nil || md.GetLeaderId() != 1 || md.GetTerm() != 5 {
		t.Fatalf("first message must be the metadata with leader and term, got %v", p.msgs[0])
	}
	multi := false
	for _, m := range p.msgs[1:] {
		c := m.GetChunk()
		if len(c.GetData()) > ChunkSize {
			t.Fatalf("chunk of %d bytes > 1 MiB", len(c.GetData()))
		}
		multi = multi || c.GetOffset() > 0
	}
	if !multi || !p.msgs[len(p.msgs)-1].GetChunk().GetLast() {
		t.Fatalf("want a multi-chunk file and a final last chunk (%d messages)", len(p.msgs))
	}

	follower := openNode(t, t.TempDir(), nil)
	follower.put(3, 8) // some old state to replace
	md, recv := p.recv(0)
	staged, err := follower.mgr.Receive(md, recv)
	if err != nil {
		t.Fatal(err)
	}
	if err := follower.mgr.Install(staged); err != nil {
		t.Fatal(err)
	}
	applied, gotHash := follower.hash()
	if applied != meta.GetLastIncludedIndex() || gotHash != wantHash {
		t.Fatalf("after install applied %d (want %d), hash equal %v", applied, meta.GetLastIncludedIndex(), gotHash == wantHash)
	}
	if names := dirNames(t, filepath.Join(follower.data, "sm")); len(names) != 2 {
		t.Fatalf("sm/ holds %v, want CURRENT and the new db only", names)
	}
	follower.close()
	// It survives a restart.
	follower = openNode(t, follower.data, nil)
	defer follower.close()
	if applied, h := follower.hash(); applied != meta.GetLastIncludedIndex() || h != wantHash {
		t.Fatalf("after restart applied %d, hash equal %v", applied, h == wantHash)
	}
}

// WBS 5.4.2: a corrupt chunk is rejected, a chunk out of position is
// rejected, a stream that breaks leaves nothing behind, and the next transfer
// starts over from chunk 0 and succeeds (D-14: no resume).
func TestReceiveRejectsAndRestarts(t *testing.T) {
	leader, _ := leaderWithSnapshot(t)
	defer leader.close()
	var p pipe
	if _, err := leader.mgr.Send(0, 1, 5, p.send); err != nil {
		t.Fatal(err)
	}
	follower := openNode(t, t.TempDir(), nil)
	defer follower.close()
	incoming := filepath.Join(follower.data, "snap", incomingDir)

	cases := []struct {
		name   string
		mutate func(msgs []*raftkvv1.InstallSnapshotRequest) []*raftkvv1.InstallSnapshotRequest
		want   error
	}{
		{"corrupt chunk", func(ms []*raftkvv1.InstallSnapshotRequest) []*raftkvv1.InstallSnapshotRequest {
			c := ms[2].GetChunk()
			if len(c.Data) > 0 {
				c.Data[0] ^= 1
			} else {
				c.Crc32C++
			}
			return ms
		}, ErrChecksum},
		{"wrong offset", func(ms []*raftkvv1.InstallSnapshotRequest) []*raftkvv1.InstallSnapshotRequest {
			ms[2].GetChunk().Offset += 7
			return ms
		}, ErrPosition},
		{"skipped chunk", func(ms []*raftkvv1.InstallSnapshotRequest) []*raftkvv1.InstallSnapshotRequest {
			return append(ms[:2:2], ms[3:]...)
		}, ErrPosition},
		{"killed transfer", func(ms []*raftkvv1.InstallSnapshotRequest) []*raftkvv1.InstallSnapshotRequest {
			return ms[:len(ms)/2]
		}, ErrPosition},
	}
	for _, tc := range cases {
		var q pipe
		for _, m := range p.msgs {
			_ = q.send(m)
		}
		q.msgs = tc.mutate(q.msgs)
		md, recv := q.recv(0)
		if _, err := follower.mgr.Receive(md, recv); !errors.Is(err, tc.want) {
			t.Fatalf("%s: err = %v, want %v", tc.name, err, tc.want)
		}
		if names, _ := os.ReadDir(incoming); len(names) != 0 {
			t.Fatalf("%s: staging left behind: %v", tc.name, names)
		}
	}
	// A broken transport error (not EOF) is handled the same way.
	md, recv := p.recv(0)
	n := 0
	failing := func() (*raftkvv1.InstallSnapshotRequest, error) {
		if n++; n == 3 {
			return nil, errors.New("connection reset")
		}
		return recv()
	}
	if _, err := follower.mgr.Receive(md, failing); err == nil {
		t.Fatal("a broken stream must fail")
	}
	// The leader starts over from chunk 0: a full transfer succeeds.
	md, recv = p.recv(0)
	staged, err := follower.mgr.Receive(md, recv)
	if err != nil {
		t.Fatalf("restarted transfer: %v", err)
	}
	if err := follower.mgr.Install(staged); err != nil {
		t.Fatal(err)
	}
	_, want := leader.hash()
	if _, got := follower.hash(); got != want {
		t.Fatal("state differs after the restarted transfer")
	}
}

// WBS 5.4.4: kill the install at each step; the restarted node holds either
// the old state or the new one, never a mix.
func TestInstallCrashAtEachStep(t *testing.T) {
	leader, meta := leaderWithSnapshot(t)
	defer leader.close()
	_, newHash := leader.hash()
	var p pipe
	if _, err := leader.mgr.Send(0, 1, 5, p.send); err != nil {
		t.Fatal(err)
	}
	crash := errors.New("crash")
	for _, step := range []string{"", StepRenamed, StepSwapped, StepReopened, StepCleaned} {
		t.Run("after-"+step, func(t *testing.T) {
			data := t.TempDir()
			f := openNode(t, data, func(s string) error {
				if s == step {
					return crash
				}
				return nil
			})
			f.put(25, 16)
			if err := f.store.Flush(); err != nil { // the old state is durable
				t.Fatal(err)
			}
			_, oldHash := f.hash()
			md, recv := p.recv(0)
			staged, err := f.mgr.Receive(md, recv)
			if err != nil {
				t.Fatal(err)
			}
			if step == "" {
				// Crash after staging, before any install step.
				err = crash
			} else {
				err = f.mgr.Install(staged)
			}
			if step != StepCleaned && !errors.Is(err, crash) {
				t.Fatalf("install did not stop at %q: %v", step, err)
			}
			// kill -9: drop the process without a clean shutdown. The
			// store is closed without flushing (Reopen's close), which is
			// what a crash does to the memtable.
			_ = f.store.Abandon()

			r := openNode(t, data, nil)
			defer r.close()
			applied, h := r.hash()
			committed := step != "" && step != StepRenamed
			switch {
			case committed && (applied != meta.GetLastIncludedIndex() || h != newHash):
				t.Fatalf("after the CURRENT swap: applied %d, new state %v", applied, h == newHash)
			case !committed && (applied != 25 || h != oldHash):
				t.Fatalf("before the CURRENT swap: applied %d, old state %v", applied, h == oldHash)
			}
			// No orphan database or staging directory survives a restart.
			if names := dirNames(t, filepath.Join(data, "sm")); len(names) != 2 {
				t.Fatalf("sm/ holds %v", names)
			}
			if _, err := os.Stat(filepath.Join(data, "snap", incomingDir)); !os.IsNotExist(err) {
				t.Fatalf("incoming/ survived the restart: %v", err)
			}
		})
	}
}
