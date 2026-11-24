package node_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
)

const goodYAML = `
node_id: 2
cluster_id: 9
data_dir: /var/lib/raftkv
metrics_addr: ":9100"
tick_interval: 50ms
peers:
  - {id: 1, addr: "n1:7000"}
  - {id: 2, addr: "n2:7000"}
  - {id: 3, addr: "n3:7000"}
`

func writeFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "raftkv.yaml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// WBS 2.3.3: defaults < YAML < flags.
func TestLoadConfigLayering(t *testing.T) {
	path := writeFile(t, goodYAML)
	cfg, err := node.LoadConfig([]string{"-config", path, "-node-id", "3", "-log-level", "debug"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NodeID != 3 { // flag beats YAML
		t.Errorf("NodeID = %d, want 3 (flag overrides yaml)", cfg.NodeID)
	}
	if cfg.ClusterID != 9 || cfg.TickInterval != 50*time.Millisecond || cfg.DataDir != "/var/lib/raftkv" {
		t.Errorf("yaml values lost: %+v", cfg)
	}
	if cfg.ElectionTick != 10 || cfg.HeartbeatTick != 1 { // D-08 defaults survive
		t.Errorf("defaults lost: election=%d heartbeat=%d", cfg.ElectionTick, cfg.HeartbeatTick)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q", cfg.LogLevel)
	}
	if got := cfg.EffectiveListenAddr(); got != ":7000" {
		t.Errorf("EffectiveListenAddr = %q, want :7000 derived from the node's own peer address", got)
	}
	if !reflect.DeepEqual(cfg.PeerIDs(), []uint64{1, 2, 3}) {
		t.Errorf("PeerIDs = %v", cfg.PeerIDs())
	}
}

func TestLoadConfigFromFlagsOnly(t *testing.T) {
	cfg, err := node.LoadConfig([]string{
		"-node-id", "1", "-data-dir", "/d", "-listen", "0.0.0.0:7001",
		"-peers", "1=a:7001, 2=b:7002", "-tick-interval", "20ms",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Peers) != 2 || cfg.Peers[1] != (node.PeerConfig{ID: 2, Addr: "b:7002"}) || cfg.ListenAddr != "0.0.0.0:7001" {
		t.Fatalf("flags not applied: %+v", cfg)
	}
}

func TestLoadConfigErrors(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		yaml    string
		wantErr string
	}{
		{"missing everything", nil, "", "node_id must be non-zero"},
		{"unknown yaml key", nil, "node_idd: 1\n", "field node_idd not found"},
		{"node not in peers", []string{"-node-id", "9", "-data-dir", "/d", "-peers", "1=a:1"}, "", "node_id 9 is not listed in peers"},
		{"duplicate peer", []string{"-node-id", "1", "-data-dir", "/d", "-peers", "1=a:1,1=b:2"}, "", "duplicate peer id 1"},
		{"bad peer addr", []string{"-node-id", "1", "-data-dir", "/d", "-peers", "1=nohostport"}, "", "peer 1 addr"},
		{"bad peers flag", []string{"-peers", "garbage"}, "", "must look like ID=host:port"},
		{"bad peer id", []string{"-peers", "x=a:1"}, "", "bad id"},
		{"election <= heartbeat", []string{"-node-id", "1", "-data-dir", "/d", "-peers", "1=a:1", "-election-tick", "1"}, "", "election_tick (1) must be greater"},
		{"bad log level", []string{"-node-id", "1", "-data-dir", "/d", "-peers", "1=a:1", "-log-level", "loud"}, "", "log_level"},
		{"zero tick", []string{"-node-id", "1", "-data-dir", "/d", "-peers", "1=a:1", "-tick-interval", "0s"}, "", "tick_interval must be positive"},
		{"bad listen", []string{"-node-id", "1", "-data-dir", "/d", "-peers", "1=a:1", "-listen", "nope"}, "", "listen_addr"},
		{"unknown flag", []string{"-nope"}, "", "flag provided but not defined"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := tt.args
			if tt.yaml != "" {
				args = append([]string{"-config", writeFile(t, tt.yaml)}, args...)
			}
			_, err := node.LoadConfig(args, io.Discard)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
	if _, err := node.LoadConfig([]string{"-config", "/nonexistent.yaml"}, io.Discard); err == nil {
		t.Error("a missing config file must be an error")
	}
}

// WBS 2.3.4: the node ID is written to the data dir and a mismatch is refused.
func TestIdentityBootstrapAndMismatch(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	want := node.Identity{NodeID: 2, ClusterID: 9}

	first, err := node.EnsureIdentity(dir, want)
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	if first.NodeID != 2 || first.ClusterID != 9 || first.Incarnation == 0 {
		t.Fatalf("first identity = %+v, want a random incarnation", first)
	}
	got, ok, err := node.ReadIdentity(dir)
	if err != nil || !ok || got != first {
		t.Fatalf("ReadIdentity = %+v, %v, %v", got, ok, err)
	}
	again, err := node.EnsureIdentity(dir, want)
	if err != nil {
		t.Fatalf("restart with the same identity: %v", err)
	}
	if again != first {
		t.Fatalf("restart changed the identity: %+v -> %+v", first, again)
	}
	for _, bad := range []node.Identity{{NodeID: 3, ClusterID: 9}, {NodeID: 2, ClusterID: 10}} {
		if _, err := node.EnsureIdentity(dir, bad); !errors.Is(err, node.ErrIdentityMismatch) {
			t.Errorf("EnsureIdentity(%+v) = %v, want ErrIdentityMismatch", bad, err)
		}
	}
}

// D-32: WAL data with no identity file means the owner is unknown; refuse.
func TestIdentityRefusesWALWithoutIdentity(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, node.WALDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, node.WALDir, "wal-0000000000000001-0000000000000001.seg"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := node.EnsureIdentity(dir, node.Identity{NodeID: 1, ClusterID: 1}); !errors.Is(err, node.ErrIdentityMismatch) {
		t.Fatalf("err = %v, want ErrIdentityMismatch", err)
	}
}

// D-32 (WBS 6.4.2): a data directory that kept its identity file but lost its
// WAL is refused: the node forgot its votes and acknowledged entries.
func TestIdentityRefusesLostState(t *testing.T) {
	dir := t.TempDir()
	if _, err := node.EnsureIdentity(dir, node.Identity{NodeID: 1, ClusterID: 1}); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(dir, node.WALDir)); err != nil {
		t.Fatal(err)
	}
	if _, err := node.EnsureIdentity(dir, node.Identity{NodeID: 1, ClusterID: 1}); !errors.Is(err, node.ErrStateLost) {
		t.Fatalf("err = %v, want ErrStateLost", err)
	}
}

// D-32: a wiped data directory gets a new incarnation, which is how peers
// recognise a member that came back empty under its old ID.
func TestIdentityNewIncarnationAfterWipe(t *testing.T) {
	dir := t.TempDir()
	a, err := node.EnsureIdentity(dir, node.Identity{NodeID: 1, ClusterID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	b, err := node.EnsureIdentity(dir, node.Identity{NodeID: 1, ClusterID: 1})
	if err != nil {
		t.Fatal(err)
	}
	if a.Incarnation == b.Incarnation {
		t.Fatal("a recreated data directory kept its incarnation")
	}
}

// An identity file written before M6 gets an incarnation on the next start.
func TestIdentityUpgradeAddsIncarnation(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, node.WALDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, node.IdentityFile), []byte(`{"node_id":1,"cluster_id":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := node.EnsureIdentity(dir, node.Identity{NodeID: 1, ClusterID: 1})
	if err != nil || got.Incarnation == 0 {
		t.Fatalf("got %+v, %v", got, err)
	}
	if again, _ := node.EnsureIdentity(dir, node.Identity{NodeID: 1, ClusterID: 1}); again.Incarnation != got.Incarnation {
		t.Fatal("the incarnation was not persisted")
	}
}

func TestIdentityCorruptFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, node.IdentityFile), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := node.EnsureIdentity(dir, node.Identity{NodeID: 1, ClusterID: 1}); err == nil {
		t.Fatal("a corrupt identity file must be refused, not overwritten")
	}
}

// node.New refuses a data dir that belongs to another node.
func TestNewRefusesIdentityMismatch(t *testing.T) {
	cfg := node.DefaultConfig()
	cfg.NodeID, cfg.DataDir = 1, t.TempDir()
	cfg.Peers = []node.PeerConfig{{ID: 1, Addr: "127.0.0.1:0"}, {ID: 2, Addr: "127.0.0.1:0"}}
	n, err := node.New(cfg, node.Options{LogWriter: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n.Close() }()
	cfg.NodeID = 2 // same data dir, different node
	if _, err := node.New(cfg, node.Options{LogWriter: io.Discard}); !errors.Is(err, node.ErrIdentityMismatch) {
		t.Fatalf("node.New with a foreign data dir = %v, want ErrIdentityMismatch", err)
	}
}

// WBS 7.1.3: the node ID comes from the StatefulSet pod ordinal (ordinal + 1).
func TestNodeIDFromOrdinal(t *testing.T) {
	for name, want := range map[string]uint64{"raftkv-0": 1, "raftkv-4": 5, "my-set-12": 13} {
		got, err := node.NodeIDFromOrdinal(name)
		if err != nil || got != want {
			t.Errorf("NodeIDFromOrdinal(%q) = %d, %v; want %d", name, got, err, want)
		}
	}
	for _, bad := range []string{"", "raftkv", "raftkv-", "-3", "raftkv-x", "raftkv-+1", "raftkv-18446744073709551615"} {
		if _, err := node.NodeIDFromOrdinal(bad); err == nil {
			t.Errorf("NodeIDFromOrdinal(%q) succeeded, want an error", bad)
		}
	}
}

func TestLoadConfigNodeIDFromOrdinal(t *testing.T) {
	path := writeFile(t, strings.Replace(goodYAML, "node_id: 2\n", "", 1))
	cfg, err := node.LoadConfig([]string{"-config", path, "-node-id-from-ordinal", "raftkv-2"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.NodeID != 3 {
		t.Errorf("NodeID = %d, want 3", cfg.NodeID)
	}
	_, err = node.LoadConfig([]string{"-config", path, "-node-id", "1", "-node-id-from-ordinal", "raftkv-2"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("both -node-id and -node-id-from-ordinal: err = %v, want mutually exclusive", err)
	}
	if _, err := node.LoadConfig([]string{"-config", path, "-node-id-from-ordinal", "raftkv"}, io.Discard); err == nil {
		t.Error("a pod name without an ordinal must be refused")
	}
}
