package node

import (
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

// WBS 7.5.1: the YAML config file is read from disk at startup. Parsing and
// validating it must never panic, and an accepted config must survive a
// marshal/parse round trip unchanged.
func FuzzParseConfigYAML(f *testing.F) {
	f.Add([]byte(`node_id: 1
cluster_id: 7
data_dir: /var/lib/raftkv
listen_addr: ":7000"
metrics_addr: "127.0.0.1:9100"
peers:
  - {id: 1, addr: "raftkv-0:7000"}
  - {id: 2, addr: "raftkv-1:7000"}
tick_interval: 100ms
election_tick: 10
heartbeat_tick: 1
log_level: debug
max_msg_bytes: 4194304
lease_reads: true
snapshot_entries: 1000
snapshot_retain_entries: 10
wal_segment_bytes: 1048576
`))
	f.Add([]byte("node_id: -1\n"))
	f.Add([]byte("tick_interval: forever\n"))
	f.Add([]byte("unknown_key: 1\n"))
	f.Add([]byte("peers: [{id: 1}]\n---\nnode_id: 2\n"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		cfg, err := ParseConfigYAML(data, DefaultConfig())
		if err != nil {
			return
		}
		_ = cfg.Validate()
		_ = cfg.EffectiveListenAddr()
		out, err := yaml.Marshal(cfg)
		if err != nil {
			t.Fatalf("marshal accepted config: %v", err)
		}
		back, err := ParseConfigYAML(out, DefaultConfig())
		if err != nil {
			t.Fatalf("re-marshalled config no longer parses: %v\n%s", err, out)
		}
		if len(cfg.Peers) == 0 && len(back.Peers) == 0 { // "peers: []" vs absent
			cfg.Peers, back.Peers = nil, nil
		}
		if !reflect.DeepEqual(back, cfg) {
			t.Fatalf("round trip changed the config:\n%+v\n%+v", cfg, back)
		}
	})
}

// Found by FuzzParseConfigYAML: a second YAML document used to be ignored
// silently, so "node_id: 2" after a "---" had no effect.
func TestParseConfigYAMLRejectsSecondDocument(t *testing.T) {
	if _, err := ParseConfigYAML([]byte("node_id: 1\n---\nnode_id: 2\n"), DefaultConfig()); err == nil {
		t.Fatal("a second YAML document was accepted")
	}
	for _, ok := range []string{"", "# only a comment\n", "node_id: 1\n", "---\nnode_id: 1\n"} {
		if _, err := ParseConfigYAML([]byte(ok), DefaultConfig()); err != nil {
			t.Fatalf("%q: %v", ok, err)
		}
	}
}

// FuzzParsePeers: the -peers flag. Never panics; every accepted peer can be
// checked by Validate.
func FuzzParsePeers(f *testing.F) {
	f.Add("1=raftkv-0:7000,2=raftkv-1:7000,3=raftkv-2:7000")
	f.Add(" 1 = a:1 ,, 2=b:2")
	f.Add("x=a:1")
	f.Add("18446744073709551616=a:1")
	f.Fuzz(func(t *testing.T, s string) {
		ps, err := ParsePeers(s)
		if err != nil {
			return
		}
		c := DefaultConfig()
		c.Peers = ps
		_ = c.Validate()
		_ = c.PeerIDs()
	})
}
