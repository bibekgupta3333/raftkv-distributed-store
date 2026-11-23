package node

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/observability"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/raft"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/transport"
)

// PeerConfig is one member of the static cluster (WBS 2.3.4).
type PeerConfig struct {
	ID   uint64 `yaml:"id"`
	Addr string `yaml:"addr"`
}

// Config is a node's configuration: defaults, overridden by a YAML file,
// overridden by command-line flags (WBS 2.3.3). Call [Config.Validate] before
// use; [LoadConfig] does.
type Config struct {
	NodeID    uint64 `yaml:"node_id"`
	ClusterID uint64 `yaml:"cluster_id"`
	DataDir   string `yaml:"data_dir"`
	// ListenAddr is the peer gRPC listen address. Empty means ":<port of this
	// node's own entry in Peers>".
	ListenAddr string `yaml:"listen_addr"`
	// MetricsAddr serves /metrics; empty disables it.
	MetricsAddr string `yaml:"metrics_addr"`
	// Peers is the static membership including this node (D-06 comes in M6).
	Peers []PeerConfig `yaml:"peers"`

	// TickInterval is the wall-clock length of one logical Raft tick (D-24).
	// With the default 100 ms and ElectionTick 10, elections time out in
	// 1-2 s and heartbeats go out every 100 ms (D-08).
	TickInterval  time.Duration `yaml:"tick_interval"`
	ElectionTick  int           `yaml:"election_tick"`
	HeartbeatTick int           `yaml:"heartbeat_tick"`

	LogLevel    string `yaml:"log_level"`
	MaxMsgBytes int    `yaml:"max_msg_bytes"`
}

// DefaultConfig returns the D-08 / D-29 defaults.
func DefaultConfig() Config {
	return Config{
		ClusterID:     1,
		TickInterval:  100 * time.Millisecond,
		ElectionTick:  raft.DefaultElectionTick,
		HeartbeatTick: raft.DefaultHeartbeatTick,
		LogLevel:      "info",
		MaxMsgBytes:   transport.DefaultMaxMsgBytes,
	}
}

// Validate reports every problem it finds, joined, so an operator fixes a
// config in one pass.
func (c Config) Validate() error {
	var errs []error
	bad := func(format string, args ...any) { errs = append(errs, fmt.Errorf("config: "+format, args...)) }

	if c.NodeID == 0 {
		bad("node_id must be non-zero")
	}
	if c.ClusterID == 0 {
		bad("cluster_id must be non-zero")
	}
	if strings.TrimSpace(c.DataDir) == "" {
		bad("data_dir is required")
	}
	if len(c.Peers) == 0 {
		bad("peers must not be empty")
	}
	seen := map[uint64]bool{}
	self := false
	for _, p := range c.Peers {
		switch {
		case p.ID == 0:
			bad("peer ID must be non-zero")
		case seen[p.ID]:
			bad("duplicate peer id %d", p.ID)
		}
		seen[p.ID] = true
		if _, _, err := net.SplitHostPort(p.Addr); err != nil {
			bad("peer %d addr %q: %v", p.ID, p.Addr, err)
		}
		self = self || p.ID == c.NodeID
	}
	if len(c.Peers) > 0 && c.NodeID != 0 && !self {
		bad("node_id %d is not listed in peers", c.NodeID)
	}
	if c.ListenAddr != "" {
		if _, _, err := net.SplitHostPort(c.ListenAddr); err != nil {
			bad("listen_addr %q: %v", c.ListenAddr, err)
		}
	}
	if c.MetricsAddr != "" {
		if _, _, err := net.SplitHostPort(c.MetricsAddr); err != nil {
			bad("metrics_addr %q: %v", c.MetricsAddr, err)
		}
	}
	if c.TickInterval <= 0 {
		bad("tick_interval must be positive")
	}
	if c.HeartbeatTick <= 0 {
		bad("heartbeat_tick must be positive")
	}
	if c.ElectionTick <= c.HeartbeatTick {
		bad("election_tick (%d) must be greater than heartbeat_tick (%d)", c.ElectionTick, c.HeartbeatTick)
	}
	if _, ok := observability.ParseLevel(c.LogLevel); !ok {
		bad("log_level %q must be debug, info, warn or error", c.LogLevel)
	}
	if c.MaxMsgBytes <= 0 {
		bad("max_msg_bytes must be positive")
	}
	return errors.Join(errs...)
}

// PeerAddrs returns node ID -> address for every member.
func (c Config) PeerAddrs() map[uint64]string {
	m := make(map[uint64]string, len(c.Peers))
	for _, p := range c.Peers {
		m[p.ID] = p.Addr
	}
	return m
}

// PeerIDs returns the member IDs in ascending order.
func (c Config) PeerIDs() []uint64 {
	ids := make([]uint64, 0, len(c.Peers))
	for _, p := range c.Peers {
		ids = append(ids, p.ID)
	}
	slices.Sort(ids)
	return ids
}

// EffectiveListenAddr is ListenAddr, or ":<port>" derived from this node's own
// peer address.
func (c Config) EffectiveListenAddr() string {
	if c.ListenAddr != "" {
		return c.ListenAddr
	}
	if _, port, err := net.SplitHostPort(c.PeerAddrs()[c.NodeID]); err == nil {
		return ":" + port
	}
	return ""
}

// ParseConfigYAML decodes a YAML document over base. Unknown keys are errors,
// so a typo cannot silently fall back to a default.
func ParseConfigYAML(data []byte, base Config) (Config, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&base); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("config: parse yaml: %w", err)
	}
	return base, nil
}

// ParsePeers parses "1=host:port,2=host:port".
func ParsePeers(s string) ([]PeerConfig, error) {
	var out []PeerConfig
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idStr, addr, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("config: peer %q must look like ID=host:port", part)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(idStr), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("config: peer %q: bad id: %w", part, err)
		}
		out = append(out, PeerConfig{ID: id, Addr: strings.TrimSpace(addr)})
	}
	return out, nil
}

// NodeIDFromOrdinal maps a StatefulSet pod name ("<set>-<ordinal>") to a node
// ID: ordinal + 1, because ID 0 means "none" in Raft. The pod name, and so the
// ID, is stable across restarts and rescheduling, and the pod keeps its PVC, so
// the data directory's identity check (D-32) keeps passing (D-16, WBS 7.1.3).
func NodeIDFromOrdinal(podName string) (uint64, error) {
	i := strings.LastIndexByte(podName, '-')
	if i <= 0 || i == len(podName)-1 {
		return 0, fmt.Errorf("config: pod name %q has no -<ordinal> suffix", podName)
	}
	ord, err := strconv.ParseUint(podName[i+1:], 10, 64)
	if err != nil || ord == math.MaxUint64 {
		return 0, fmt.Errorf("config: pod name %q: bad ordinal %q", podName, podName[i+1:])
	}
	return ord + 1, nil
}

func isFlagSet(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(f *flag.Flag) { set = set || f.Name == name })
	return set
}

// LoadConfig builds a validated Config from command-line args: defaults, then
// the YAML file named by -config, then any flag that was explicitly set.
// Flag usage and parse errors go to out.
func LoadConfig(args []string, out io.Writer) (Config, error) {
	cfg := DefaultConfig()
	fs := flag.NewFlagSet("raftkvd", flag.ContinueOnError)
	fs.SetOutput(out)
	path := fs.String("config", "", "path to a YAML config file")
	nodeID := fs.Uint64("node-id", 0, "this node's ID")
	clusterID := fs.Uint64("cluster-id", 0, "cluster ID")
	dataDir := fs.String("data-dir", "", "data directory (WAL, node identity)")
	listen := fs.String("listen", "", "peer gRPC listen address")
	metrics := fs.String("metrics-addr", "", "metrics HTTP listen address (empty disables)")
	peers := fs.String("peers", "", "static members as ID=host:port,ID=host:port")
	tick := fs.Duration("tick-interval", 0, "wall-clock length of one Raft tick")
	electionTick := fs.Int("election-tick", 0, "election timeout base, in ticks")
	heartbeatTick := fs.Int("heartbeat-tick", 0, "heartbeat interval, in ticks")
	logLevel := fs.String("log-level", "", "debug, info, warn or error")
	fromOrdinal := fs.String("node-id-from-ordinal", "",
		"derive the node ID from a StatefulSet pod name: 1 + its ordinal (raftkv-2 -> 3)")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	if *path != "" {
		data, err := os.ReadFile(*path)
		if err != nil {
			return Config{}, fmt.Errorf("config: %w", err)
		}
		if cfg, err = ParseConfigYAML(data, cfg); err != nil {
			return Config{}, err
		}
	}

	var flagErr error
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "node-id":
			cfg.NodeID = *nodeID
		case "cluster-id":
			cfg.ClusterID = *clusterID
		case "data-dir":
			cfg.DataDir = *dataDir
		case "listen":
			cfg.ListenAddr = *listen
		case "metrics-addr":
			cfg.MetricsAddr = *metrics
		case "peers":
			ps, err := ParsePeers(*peers)
			if err != nil {
				flagErr = err
				return
			}
			cfg.Peers = ps
		case "tick-interval":
			cfg.TickInterval = *tick
		case "election-tick":
			cfg.ElectionTick = *electionTick
		case "heartbeat-tick":
			cfg.HeartbeatTick = *heartbeatTick
		case "log-level":
			cfg.LogLevel = *logLevel
		case "node-id-from-ordinal":
			if isFlagSet(fs, "node-id") {
				flagErr = errors.New("config: -node-id and -node-id-from-ordinal are mutually exclusive")
				return
			}
			id, err := NodeIDFromOrdinal(*fromOrdinal)
			if err != nil {
				flagErr = err
				return
			}
			cfg.NodeID = id
		}
	})
	if flagErr != nil {
		return Config{}, flagErr
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
