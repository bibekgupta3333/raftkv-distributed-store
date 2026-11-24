package raft

import (
	"strings"
	"testing"
)

func TestConfigValidate(t *testing.T) {
	good := func() Config {
		return DefaultConfig(1, []uint64{1, 2, 3}, NewMemoryStorage(), testRand(1))
	}
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string // substring; empty means valid
	}{
		{"defaults are valid", func(*Config) {}, ""},
		{"zero ID", func(c *Config) { c.ID = 0 }, "ID must be non-zero"},
		{"no bootstrap voters (joining node, D-32)", func(c *Config) { c.Peers = nil }, ""},
		{"zero peer", func(c *Config) { c.Peers = []uint64{1, 0} }, "zero ID"},
		{"duplicate peer", func(c *Config) { c.Peers = []uint64{1, 2, 2} }, "duplicate peer 2"},
		{"ID not in peers (a learner-to-be)", func(c *Config) { c.ID = 9 }, ""},
		{"zero heartbeat", func(c *Config) { c.HeartbeatTick = 0 }, "HeartbeatTick"},
		{"election <= heartbeat", func(c *Config) { c.ElectionTick = c.HeartbeatTick }, "ElectionTick"},
		{"nil storage", func(c *Config) { c.Storage = nil }, "Storage"},
		{"nil rand", func(c *Config) { c.Rand = nil }, "Rand"},
		{"zero inflight", func(c *Config) { c.MaxInflight = 0 }, "MaxInflight"},
		{"unlimited uncommitted is ok", func(c *Config) { c.MaxUncommittedBytes = 0 }, ""},
		{"single node", func(c *Config) { c.Peers = []uint64{1} }, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := good()
			tt.mutate(&cfg)
			err := cfg.Validate()
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestDefaultConfigFollowsDecisions(t *testing.T) {
	c := DefaultConfig(1, []uint64{1}, NewMemoryStorage(), testRand(1))
	if !c.PreVote || !c.CheckQuorum {
		t.Error("D-08: PreVote and CheckQuorum must default on")
	}
	if c.MaxUncommittedBytes != 64<<20 {
		t.Errorf("D-29: MaxUncommittedBytes = %d, want 64 MiB", c.MaxUncommittedBytes)
	}
	if c.MaxInflight != 256 {
		t.Errorf("D-29: MaxInflight = %d, want 256", c.MaxInflight)
	}
	if c.ElectionTick != 10 || c.HeartbeatTick != 1 {
		t.Errorf("D-08: ticks = %d/%d, want 10/1", c.ElectionTick, c.HeartbeatTick)
	}
}

func TestNewRawNodeRejectsBadStartState(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*MemoryStorage, *Config)
	}{
		{"invalid config", func(_ *MemoryStorage, c *Config) { c.ID = 0 }},
		{"commit beyond log", func(s *MemoryStorage, _ *Config) { s.SetHardState(HardState{Term: 1, Commit: 5}) }},
		{"applied beyond commit", func(s *MemoryStorage, c *Config) {
			s.Append([]Entry{{Term: 1, Index: 1}})
			s.SetHardState(HardState{Term: 1, Commit: 0})
			c.Applied = 1
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := NewMemoryStorage()
			cfg := DefaultConfig(1, []uint64{1}, st, testRand(1))
			tt.setup(st, &cfg)
			if _, err := NewRawNode(cfg); err == nil {
				t.Fatal("NewRawNode succeeded, want error")
			}
		})
	}
}

func TestRawNodeRestartsFromStorage(t *testing.T) {
	st := storageWith(3, 1, 2, 3)
	st.SetHardState(HardState{Term: 3, Vote: 2, Commit: 2})
	cfg := baseConfig(1, []uint64{1, 2, 3}, st)
	cfg.Applied = 1
	rn, err := NewRawNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := rn.Status()
	if s.Term != 3 || s.Vote != 2 || s.Commit != 2 || s.Applied != 1 || s.LastIndex != 3 || s.State != StateFollower {
		t.Fatalf("unexpected status after restart: %+v", s)
	}
	if rn.HasReady() == false {
		t.Fatal("entry 2 is committed but unapplied: expected a Ready")
	}
	rd := rn.Ready()
	if len(rd.CommittedEntries) != 1 || rd.CommittedEntries[0].Index != 2 {
		t.Fatalf("CommittedEntries = %+v, want only index 2", rd.CommittedEntries)
	}
	if !rd.HardState.IsEmpty() || len(rd.Entries) != 0 {
		t.Fatalf("restart must not re-emit persisted state: %+v", rd)
	}
}
