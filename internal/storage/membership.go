package storage

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	raftkvv1 "github.com/bibekgupta3333/raftkv-distributed-store/api/raftkv/v1"
)

// keyMembership holds the applied member table (D-06, WBS 6.1.6): written in
// the apply batch of every conf-change entry, so it is always consistent with
// appliedIndex (D-04), and it travels inside every snapshot checkpoint.
var keyMembership = metaKey("/membership")

func readMembership(get func([]byte) ([]byte, func() error, error)) (*raftkvv1.ClusterConfig, error) {
	v, closer, err := get(keyMembership)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer() }()
	cfg := &raftkvv1.ClusterConfig{}
	if err := proto.Unmarshal(v, cfg); err != nil {
		return nil, fmt.Errorf("storage: corrupt membership record: %w", err)
	}
	return cfg, nil
}

// Membership returns the member table as of the applied index, or nil if no
// conf-change entry was ever applied (the bootstrap configuration is then in
// effect).
func (s *Store) Membership() (*raftkvv1.ClusterConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return readMembership(func(k []byte) ([]byte, func() error, error) {
		v, c, err := s.db.Get(k)
		if err != nil {
			return nil, nil, err
		}
		return v, c.Close, nil
	})
}

// Membership returns the member table including this batch's own writes.
func (a *ApplyBatch) Membership() (*raftkvv1.ClusterConfig, error) {
	return readMembership(func(k []byte) ([]byte, func() error, error) {
		v, c, err := a.b.Get(k)
		if err != nil {
			return nil, nil, err
		}
		return v, c.Close, nil
	})
}

// SetMembership stages the member table established by the entry declared by
// the last [ApplyBatch.Next].
func (a *ApplyBatch) SetMembership(cfg *raftkvv1.ClusterConfig) error {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(cfg)
	if err != nil {
		return err
	}
	return a.b.Set(keyMembership, b, nil)
}
