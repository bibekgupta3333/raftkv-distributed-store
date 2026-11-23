//go:build raftkv_mutation

package server

import "context"

// MutationLocalReads reports whether this binary carries the deliberate
// read-path bug of testing.md 3.4. With -tags raftkv_mutation it is true.
const MutationLocalReads = true

// linearizableRead is DELIBERATELY BROKEN in raftkv_mutation builds
// (testing.md 3.4): it skips ReadIndex (D-05) and serves "linearizable" reads
// from local state on any member, so a partitioned follower or a deposed
// leader answers with stale data. The linearizability checker must report a
// violation against this build; if it does not, the checked run proves
// nothing. Never part of a normal build.
func (s *KV) linearizableRead(_ context.Context, read func() error) error {
	return read()
}
