//go:build !raftkv_mutation

package server

import "context"

// MutationLocalReads reports whether this binary carries the deliberate
// read-path bug of testing.md 3.4. It is false in every normal build.
const MutationLocalReads = false

// linearizableRead serves a LINEARIZABLE Get or Range page: ReadIndex (or a
// valid lease) on this member, then the read (D-05).
func (s *KV) linearizableRead(ctx context.Context, read func() error) error {
	return s.member.LinearizableRead(ctx, read)
}
