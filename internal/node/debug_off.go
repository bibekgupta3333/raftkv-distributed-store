//go:build !debugpropose

package node

import "google.golang.org/grpc"

// registerDebug is a no-op in normal builds: the debug Propose RPC exists only
// when built with -tags debugpropose (WBS 2.3.2), so a production binary has no
// way to inject raw log entries.
func registerDebug(*grpc.Server, *Node) {}
