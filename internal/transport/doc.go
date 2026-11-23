// Package transport is RaftKV's gRPC peer transport (D-12, D-36).
//
// # Shape
//
// Leader-to-follower traffic (MsgApp, MsgHeartbeat) rides one long-lived
// bidirectional Replicate stream per peer, opened by the sender. The follower's
// replies (MsgAppResp, MsgHeartbeatResp) travel back on the same stream, so
// per-stream order is preserved. Votes and pre-votes use the unary RequestVote
// RPC; the transport converts its response into a MsgVoteResp or MsgPreVoteResp
// and hands it to the node like any other inbound message. A follower's
// ReadIndex request (MsgReadIndex, D-05) uses the unary ReadIndex RPC the same
// way; a failed call comes back as a rejected MsgReadIndexResp so the reader
// retries instead of waiting out its deadline.
//
// # Contract with the driver (D-35)
//
// The transport never calls the Raft core. Inbound messages go to a [Handler]
// (the node's inbox), outbound messages arrive through [Transport.Send] after
// the driver has persisted them, and failures are reported through
// [Config.OnUnreachable] so the driver can call RawNode.ReportUnreachable.
// Every queue is bounded; overflow drops the message, which Raft retries
// (docs/architecture.md section 5). Nothing here is needed for safety: a
// dropped, duplicated or reordered message cannot break Raft.
//
// # Flow control (D-29)
//
// The core already caps unacknowledged appends per follower; the transport
// enforces the same cap (MaxInflight, default 256) independently so a buggy or
// slow follower can never make the sender buffer without bound.
package transport
