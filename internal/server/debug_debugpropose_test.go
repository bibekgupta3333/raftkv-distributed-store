//go:build debugpropose

package server_test

import (
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/bibekgupta3333/raftkv-distributed-store/internal/command"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/kvtest"
	"github.com/bibekgupta3333/raftkv-distributed-store/internal/node"
)

// Regression for the e2e suite after M3: a raw payload sent through the debug
// Propose RPC used to be committed and then crash every replica in Apply
// (D-33). It must be refused up front, and a valid encoded command must still
// commit and be readable through the KV API.
func TestDebugProposeRejectsUndecodablePayload(t *testing.T) {
	c := kvtest.New(t, 3, kvtest.Options{})
	lead := c.Leader(15 * time.Second)
	conn, err := grpc.NewClient("passthrough:///"+c.Addr(lead), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx := ctx5(t)

	if _, err := node.DebugPropose(ctx, conn, []byte("smoke")); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("raw payload: err = %v, want InvalidArgument", err)
	}
	cmd, err := command.Put(0, 0, []byte("k"), []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	data, err := command.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	idx, err := node.DebugPropose(ctx, conn, data)
	if err != nil || idx == 0 {
		t.Fatalf("encoded Put: idx %d, err %v", idx, err)
	}
	g, err := newClient(t, c).Get(ctx, []byte("k"))
	if err != nil || !g.Found || string(g.KV.Value) != "v" || g.KV.ModRevision != idx {
		t.Fatalf("Get after debug Put = %+v, %v (want value v at revision %d)", g, err, idx)
	}
}
