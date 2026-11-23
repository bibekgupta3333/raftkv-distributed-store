package node

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// RunUntilSignal runs n until one of sigs arrives (SIGTERM and SIGINT if none
// are given), then shuts it down gracefully and returns Run's result (WBS
// 2.3.5). The handler is installed before the node starts, so a signal can
// never be missed.
func RunUntilSignal(n *Node, sigs ...os.Signal) error {
	if len(sigs) == 0 {
		sigs = []os.Signal{syscall.SIGTERM, syscall.SIGINT}
	}
	ctx, stop := signal.NotifyContext(context.Background(), sigs...)
	defer stop()
	return n.Run(ctx)
}
