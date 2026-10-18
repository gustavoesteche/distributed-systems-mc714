// Command node runs one cluster member: config.Load → node.New → Start →
// wait for a signal → Stop.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mc714/project2/internal/config"
	"mc714/project2/internal/logging"
	"mc714/project2/internal/node"
)

func main() {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}
	logger := logging.New(cfg.LogLevel, cfg.NodeID)

	n, err := node.New(cfg, logger)
	if err != nil {
		logger.Error("cannot create node", "event", logging.EventNodeError, "err", err)
		os.Exit(1)
	}
	if err := n.Start(); err != nil {
		logger.Error("cannot start node", "event", logging.EventNodeError, "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.Stop(shutdown); err != nil {
		logger.Error("unclean shutdown", "event", logging.EventNodeError, "err", err)
		os.Exit(1)
	}
}
