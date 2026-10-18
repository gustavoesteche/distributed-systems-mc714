// Package logging configures the node's structured logger and holds the
// names of every event the system logs.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// Event names. Every log line carries one of them as event=<name>.
const (
	EventConfiguredNeighbors = "configured_neighbors"
	EventPeerConnected       = "gossip_peer_connected"
	EventPeerUnreachable     = "gossip_peer_unreachable"
	EventPeerReconnected     = "gossip_peer_reconnected"
	EventAntiEntropySuccess  = "anti_entropy_success"
	EventAntiEntropyFailed   = "anti_entropy_failed"
	EventOpsReceived         = "ops_received"
	EventOpCreated           = "op_created"
	EventConcurrentOps       = "concurrent_ops_detected"
	EventOpRejected          = "op_rejected"
	EventNodeStarted         = "node_started"
	EventNodeStopped         = "node_stopped"
	EventNodeError           = "node_error"
)

// ParseLevel maps debug, info, warn and error to slog levels.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return slog.LevelInfo, fmt.Errorf("unknown log level %q", s)
}

// New returns a text logger on stdout whose lines all carry node=<nodeID>.
// An unknown level falls back to info; config validates it beforehand.
func New(level, nodeID string) *slog.Logger {
	return NewWithWriter(os.Stdout, level, nodeID)
}

// NewWithWriter is New with an explicit destination.
func NewWithWriter(w io.Writer, level, nodeID string) *slog.Logger {
	lvl, err := ParseLevel(level)
	if err != nil {
		lvl = slog.LevelInfo
	}
	h := slog.NewTextHandler(w, &slog.HandlerOptions{Level: lvl})
	return slog.New(h).With("node", nodeID)
}
