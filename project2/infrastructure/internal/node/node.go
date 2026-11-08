// Package node wires config, topology, replica, engine, HTTP server and
// client into one running node, and owns its lifecycle.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mc714/project2/internal/cart"
	"mc714/project2/internal/config"
	"mc714/project2/internal/logging"
	"mc714/project2/internal/metrics"
	"mc714/project2/internal/replication"
	"mc714/project2/internal/topology"
	"mc714/project2/internal/transport"
)

// Node is one incarnation of a cluster member.
type Node struct {
	cfg       config.Config
	logger    *slog.Logger
	neighbors []string
	replicaID string
	replica   *cart.Replica
	engine    *replication.Engine
	metrics   *metrics.Registry
	server    *http.Server

	mu       sync.Mutex
	listener net.Listener
	cancel   context.CancelFunc
	loopDone chan struct{}
}

// lastBoot makes incarnation IDs strictly increasing within a process, so
// two nodes started in the same nanosecond (as in tests) still differ.
var lastBoot atomic.Int64

func newReplicaID(nodeID string) string {
	for {
		prev := lastBoot.Load()
		now := time.Now().UnixNano()
		if now <= prev {
			now = prev + 1
		}
		if lastBoot.CompareAndSwap(prev, now) {
			return fmt.Sprintf("%s@%d", nodeID, now)
		}
	}
}

// New computes the topology, logs the neighbors and creates a new
// incarnation with an empty replica. Every call gets a fresh replica ID.
func New(cfg config.Config, logger *slog.Logger) (*Node, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	g, err := topology.Generate(cfg.NodeCount, cfg.MaxNeighbors, cfg.ExtraEdges, cfg.TopologySeed)
	if err != nil {
		return nil, fmt.Errorf("topology: %w", err)
	}
	if cfg.NodeIndex < 1 || cfg.NodeIndex > g.Size() || topology.NodeName(cfg.NodeIndex) != cfg.NodeID {
		return nil, fmt.Errorf("node %q is not part of the %d-node topology", cfg.NodeID, g.Size())
	}
	neighbors := g.Neighbors(cfg.NodeID)
	logger.Info("configured neighbors", "event", logging.EventConfiguredNeighbors,
		"neighbors", "["+strings.Join(neighbors, ",")+"]")

	n := &Node{cfg: cfg, logger: logger, neighbors: neighbors, replicaID: newReplicaID(cfg.NodeID),
		metrics: metrics.New()}
	n.replica = cart.NewReplica(cfg.NodeID, n.replicaID, logger)
	n.replica.SetMetrics(n.metrics)
	peers := make([]replication.Peer, len(neighbors))
	for i, id := range neighbors {
		peers[i] = replication.Peer{ID: id, URL: cfg.PeerURL(id)}
	}
	transportClient := transport.NewHTTPTransport(cfg.HTTPTimeout)
	transportClient.SetMetrics(n.metrics)
	n.engine = replication.NewEngine(replication.EngineConfig{
		NodeID:           cfg.NodeID,
		Replica:          n.replica,
		Peers:            peers,
		Transport:        transportClient,
		Interval:         cfg.GossipInterval,
		FailureThreshold: cfg.FailureThreshold,
		Logger:           logger,
		Metrics:          n.metrics,
	})
	n.server = &http.Server{
		Handler:           transport.NewServer(n.replica, n.engine, n.metrics),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	return n, nil
}

// Start listens on HTTP_ADDR, logs node_started, then starts the gossip loop.
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.listener != nil {
		return errors.New("node already started")
	}
	ln, err := net.Listen("tcp", n.cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", n.cfg.HTTPAddr, err)
	}
	n.listener = ln
	go func() {
		if err := n.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			n.logger.Error("http server failed", "event", logging.EventNodeError, "err", err)
		}
	}()
	n.logger.Info("node started", "event", logging.EventNodeStarted, "replica", n.replicaID, "addr", ln.Addr().String())

	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	n.loopDone = make(chan struct{})
	go func() {
		defer close(n.loopDone)
		n.engine.Run(ctx)
	}()
	return nil
}

// Stop ends the gossip loop, then shuts the HTTP server down.
func (n *Node) Stop(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.listener == nil {
		return errors.New("node not started")
	}
	n.cancel()
	select {
	case <-n.loopDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Shutdown also waits on idle peer connections; force-close after 1s.
	grace, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := n.server.Shutdown(grace)
	if err != nil && ctx.Err() == nil {
		err = n.server.Close()
	}
	n.logger.Info("node stopped", "event", logging.EventNodeStopped, "replica", n.replicaID)
	return err
}

// Addr returns the real listen address once started (it resolves ":0").
func (n *Node) Addr() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.listener == nil {
		return n.cfg.HTTPAddr
	}
	return n.listener.Addr().String()
}

// PauseGossip stops outgoing rounds (test hook); it waits for a round in flight.
func (n *Node) PauseGossip() { n.engine.Pause() }

// ResumeGossip restarts outgoing rounds (test hook).
func (n *Node) ResumeGossip() { n.engine.Resume() }

// Replica returns the node's replica.
func (n *Node) Replica() *cart.Replica { return n.replica }

// ReplicaID returns this incarnation's ID.
func (n *Node) ReplicaID() string { return n.replicaID }

// Neighbors returns the configured neighbors, sorted.
func (n *Node) Neighbors() []string { return append([]string(nil), n.neighbors...) }
