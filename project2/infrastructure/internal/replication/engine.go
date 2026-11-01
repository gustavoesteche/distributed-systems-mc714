package replication

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"mc714/project2/internal/cart"
	"mc714/project2/internal/logging"
	"mc714/project2/internal/metrics"
)

// ErrForbidden is returned for gossip from a node that is not a neighbor.
var ErrForbidden = errors.New("forbidden: sender is not a configured neighbor")

// EngineConfig wires an Engine.
type EngineConfig struct {
	NodeID           string
	Replica          *cart.Replica
	Peers            []Peer
	Transport        Transport
	Interval         time.Duration
	FailureThreshold int
	Logger           *slog.Logger
	Metrics          *metrics.Registry
}

// PeerResult is the outcome of one exchange in a round.
type PeerResult struct {
	Peer    string
	Pulled  int // ops received in the pull response
	Applied int // ops applied locally during the pull's Apply
	Pushed  int // ops sent in the push (0 when nothing was missing)
	Err     error
}

// PeerStatus is a neighbor's monitor state, for /debug/state.
type PeerStatus struct {
	ID                  string `json:"id"`
	URL                 string `json:"url"`
	State               string `json:"state"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
}

// Engine runs anti-entropy rounds and answers gossip requests.
type Engine struct {
	nodeID    string
	replica   *cart.Replica
	peers     []Peer // sorted by ID
	neighbors map[string]bool
	transport Transport
	interval  time.Duration
	logger    *slog.Logger
	metrics   *metrics.Registry

	monMu    sync.Mutex
	monitors map[string]*Monitor

	roundMu sync.Mutex // held by Run for the duration of each round
	paused  atomic.Bool
}

// NewEngine builds an engine. Only cfg.Peers may gossip with it.
func NewEngine(cfg EngineConfig) *Engine {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	peers := append([]Peer(nil), cfg.Peers...)
	sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })
	e := &Engine{
		nodeID:    cfg.NodeID,
		replica:   cfg.Replica,
		peers:     peers,
		neighbors: map[string]bool{},
		transport: cfg.Transport,
		interval:  cfg.Interval,
		logger:    logger,
		metrics:   cfg.Metrics,
		monitors:  map[string]*Monitor{},
	}
	for _, p := range peers {
		e.neighbors[p.ID] = true
		e.monitors[p.ID] = NewMonitor(cfg.FailureThreshold)
	}
	return e
}

// Round runs one push-pull exchange with every neighbor in parallel.
func (e *Engine) Round(ctx context.Context) []PeerResult {
	e.metrics.Inc(metrics.GossipRounds)
	e.metrics.Add(metrics.GossipExchanges, uint64(len(e.peers)))
	results := make([]PeerResult, len(e.peers))
	var wg sync.WaitGroup
	for i, p := range e.peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = e.exchange(ctx, p)
		}()
	}
	wg.Wait()
	return results
}

// exchange pulls what this replica lacks from p, then pushes what p lacks.
func (e *Engine) exchange(ctx context.Context, p Peer) PeerResult {
	res := PeerResult{Peer: p.ID}
	res.Err = func() error {
		resp, err := e.transport.Pull(ctx, p.URL, PullRequest{
			SenderNode: e.nodeID, SenderReplica: e.replica.ReplicaID(), Clock: e.replica.Clock(),
		})
		if err != nil {
			return fmt.Errorf("pull: %w", err)
		}
		if resp.Node != p.ID {
			return fmt.Errorf("pull: answered by %q, expected %q", resp.Node, p.ID)
		}
		res.Pulled = len(resp.Ops)
		e.metrics.Add(metrics.OpsPulled, uint64(res.Pulled))
		applied, err := e.replica.Apply(resp.Ops)
		if err != nil {
			return fmt.Errorf("apply pulled ops: %w", err)
		}
		res.Applied = applied.Applied
		missing := e.replica.Delta(resp.Clock)
		if len(missing) == 0 {
			return nil
		}
		if _, err := e.transport.Push(ctx, p.URL, PushRequest{
			SenderNode: e.nodeID, SenderReplica: e.replica.ReplicaID(), Ops: missing,
		}); err != nil {
			return fmt.Errorf("push: %w", err)
		}
		res.Pushed = len(missing)
		e.metrics.Add(metrics.OpsPushed, uint64(res.Pushed))
		return nil
	}()

	if res.Err != nil && ctx.Err() != nil {
		return res // shutting down: not the peer's fault
	}
	if res.Err != nil {
		e.metrics.Inc(metrics.GossipExchangesFailed)
		e.logger.Warn("anti-entropy failed", "event", logging.EventAntiEntropyFailed, "peer", p.ID, "err", res.Err)
	} else {
		level := slog.LevelDebug
		if res.Applied > 0 || res.Pushed > 0 {
			level = slog.LevelInfo
		}
		e.logger.Log(ctx, level, "anti-entropy exchange", "event", logging.EventAntiEntropySuccess, "peer", p.ID,
			"applied", res.Applied, "pushed", res.Pushed)
	}
	e.observe(p.ID, res.Err == nil)
	return res
}

func (e *Engine) observe(peer string, ok bool) {
	e.monMu.Lock()
	m := e.monitors[peer]
	ev := m.Observe(ok)
	failures := m.Failures()
	e.monMu.Unlock()
	switch ev {
	case logging.EventPeerConnected:
		e.metrics.Inc(metrics.PeerConnected)
		e.logger.Info("peer connected", "event", ev, "peer", peer)
	case logging.EventPeerUnreachable:
		e.metrics.Inc(metrics.PeerUnreachable)
		e.logger.Warn("peer unreachable", "event", ev, "peer", peer, "failures", failures)
	case logging.EventPeerReconnected:
		e.metrics.Inc(metrics.PeerReconnected)
		e.logger.Info("peer reconnected", "event", ev, "peer", peer)
	}
}

// Run calls Round immediately and then every interval, skipping ticks while
// paused, until ctx is done.
func (e *Engine) Run(ctx context.Context) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		e.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (e *Engine) tick(ctx context.Context) {
	e.roundMu.Lock()
	defer e.roundMu.Unlock()
	if e.paused.Load() || ctx.Err() != nil {
		return
	}
	e.Round(ctx)
}

// Pause stops Run's outgoing rounds. It returns once any round in flight has
// finished, so no exchange starts or completes after it returns. Incoming
// gossip is still answered.
func (e *Engine) Pause() {
	e.paused.Store(true)
	e.roundMu.Lock()
	e.roundMu.Unlock() // empty critical section: only waits for the in-flight round
}

// Resume lets Run's rounds continue.
func (e *Engine) Resume() { e.paused.Store(false) }

func (e *Engine) checkSender(sender string) error {
	if !e.neighbors[sender] {
		return fmt.Errorf("%w: %q", ErrForbidden, sender)
	}
	return nil
}

// HandlePull answers a neighbor with this replica's clock and the ops the
// neighbor's clock does not cover.
func (e *Engine) HandlePull(ctx context.Context, req PullRequest) (PullResponse, error) {
	if err := e.checkSender(req.SenderNode); err != nil {
		return PullResponse{}, err
	}
	e.metrics.Inc(metrics.GossipPullServed)
	ops, clk := e.replica.DeltaAndClock(req.Clock)
	return PullResponse{Node: e.nodeID, Replica: e.replica.ReplicaID(), Clock: clk, Ops: ops}, nil
}

// HandlePush merges a neighbor's ops through Replica.Apply. A structurally
// invalid batch returns an error wrapping cart.ErrInvalidOp and changes nothing.
func (e *Engine) HandlePush(ctx context.Context, req PushRequest) (PushResponse, error) {
	if err := e.checkSender(req.SenderNode); err != nil {
		return PushResponse{}, err
	}
	e.metrics.Inc(metrics.GossipPushServed)
	res, err := e.replica.Apply(req.Ops)
	if err != nil {
		return PushResponse{}, err
	}
	if res.Applied > 0 {
		e.logger.Info("ops received", "event", logging.EventOpsReceived, "peer", req.SenderNode, "applied", res.Applied)
	}
	return PushResponse{Applied: res.Applied, Duplicates: res.Duplicates, Buffered: res.Buffered, Rejected: res.Rejected}, nil
}

// Neighbors returns the configured neighbor IDs, sorted.
func (e *Engine) Neighbors() []string {
	ids := make([]string, len(e.peers))
	for i, p := range e.peers {
		ids[i] = p.ID
	}
	return ids
}

// PeerStatuses returns every neighbor's monitor state, sorted by ID.
func (e *Engine) PeerStatuses() []PeerStatus {
	e.monMu.Lock()
	defer e.monMu.Unlock()
	out := make([]PeerStatus, len(e.peers))
	for i, p := range e.peers {
		m := e.monitors[p.ID]
		out[i] = PeerStatus{ID: p.ID, URL: p.URL, State: m.State().String(), ConsecutiveFailures: m.Failures()}
	}
	return out
}
