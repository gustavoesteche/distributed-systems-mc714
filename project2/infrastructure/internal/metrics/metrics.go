package metrics

import (
	"sort"
	"sync"
	"sync/atomic"
)

// Contadores (JSON de metricas)
const (
	OpsCreated            = "ops_created_total"
	OpsApplied            = "ops_applied_total"
	OpsDuplicated         = "ops_duplicated_total"
	OpsBuffered           = "ops_buffered_total"
	OpsRejected           = "ops_rejected_total"
	ConcurrentOpsDetected = "concurrent_ops_detected_total"

	GossipRounds          = "gossip_rounds_total"
	GossipExchanges       = "gossip_exchanges_total"
	GossipExchangesFailed = "gossip_exchanges_failed_total"
	OpsPulled             = "ops_pulled_total"
	OpsPushed             = "ops_pushed_total"

	MessagesSent     = "messages_sent_total"
	MessagesReceived = "messages_received_total"
	BytesSent        = "bytes_sent_total"
	BytesReceived    = "bytes_received_total"

	PeerConnected    = "peer_connected_total"
	PeerReconnected  = "peer_reconnected_total"
	PeerUnreachable  = "peer_unreachable_total"
	GossipPullServed = "gossip_pull_served_total"
	GossipPushServed = "gossip_push_served_total"
)

// registro de metricas race-safe
type Registry struct {
	mu       sync.RWMutex
	counters map[string]*atomic.Uint64
}

func New() *Registry {
	return &Registry{counters: map[string]*atomic.Uint64{}}
}

// provisionamento lazy do contador
func (r *Registry) counter(name string) *atomic.Uint64 {
	r.mu.RLock()
	c := r.counters[name]
	r.mu.RUnlock()
	if c != nil {
		return c
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if c = r.counters[name]; c == nil {
		c = &atomic.Uint64{}
		r.counters[name] = c
	}
	return c
}

func (r *Registry) Inc(name string) { r.Add(name, 1) }

func (r *Registry) Add(name string, n uint64) {
	if r == nil {
		return
	}
	r.counter(name).Add(n)
}

func (r *Registry) Snapshot() map[string]uint64 {
	if r == nil {
		return map[string]uint64{}
	}
	r.mu.RLock()
	names := make([]string, 0, len(r.counters))
	for name := range r.counters {
		names = append(names, name)
	}
	r.mu.RUnlock()
	sort.Strings(names)

	out := make(map[string]uint64, len(names))
	for _, name := range names {
		out[name] = r.counter(name).Load()
	}
	return out
}

func (r *Registry) Value(name string) uint64 {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	c := r.counters[name]
	r.mu.RUnlock()
	if c == nil {
		return 0
	}
	return c.Load()
}
