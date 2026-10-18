package replication

import (
	"fmt"

	"mc714/project2/internal/logging"
)

// PeerState is the monitor's view of a neighbor.
type PeerState int

const (
	Unknown PeerState = iota
	Reachable
	Unreachable
)

func (s PeerState) String() string {
	switch s {
	case Unknown:
		return "unknown"
	case Reachable:
		return "reachable"
	case Unreachable:
		return "unreachable"
	}
	return fmt.Sprintf("PeerState(%d)", int(s))
}

// Monitor tracks one neighbor's reachability. The engine serializes calls.
type Monitor struct {
	threshold int
	state     PeerState
	failures  int
}

// NewMonitor starts in Unknown. A threshold below 1 is treated as 1.
func NewMonitor(threshold int) *Monitor {
	if threshold < 1 {
		threshold = 1
	}
	return &Monitor{threshold: threshold}
}

// Observe records the outcome of one exchange and returns the event name to
// log for this transition, or "" if none.
func (m *Monitor) Observe(success bool) string {
	if success {
		m.failures = 0
		switch m.state {
		case Unknown:
			m.state = Reachable
			return logging.EventPeerConnected
		case Unreachable:
			m.state = Reachable
			return logging.EventPeerReconnected
		}
		return ""
	}
	m.failures++
	if m.state != Unreachable && m.failures >= m.threshold {
		m.state = Unreachable
		return logging.EventPeerUnreachable
	}
	return ""
}

// State returns the current state.
func (m *Monitor) State() PeerState { return m.state }

// Failures returns the number of consecutive failures.
func (m *Monitor) Failures() int { return m.failures }
