package replication

import (
	"fmt"
	"testing"

	"mc714/project2/internal/logging"
)

func TestPeerMonitorTransitions(t *testing.T) {
	const S, F = true, false
	type step struct {
		in    bool
		event string
		state PeerState
	}
	cases := []struct {
		name      string
		threshold int
		steps     []step
	}{
		{"unknown+success -> reachable (connected)", 3, []step{
			{S, logging.EventPeerConnected, Reachable},
		}},
		{"unknown+failures below threshold stay unknown silently", 3, []step{
			{F, "", Unknown}, {F, "", Unknown},
		}},
		{"unknown+failure at threshold -> unreachable", 3, []step{
			{F, "", Unknown}, {F, "", Unknown}, {F, logging.EventPeerUnreachable, Unreachable},
		}},
		{"reachable+success stays reachable and resets count", 3, []step{
			{S, logging.EventPeerConnected, Reachable},
			{F, "", Reachable}, {F, "", Reachable},
			{S, "", Reachable},
			{F, "", Reachable}, {F, "", Reachable},
			{F, logging.EventPeerUnreachable, Unreachable},
		}},
		{"reachable+failures below threshold stay reachable", 3, []step{
			{S, logging.EventPeerConnected, Reachable}, {F, "", Reachable}, {F, "", Reachable},
		}},
		{"reachable+failure at threshold -> unreachable", 3, []step{
			{S, logging.EventPeerConnected, Reachable},
			{F, "", Reachable}, {F, "", Reachable}, {F, logging.EventPeerUnreachable, Unreachable},
		}},
		{"unreachable+failure never repeats", 3, []step{
			{F, "", Unknown}, {F, "", Unknown}, {F, logging.EventPeerUnreachable, Unreachable},
			{F, "", Unreachable}, {F, "", Unreachable}, {F, "", Unreachable}, {F, "", Unreachable},
		}},
		{"unreachable+success -> reachable (reconnected)", 3, []step{
			{F, "", Unknown}, {F, "", Unknown}, {F, logging.EventPeerUnreachable, Unreachable},
			{S, logging.EventPeerReconnected, Reachable},
			{S, "", Reachable},
			{F, "", Reachable}, {F, "", Reachable}, {F, logging.EventPeerUnreachable, Unreachable},
			{S, logging.EventPeerReconnected, Reachable},
		}},
		{"threshold 1", 1, []step{
			{F, logging.EventPeerUnreachable, Unreachable}, {F, "", Unreachable},
			{S, logging.EventPeerReconnected, Reachable}, {F, logging.EventPeerUnreachable, Unreachable},
		}},
	}
	for _, c := range cases {
		m := NewMonitor(c.threshold)
		if m.State() != Unknown || m.Failures() != 0 {
			t.Fatalf("%s: initial state %v/%d", c.name, m.State(), m.Failures())
		}
		counts := map[string]int{}
		wantCounts := map[string]int{}
		consecutive := 0
		for i, s := range c.steps {
			ev := m.Observe(s.in)
			if ev != s.event || m.State() != s.state {
				t.Fatalf("%s: step %d (%v): got (%q, %v), want (%q, %v)", c.name, i, s.in, ev, m.State(), s.event, s.state)
			}
			if s.in {
				consecutive = 0
			} else {
				consecutive++
			}
			if m.Failures() != consecutive {
				t.Fatalf("%s: step %d: failures %d, want %d", c.name, i, m.Failures(), consecutive)
			}
			if ev != "" {
				counts[ev]++
			}
			if s.event != "" {
				wantCounts[s.event]++
			}
		}
		if fmt.Sprint(counts) != fmt.Sprint(wantCounts) {
			t.Fatalf("%s: events %v, want %v", c.name, counts, wantCounts)
		}
	}
	for s, want := range map[PeerState]string{Unknown: "unknown", Reachable: "reachable", Unreachable: "unreachable"} {
		if s.String() != want {
			t.Errorf("%d.String() = %q, want %q", int(s), s.String(), want)
		}
	}
}
