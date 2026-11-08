// Package replication runs epidemic anti-entropy: periodic vector-clock
// digest push-pull rounds with every configured neighbor.
package replication

import (
	"context"

	"mc714/project2/internal/cart"
	"mc714/project2/internal/clock"
)

// PullRequest asks a peer for the ops the sender's clock does not cover.
type PullRequest struct {
	SenderNode    string   `json:"sender_node"`
	SenderReplica string   `json:"sender_replica"`
	Clock         clock.VC `json:"clock"`
}

// PullResponse carries the peer's clock and Delta(req.Clock).
type PullResponse struct {
	Node    string           `json:"node"`
	Replica string           `json:"replica"`
	Clock   clock.VC         `json:"clock"`
	Ops     []cart.Operation `json:"ops"`
}

// PushRequest sends a peer the ops it lacks.
type PushRequest struct {
	SenderNode    string           `json:"sender_node"`
	SenderReplica string           `json:"sender_replica"`
	Ops           []cart.Operation `json:"ops"`
}

// PushResponse reports what the receiver's Apply did.
type PushResponse struct {
	Applied    int `json:"applied"`
	Duplicates int `json:"duplicates"`
	Buffered   int `json:"buffered"`
	Rejected   int `json:"rejected"`
}

// Transport carries pull and push requests to a peer.
type Transport interface {
	Pull(ctx context.Context, peerURL string, req PullRequest) (PullResponse, error)
	Push(ctx context.Context, peerURL string, req PushRequest) (PushResponse, error)
}

// Peer is a configured neighbor.
type Peer struct{ ID, URL string }
