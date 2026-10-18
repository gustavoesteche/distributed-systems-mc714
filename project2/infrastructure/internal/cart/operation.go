// Package cart holds a shopping-cart replica: operations, units, tombstones,
// causal delivery and anti-entropy deltas.
package cart

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"mc714/project2/internal/clock"
)

// OpType is ADD or REMOVE.
type OpType string

const (
	Add    OpType = "ADD"
	Remove OpType = "REMOVE"
)

// MaxQuantity bounds the units one operation may create or remove.
const MaxQuantity = 10000

var (
	// ErrInvalidOp marks a structurally invalid operation.
	ErrInvalidOp = errors.New("invalid operation")
	// ErrInvalidInput marks a bad client request (empty product, bad quantity).
	ErrInvalidInput = errors.New("invalid input")
	// ErrInsufficient means a remove asked for more units than are visible.
	ErrInsufficient = errors.New("insufficient quantity")
)

// Operation is one replicated cart update.
type Operation struct {
	ID        string   `json:"id"`                // "<origin>:<counter>"
	Origin    string   `json:"origin"`            // replica ID that created the op
	NodeID    string   `json:"node_id"`           // node name; Origin starts with NodeID+"@"
	Counter   uint64   `json:"counter"`           // equals Clock[Origin]
	Type      OpType   `json:"type"`              // ADD or REMOVE
	ProductID string   `json:"product_id"`        // non-empty
	Quantity  int      `json:"quantity"`          // 1 … MaxQuantity
	Clock     clock.VC `json:"clock"`             // origin's VC right after the increment
	Removes   []string `json:"removes,omitempty"` // REMOVE only: the unit IDs it tombstones
}

// OpID formats "<origin>:<counter>".
func OpID(origin string, counter uint64) string {
	return origin + ":" + strconv.FormatUint(counter, 10)
}

// UnitID formats "<opID>/<i>".
func UnitID(opID string, i int) string {
	return opID + "/" + strconv.Itoa(i)
}

// parseOpID splits "<node>@<boot>:<counter>", requiring a non-empty node and
// boot part and a canonical counter ≥ 1.
func parseOpID(s string) (origin string, counter uint64, err error) {
	i := strings.LastIndexByte(s, ':')
	if i < 0 {
		return "", 0, fmt.Errorf("op ID %q has no counter", s)
	}
	origin, c := s[:i], s[i+1:]
	counter, err = strconv.ParseUint(c, 10, 64)
	if err != nil || counter == 0 || strconv.FormatUint(counter, 10) != c {
		return "", 0, fmt.Errorf("op ID %q has a bad counter", s)
	}
	at := strings.IndexByte(origin, '@')
	if at <= 0 || at == len(origin)-1 {
		return "", 0, fmt.Errorf("op ID %q has a bad origin", s)
	}
	return origin, counter, nil
}

// ParseUnitID splits "<opID>/<i>" and checks both parts.
func ParseUnitID(s string) (opID string, index int, err error) {
	i := strings.LastIndexByte(s, '/')
	if i < 0 {
		return "", 0, fmt.Errorf("unit ID %q has no index", s)
	}
	opID, idx := s[:i], s[i+1:]
	index, err = strconv.Atoi(idx)
	if err != nil || index < 0 || index >= MaxQuantity || strconv.Itoa(index) != idx {
		return "", 0, fmt.Errorf("unit ID %q has a bad index", s)
	}
	if _, _, err := parseOpID(opID); err != nil {
		return "", 0, fmt.Errorf("unit ID %q: %v", s, err)
	}
	return opID, index, nil
}

// Validate checks an op's structure. Errors wrap ErrInvalidOp.
func (op Operation) Validate() error {
	fail := func(format string, args ...any) error {
		return fmt.Errorf("%w %q: %s", ErrInvalidOp, op.ID, fmt.Sprintf(format, args...))
	}
	if op.NodeID == "" || !strings.HasPrefix(op.Origin, op.NodeID+"@") || len(op.Origin) == len(op.NodeID)+1 {
		return fail("origin %q does not start with node %q followed by @", op.Origin, op.NodeID)
	}
	if op.Counter < 1 {
		return fail("counter must be at least 1")
	}
	if op.ID != OpID(op.Origin, op.Counter) {
		return fail("ID does not match origin and counter")
	}
	if op.Clock[op.Origin] != op.Counter {
		return fail("clock[origin]=%d, counter=%d", op.Clock[op.Origin], op.Counter)
	}
	if op.ProductID == "" {
		return fail("empty product")
	}
	if op.Quantity < 1 || op.Quantity > MaxQuantity {
		return fail("quantity %d outside 1 … %d", op.Quantity, MaxQuantity)
	}
	switch op.Type {
	case Add:
		if len(op.Removes) != 0 {
			return fail("ADD must not name removed units")
		}
	case Remove:
		if len(op.Removes) != op.Quantity {
			return fail("REMOVE names %d units for quantity %d", len(op.Removes), op.Quantity)
		}
		seen := make(map[string]bool, len(op.Removes))
		for _, u := range op.Removes {
			if seen[u] {
				return fail("unit %q named twice", u)
			}
			seen[u] = true
			if _, _, err := ParseUnitID(u); err != nil {
				return fail("%v", err)
			}
		}
	default:
		return fail("unknown type %q", op.Type)
	}
	return nil
}

// Copy returns a deep copy.
func (op Operation) Copy() Operation {
	op.Clock = op.Clock.Copy()
	if op.Removes != nil {
		op.Removes = append([]string(nil), op.Removes...)
	}
	return op
}
