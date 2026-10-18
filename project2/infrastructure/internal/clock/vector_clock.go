// Package clock implements vector clocks keyed by replica ID.
package clock

import "fmt"

// VC is a vector clock. A missing key means 0. Every method is pure: none
// mutates its receiver or arguments, and results never alias them.
type VC map[string]uint64

// Order is the result of comparing two vector clocks.
type Order int

const (
	Equal      Order = iota
	Before           // v happened-before o: v ≤ o pointwise, and v ≠ o
	After            // o happened-before v
	Concurrent       // neither one dominates the other
)

func (o Order) String() string {
	switch o {
	case Equal:
		return "Equal"
	case Before:
		return "Before"
	case After:
		return "After"
	case Concurrent:
		return "Concurrent"
	}
	return fmt.Sprintf("Order(%d)", int(o))
}

// Copy returns an independent copy; the copy of nil is an empty VC.
func (v VC) Copy() VC {
	out := make(VC, len(v))
	for k, n := range v {
		out[k] = n
	}
	return out
}

// Get returns the entry for id, or 0.
func (v VC) Get(id string) uint64 { return v[id] }

// Increment returns a copy with id's entry raised by one.
func (v VC) Increment(id string) VC {
	out := v.Copy()
	out[id]++
	return out
}

// Merge returns the pointwise maximum of v and o.
func (v VC) Merge(o VC) VC {
	out := v.Copy()
	for k, n := range o {
		if n > out[k] {
			out[k] = n
		}
	}
	return out
}

// Compare orders v against o over the union of their keys.
func (v VC) Compare(o VC) Order {
	less, greater := false, false
	for k, n := range v {
		if m := o[k]; n < m {
			less = true
		} else if n > m {
			greater = true
		}
	}
	for k, m := range o {
		if _, seen := v[k]; seen {
			continue
		}
		if m > 0 {
			less = true
		}
	}
	switch {
	case less && greater:
		return Concurrent
	case less:
		return Before
	case greater:
		return After
	}
	return Equal
}

// Sum adds every entry. If a happened-before b then a.Sum() < b.Sum(), so
// sorting by Sum gives a linear extension of causality.
func (v VC) Sum() uint64 {
	var s uint64
	for _, n := range v {
		s += n
	}
	return s
}
