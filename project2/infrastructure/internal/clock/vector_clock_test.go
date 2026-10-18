package clock

import (
	"math/rand/v2"
	"reflect"
	"testing"
)

// eq compares two clocks treating missing keys as 0.
func eq(a, b VC) bool {
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}

func TestIncrement(t *testing.T) {
	v := VC{}
	w := v.Increment("a")
	if !eq(w, VC{"a": 1}) {
		t.Fatalf("from 0: got %v, want {a:1}", w)
	}
	if len(v) != 0 {
		t.Fatalf("receiver mutated: %v", v)
	}

	v = VC{"a": 5, "b": 2}
	w = v.Increment("a")
	if !eq(w, VC{"a": 6, "b": 2}) {
		t.Fatalf("from n: got %v, want {a:6,b:2}", w)
	}
	if !eq(v, VC{"a": 5, "b": 2}) {
		t.Fatalf("receiver mutated: %v", v)
	}
	w["b"] = 99
	if v["b"] != 2 {
		t.Fatalf("result aliases receiver")
	}

	var nilVC VC
	if got := nilVC.Increment("x"); !eq(got, VC{"x": 1}) {
		t.Fatalf("nil receiver: got %v", got)
	}
}

func TestMergePointwiseMax(t *testing.T) {
	a := VC{"a": 3, "b": 1}
	b := VC{"b": 4, "c": 2}
	m := a.Merge(b)
	if !eq(m, VC{"a": 3, "b": 4, "c": 2}) {
		t.Fatalf("got %v", m)
	}
	if !eq(a, VC{"a": 3, "b": 1}) || !eq(b, VC{"b": 4, "c": 2}) {
		t.Fatalf("inputs mutated: %v %v", a, b)
	}
	m["a"] = 100
	if a["a"] != 3 {
		t.Fatalf("result aliases input")
	}
	if got := (VC(nil)).Merge(nil); len(got) != 0 {
		t.Fatalf("nil merge: %v", got)
	}
}

func randomVC(r *rand.Rand) VC {
	keys := []string{"a", "b", "c", "d", "e"}
	v := VC{}
	for _, k := range keys {
		if r.IntN(3) > 0 {
			v[k] = uint64(r.IntN(5))
		}
	}
	return v
}

func TestMergeLaws(t *testing.T) {
	r := rand.New(rand.NewPCG(2026, 0))
	for i := 0; i < 1000; i++ {
		a, b, c := randomVC(r), randomVC(r), randomVC(r)
		if !eq(a.Merge(b), b.Merge(a)) {
			t.Fatalf("iter %d: not commutative: %v %v", i, a, b)
		}
		if !eq(a.Merge(b).Merge(c), a.Merge(b.Merge(c))) {
			t.Fatalf("iter %d: not associative: %v %v %v", i, a, b, c)
		}
		if !eq(a.Merge(a), a) {
			t.Fatalf("iter %d: not idempotent: %v", i, a)
		}
	}
}

func TestCompareOrders(t *testing.T) {
	cases := []struct {
		a, b VC
		want Order
	}{
		{VC{}, VC{}, Equal},
		{VC{"a": 1, "b": 2}, VC{"a": 1, "b": 2}, Equal},
		{VC{"a": 1}, VC{"a": 2}, Before},
		{VC{"a": 1}, VC{"a": 1, "b": 1}, Before},
		{VC{"a": 2, "b": 1}, VC{"a": 1, "b": 1}, After},
		{VC{"a": 1, "c": 3}, VC{"a": 1}, After},
		{VC{"a": 1}, VC{"b": 1}, Concurrent},
		{VC{"a": 2, "b": 1}, VC{"a": 1, "b": 2}, Concurrent},
	}
	for _, c := range cases {
		if got := c.a.Compare(c.b); got != c.want {
			t.Errorf("%v.Compare(%v) = %v, want %v", c.a, c.b, got, c.want)
		}
	}

	r := rand.New(rand.NewPCG(7, 0))
	for i := 0; i < 1000; i++ {
		a, b := randomVC(r), randomVC(r)
		ab, ba := a.Compare(b), b.Compare(a)
		if (ab == Before) != (ba == After) {
			t.Fatalf("antisymmetry broken: %v vs %v: %v / %v", a, b, ab, ba)
		}
		if (ab == Equal) != (ba == Equal) || (ab == Concurrent) != (ba == Concurrent) {
			t.Fatalf("symmetry broken: %v vs %v: %v / %v", a, b, ab, ba)
		}
	}
}

func TestCompareMissingKeysEqualZero(t *testing.T) {
	if got := (VC{"a": 1}).Compare(VC{"a": 1, "b": 0}); got != Equal {
		t.Fatalf("{a:1} vs {a:1,b:0} = %v, want Equal", got)
	}
	if got := (VC{}).Compare(VC{"a": 1}); got != Before {
		t.Fatalf("{} vs {a:1} = %v, want Before", got)
	}
	if got := (VC{"a": 1}).Get("missing"); got != 0 {
		t.Fatalf("Get(missing) = %d", got)
	}
}

func TestCopyDoesNotAlias(t *testing.T) {
	orig := VC{"a": 1, "b": 2}
	cp := orig.Copy()
	if !reflect.DeepEqual(orig, cp) {
		t.Fatalf("copy differs: %v vs %v", orig, cp)
	}
	cp["a"] = 50
	cp["z"] = 1
	if orig["a"] != 1 || len(orig) != 2 {
		t.Fatalf("changing copy changed original: %v", orig)
	}
	cp2 := orig.Copy()
	orig["b"] = 77
	if cp2["b"] != 2 {
		t.Fatalf("changing original changed copy: %v", cp2)
	}
	if (VC(nil)).Copy() == nil {
		t.Fatalf("copy of nil should be an empty, non-nil VC")
	}
}
