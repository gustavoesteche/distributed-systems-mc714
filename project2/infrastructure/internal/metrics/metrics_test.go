package metrics

import (
	"reflect"
	"sync"
	"testing"
)

func TestRegistryCounters(t *testing.T) {
	r := New()
	r.Inc(OpsCreated)
	r.Inc(OpsCreated)
	r.Add(BytesSent, 42)
	r.Inc(OpsCreated)

	if got := r.Value(OpsCreated); got != 3 {
		t.Fatalf("OpsCreated = %d, want 3", got)
	}
	if got := r.Value(BytesSent); got != 42 {
		t.Fatalf("BytesSent = %d, want 42", got)
	}
	if got := r.Value("never_seen"); got != 0 {
		t.Fatalf("unknown counter = %d, want 0", got)
	}

	want := map[string]uint64{OpsCreated: 3, BytesSent: 42}
	if got := r.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot = %v, want %v", got, want)
	}
}

func TestNilRegistryIsSafe(t *testing.T) {
	var r *Registry
	r.Inc(OpsCreated)
	r.Add(BytesSent, 7)
	if got := r.Value(OpsCreated); got != 0 {
		t.Fatalf("nil Value = %d, want 0", got)
	}
	if got := r.Snapshot(); len(got) != 0 {
		t.Fatalf("nil Snapshot = %v, want empty", got)
	}
}

func TestRaceSafety(t *testing.T) {
	r := New()
	wg := sync.WaitGroup{}
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			r.Inc(OpsCreated)
			wg.Done()
		}()
	}
	wg.Wait()
	if got := r.Value(OpsCreated); got != 1000 {
		t.Fatalf("OpsCreated = %d, want 1000", got)
	}
}
