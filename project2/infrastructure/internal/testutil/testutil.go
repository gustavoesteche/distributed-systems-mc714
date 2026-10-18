// Package testutil holds helpers shared by the test suites: polling and a
// log handler that records every record for later assertions.
package testutil

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// Eventually polls cond every 10ms and fails the test after timeout.
func Eventually(t testing.TB, timeout time.Duration, cond func() bool, msgAndArgs ...any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			msg := "condition not met"
			if len(msgAndArgs) > 0 {
				msg = fmt.Sprintf(msgAndArgs[0].(string), msgAndArgs[1:]...)
			}
			t.Fatalf("eventually: %s within %v", msg, timeout)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Record is a flattened log record with every attribute as a string.
type Record struct {
	Level   slog.Level
	Message string
	Attrs   map[string]string
}

type logStore struct {
	mu      sync.Mutex
	records []Record
}

// LogCapture is a slog.Handler that keeps every record, at every level.
type LogCapture struct {
	store *logStore
	attrs []slog.Attr
	group string
}

func NewLogCapture() *LogCapture {
	return &LogCapture{store: &logStore{}}
}

// Logger returns a logger writing into the capture, tagged with node=nodeID.
func (c *LogCapture) Logger(nodeID string) *slog.Logger {
	return slog.New(c).With("node", nodeID)
}

func (c *LogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *LogCapture) Handle(_ context.Context, r slog.Record) error {
	rec := Record{Level: r.Level, Message: r.Message, Attrs: map[string]string{}}
	for _, a := range c.attrs {
		rec.Attrs[a.Key] = a.Value.String()
	}
	prefix := ""
	if c.group != "" {
		prefix = c.group + "."
	}
	r.Attrs(func(a slog.Attr) bool {
		rec.Attrs[prefix+a.Key] = a.Value.String()
		return true
	})
	c.store.mu.Lock()
	c.store.records = append(c.store.records, rec)
	c.store.mu.Unlock()
	return nil
}

func (c *LogCapture) WithAttrs(attrs []slog.Attr) slog.Handler {
	n := &LogCapture{store: c.store, group: c.group}
	n.attrs = append(append([]slog.Attr{}, c.attrs...), attrs...)
	return n
}

func (c *LogCapture) WithGroup(name string) slog.Handler {
	n := &LogCapture{store: c.store, attrs: c.attrs, group: name}
	if c.group != "" {
		n.group = c.group + "." + name
	}
	return n
}

func (c *LogCapture) Records() []Record {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	return append([]Record(nil), c.store.records...)
}

// HasEvent reports whether some record has event=name and every given
// key/value attribute pair.
func (c *LogCapture) HasEvent(name string, attrs ...string) bool {
	return c.CountEvent(name, attrs...) > 0
}

// CountEvent counts the records that HasEvent would match.
func (c *LogCapture) CountEvent(name string, attrs ...string) int {
	n := 0
	for _, r := range c.Records() {
		if r.Attrs["event"] != name {
			continue
		}
		ok := true
		for i := 0; i+1 < len(attrs); i += 2 {
			if r.Attrs[attrs[i]] != attrs[i+1] {
				ok = false
				break
			}
		}
		if ok {
			n++
		}
	}
	return n
}

// String renders the captured records, one per line, for failure messages.
func (c *LogCapture) String() string {
	var b strings.Builder
	for _, r := range c.Records() {
		fmt.Fprintf(&b, "%s %q %v\n", r.Level, r.Message, r.Attrs)
	}
	return b.String()
}
