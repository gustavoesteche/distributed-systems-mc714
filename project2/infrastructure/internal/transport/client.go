package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"mc714/project2/internal/metrics"
	"mc714/project2/internal/replication"
)

// HTTPTransport implements replication.Transport over HTTP/JSON. Every
// request gets its own timeout, and any non-2xx response is an error.
type HTTPTransport struct {
	client  *http.Client
	timeout time.Duration
	metrics *metrics.Registry
}

// NewHTTPTransport returns a client whose requests time out after timeout.
func NewHTTPTransport(timeout time.Duration) *HTTPTransport {
	return &HTTPTransport{client: &http.Client{}, timeout: timeout}
}

// SetMetrics attaches the process-wide registry. It is called once during
// wiring, before the node starts gossiping.
func (t *HTTPTransport) SetMetrics(m *metrics.Registry) { t.metrics = m }

// Pull calls POST <peerURL>/internal/gossip/pull.
func (t *HTTPTransport) Pull(ctx context.Context, peerURL string, req replication.PullRequest) (replication.PullResponse, error) {
	var resp replication.PullResponse
	err := t.post(ctx, peerURL, "/internal/gossip/pull", req, &resp)
	return resp, err
}

// Push calls POST <peerURL>/internal/gossip/push.
func (t *HTTPTransport) Push(ctx context.Context, peerURL string, req replication.PushRequest) (replication.PushResponse, error) {
	var resp replication.PushResponse
	err := t.post(ctx, peerURL, "/internal/gossip/push", req, &resp)
	return resp, err
}

func (t *HTTPTransport) post(ctx context.Context, peerURL, path string, in, out any) error {
	ctx, cancel := context.WithTimeout(ctx, t.timeout)
	defer cancel()
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	url := strings.TrimRight(peerURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	t.metrics.Inc(metrics.MessagesSent)
	t.metrics.Add(metrics.BytesSent, uint64(len(body)))
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("POST %s: read response: %w", url, err)
	}
	t.metrics.Inc(metrics.MessagesReceived)
	t.metrics.Add(metrics.BytesReceived, uint64(len(respBody)))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		msg := respBody
		if len(msg) > 1024 {
			msg = msg[:1024]
		}
		return fmt.Errorf("POST %s: status %d: %s", url, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("POST %s: decode response: %w", url, err)
	}
	return nil
}
