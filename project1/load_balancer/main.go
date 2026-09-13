package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type strategy string

const (
	strategyRoundRobin   strategy = "round-robin"
	strategyRandom       strategy = "random"
	strategyShortestQueue strategy = "shortest-queue"
)

var backends = []string{
	"http://project1-api-1:8080",
	"http://project1-api-2:8080",
	"http://project1-api-3:8080",
}

type balancer struct {
	mode strategy
	mu   sync.Mutex
	rr   int
	rng  *rand.Rand
	http *http.Client
}

func experimentSeed() int64 {
	const fallback int64 = 1337
	s := os.Getenv("EXPERIMENT_SEED")
	if s == "" {
		return fallback
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		log.Printf("invalid EXPERIMENT_SEED %q, using %d", s, fallback)
		return fallback
	}
	return v
}

func newBalancer(mode strategy) *balancer {
	seed := experimentSeed()
	log.Printf("experiment seed: %d\n", seed)
	return &balancer{
		mode: mode,
		rng:  rand.New(rand.NewSource(seed)),
		http: &http.Client{Timeout: 30 * time.Second},
	}
}

func parseStrategy(arg string) (strategy, error) {
	switch strategy(arg) {
	case strategyRoundRobin, strategyRandom, strategyShortestQueue:
		return strategy(arg), nil
	default:
		return "", fmt.Errorf("invalid mode %q", arg)
	}
}

func (b *balancer) nextBackend() (string, error) {
	switch b.mode {
	case strategyRoundRobin:
		b.mu.Lock()
		idx := b.rr % len(backends)
		b.rr++
		b.mu.Unlock()
		return backends[idx], nil
	case strategyRandom:
		b.mu.Lock()
		idx := b.rng.Intn(len(backends))
		b.mu.Unlock()
		return backends[idx], nil
	case strategyShortestQueue:
		return b.pickShortestQueue()
	default:
		return "", fmt.Errorf("unsupported mode")
	}
}

func (b *balancer) pickShortestQueue() (string, error) {
	bestBackend := ""
	bestSize := int(^uint(0) >> 1)

	for _, backend := range backends {
		queueSizeURL := backend + "/queue-size"
		resp, err := b.http.Get(queueSizeURL)
		if err != nil {
			continue
		}

		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil || resp.StatusCode != http.StatusOK {
			continue
		}

		size, convErr := strconv.Atoi(strings.TrimSpace(string(body)))
		if convErr != nil {
			continue
		}

		if size < bestSize {
			bestSize = size
			bestBackend = backend
		}
	}

	if bestBackend == "" {
		return "", fmt.Errorf("no backend available")
	}

	return bestBackend, nil
}

func (b *balancer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	backend, err := b.nextBackend()
	if err != nil {
		http.Error(w, "no backend available", http.StatusBadGateway)
		return
	}
	log.Printf("forwarding %s %s to %s\n", r.Method, r.URL.RequestURI(), backend)

	baseURL, err := url.Parse(backend)
	if err != nil {
		http.Error(w, "invalid backend URL", http.StatusInternalServerError)
		return
	}

	targetURL := *r.URL
	targetURL.Scheme = baseURL.Scheme
	targetURL.Host = baseURL.Host

	upstreamReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL.String(), r.Body)
	if err != nil {
		http.Error(w, "failed to build upstream request", http.StatusInternalServerError)
		return
	}
	upstreamReq.Header = r.Header.Clone()
	upstreamReq.Host = baseURL.Host

	resp, err := b.http.Do(upstreamReq)
	if err != nil {
		http.Error(w, "failed to reach backend", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("usage: %s <round-robin|random|shortest-queue>\n", os.Args[0])
	}

	mode, err := parseStrategy(os.Args[1])
	if err != nil {
		log.Fatalf("%v. valid modes: round-robin, random, shortest-queue\n", err)
	}

	b := newBalancer(mode)

	log.Printf("load balancer mode: %s\n", mode)
	log.Println("listening on port 8080")
	log.Fatal(http.ListenAndServe("0.0.0.0:8080", http.HandlerFunc(b.serveHTTP)))
}
