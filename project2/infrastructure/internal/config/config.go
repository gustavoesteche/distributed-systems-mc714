// Package config reads and validates the node configuration from the environment.
package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"mc714/project2/internal/topology"
)

// Config is the validated node configuration.
type Config struct {
	NodeID           string
	NodeIndex        int
	NodeCount        int
	MaxNeighbors     int
	ExtraEdges       int
	TopologySeed     uint64
	GossipInterval   time.Duration
	HTTPAddr         string
	HTTPTimeout      time.Duration
	FailureThreshold int
	PeerURLTemplate  string
	LogLevel         string
}

// Load reads every variable through getenv, applies defaults and validates.
// Every error names the offending variable.
func Load(getenv func(string) string) (Config, error) {
	get := func(key, def string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return def
	}
	atoi := func(key, def string) (int, error) {
		v := get(key, def)
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("%s: %q is not an integer", key, v)
		}
		return n, nil
	}
	duration := func(key, def string) (time.Duration, error) {
		v := get(key, def)
		d, err := time.ParseDuration(v)
		if err != nil {
			return 0, fmt.Errorf("%s: %q is not a Go duration", key, v)
		}
		if d <= 0 {
			return 0, fmt.Errorf("%s: %q must be positive", key, v)
		}
		return d, nil
	}

	var c Config
	var err error
	if c.NodeCount, err = atoi("NODE_COUNT", "4"); err != nil {
		return Config{}, err
	}
	if c.NodeCount < 3 {
		return Config{}, fmt.Errorf("NODE_COUNT: %d must be at least 3", c.NodeCount)
	}

	c.NodeID = getenv("NODE_ID")
	if c.NodeID == "" {
		return Config{}, fmt.Errorf("NODE_ID: required")
	}
	if c.NodeIndex, err = topology.ParseNodeName(c.NodeID); err != nil {
		return Config{}, fmt.Errorf("NODE_ID: %v", err)
	}
	if c.NodeIndex > c.NodeCount {
		return Config{}, fmt.Errorf("NODE_ID: %s is outside node-1 … node-%d", c.NodeID, c.NodeCount)
	}

	if c.MaxNeighbors, err = atoi("MAX_NEIGHBORS", "3"); err != nil {
		return Config{}, err
	}
	if c.MaxNeighbors < 2 || c.MaxNeighbors > c.NodeCount-1 {
		return Config{}, fmt.Errorf("MAX_NEIGHBORS: %d must be between 2 and %d", c.MaxNeighbors, c.NodeCount-1)
	}
	if c.ExtraEdges, err = atoi("EXTRA_EDGES", "1"); err != nil {
		return Config{}, err
	}
	if c.ExtraEdges < 0 {
		return Config{}, fmt.Errorf("EXTRA_EDGES: %d must be at least 0", c.ExtraEdges)
	}
	seed := get("TOPOLOGY_SEED", "42")
	if c.TopologySeed, err = strconv.ParseUint(seed, 10, 64); err != nil {
		return Config{}, fmt.Errorf("TOPOLOGY_SEED: %q is not a uint64", seed)
	}
	if _, err := topology.Generate(c.NodeCount, c.MaxNeighbors, c.ExtraEdges, c.TopologySeed); err != nil {
		return Config{}, fmt.Errorf("EXTRA_EDGES: %v", err)
	}

	if c.GossipInterval, err = duration("GOSSIP_INTERVAL", "1s"); err != nil {
		return Config{}, err
	}
	c.HTTPAddr = get("HTTP_ADDR", ":8080")
	if strings.TrimSpace(c.HTTPAddr) == "" {
		return Config{}, fmt.Errorf("HTTP_ADDR: must not be empty")
	}
	if c.HTTPTimeout, err = duration("HTTP_TIMEOUT", "2s"); err != nil {
		return Config{}, err
	}
	if c.FailureThreshold, err = atoi("FAILURE_THRESHOLD", "3"); err != nil {
		return Config{}, err
	}
	if c.FailureThreshold < 1 {
		return Config{}, fmt.Errorf("FAILURE_THRESHOLD: %d must be at least 1", c.FailureThreshold)
	}
	c.PeerURLTemplate = get("PEER_URL_TEMPLATE", "http://{id}:8080")
	if !strings.Contains(c.PeerURLTemplate, "{id}") && !strings.Contains(c.PeerURLTemplate, "{index}") {
		return Config{}, fmt.Errorf("PEER_URL_TEMPLATE: %q must contain {id} or {index}", c.PeerURLTemplate)
	}
	c.LogLevel = strings.ToLower(get("LOG_LEVEL", "info"))
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return Config{}, fmt.Errorf("LOG_LEVEL: %q must be debug, info, warn or error", c.LogLevel)
	}
	return c, nil
}

// PeerURL expands the template for node id ("node-k").
func (c Config) PeerURL(id string) string {
	u := strings.ReplaceAll(c.PeerURLTemplate, "{id}", id)
	if k, err := topology.ParseNodeName(id); err == nil {
		u = strings.ReplaceAll(u, "{index}", strconv.Itoa(k))
	}
	return u
}
