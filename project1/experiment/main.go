package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

var balancerURL = map[string]string{
	"round-robin":    "http://load-balancer-round-robin:8080",
	"random":         "http://load-balancer-random:8080",
	"shortest-queue": "http://load-balancer-shortest-queue:8080",
}

var processStart = time.Now()

type row struct {
	startTime   float64
	totalTime   float64
	machineTime float64
	machine     string
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	lambda := mustEnvFloat("LAMBDA")
	mu := envFloat("MU", 1)
	duration := mustEnvFloat("DURATION")
	strategy := envString("STRATEGY", "round-robin")
	dataDir := envString("DATA_DIR", "/app/data")

	log.Printf("settings: lambda=%g mu=%g duration=%gs strategy=%q data_dir=%s",
		lambda, mu, duration, strategy, dataDir)

	strategies, err := strategiesToRun(strategy)
	if err != nil {
		log.Fatal(err)
	}

	if err := waitForStrategies(strategies); err != nil {
		log.Fatalf("system not ready: %v", err)
	}

	client := newExperimentClient()
	multi := len(strategies) > 1
	for _, name := range strategies {
		filename := resultFilename(dataDir, lambda, mu, name, multi)
		baseURL := balancerURL[name]
		log.Printf("running experiment strategy=%s url=%s file=%s", name, baseURL, filename)
		if err := runExperiment(client, lambda, duration, baseURL, filename); err != nil {
			log.Fatalf("experiment %s: %v", name, err)
		}
		log.Printf("finished strategy=%s", name)
	}
}

func mustEnvFloat(key string) float64 {
	s := os.Getenv(key)
	if s == "" {
		log.Fatalf("missing required env %s", key)
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		log.Fatalf("parse %s=%q: %v", key, s, err)
	}
	return v
}

func envFloat(key string, fallback float64) float64 {
	s := os.Getenv(key)
	if s == "" {
		return fallback
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		log.Fatalf("parse %s=%q: %v", key, s, err)
	}
	return v
}

func envString(key, fallback string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return fallback
}

func strategiesToRun(strategy string) ([]string, error) {
	if strategy == "all" {
		return []string{"round-robin", "random", "shortest-queue"}, nil
	}
	if _, ok := balancerURL[strategy]; !ok {
		return nil, fmt.Errorf("unknown strategy %q (use round-robin, random, shortest-queue, or all)", strategy)
	}
	return []string{strategy}, nil
}

func waitForStrategies(strategies []string) error {
	deadline := time.Now().Add(2 * time.Minute)
	client := &http.Client{Timeout: 2 * time.Second}
	for _, strategy := range strategies {
		url := balancerURL[strategy] + "/ping"
		for {
			resp, err := client.Get(url)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					log.Printf("ready: %s at %s", strategy, url)
					break
				}
			}
			if time.Now().After(deadline) {
				if err != nil {
					return fmt.Errorf("timed out waiting for %s (%s): %w", strategy, url, err)
				}
				return fmt.Errorf("timed out waiting for %s (%s)", strategy, url)
			}
			time.Sleep(time.Second)
		}
	}
	return nil
}

func resultFilename(dataDir string, lambda, mu float64, strategy string, multi bool) string {
	name := fmt.Sprintf("lambda_%.1f_mu_%.1f.csv", lambda, mu)
	if multi {
		name = fmt.Sprintf("lambda_%.1f_mu_%.1f_%s.csv", lambda, mu, strategy)
	}
	return filepath.Join(dataDir, name)
}

func newExperimentClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        8192,
			MaxIdleConnsPerHost: 8192,
			MaxConnsPerHost:     8192,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

func sampleTime(lambda float64) float64 {
	return -math.Log(rand.Float64()) / lambda
}

func seconds(s float64) time.Duration {
	return time.Duration(s * float64(time.Second))
}

func perfCounter() float64 {
	return time.Since(processStart).Seconds()
}

func runExperiment(client *http.Client, lambda, duration float64, baseURL, filename string) error {
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		return err
	}

	results := make(chan row, 4096)
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- writeRows(filename, results)
	}()

	var wg sync.WaitGroup
	start := time.Now()
	end := start.Add(seconds(duration))
	nextArrival := start.Add(seconds(sampleTime(lambda)))

	for {
		if delay := time.Until(nextArrival); delay > 0 {
			time.Sleep(delay)
		}
		pastEnd := nextArrival.After(end)
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := sendEvent(client, baseURL)
			if err != nil {
				log.Printf("request failed: %v", err)
				return
			}
			results <- r
		}()
		if pastEnd {
			break
		}
		nextArrival = nextArrival.Add(seconds(sampleTime(lambda)))
	}

	wg.Wait()
	close(results)
	return <-writeDone
}

func sendEvent(client *http.Client, baseURL string) (row, error) {
	start := perfCounter()
	t0 := time.Now()
	resp, err := client.Post(baseURL+"/process", "application/json", http.NoBody)
	if err != nil {
		return row{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	total := time.Since(t0).Seconds()
	if err != nil {
		return row{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return row{}, fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}

	var parsed struct {
		Time   float64 `json:"time"`
		Server string  `json:"server"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return row{}, fmt.Errorf("decode response: %w (%q)", err, body)
	}

	return row{
		startTime:   start,
		totalTime:   total,
		machineTime: parsed.Time,
		machine:     parsed.Server,
	}, nil
}

func writeRows(filename string, results <-chan row) error {
	_, statErr := os.Stat(filename)
	fileExists := statErr == nil

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if !fileExists {
		if err := w.Write([]string{"start_time", "total_time", "machine_time", "machine"}); err != nil {
			return err
		}
		w.Flush()
		if err := w.Error(); err != nil {
			return err
		}
	}

	n := 0
	for r := range results {
		record := []string{
			strconv.FormatFloat(r.startTime, 'f', -1, 64),
			strconv.FormatFloat(r.totalTime, 'f', -1, 64),
			strconv.FormatFloat(r.machineTime, 'f', -1, 64),
			r.machine,
		}
		if err := w.Write(record); err != nil {
			return err
		}
		w.Flush()
		if err := w.Error(); err != nil {
			return err
		}
		n++
	}
	log.Printf("wrote %d rows to %s", n, filename)
	return nil
}
