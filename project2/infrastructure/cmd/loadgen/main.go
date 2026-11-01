// Command loadgen generates a reproducible cart workload against a running
// cluster. It sends ADD and REMOVE requests at a configured global rate,
// records one CSV row per request, and writes a JSON summary that experiment
// orchestration uses to find the end of the measured window.
package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type options struct {
	targets         string
	rate            float64
	duration        time.Duration
	warmup          time.Duration
	concurrency     int
	seed            uint64
	removeRatio     float64
	products        int
	initialStock    int
	addQuantity     int
	removeQuantity  int
	prefix          string
	timeout         time.Duration
	bootstrapSettle time.Duration
	out             string
	summary         string
}

type sample struct {
	runID        string
	seed         uint64
	worker       int
	target       string
	op           string
	product      string
	quantity     int
	status       int
	latencyMS    float64
	startedUnix  int64
	finishedUnix int64
	opID         string
	err          string
}

type summary struct {
	Seed                 uint64           `json:"seed"`
	Targets              []string         `json:"targets"`
	Rate                 float64          `json:"rate_ops_per_second"`
	Concurrency          int              `json:"concurrency"`
	WarmupSeconds        float64          `json:"warmup_seconds"`
	DurationSeconds      float64          `json:"duration_seconds"`
	StartedUnixNano      int64            `json:"started_unix_nano"`
	RecordedFromUnixNano int64            `json:"recorded_from_unix_nano"`
	RecordedToUnixNano   int64            `json:"recorded_to_unix_nano"`
	LastWriteUnixNano    int64            `json:"last_write_unix_nano"`
	Requests             int64            `json:"requests"`
	Recorded             int64            `json:"recorded"`
	Errors               int64            `json:"errors"`
	StatusCounts         map[string]int64 `json:"status_counts"`
	BootstrapOps         int64            `json:"bootstrap_ops"`
	BootstrapErrors      int64            `json:"bootstrap_errors"`
	Completed            bool             `json:"completed"`
}

type counters struct {
	requests     atomic.Int64
	recorded     atomic.Int64
	errors       atomic.Int64
	lastWrite    atomic.Int64
	recordedFrom atomic.Int64
	recordedTo   atomic.Int64
	mu           sync.Mutex
	status       map[string]int64
}

func (c *counters) statusInc(code int) {
	c.mu.Lock()
	c.status[strconv.Itoa(code)]++
	c.mu.Unlock()
}

func main() {
	var o options
	flag.StringVar(&o.targets, "targets", "http://127.0.0.1:8001,http://127.0.0.1:8002", "comma-separated node base URLs")
	flag.Float64Var(&o.rate, "rate", 20, "global operations per second")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "measured workload duration")
	flag.DurationVar(&o.warmup, "warmup", 5*time.Second, "warm-up before samples are recorded")
	flag.IntVar(&o.concurrency, "concurrency", 8, "number of client workers")
	flag.Uint64Var(&o.seed, "seed", 1, "pseudo-random seed")
	flag.Float64Var(&o.removeRatio, "remove-ratio", 0.2, "fraction of REMOVE operations, 0..1")
	flag.IntVar(&o.products, "products", 100, "number of product IDs")
	flag.IntVar(&o.initialStock, "initial-stock", 1000, "units added to every product before the run (0 disables)")
	flag.IntVar(&o.addQuantity, "add-quantity", 1, "units per ADD")
	flag.IntVar(&o.removeQuantity, "remove-quantity", 1, "units per REMOVE")
	flag.StringVar(&o.prefix, "prefix", "load", "product ID prefix")
	flag.DurationVar(&o.timeout, "timeout", 2*time.Second, "per-request timeout")
	flag.DurationVar(&o.bootstrapSettle, "bootstrap-settle", 2*time.Second, "wait after pre-populating products")
	flag.StringVar(&o.out, "out", "load.csv", "CSV output path, or - for stdout")
	flag.StringVar(&o.summary, "summary", "", "optional JSON summary path")
	flag.Parse()

	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run(o options) error {
	targets := splitTargets(o.targets)
	if len(targets) == 0 {
		return fmt.Errorf("no targets")
	}
	if o.rate <= 0 {
		return fmt.Errorf("rate must be positive")
	}
	if o.concurrency < 1 {
		return fmt.Errorf("concurrency must be >= 1")
	}
	if o.removeRatio < 0 || o.removeRatio > 1 {
		return fmt.Errorf("remove-ratio must be in [0,1]")
	}
	if o.products < 1 {
		return fmt.Errorf("products must be >= 1")
	}
	if o.duration <= 0 {
		return fmt.Errorf("duration must be positive")
	}
	if o.warmup < 0 {
		return fmt.Errorf("warmup must be >= 0")
	}
	if o.timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}

	client := &http.Client{Timeout: o.timeout}
	if o.initialStock > 0 && o.removeRatio > 0 {
		if err := bootstrap(client, o, targets); err != nil {
			return err
		}
		if o.bootstrapSettle > 0 {
			time.Sleep(o.bootstrapSettle)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	out, closeOut, err := openOutput(o.out)
	if err != nil {
		return err
	}
	defer closeOut()

	samples := make(chan sample, 1024)
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- writeCSV(out, samples)
	}()

	c := &counters{status: map[string]int64{}}
	start := time.Now()
	recordStart := start.Add(o.warmup)
	end := recordStart.Add(o.duration)
	c.recordedFrom.Store(recordStart.UnixNano())

	var wg sync.WaitGroup
	for worker := 0; worker < o.concurrency; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			runWorker(ctx, client, o, targets, worker, start, recordStart, end, samples, c)
		}(worker)
	}
	wg.Wait()
	close(samples)
	if err := <-writerDone; err != nil {
		return err
	}

	c.recordedTo.Store(time.Now().UnixNano())
	bootstrapOps := int64(0)
	if o.initialStock > 0 && o.removeRatio > 0 {
		bootstrapOps = int64(o.products)
	}
	s := summary{
		Seed:                 o.seed,
		Targets:              targets,
		Rate:                 o.rate,
		Concurrency:          o.concurrency,
		WarmupSeconds:        o.warmup.Seconds(),
		DurationSeconds:      o.duration.Seconds(),
		StartedUnixNano:      start.UnixNano(),
		RecordedFromUnixNano: c.recordedFrom.Load(),
		RecordedToUnixNano:   c.recordedTo.Load(),
		LastWriteUnixNano:    c.lastWrite.Load(),
		Requests:             c.requests.Load(),
		Recorded:             c.recorded.Load(),
		Errors:               c.errors.Load(),
		BootstrapOps:         bootstrapOps,
		BootstrapErrors:      0,
		Completed:            ctx.Err() == nil,
		StatusCounts:         map[string]int64{},
	}
	c.mu.Lock()
	for k, v := range c.status {
		s.StatusCounts[k] = v
	}
	c.mu.Unlock()

	if o.summary != "" {
		if err := writeSummary(o.summary, s); err != nil {
			return err
		}
	}
	fmt.Fprintf(os.Stderr, "loadgen: recorded=%d requests=%d errors=%d last_write=%d\n",
		s.Recorded, s.Requests, s.Errors, s.LastWriteUnixNano)
	return nil
}

func splitTargets(raw string) []string {
	var out []string
	for _, t := range strings.Split(raw, ",") {
		t = strings.TrimRight(strings.TrimSpace(t), "/")
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

func openOutput(path string) (io.Writer, func(), error) {
	if path == "-" {
		return os.Stdout, func() {}, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	return f, func() { _ = f.Close() }, nil
}

func writeCSV(w io.Writer, samples <-chan sample) error {
	bw := bufio.NewWriter(w)
	cw := csv.NewWriter(bw)
	header := []string{
		"run_id", "seed", "worker", "target", "op", "product", "quantity",
		"status", "latency_ms", "started_unix_nano", "finished_unix_nano", "op_id", "error",
	}
	if err := cw.Write(header); err != nil {
		return err
	}
	for s := range samples {
		row := []string{
			s.runID, strconv.FormatUint(s.seed, 10), strconv.Itoa(s.worker), s.target, s.op, s.product,
			strconv.Itoa(s.quantity), strconv.Itoa(s.status), fmt.Sprintf("%.3f", s.latencyMS),
			strconv.FormatInt(s.startedUnix, 10), strconv.FormatInt(s.finishedUnix, 10), s.opID, s.err,
		}
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return err
	}
	return bw.Flush()
}

func writeSummary(path string, s summary) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func bootstrap(client *http.Client, o options, targets []string) error {
	for i := 0; i < o.products; i++ {
		product := fmt.Sprintf("%s-%04d", o.prefix, i)
		code, _, err := send(client, targets[0], "ADD", product, o.initialStock, o.timeout)
		if err != nil || code != http.StatusCreated {
			return fmt.Errorf("bootstrap %s: status=%d err=%v", product, code, err)
		}
	}
	return nil
}

func runWorker(ctx context.Context, client *http.Client, o options, targets []string, worker int,
	start, recordStart, end time.Time, samples chan<- sample, c *counters) {
	rng := rand.New(rand.NewPCG(o.seed, uint64(worker)+1))
	interval := time.Duration(0)
	if o.rate > 0 {
		interval = time.Duration(float64(time.Second) * float64(o.concurrency) / o.rate)
	}
	next := start
	for {
		if !next.Before(end) {
			return
		}
		if wait := time.Until(next); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		if !time.Now().Before(end) {
			return
		}

		target := targets[rng.IntN(len(targets))]
		product := fmt.Sprintf("%s-%04d", o.prefix, rng.IntN(o.products))
		op := "ADD"
		quantity := o.addQuantity
		if rng.Float64() < o.removeRatio {
			op = "REMOVE"
			quantity = o.removeQuantity
		}

		started := time.Now()
		code, opID, err := send(client, target, op, product, quantity, o.timeout)
		finished := time.Now()
		c.requests.Add(1)
		c.statusInc(code)
		if err != nil || (code != http.StatusCreated && code != http.StatusConflict) {
			c.errors.Add(1)
		}
		if code == http.StatusCreated {
			c.lastWrite.Store(finished.UnixNano())
		}
		if !started.Before(recordStart) {
			c.recorded.Add(1)
			c.recordedTo.Store(finished.UnixNano())
			errText := ""
			if err != nil {
				errText = err.Error()
			}
			samples <- sample{
				runID:        fmt.Sprintf("seed-%d", o.seed),
				seed:         o.seed,
				worker:       worker,
				target:       target,
				op:           op,
				product:      product,
				quantity:     quantity,
				status:       code,
				latencyMS:    float64(finished.Sub(started)) / float64(time.Millisecond),
				startedUnix:  started.UnixNano(),
				finishedUnix: finished.UnixNano(),
				opID:         opID,
				err:          errText,
			}
		}

		if interval <= 0 {
			continue
		}
		// Keep the mean interval stable while avoiding lock-step workers.
		jitter := time.Duration((rng.Float64()*0.4 - 0.2) * float64(interval))
		next = next.Add(interval + jitter)
		if next.Before(time.Now().Add(-interval)) {
			next = time.Now()
		}
	}
}

func send(client *http.Client, base, op, product string, quantity int, timeout time.Duration) (int, string, error) {
	body, err := json.Marshal(map[string]any{"product_id": product, "quantity": quantity})
	if err != nil {
		return 0, "", err
	}
	path := "/cart/add"
	if op == "REMOVE" {
		path = "/cart/remove"
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, strings.NewReader(string(body)))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, "", err
	}
	var out struct {
		ID    string `json:"id"`
		Error string `json:"error"`
	}
	_ = json.Unmarshal(respBody, &out)
	if resp.StatusCode >= 300 && out.Error != "" {
		return resp.StatusCode, out.ID, fmt.Errorf("%s", out.Error)
	}
	return resp.StatusCode, out.ID, nil
}
