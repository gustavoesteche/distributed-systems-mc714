package main

import (
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

var hostname = os.Getenv("HOSTNAME")

type requestJob struct {
	path string
	resp chan jobResult
}

type jobResult struct {
	status int
	body   string
}

// unboundedQueue is a FIFO queue with no fixed capacity.
type unboundedQueue struct {
	mu    sync.Mutex
	cond  *sync.Cond
	items []requestJob
}

func newUnboundedQueue() *unboundedQueue {
	q := &unboundedQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *unboundedQueue) Enqueue(job requestJob) {
	q.mu.Lock()
	q.items = append(q.items, job)
	q.cond.Signal()
	q.mu.Unlock()
}

func (q *unboundedQueue) Dequeue() requestJob {
	q.mu.Lock()
	for len(q.items) == 0 {
		q.cond.Wait()
	}
	job := q.items[0]
	q.items = q.items[1:]
	q.mu.Unlock()
	return job
}

func (q *unboundedQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func sampleExpDist(r *rand.Rand, mu float64) float64 {
	return math.Log(r.Float64()) / (-mu)
}

func processLoop(q *unboundedQueue, mu float64, r *rand.Rand) {
	for {
		job := q.Dequeue()

		switch job.path {
		case "/ping":
			job.resp <- jobResult{status: http.StatusOK, body: "pong"}
		case "/queue-size":
			job.resp <- jobResult{status: http.StatusOK, body: fmt.Sprintf("%d", q.Len())}
		case "/process":
			sample := sampleExpDist(r, mu)
			timeSpent := time.Duration(sample * float64(time.Second))

			time.Sleep(timeSpent)
			response := fmt.Sprintf("{\"time\": %f\n, \"server\": \"%s\"}", sample, hostname)
			job.resp <- jobResult{status: http.StatusOK, body: response}
		default:
			job.resp <- jobResult{status: http.StatusNotFound, body: "not found"}
		}
	}
}

func newRNG() *rand.Rand {
	seed := uint64(42) // default seed
	if s := os.Getenv("SEED"); s != "" {
		if v, err := strconv.ParseUint(s, 10, 64); err == nil {
			seed = v
		}
	}
	stream := hashStream(hostname)
	log.Println("Generator seed: ", seed)
	return rand.New(rand.NewPCG(seed, stream))
}

func hashStream(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func setup() (float64, *rand.Rand) {
	mu := 1.0
	if s, err := strconv.ParseFloat(os.Getenv("MU"), 64); err == nil {
		mu = s
	}
	log.Println("Service time: ", mu)

	return mu, newRNG()
}

func main() {
	mu, randGen := setup()
	q := newUnboundedQueue()
	go processLoop(q, mu, randGen)

	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		resp := make(chan jobResult)
		q.Enqueue(requestJob{path: r.URL.Path, resp: resp})
		result := <-resp

		w.WriteHeader(result.status)
		_, _ = w.Write([]byte(result.body))
	})

	http.HandleFunc("/queue-size", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		resp := make(chan jobResult)
		q.Enqueue(requestJob{path: r.URL.Path, resp: resp})
		result := <-resp

		w.WriteHeader(result.status)
		_, _ = w.Write([]byte(result.body))
	})

	http.HandleFunc("/process", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		resp := make(chan jobResult)
		q.Enqueue(requestJob{path: r.URL.Path, resp: resp})

		result := <-resp

		w.WriteHeader(result.status)
		_, _ = w.Write([]byte(result.body))
	})

	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
