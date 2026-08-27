package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"
)

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

func processLoop(q *unboundedQueue) {
	for {
		job := q.Dequeue()

		switch job.path {
		case "/ping":
			job.resp <- jobResult{status: http.StatusOK, body: "pong"}
		case "/queue-size":
			job.resp <- jobResult{status: http.StatusOK, body: fmt.Sprintf("%d", q.Len())}
		default:
			job.resp <- jobResult{status: http.StatusNotFound, body: "not found"}
		}
	}
}

func main() {
	q := newUnboundedQueue()
	go processLoop(q)

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

	log.Println("listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
