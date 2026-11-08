package main

import (
	"bytes"
	"encoding/csv"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSendSuccessAndConflict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cart/remove" {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"insufficient quantity"}`))
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"node-1@1:1"}`))
	}))
	defer srv.Close()

	code, id, err := send(srv.Client(), srv.URL, "ADD", "apple", 2, time.Second)
	if code != http.StatusCreated || id != "node-1@1:1" || err != nil {
		t.Fatalf("add: code=%d id=%q err=%v", code, id, err)
	}
	code, _, err = send(srv.Client(), srv.URL, "REMOVE", "apple", 1, time.Second)
	if code != http.StatusConflict || err == nil {
		t.Fatalf("remove: code=%d err=%v, want 409 and an error", code, err)
	}
}

func TestSplitTargets(t *testing.T) {
	got := splitTargets(" http://a:1/ , http://b:2,, ")
	want := []string{"http://a:1", "http://b:2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("splitTargets = %v, want %v", got, want)
	}
}

func TestWriteCSV(t *testing.T) {
	samples := make(chan sample, 1)
	samples <- sample{
		runID: "seed-1", seed: 1, worker: 0, target: "http://a", op: "ADD", product: "apple",
		quantity: 2, status: 201, latencyMS: 1.5, startedUnix: 10, finishedUnix: 20, opID: "x:1",
	}
	close(samples)
	var buf bytes.Buffer
	if err := writeCSV(&buf, samples); err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(strings.NewReader(buf.String())).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0][0] != "run_id" || rows[1][3] != "http://a" || rows[1][7] != "201" {
		t.Fatalf("unexpected CSV: %v", rows)
	}
}
