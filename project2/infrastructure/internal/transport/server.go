// Package transport exposes the node over HTTP/JSON and implements the
// replication Transport as an HTTP client. It holds no reconciliation logic:
// handlers decode, delegate and encode.
package transport

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"

	"mc714/project2/internal/cart"
	"mc714/project2/internal/replication"
)

// MaxBodyBytes limits every request body.
const MaxBodyBytes = 8 << 20

type server struct {
	replica *cart.Replica
	engine  *replication.Engine
}

// NewServer returns the node's HTTP handler.
func NewServer(replica *cart.Replica, engine *replication.Engine) http.Handler {
	s := &server{replica: replica, engine: engine}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /cart", s.cart)
	mux.HandleFunc("POST /cart/add", s.add)
	mux.HandleFunc("POST /cart/remove", s.remove)
	mux.HandleFunc("POST /internal/gossip/pull", s.pull)
	mux.HandleFunc("POST /internal/gossip/push", s.push)
	mux.HandleFunc("GET /debug/state", s.debug)
	mux.HandleFunc("GET /debug/clean-state", s.cleanState)
	// Answer wrong methods with JSON instead of the mux's text/plain.
	for path, allow := range map[string]string{
		"/health": "GET", "/cart": "GET", "/cart/add": "POST", "/cart/remove": "POST",
		"/internal/gossip/pull": "POST", "/internal/gossip/push": "POST", "/debug/state": "GET",
		"/debug/clean-state": "GET",
	} {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Allow", allow)
			writeJSON(w, http.StatusMethodNotAllowed, errorBody(fmt.Errorf("method %s not allowed on %s", r.Method, path)))
		})
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, errorBody(fmt.Errorf("no route for %s %s", r.Method, r.URL.Path)))
	})
	return mux
}

type itemRequest struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

type debugState struct {
	cart.Snapshot
	Neighbors []string                 `json:"neighbors"`
	Peers     []replication.PeerStatus `json:"peers"`
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"node": s.replica.NodeID(), "replica": s.replica.ReplicaID()})
}

func (s *server) cart(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"node": s.replica.NodeID(), "replica": s.replica.ReplicaID(), "items": s.replica.Quantities(),
	})
}

func (s *server) add(w http.ResponseWriter, r *http.Request) {
	var req itemRequest
	if !decode(w, r, &req) {
		return
	}
	op, err := s.replica.Add(req.ProductID, req.Quantity)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, op)
}

func (s *server) remove(w http.ResponseWriter, r *http.Request) {
	var req itemRequest
	if !decode(w, r, &req) {
		return
	}
	op, err := s.replica.Remove(req.ProductID, req.Quantity)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, op)
}

func (s *server) pull(w http.ResponseWriter, r *http.Request) {
	var req replication.PullRequest
	if !decode(w, r, &req) {
		return
	}
	resp, err := s.engine.HandlePull(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *server) push(w http.ResponseWriter, r *http.Request) {
	var req replication.PushRequest
	if !decode(w, r, &req) {
		return
	}
	resp, err := s.engine.HandlePush(r.Context(), req)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *server) debug(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, debugState{
		Snapshot: s.replica.Snapshot(), Neighbors: s.engine.Neighbors(), Peers: s.engine.PeerStatuses(),
	})
}

// cleanState reports only what the cart shows: product and quantity.
func (s *server) cleanState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"node": s.replica.NodeID(), "items": s.replica.Quantities()})
}

// decode reads exactly one JSON value of at most MaxBodyBytes, rejecting
// unknown fields. On failure it writes the error response and returns false.
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeJSON(w, http.StatusRequestEntityTooLarge, errorBody(fmt.Errorf("body exceeds %d bytes", MaxBodyBytes)))
		return false
	}
	if err == nil {
		err = decodeStrict(body, v)
	}
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody(fmt.Errorf("bad request body: %v", err)))
		return false
	}
	return true
}

func decodeStrict(body []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if extra := dec.Decode(&json.RawMessage{}); extra != io.EOF {
		return errors.New("trailing data after the JSON body")
	}
	// encoding/json matches keys case-insensitively even with
	// DisallowUnknownFields, so check that every key is spelled exactly.
	var generic any
	if err := json.Unmarshal(body, &generic); err != nil {
		return err
	}
	return checkKeys(generic, reflect.TypeOf(v))
}

// checkKeys walks a generic JSON value alongside the Go type it was decoded
// into and rejects any object key that is not exactly a field's json name.
func checkKeys(v any, t reflect.Type) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Struct:
		obj, ok := v.(map[string]any)
		if !ok {
			return nil // null; the typed decode already checked the shape
		}
		fields := map[string]reflect.Type{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			if name == "-" || !f.IsExported() {
				continue
			}
			if name == "" {
				name = f.Name
			}
			fields[name] = f.Type
		}
		for key, val := range obj {
			ft, ok := fields[key]
			if !ok {
				return fmt.Errorf("unknown field %q", key)
			}
			if err := checkKeys(val, ft); err != nil {
				return err
			}
		}
	case reflect.Slice:
		arr, _ := v.([]any)
		for _, el := range arr {
			if err := checkKeys(el, t.Elem()); err != nil {
				return err
			}
		}
	case reflect.Map:
		obj, _ := v.(map[string]any)
		for _, val := range obj {
			if err := checkKeys(val, t.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}

func statusOf(err error) int {
	switch {
	case errors.Is(err, replication.ErrForbidden):
		return http.StatusForbidden
	case errors.Is(err, cart.ErrInvalidOp), errors.Is(err, cart.ErrInvalidInput):
		return http.StatusBadRequest
	case errors.Is(err, cart.ErrInsufficient):
		return http.StatusConflict
	}
	return http.StatusInternalServerError
}

func errorBody(err error) map[string]string { return map[string]string{"error": err.Error()} }

func writeError(w http.ResponseWriter, err error) { writeJSON(w, statusOf(err), errorBody(err)) }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
