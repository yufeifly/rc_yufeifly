package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type record struct {
	Sequence       int64     `json:"sequence"`
	Path           string    `json:"path"`
	NotificationID string    `json:"notification_id"`
	Attempt        string    `json:"attempt"`
	Status         int       `json:"status"`
	ReceivedAt     time.Time `json:"received_at"`
}

type control struct {
	status  int
	blocked bool
	release chan struct{}
}

type mockProvider struct {
	mu                sync.Mutex
	sequence          atomic.Int64
	records           []record
	attempts          map[string]int
	controls          map[string]*control
	inFlightByPath    map[string]int
	maxInFlightByPath map[string]int
	delay             time.Duration
}

func main() {
	address := flag.String("address", "127.0.0.1:18080", "listen address")
	delay := flag.Duration("delay", 2*time.Second, "delay for /delayed")
	flag.Parse()

	mock := &mockProvider{
		attempts:          make(map[string]int),
		controls:          make(map[string]*control),
		inFlightByPath:    make(map[string]int),
		maxInFlightByPath: make(map[string]int),
		delay:             *delay,
	}
	server := &http.Server{
		Addr:              *address,
		Handler:           mock.routes(),
		ReadHeaderTimeout: 2 * time.Second,
	}
	log.Printf("mock provider listening on %s", *address)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func (m *mockProvider) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(response http.ResponseWriter, _ *http.Request) {
		writeJSON(response, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /always-200", m.fixedStatus(http.StatusOK))
	mux.HandleFunc("POST /always-400", m.fixedStatus(http.StatusBadRequest))
	mux.HandleFunc("POST /always-503", m.fixedStatus(http.StatusServiceUnavailable))
	mux.HandleFunc("POST /fail-twice-then-200", m.failTwice)
	mux.HandleFunc("POST /delayed", m.delayed)
	mux.HandleFunc("POST /controlled/{key}", m.controlled)
	mux.HandleFunc("PUT /controlled/{key}", m.updateControl)
	mux.HandleFunc("GET /records", m.getRecords)
	return mux
}

func (m *mockProvider) fixedStatus(status int) http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		finish := m.begin(request)
		defer finish(status)
		response.WriteHeader(status)
	}
}

func (m *mockProvider) failTwice(response http.ResponseWriter, request *http.Request) {
	finish := m.begin(request)
	id := request.Header.Get("X-Notification-Id")
	m.mu.Lock()
	m.attempts[id]++
	attempt := m.attempts[id]
	m.mu.Unlock()

	status := http.StatusOK
	if attempt <= 2 {
		status = http.StatusServiceUnavailable
	}
	defer finish(status)
	response.WriteHeader(status)
}

func (m *mockProvider) delayed(response http.ResponseWriter, request *http.Request) {
	finish := m.begin(request)
	status := http.StatusOK
	defer func() { finish(status) }()
	timer := time.NewTimer(m.delay)
	defer timer.Stop()
	select {
	case <-request.Context().Done():
		status = 499
	case <-timer.C:
		response.WriteHeader(status)
	}
}

func (m *mockProvider) controlled(response http.ResponseWriter, request *http.Request) {
	finish := m.begin(request)
	key := request.PathValue("key")
	m.mu.Lock()
	current := m.controlFor(key)
	status := current.status
	blocked := current.blocked
	release := current.release
	m.mu.Unlock()

	if blocked {
		select {
		case <-request.Context().Done():
			finish(499)
			return
		case <-release:
		}
		m.mu.Lock()
		status = m.controlFor(key).status
		m.mu.Unlock()
	}
	finish(status)
	response.WriteHeader(status)
}

func (m *mockProvider) updateControl(response http.ResponseWriter, request *http.Request) {
	key := request.PathValue("key")
	status, err := strconv.Atoi(request.URL.Query().Get("status"))
	if err != nil || status < 100 || status > 599 {
		http.Error(response, "valid status is required", http.StatusBadRequest)
		return
	}
	blocked, err := strconv.ParseBool(request.URL.Query().Get("blocked"))
	if err != nil {
		http.Error(response, "valid blocked flag is required", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	current := m.controlFor(key)
	current.status = status
	if blocked && !current.blocked {
		current.blocked = true
		current.release = make(chan struct{})
	}
	if !blocked && current.blocked {
		current.blocked = false
		close(current.release)
	}
	m.mu.Unlock()
	writeJSON(response, http.StatusOK, map[string]any{"key": key, "status": status, "blocked": blocked})
}

func (m *mockProvider) getRecords(response http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	records := append([]record(nil), m.records...)
	maximums := make(map[string]int, len(m.maxInFlightByPath))
	for path, maximum := range m.maxInFlightByPath {
		maximums[path] = maximum
	}
	m.mu.Unlock()
	writeJSON(response, http.StatusOK, map[string]any{
		"records":               records,
		"max_in_flight_by_path": maximums,
	})
}

func (m *mockProvider) begin(request *http.Request) func(int) {
	path := request.URL.Path
	entry := record{
		Sequence:       m.sequence.Add(1),
		Path:           path,
		NotificationID: request.Header.Get("X-Notification-Id"),
		Attempt:        request.Header.Get("X-Delivery-Attempt"),
		ReceivedAt:     time.Now().UTC(),
	}
	m.mu.Lock()
	index := len(m.records)
	m.records = append(m.records, entry)
	m.inFlightByPath[path]++
	if m.inFlightByPath[path] > m.maxInFlightByPath[path] {
		m.maxInFlightByPath[path] = m.inFlightByPath[path]
	}
	m.mu.Unlock()

	return func(status int) {
		m.mu.Lock()
		m.records[index].Status = status
		m.inFlightByPath[path]--
		m.mu.Unlock()
	}
}

func (m *mockProvider) controlFor(key string) *control {
	current, ok := m.controls[key]
	if !ok {
		current = &control{status: http.StatusOK, release: closedChannel()}
		m.controls[key] = current
	}
	return current
}

func closedChannel() chan struct{} {
	channel := make(chan struct{})
	close(channel)
	return channel
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
