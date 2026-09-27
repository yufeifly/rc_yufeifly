package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCreateReturns202OnlyAfterCommit(t *testing.T) {
	store, handler := newHTTPTestFixture(t, 4096)
	response := performJSONRequest(t, handler, http.MethodPost, "/v1/notifications", "token-a", `{
		"idempotency_key":"order-1",
		"provider":"crm",
		"event_type":"payment.succeeded",
		"payload":{"amount":100}
	}`)
	if response.Code != http.StatusAccepted {
		t.Fatalf("POST status = %d, body = %s", response.Code, response.Body.String())
	}

	var body struct {
		NotificationID string `json:"notification_id"`
		Status         Status `json:"status"`
		Duplicate      bool   `json:"duplicate"`
		StatusURL      string `json:"status_url"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.NotificationID == "" || body.Status != StatusPending || body.Duplicate {
		t.Fatalf("unexpected response: %+v", body)
	}
	if body.StatusURL != "/v1/notifications/"+body.NotificationID {
		t.Fatalf("status_url = %q", body.StatusURL)
	}
	if _, err := store.Get(context.Background(), body.NotificationID); err != nil {
		t.Fatalf("accepted task is not queryable: %v", err)
	}
}

func TestStoreFailureNeverReturns202(t *testing.T) {
	store, handler := newHTTPTestFixture(t, 4096)
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	response := performJSONRequest(t, handler, http.MethodPost, "/v1/notifications", "token-a", `{
		"idempotency_key":"order-1",
		"provider":"crm",
		"event_type":"payment.succeeded",
		"payload":{"amount":100}
	}`)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("POST status = %d, want 503; body = %s", response.Code, response.Body.String())
	}
}

func TestProducerCannotReadAnotherCallersTask(t *testing.T) {
	_, handler := newHTTPTestFixture(t, 4096)
	created := performJSONRequest(t, handler, http.MethodPost, "/v1/notifications", "token-a", `{
		"idempotency_key":"order-1",
		"provider":"crm",
		"event_type":"payment.succeeded",
		"payload":{"amount":100}
	}`)
	var body struct {
		NotificationID string `json:"notification_id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode create response: %v", err)
	}

	response := performJSONRequest(
		t,
		handler,
		http.MethodGet,
		"/v1/notifications/"+body.NotificationID,
		"token-b",
		"",
	)
	if response.Code != http.StatusNotFound {
		t.Fatalf("GET status = %d, want 404; body = %s", response.Code, response.Body.String())
	}
}

func TestCreateRejectsUnknownJSONField(t *testing.T) {
	_, handler := newHTTPTestFixture(t, 4096)
	response := performJSONRequest(t, handler, http.MethodPost, "/v1/notifications", "token-a", `{
		"idempotency_key":"order-1",
		"provider":"crm",
		"event_type":"payment.succeeded",
		"payload":{"amount":100},
		"priority":"HIGH"
	}`)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("POST status = %d, want 400; body = %s", response.Code, response.Body.String())
	}
}

func TestCreateRejectsOversizedBody(t *testing.T) {
	_, handler := newHTTPTestFixture(t, 32)
	response := performJSONRequest(t, handler, http.MethodPost, "/v1/notifications", "token-a", `{
		"idempotency_key":"order-1",
		"provider":"crm",
		"event_type":"payment.succeeded",
		"payload":{"amount":100}
	}`)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("POST status = %d, want 413; body = %s", response.Code, response.Body.String())
	}
}

func TestHealthRoutesSeparateLivenessFromReadiness(t *testing.T) {
	store, handler := newHTTPTestFixture(t, 4096)
	live := performJSONRequest(t, handler, http.MethodGet, "/health/live", "", "")
	ready := performJSONRequest(t, handler, http.MethodGet, "/health/ready", "", "")
	if live.Code != http.StatusOK || ready.Code != http.StatusOK {
		t.Fatalf("healthy status: live=%d ready=%d", live.Code, ready.Code)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	live = performJSONRequest(t, handler, http.MethodGet, "/health/live", "", "")
	ready = performJSONRequest(t, handler, http.MethodGet, "/health/ready", "", "")
	if live.Code != http.StatusOK || ready.Code != http.StatusServiceUnavailable {
		t.Fatalf("closed-store status: live=%d ready=%d", live.Code, ready.Code)
	}
}

func newHTTPTestFixture(t *testing.T, maxBodyBytes int64) (*Store, http.Handler) {
	t.Helper()
	store := openTestStore(t)
	service := newTestService(store, &fakeClock{
		now: time.Date(2026, time.September, 27, 12, 0, 0, 0, time.UTC),
	})
	auth, err := NewAuthenticator(AuthConfig{
		Mode: "static_token",
		Principals: []AuthPrincipalConfig{
			{CallerID: "caller-a", Role: "producer", token: "token-a"},
			{CallerID: "caller-b", Role: "producer", token: "token-b"},
			{CallerID: "admin", Role: "admin", token: "token-admin"},
		},
	})
	if err != nil {
		t.Fatalf("NewAuthenticator(): %v", err)
	}
	return store, NewHTTPAPI(service, auth, maxBodyBytes)
}

func performJSONRequest(
	t *testing.T,
	handler http.Handler,
	method string,
	path string,
	token string,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
