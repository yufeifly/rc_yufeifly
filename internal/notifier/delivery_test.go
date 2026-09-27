package notifier

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProviderBuildsEnvelopeAndBearerAuth(t *testing.T) {
	var received struct {
		NotificationID string          `json:"notification_id"`
		EventType      string          `json:"event_type"`
		OccurredAt     time.Time       `json:"occurred_at"`
		Payload        json.RawMessage `json:"payload"`
	}
	var headers http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers = r.Header.Clone()
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)

	cfg := deliveryTestConfig(server.URL, ProviderConfig{
		Version:            "v1",
		Enabled:            true,
		Method:             http.MethodPost,
		BodyMode:           "envelope",
		Headers:            map[string]string{"X-Tenant": "payments", "Authorization": "must-not-win"},
		SuccessStatusCodes: []int{http.StatusAccepted},
		IdempotencyHeader:  "Idempotency-Key",
		Auth:               ProviderAuthConfig{Type: "bearer", secret: "top-secret"},
		Timeout:            ProviderTimeoutConfig{Connect: time.Second, Request: time.Second},
		MaxConcurrency:     1,
	})
	registry, err := NewProviderRegistry(cfg)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	provider, ok := registry.Get("test")
	if !ok {
		t.Fatal("provider not found")
	}

	occurredAt := time.Date(2026, 9, 27, 2, 30, 0, 0, time.UTC)
	outcome := provider.Deliver(context.Background(), ClaimedNotification{
		Notification: Notification{
			ID:                "ntf_01TEST",
			RequestID:         "req-1",
			EventType:         "payment.succeeded",
			OccurredAt:        occurredAt,
			Payload:           json.RawMessage(`{"amount":19900}`),
			DeliveryRound:     2,
			AttemptInRound:    3,
			TotalAttemptCount: 7,
		},
		AttemptID: "att-1",
	})

	if !outcome.Success || outcome.HTTPStatus == nil || *outcome.HTTPStatus != http.StatusAccepted {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if received.NotificationID != "ntf_01TEST" || received.EventType != "payment.succeeded" ||
		!received.OccurredAt.Equal(occurredAt) || string(received.Payload) != `{"amount":19900}` {
		t.Fatalf("unexpected envelope: %+v", received)
	}
	assertHeader(t, headers, "Authorization", "Bearer top-secret")
	assertHeader(t, headers, "X-Tenant", "payments")
	assertHeader(t, headers, "Idempotency-Key", "ntf_01TEST")
	assertHeader(t, headers, "X-Notification-Id", "ntf_01TEST")
	assertHeader(t, headers, "X-Delivery-Attempt", "7")
	assertHeader(t, headers, "X-Delivery-Round", "2")
	assertHeader(t, headers, "X-Request-Id", "req-1")
	assertHeader(t, headers, "Content-Type", "application/json")
}

func TestProviderBuildsPassthroughAndAPIKey(t *testing.T) {
	var body string
	var apiKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
		}
		body = string(data)
		apiKey = r.Header.Get("X-API-Key")
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	cfg := deliveryTestConfig(server.URL, ProviderConfig{
		Enabled:            true,
		Method:             http.MethodPost,
		BodyMode:           "passthrough",
		SuccessStatusCodes: []int{http.StatusNoContent},
		Auth: ProviderAuthConfig{
			Type:   "api_key_header",
			Header: "X-API-Key",
			secret: "api-secret",
		},
		Timeout:        ProviderTimeoutConfig{Connect: time.Second, Request: time.Second},
		MaxConcurrency: 1,
	})
	registry, err := NewProviderRegistry(cfg)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	provider, _ := registry.Get("test")
	outcome := provider.Deliver(context.Background(), ClaimedNotification{
		Notification: Notification{
			ID:      "ntf_passthrough",
			Payload: json.RawMessage(`{"sku":"A-1"}`),
		},
	})

	if !outcome.Success {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if body != `{"sku":"A-1"}` {
		t.Fatalf("body = %s", body)
	}
	if apiKey != "api-secret" {
		t.Fatalf("API key = %q", apiKey)
	}
}

func TestProviderOnlyAcceptsConfiguredSuccessStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	cfg := deliveryTestConfig(server.URL, ProviderConfig{
		Enabled:            true,
		Method:             http.MethodPost,
		BodyMode:           "passthrough",
		SuccessStatusCodes: []int{http.StatusOK},
		Timeout:            ProviderTimeoutConfig{Connect: time.Second, Request: time.Second},
		MaxConcurrency:     1,
	})
	registry, err := NewProviderRegistry(cfg)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	provider, _ := registry.Get("test")
	outcome := provider.Deliver(context.Background(), ClaimedNotification{
		Notification: Notification{ID: "ntf_status", Payload: json.RawMessage(`{}`)},
	})

	if outcome.Success || outcome.Retryable || outcome.HTTPStatus == nil || *outcome.HTTPStatus != http.StatusNoContent {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
}

func TestProviderConcurrencyPermitIsBounded(t *testing.T) {
	provider := &Provider{semaphore: make(chan struct{}, 1)}
	if !provider.TryAcquire() {
		t.Fatal("first acquire failed")
	}
	if provider.TryAcquire() {
		t.Fatal("second acquire exceeded configured concurrency")
	}
	provider.Release()
	if !provider.TryAcquire() {
		t.Fatal("permit was not released")
	}
	provider.Release()
}

func TestProviderRejectsReservedIdempotencyHeader(t *testing.T) {
	cfg := deliveryTestConfig("http://127.0.0.1:18080", ProviderConfig{
		Enabled:            true,
		Method:             http.MethodPost,
		BodyMode:           "passthrough",
		SuccessStatusCodes: []int{http.StatusOK},
		IdempotencyHeader:  "Authorization",
		MaxConcurrency:     1,
	})
	if _, err := NewProviderRegistry(cfg); err == nil {
		t.Fatal("expected reserved header conflict")
	}
}

func TestProviderDoesNotFollowRedirect(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	t.Cleanup(redirect.Close)

	cfg := deliveryTestConfig(redirect.URL, ProviderConfig{
		Enabled:            true,
		Method:             http.MethodPost,
		BodyMode:           "passthrough",
		SuccessStatusCodes: []int{http.StatusOK},
		Timeout:            ProviderTimeoutConfig{Connect: time.Second, Request: time.Second},
		MaxConcurrency:     1,
	})
	registry, err := NewProviderRegistry(cfg)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	provider, _ := registry.Get("test")
	outcome := provider.Deliver(context.Background(), ClaimedNotification{
		Notification: Notification{ID: "ntf_redirect", Payload: json.RawMessage(`{}`)},
	})

	if outcome.Category != ErrorUpstreamRedirect || outcome.Retryable {
		t.Fatalf("unexpected outcome: %+v", outcome)
	}
	if targetCalls.Load() != 0 {
		t.Fatalf("redirect target received %d calls", targetCalls.Load())
	}
}

func TestSafeDialRejectsLoopbackInProduction(t *testing.T) {
	dial := safeDialContext(&net.Dialer{Timeout: time.Second}, net.DefaultResolver, false)
	conn, err := dial(context.Background(), "tcp", "127.0.0.1:443")
	if conn != nil {
		conn.Close()
	}
	if !errors.Is(err, ErrUnsafeDestination) {
		t.Fatalf("error = %v, want ErrUnsafeDestination", err)
	}
}

func TestClassifyResponse(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		status    int
		category  ErrorCategory
		retryable bool
		success   bool
	}{
		{200, ErrorNone, false, true},
		{302, ErrorUpstreamRedirect, false, false},
		{400, ErrorUpstream4XX, false, false},
		{401, ErrorAuthRejected, false, false},
		{403, ErrorAuthRejected, false, false},
		{408, ErrorUpstreamTimeout, true, false},
		{425, ErrorUpstreamTooEarly, true, false},
		{429, ErrorRateLimited, true, false},
		{500, ErrorUpstream5XX, true, false},
	}
	for _, test := range tests {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			outcome := classifyResponse(test.status, "", now)
			if outcome.Category != test.category || outcome.Retryable != test.retryable || outcome.Success != test.success {
				t.Fatalf("outcome = %+v", outcome)
			}
			if outcome.HTTPStatus == nil || *outcome.HTTPStatus != test.status {
				t.Fatalf("HTTP status = %v", outcome.HTTPStatus)
			}
		})
	}
}

func TestClassifyCallError(t *testing.T) {
	timeout := &net.DNSError{Err: "timeout", Name: "provider.invalid", IsTimeout: true}
	dns := &net.DNSError{Err: "no such host", Name: "provider.invalid", IsNotFound: true}
	connect := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("refused")}

	tests := []struct {
		name       string
		ctx        context.Context
		err        error
		wrote      bool
		category   ErrorCategory
		retryable  bool
		safeAbsent string
	}{
		{"dns", context.Background(), dns, false, ErrorDNSError, true, "provider.invalid"},
		{"connect", context.Background(), connect, false, ErrorConnect, true, "refused"},
		{"transport unknown", context.Background(), io.ErrUnexpectedEOF, true, ErrorTransportUnknown, true, ""},
		{"timeout", context.Background(), timeout, true, ErrorTimeoutUnknown, true, "provider.invalid"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			outcome := classifyCallError(test.ctx, test.err, test.wrote)
			if outcome.Category != test.category || outcome.Retryable != test.retryable {
				t.Fatalf("outcome = %+v", outcome)
			}
			if test.safeAbsent != "" && strings.Contains(outcome.SafeMessage, test.safeAbsent) {
				t.Fatalf("unsafe error detail leaked: %q", outcome.SafeMessage)
			}
		})
	}
}

func TestRetryAfterAndExponentialBackoff(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	if got := parseRetryAfter("17", now); got == nil || *got != 17*time.Second {
		t.Fatalf("seconds Retry-After = %v", got)
	}
	date := now.Add(45 * time.Second).Format(http.TimeFormat)
	if got := parseRetryAfter(date, now); got == nil || *got != 45*time.Second {
		t.Fatalf("date Retry-After = %v", got)
	}
	if got := parseRetryAfter("invalid", now); got != nil {
		t.Fatalf("invalid Retry-After = %v", *got)
	}

	policy := RetryConfig{InitialDelay: 5 * time.Second, MaxDelay: 30 * time.Second, JitterRatio: 0.2}
	if got := retryDelay(policy, 1, 0); got != 4*time.Second {
		t.Fatalf("first low-jitter delay = %v", got)
	}
	if got := retryDelay(policy, 3, 1); got != 24*time.Second {
		t.Fatalf("third high-jitter delay = %v", got)
	}
	if got := retryDelay(policy, math.MaxInt, 0.5); got != 30*time.Second {
		t.Fatalf("capped delay = %v", got)
	}
}

func deliveryTestConfig(rawURL string, provider ProviderConfig) Config {
	provider.URL = rawURL
	return Config{
		Environment: "development",
		NetworkPolicy: NetworkPolicyConfig{
			AllowInsecureHTTP: true,
			AllowPrivateIP:    true,
		},
		Providers: map[string]ProviderConfig{"test": provider},
	}
}

func assertHeader(t *testing.T, headers http.Header, name, want string) {
	t.Helper()
	if got := headers.Get(name); got != want {
		t.Fatalf("%s = %q, want %q", name, got, want)
	}
}
