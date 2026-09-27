package notifier

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestMetricsExposeProviderFailureCounters(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry, 4, map[string]int{"crm": 2})
	metrics.ObserveDelivery("crm", DeliveryOutcome{Success: true}, 10*time.Millisecond)
	metrics.ObserveDelivery("crm", DeliveryOutcome{
		Category:    ErrorUpstream5XX,
		Retryable:   true,
		SafeMessage: "upstream returned a server error",
	}, 20*time.Millisecond)
	metrics.ObserveDelivery("crm", DeliveryOutcome{
		Category:    ErrorTimeoutUnknown,
		Retryable:   true,
		SafeMessage: "delivery result is unknown after timeout",
	}, 30*time.Millisecond)

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	family := findMetricFamily(t, families, "notifier_delivery_attempts_total")
	labels := make(map[string]bool)
	for _, metric := range family.Metric {
		var outcome, category string
		for _, label := range metric.Label {
			switch label.GetName() {
			case "outcome":
				outcome = label.GetValue()
			case "error_category":
				category = label.GetValue()
			}
		}
		labels[outcome+"/"+category] = true
	}
	for _, want := range []string{
		"success/NONE",
		"retryable_failure/UPSTREAM_5XX",
		"unknown/TIMEOUT_UNKNOWN",
	} {
		if !labels[want] {
			t.Fatalf("missing delivery metric %q in %v", want, labels)
		}
	}
}

func TestMetricsNeverUseNotificationIDAsLabel(t *testing.T) {
	registry := prometheus.NewRegistry()
	_ = NewMetrics(registry, 4, map[string]int{"crm": 2})
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if strings.Contains(label.GetName(), "notification") ||
					strings.Contains(label.GetName(), "caller") ||
					strings.Contains(label.GetName(), "url") {
					t.Fatalf("metric %s has high-cardinality label %q", family.GetName(), label.GetName())
				}
			}
		}
	}
}

func TestShutdownStopsClaimingAndDrainsWorkers(t *testing.T) {
	started := make(chan struct{}, 1)
	releaseRequest := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}
		<-releaseRequest
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	store := openTestStore(t)
	now := time.Now().UTC().Add(-time.Second)
	createDueFixture(t, store, "ntf-drain-first", PriorityNormal, now)
	createDueFixture(t, store, "ntf-drain-second", PriorityNormal, now)
	cfg := deliveryTestConfig(server.URL, ProviderConfig{
		Version:            "v1",
		Enabled:            true,
		Method:             http.MethodPost,
		BodyMode:           "passthrough",
		SuccessStatusCodes: []int{http.StatusOK},
		Timeout:            ProviderTimeoutConfig{Connect: time.Second, Request: 5 * time.Second},
		MaxConcurrency:     1,
	})
	cfg.Providers["crm"] = cfg.Providers["test"]
	delete(cfg.Providers, "test")
	cfg.Workers.Concurrency = 1
	cfg.Priority.AgingInterval = 30 * time.Second
	cfg.Scheduler.PollInterval = 10 * time.Millisecond
	cfg.Scheduler.LeaseDuration = 10 * time.Second
	cfg.Scheduler.RecoveryBatchSize = 10
	cfg.Retry = RetryConfig{InitialDelay: time.Second, MaxDelay: time.Minute}
	providers, err := NewProviderRegistry(cfg)
	if err != nil {
		t.Fatalf("new provider registry: %v", err)
	}
	scheduler := NewScheduler(cfg, store, providers, nil, nil)
	scheduler.Start(context.Background())

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first delivery did not start")
	}
	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		shutdownDone <- scheduler.Shutdown(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	second, err := store.Get(context.Background(), "ntf-drain-second")
	if err != nil {
		t.Fatalf("get second task: %v", err)
	}
	if second.TotalAttemptCount != 0 {
		t.Fatalf("second task was claimed during shutdown: %+v", second)
	}
	close(releaseRequest)
	if err := <-shutdownDone; err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	first, err := store.Get(context.Background(), "ntf-drain-first")
	if err != nil {
		t.Fatalf("get first task: %v", err)
	}
	if first.Status != StatusSucceeded {
		t.Fatalf("in-flight task was not drained: %+v", first)
	}
}

func TestMetricsEndpointRequiresAdmin(t *testing.T) {
	store, handler := newHTTPTestFixture(t, 4096)
	_ = store
	api := handler.(*HTTPAPI)
	registry := prometheus.NewRegistry()
	api.SetMetrics(NewMetrics(registry, 1, nil))

	producer := performJSONRequest(t, api, http.MethodGet, "/metrics", "token-a", "")
	if producer.Code != http.StatusForbidden {
		t.Fatalf("producer metrics status = %d", producer.Code)
	}
	admin := performJSONRequest(t, api, http.MethodGet, "/metrics", "token-admin", "")
	if admin.Code != http.StatusOK {
		t.Fatalf("admin metrics status = %d, body = %s", admin.Code, admin.Body.String())
	}
}

func findMetricFamily(t *testing.T, families []*dto.MetricFamily, name string) *dto.MetricFamily {
	t.Helper()
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric family %q not found", name)
	return nil
}
