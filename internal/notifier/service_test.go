package notifier

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestCanonicalJSONProducesStableHash(t *testing.T) {
	left := json.RawMessage(`{"amount":100,"id":"o1"}`)
	right := json.RawMessage(`{"id":"o1", "amount":100}`)

	if hashRequest(left) != hashRequest(right) {
		t.Fatal("equivalent JSON must have the same hash")
	}
}

func TestCreateAssignsPriorityFromEventType(t *testing.T) {
	store := openTestStore(t)
	clock := &fakeClock{now: time.Date(2026, time.September, 27, 12, 0, 0, 0, time.UTC)}
	service := newTestService(store, clock)

	result, err := service.Create(context.Background(), CreateInput{
		RequestID:      "req-1",
		CallerID:       "order-service",
		IdempotencyKey: "order-1",
		ProviderID:     "crm",
		EventType:      "payment.succeeded",
		Payload:        json.RawMessage(`{"amount":100}`),
	})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	if result.Notification.Priority != PriorityHigh {
		t.Fatalf("priority = %v, want %v", result.Notification.Priority, PriorityHigh)
	}

	persisted, err := store.Get(context.Background(), result.Notification.ID)
	if err != nil {
		t.Fatalf("Get(): %v", err)
	}
	if persisted.Priority != PriorityHigh {
		t.Fatalf("persisted priority = %v, want %v", persisted.Priority, PriorityHigh)
	}
}

func TestCreateWithoutOccurredAtRemainsIdempotentAcrossTime(t *testing.T) {
	store := openTestStore(t)
	clock := &fakeClock{now: time.Date(2026, time.September, 27, 12, 0, 0, 0, time.UTC)}
	service := newTestService(store, clock)
	input := CreateInput{
		RequestID:      "req-1",
		CallerID:       "order-service",
		IdempotencyKey: "order-1",
		ProviderID:     "crm",
		EventType:      "payment.succeeded",
		Payload:        json.RawMessage(`{"amount":100}`),
	}

	first, err := service.Create(context.Background(), input)
	if err != nil {
		t.Fatalf("first Create(): %v", err)
	}
	clock.now = clock.now.Add(time.Hour)
	input.RequestID = "req-2"
	second, err := service.Create(context.Background(), input)
	if err != nil {
		t.Fatalf("duplicate Create(): %v", err)
	}
	if !second.Duplicate {
		t.Fatal("duplicate = false, want true")
	}
	if second.Notification.ID != first.Notification.ID {
		t.Fatalf("duplicate ID = %q, want %q", second.Notification.ID, first.Notification.ID)
	}
}

func TestCreateRejectsUnknownProvider(t *testing.T) {
	store := openTestStore(t)
	service := newTestService(store, &fakeClock{now: time.Now()})

	_, err := service.Create(context.Background(), CreateInput{
		RequestID:      "req-1",
		CallerID:       "order-service",
		IdempotencyKey: "order-1",
		ProviderID:     "missing",
		EventType:      "payment.succeeded",
		Payload:        json.RawMessage(`{"amount":100}`),
	})
	if !errors.Is(err, ErrProviderNotFound) {
		t.Fatalf("Create() error = %v, want %v", err, ErrProviderNotFound)
	}
}

func newTestService(store *Store, clock Clock) *Service {
	return NewService(store, Config{
		Priority: PriorityConfig{
			Default: "NORMAL",
			Events:  map[string]string{"payment.succeeded": "HIGH"},
		},
		Retry: RetryConfig{DefaultMaxAttempts: 3},
		Providers: map[string]ProviderConfig{
			"crm": {
				Enabled: true,
				Retry:   ProviderRetryConfig{MaxAttempts: 2},
			},
		},
	}, clock)
}

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	return c.now
}
