package notifier

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStoreCreateOrGetIsIdempotent(t *testing.T) {
	store := openTestStore(t)
	first := createFixture("ntf-first", "caller-a", "order-1", `{"a":1}`)

	created, err := store.CreateOrGet(context.Background(), first)
	if err != nil || created.Duplicate {
		t.Fatalf("first create = %+v, error = %v", created, err)
	}

	duplicateInput := first
	duplicateInput.ID = "ntf-second"
	duplicate, err := store.CreateOrGet(context.Background(), duplicateInput)
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("duplicate create = %+v, error = %v", duplicate, err)
	}
	if duplicate.Notification.ID != created.Notification.ID {
		t.Fatalf("duplicate ID = %q, want %q", duplicate.Notification.ID, created.Notification.ID)
	}
}

func TestStoreRejectsIdempotencyConflict(t *testing.T) {
	store := openTestStore(t)
	first := createFixture("ntf-first", "caller-a", "order-1", `{"a":1}`)
	if _, err := store.CreateOrGet(context.Background(), first); err != nil {
		t.Fatalf("first create: %v", err)
	}

	conflict := createFixture("ntf-second", "caller-a", "order-1", `{"a":2}`)
	conflict.RequestHash = "different-hash"
	if _, err := store.CreateOrGet(context.Background(), conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting create error = %v, want %v", err, ErrIdempotencyConflict)
	}
}

func TestStoreGetsNotificationByIDAndIdempotencyKey(t *testing.T) {
	store := openTestStore(t)
	input := createFixture("ntf-get", "caller-a", "order-1", `{"a":1}`)
	created, err := store.CreateOrGet(context.Background(), input)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	byID, err := store.Get(context.Background(), created.Notification.ID)
	if err != nil {
		t.Fatalf("Get(): %v", err)
	}
	byKey, err := store.GetByIdempotencyKey(context.Background(), "caller-a", "order-1")
	if err != nil {
		t.Fatalf("GetByIdempotencyKey(): %v", err)
	}
	if byID.ID != created.Notification.ID || byKey.ID != created.Notification.ID {
		t.Fatalf("lookup IDs = %q and %q, want %q", byID.ID, byKey.ID, created.Notification.ID)
	}
	if string(byID.Payload) != `{"a":1}` {
		t.Fatalf("stored payload = %s", byID.Payload)
	}
}

func TestConcurrentCreateProducesOneLogicalTask(t *testing.T) {
	store := openTestStore(t)
	const goroutines = 16

	start := make(chan struct{})
	results := make(chan CreateResult, goroutines)
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			input := createFixture(fmt.Sprintf("ntf-%02d", index), "caller-a", "order-1", `{"a":1}`)
			result, err := store.CreateOrGet(context.Background(), input)
			results <- result
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent CreateOrGet(): %v", err)
		}
	}

	var expectedID string
	createdCount := 0
	for result := range results {
		if expectedID == "" {
			expectedID = result.Notification.ID
		}
		if result.Notification.ID != expectedID {
			t.Fatalf("result ID = %q, want %q", result.Notification.ID, expectedID)
		}
		if !result.Duplicate {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("newly created results = %d, want 1", createdCount)
	}

	var rowCount int
	if err := store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM notifications").Scan(&rowCount); err != nil {
		t.Fatalf("count notifications: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("notification rows = %d, want 1", rowCount)
	}
}

func TestOpenStoreConfiguresSQLiteSafetyPragmas(t *testing.T) {
	store := openTestStore(t)

	assertPragmaValue(t, store, "journal_mode", "wal")
	assertPragmaValue(t, store, "synchronous", "2")
	assertPragmaValue(t, store, "foreign_keys", "1")
	assertPragmaValue(t, store, "busy_timeout", "2500")
}

func TestOpenStoreRejectsFutureSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open fixture database: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE schema_migrations (
			version INTEGER PRIMARY KEY,
			applied_at_ms INTEGER NOT NULL
		) STRICT;
		INSERT INTO schema_migrations(version, applied_at_ms) VALUES (2, 0);
	`); err != nil {
		t.Fatalf("create future schema fixture: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close fixture database: %v", err)
	}

	store, err := OpenStore(context.Background(), DatabaseConfig{
		Path:        path,
		BusyTimeout: time.Second,
	})
	if store != nil {
		_ = store.Close()
	}
	if err == nil {
		t.Fatal("OpenStore() error = nil, want future schema rejection")
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := OpenStore(context.Background(), DatabaseConfig{
		Path:        filepath.Join(t.TempDir(), "notifier.db"),
		BusyTimeout: 2500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("OpenStore(): %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close(): %v", err)
		}
	})
	return store
}

func createFixture(id, callerID, key, payload string) CreateNotification {
	now := time.Date(2026, time.September, 27, 12, 0, 0, 0, time.UTC)
	return CreateNotification{
		ID:                 id,
		RequestID:          "req-test",
		CallerID:           callerID,
		IdempotencyKey:     key,
		RequestHash:        "same-hash",
		RequestHashVersion: 1,
		ProviderID:         "crm",
		EventType:          "order.created",
		Priority:           PriorityNormal,
		OccurredAt:         now,
		Payload:            json.RawMessage(payload),
		MaxAttempts:        3,
		CreatedAt:          now,
	}
}

func assertPragmaValue(t *testing.T, store *Store, name, want string) {
	t.Helper()
	var got string
	if err := store.db.QueryRowContext(context.Background(), "PRAGMA "+name).Scan(&got); err != nil {
		t.Fatalf("read PRAGMA %s: %v", name, err)
	}
	if got != want {
		t.Fatalf("PRAGMA %s = %q, want %q", name, got, want)
	}
}
