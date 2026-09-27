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

func TestRetryableFailureSchedulesNextAttempt(t *testing.T) {
	store := openTestStore(t)
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	createDueFixture(t, store, "ntf-retry", PriorityNormal, now)
	claimed := claimFixture(t, store, "ntf-retry", now)
	next := now.Add(10 * time.Second)
	status := httpStatusPointer(503)

	err := store.Complete(context.Background(), CompleteRequest{
		ID:            claimed.ID,
		AttemptID:     claimed.AttemptID,
		WorkerID:      claimed.LeaseOwner,
		LeaseEpoch:    claimed.LeaseEpoch,
		Version:       claimed.Version,
		Outcome:       DeliveryOutcome{Category: ErrorUpstream5XX, HTTPStatus: status, Retryable: true, SafeMessage: "upstream returned a server error"},
		CompletedAt:   now.Add(time.Second),
		NextAttemptAt: &next,
	})
	if err != nil {
		t.Fatalf("complete retryable failure: %v", err)
	}

	notification, err := store.Get(context.Background(), claimed.ID)
	if err != nil {
		t.Fatalf("get notification: %v", err)
	}
	if notification.Status != StatusRetryWait || notification.NextAttemptAt == nil || !notification.NextAttemptAt.Equal(next) {
		t.Fatalf("notification was not scheduled for retry: %+v", notification)
	}
	if notification.LastErrorCategory != ErrorUpstream5XX || notification.LastHTTPStatus == nil || *notification.LastHTTPStatus != 503 {
		t.Fatalf("notification error fields = %+v", notification)
	}
}

func TestPermanentFailureBecomesDead(t *testing.T) {
	store := openTestStore(t)
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	createDueFixture(t, store, "ntf-dead", PriorityNormal, now)
	claimed := claimFixture(t, store, "ntf-dead", now)

	err := store.Complete(context.Background(), CompleteRequest{
		ID:          claimed.ID,
		AttemptID:   claimed.AttemptID,
		WorkerID:    claimed.LeaseOwner,
		LeaseEpoch:  claimed.LeaseEpoch,
		Version:     claimed.Version,
		Outcome:     DeliveryOutcome{Category: ErrorUpstream4XX, SafeMessage: "upstream rejected the notification"},
		CompletedAt: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("complete permanent failure: %v", err)
	}
	notification, err := store.Get(context.Background(), claimed.ID)
	if err != nil {
		t.Fatalf("get notification: %v", err)
	}
	if notification.Status != StatusDead || notification.DeadAt == nil || notification.NextAttemptAt != nil {
		t.Fatalf("notification did not become dead: %+v", notification)
	}
}

func TestExpiredStartedAttemptBecomesAbandoned(t *testing.T) {
	store := openTestStore(t)
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	createDueFixture(t, store, "ntf-expired", PriorityNormal, now)
	claimed := claimFixture(t, store, "ntf-expired", now)

	recovered, err := store.RecoverExpired(context.Background(), now.Add(31*time.Second), 10)
	if err != nil {
		t.Fatalf("recover expired: %v", err)
	}
	if recovered != 1 {
		t.Fatalf("recovered = %d, want 1", recovered)
	}
	notification, err := store.Get(context.Background(), claimed.ID)
	if err != nil {
		t.Fatalf("get notification: %v", err)
	}
	if notification.Status != StatusPending || notification.NextAttemptAt == nil ||
		notification.LastErrorCategory != ErrorLeaseExpiredUnknown {
		t.Fatalf("unexpected recovered notification: %+v", notification)
	}
	var attemptStatus string
	var category string
	if err := store.db.QueryRow(`
		SELECT status, error_category FROM delivery_attempts WHERE id = ?
	`, claimed.AttemptID).Scan(&attemptStatus, &category); err != nil {
		t.Fatalf("read recovered attempt: %v", err)
	}
	if attemptStatus != "ABANDONED" || category != string(ErrorLeaseExpiredUnknown) {
		t.Fatalf("attempt status=%q category=%q", attemptStatus, category)
	}
}

func TestReplayPreservesNotificationIDAndHistory(t *testing.T) {
	store := openTestStore(t)
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	createDueFixture(t, store, "ntf-replay", PriorityNormal, now)
	claimed := claimFixture(t, store, "ntf-replay", now)
	if err := store.Complete(context.Background(), CompleteRequest{
		ID:          claimed.ID,
		AttemptID:   claimed.AttemptID,
		WorkerID:    claimed.LeaseOwner,
		LeaseEpoch:  claimed.LeaseEpoch,
		Version:     claimed.Version,
		Outcome:     DeliveryOutcome{Category: ErrorUpstream4XX, SafeMessage: "rejected"},
		CompletedAt: now.Add(time.Second),
	}); err != nil {
		t.Fatalf("make task dead: %v", err)
	}

	replayed, err := store.ReplayDead(context.Background(), ReplayRequest{
		ID:         claimed.ID,
		AuditID:    "rpl_test",
		OperatorID: "admin",
		Reason:     "credential rotated",
		Now:        now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("replay dead task: %v", err)
	}
	if replayed.ID != claimed.ID || replayed.Status != StatusPending ||
		replayed.DeliveryRound != 2 || replayed.AttemptInRound != 0 ||
		replayed.TotalAttemptCount != 1 || replayed.LeaseEpoch != claimed.LeaseEpoch {
		t.Fatalf("unexpected replayed task: %+v", replayed)
	}
	var attempts int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM delivery_attempts WHERE notification_id = ?`, claimed.ID).Scan(&attempts); err != nil {
		t.Fatalf("count attempts: %v", err)
	}
	var audits int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM replay_audits WHERE notification_id = ?`, claimed.ID).Scan(&audits); err != nil {
		t.Fatalf("count audits: %v", err)
	}
	if attempts != 1 || audits != 1 {
		t.Fatalf("history counts: attempts=%d audits=%d", attempts, audits)
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

func claimFixture(t *testing.T, store *Store, id string, now time.Time) ClaimedNotification {
	t.Helper()
	claimed, err := store.ClaimAndStart(context.Background(), ClaimRequest{
		ProviderID:      "crm",
		ProviderVersion: "v1",
		WorkerID:        "worker-a",
		Now:             now,
		LeaseDuration:   30 * time.Second,
		AgingInterval:   30 * time.Second,
	})
	if err != nil {
		t.Fatalf("claim %s: %v", id, err)
	}
	if claimed.ID != id {
		t.Fatalf("claimed %s, want %s", claimed.ID, id)
	}
	return claimed
}

func httpStatusPointer(status int) *int {
	return &status
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
