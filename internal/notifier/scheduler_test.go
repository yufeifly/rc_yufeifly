package notifier

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestHigherPriorityIsClaimedFirst(t *testing.T) {
	store := openTestStore(t)
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	createDueFixture(t, store, "ntf-low", PriorityLow, now)
	createDueFixture(t, store, "ntf-normal", PriorityNormal, now)
	createDueFixture(t, store, "ntf-high", PriorityHigh, now)

	claimed, err := store.ClaimAndStart(context.Background(), ClaimRequest{
		ProviderID:      "crm",
		ProviderVersion: "v1",
		WorkerID:        "worker-a",
		Now:             now,
		LeaseDuration:   30 * time.Second,
		AgingInterval:   30 * time.Second,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.ID != "ntf-high" {
		t.Fatalf("claimed %q, want high-priority task", claimed.ID)
	}
}

func TestLowPriorityAgesWithoutStarvation(t *testing.T) {
	store := openTestStore(t)
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	aging := 30 * time.Second
	createDueFixture(t, store, "ntf-old-low", PriorityLow, now.Add(-2*aging))
	createDueFixture(t, store, "ntf-new-high", PriorityHigh, now)

	claimed, err := store.ClaimAndStart(context.Background(), ClaimRequest{
		ProviderID:      "crm",
		ProviderVersion: "v1",
		WorkerID:        "worker-a",
		Now:             now,
		LeaseDuration:   30 * time.Second,
		AgingInterval:   aging,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.ID != "ntf-old-low" {
		t.Fatalf("claimed %q, want aged low-priority task", claimed.ID)
	}
}

func TestRetryAgingStartsAtNextAttemptTime(t *testing.T) {
	store := openTestStore(t)
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	old := createFixture("ntf-retry-low", "caller-ntf-retry-low", "key-ntf-retry-low", `{}`)
	old.Priority = PriorityLow
	old.CreatedAt = now.Add(-24 * time.Hour)
	if _, err := store.CreateOrGet(context.Background(), old); err != nil {
		t.Fatalf("create retry fixture: %v", err)
	}
	if _, err := store.db.Exec(`
		UPDATE notifications
		SET status = ?, next_attempt_at_ms = ?, updated_at_ms = ?
		WHERE id = ?
	`, StatusRetryWait, now.UnixMilli(), now.UnixMilli(), old.ID); err != nil {
		t.Fatalf("prepare retry fixture: %v", err)
	}
	createDueFixture(t, store, "ntf-normal", PriorityNormal, now)

	claimed, err := store.ClaimAndStart(context.Background(), ClaimRequest{
		ProviderID:      "crm",
		ProviderVersion: "v1",
		WorkerID:        "worker-a",
		Now:             now,
		LeaseDuration:   30 * time.Second,
		AgingInterval:   30 * time.Second,
	})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.ID != "ntf-normal" {
		t.Fatalf("claimed %q, retry task incorrectly aged from creation time", claimed.ID)
	}
}

func TestExpiredLeaseRejectsOldWorker(t *testing.T) {
	store := openTestStore(t)
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	createDueFixture(t, store, "ntf-fenced", PriorityNormal, now)

	first, err := store.ClaimAndStart(context.Background(), ClaimRequest{
		ProviderID:      "crm",
		ProviderVersion: "v1",
		WorkerID:        "worker-a",
		Now:             now,
		LeaseDuration:   time.Second,
		AgingInterval:   30 * time.Second,
	})
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if _, err := store.db.Exec(`
		UPDATE delivery_attempts SET status = 'ABANDONED', finished_at_ms = ?
		WHERE id = ?;
	`, now.Add(2*time.Second).UnixMilli(), first.AttemptID); err != nil {
		t.Fatalf("abandon first attempt: %v", err)
	}
	if _, err := store.db.Exec(`
		UPDATE notifications
		SET status = ?, next_attempt_at_ms = ?, lease_owner = NULL,
		    lease_until_ms = NULL, version = version + 1
		WHERE id = ?;
	`, StatusPending, now.Add(2*time.Second).UnixMilli(), first.ID); err != nil {
		t.Fatalf("requeue expired task: %v", err)
	}

	second, err := store.ClaimAndStart(context.Background(), ClaimRequest{
		ProviderID:      "crm",
		ProviderVersion: "v1",
		WorkerID:        "worker-b",
		Now:             now.Add(2 * time.Second),
		LeaseDuration:   time.Second,
		AgingInterval:   30 * time.Second,
	})
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if second.LeaseEpoch <= first.LeaseEpoch {
		t.Fatalf("lease epoch did not advance: first=%d second=%d", first.LeaseEpoch, second.LeaseEpoch)
	}

	err = store.Complete(context.Background(), CompleteRequest{
		ID:          first.ID,
		AttemptID:   first.AttemptID,
		WorkerID:    first.LeaseOwner,
		LeaseEpoch:  first.LeaseEpoch,
		Version:     first.Version,
		Outcome:     DeliveryOutcome{Success: true, Category: ErrorNone},
		CompletedAt: now.Add(3 * time.Second),
	})
	if !errors.Is(err, ErrStaleLease) {
		t.Fatalf("old worker completion error = %v, want ErrStaleLease", err)
	}
	current, err := store.Get(context.Background(), first.ID)
	if err != nil {
		t.Fatalf("get current task: %v", err)
	}
	if current.LeaseOwner != "worker-b" || current.LeaseEpoch != second.LeaseEpoch || current.Status != StatusDelivering {
		t.Fatalf("old worker changed current lease: %+v", current)
	}
}

func TestEffectivePriorityCapsAtHigh(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	if got := EffectivePriority(PriorityLow, now.Add(-2*time.Minute), now, 30*time.Second); got != PriorityHigh {
		t.Fatalf("effective priority = %v, want HIGH", got)
	}
}

func createDueFixture(t *testing.T, store *Store, id string, priority Priority, due time.Time) {
	t.Helper()
	input := createFixture(id, "caller-"+id, "key-"+id, `{}`)
	input.Priority = priority
	input.CreatedAt = due
	if _, err := store.CreateOrGet(context.Background(), input); err != nil {
		t.Fatalf("create %s: %v", id, err)
	}
}
