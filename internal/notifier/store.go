package notifier

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/oklog/ulid/v2"
	_ "modernc.org/sqlite"
)

const schemaVersion = 1

type Store struct {
	db *sql.DB
}

func OpenStore(ctx context.Context, cfg DatabaseConfig) (*Store, error) {
	if cfg.Path == "" {
		return nil, errors.New("database path is required")
	}
	if cfg.BusyTimeout <= 0 {
		return nil, errors.New("database busy timeout must be positive")
	}

	absolutePath, err := filepath.Abs(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("resolve database path: %w", err)
	}
	if mkdirErr := os.MkdirAll(filepath.Dir(absolutePath), 0o700); mkdirErr != nil {
		return nil, fmt.Errorf("create database directory: %w", mkdirErr)
	}

	databaseURL := &url.URL{Scheme: "file", Path: absolutePath}
	query := databaseURL.Query()
	query.Set("_txlock", "immediate")
	databaseURL.RawQuery = query.Encode()

	db, err := sql.Open("sqlite", databaseURL.String())
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	store := &Store{db: db}
	if err := store.initialize(ctx, cfg.BusyTimeout); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := os.Chmod(absolutePath, 0o600); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("secure database file: %w", err)
	}
	return store, nil
}

func (s *Store) initialize(ctx context.Context, busyTimeout time.Duration) error {
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	pragmas := []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = FULL",
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = " + strconv.FormatInt(busyTimeout.Milliseconds(), 10),
	}
	for _, statement := range pragmas {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("configure database with %q: %w", statement, err)
		}
	}

	if _, err := s.db.ExecContext(ctx, migrationTableSQL); err != nil {
		return fmt.Errorf("create schema migration metadata: %w", err)
	}

	var version sql.NullInt64
	if err := s.db.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_migrations").Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version.Valid && version.Int64 > schemaVersion {
		return fmt.Errorf("database schema version %d is newer than supported version %d", version.Int64, schemaVersion)
	}
	if !version.Valid || version.Int64 < schemaVersion {
		if err := s.migrateV1(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) migrateV1(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin schema migration: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, schemaV1SQL); err != nil {
		return fmt.Errorf("apply schema version 1: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		"INSERT INTO schema_migrations(version, applied_at_ms) VALUES (?, ?)",
		schemaVersion,
		time.Now().UTC().UnixMilli(),
	); err != nil {
		return fmt.Errorf("record schema version 1: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit schema version 1: %w", err)
	}
	return nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *Store) CreateOrGet(ctx context.Context, in CreateNotification) (CreateResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CreateResult{}, fmt.Errorf("begin create notification: %w", err)
	}
	defer tx.Rollback()

	result, err := tx.ExecContext(ctx, `
		INSERT INTO notifications (
			id, request_id, caller_id, idempotency_key, request_hash,
			request_hash_version, provider_id, event_type, priority,
			occurred_at_ms, payload_json, status, max_attempts,
			next_attempt_at_ms, created_at_ms, updated_at_ms
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(caller_id, idempotency_key) DO NOTHING
	`,
		in.ID,
		in.RequestID,
		in.CallerID,
		in.IdempotencyKey,
		in.RequestHash,
		in.RequestHashVersion,
		in.ProviderID,
		in.EventType,
		in.Priority,
		in.OccurredAt.UTC().UnixMilli(),
		string(in.Payload),
		StatusPending,
		in.MaxAttempts,
		in.CreatedAt.UTC().UnixMilli(),
		in.CreatedAt.UTC().UnixMilli(),
		in.CreatedAt.UTC().UnixMilli(),
	)
	if err != nil {
		return CreateResult{}, fmt.Errorf("insert notification: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return CreateResult{}, fmt.Errorf("inspect notification insert: %w", err)
	}

	var notification Notification
	duplicate := rowsAffected == 0
	if duplicate {
		notification, err = getNotification(
			ctx,
			tx.QueryRowContext(ctx, notificationSelect+` WHERE caller_id = ? AND idempotency_key = ?`, in.CallerID, in.IdempotencyKey),
		)
		if err != nil {
			return CreateResult{}, err
		}
		if notification.RequestHashVersion != in.RequestHashVersion || notification.RequestHash != in.RequestHash {
			return CreateResult{}, ErrIdempotencyConflict
		}
	} else {
		notification, err = getNotification(
			ctx,
			tx.QueryRowContext(ctx, notificationSelect+` WHERE id = ?`, in.ID),
		)
		if err != nil {
			return CreateResult{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return CreateResult{}, fmt.Errorf("commit notification: %w", err)
	}
	return CreateResult{Notification: notification, Duplicate: duplicate}, nil
}

func (s *Store) Get(ctx context.Context, id string) (Notification, error) {
	return getNotification(ctx, s.db.QueryRowContext(ctx, notificationSelect+` WHERE id = ?`, id))
}

func (s *Store) GetByIdempotencyKey(ctx context.Context, callerID, key string) (Notification, error) {
	return getNotification(
		ctx,
		s.db.QueryRowContext(ctx, notificationSelect+` WHERE caller_id = ? AND idempotency_key = ?`, callerID, key),
	)
}

func EffectivePriority(original Priority, dueSince, now time.Time, aging time.Duration) Priority {
	if original >= PriorityHigh || aging <= 0 || !now.After(dueSince) {
		return original
	}
	promotions := int(now.Sub(dueSince) / aging)
	effective := original + Priority(promotions)
	if effective > PriorityHigh {
		return PriorityHigh
	}
	return effective
}

func (s *Store) ListDueProviders(ctx context.Context, now time.Time, aging time.Duration) ([]DueProvider, error) {
	if aging <= 0 {
		return nil, errors.New("aging interval must be positive")
	}
	nowMilliseconds := now.UTC().UnixMilli()
	agingMilliseconds := aging.Milliseconds()
	rows, err := s.db.QueryContext(ctx, `
		WITH due AS (
			SELECT
				provider_id,
				MIN(3, priority + CAST(MAX(0, ? - next_attempt_at_ms) / ? AS INTEGER)) AS effective_priority,
				next_attempt_at_ms
			FROM notifications
			WHERE status IN ('PENDING','RETRY_WAIT')
			  AND next_attempt_at_ms <= ?
			  AND attempt_in_round < max_attempts
		),
		highest AS (
			SELECT MAX(effective_priority) AS priority FROM due
		)
		SELECT provider_id, effective_priority, MIN(next_attempt_at_ms)
		FROM due
		WHERE effective_priority = (SELECT priority FROM highest)
		GROUP BY provider_id, effective_priority
		ORDER BY MIN(next_attempt_at_ms), provider_id
	`, nowMilliseconds, agingMilliseconds, nowMilliseconds)
	if err != nil {
		return nil, fmt.Errorf("list due providers: %w", err)
	}
	defer rows.Close()

	var providers []DueProvider
	for rows.Next() {
		var provider DueProvider
		var dueAtMilliseconds int64
		if err := rows.Scan(&provider.ProviderID, &provider.EffectivePriority, &dueAtMilliseconds); err != nil {
			return nil, fmt.Errorf("scan due provider: %w", err)
		}
		provider.DueAt = time.UnixMilli(dueAtMilliseconds).UTC()
		providers = append(providers, provider)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due providers: %w", err)
	}
	return providers, nil
}

func (s *Store) ClaimAndStart(ctx context.Context, in ClaimRequest) (ClaimedNotification, error) {
	if in.ProviderID == "" || in.WorkerID == "" || in.LeaseDuration <= 0 || in.AgingInterval <= 0 {
		return ClaimedNotification{}, errors.New("invalid claim request")
	}
	nowMilliseconds := in.Now.UTC().UnixMilli()
	agingMilliseconds := in.AgingInterval.Milliseconds()
	if agingMilliseconds <= 0 {
		return ClaimedNotification{}, errors.New("aging interval must be at least one millisecond")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ClaimedNotification{}, fmt.Errorf("begin claim: %w", err)
	}
	defer tx.Rollback()

	var id string
	var oldVersion int64
	err = tx.QueryRowContext(ctx, `
		WITH due AS (
			SELECT
				id,
				provider_id,
				version,
				next_attempt_at_ms,
				created_at_ms,
				MIN(3, priority + CAST(MAX(0, ? - next_attempt_at_ms) / ? AS INTEGER)) AS effective_priority
			FROM notifications
			WHERE status IN ('PENDING','RETRY_WAIT')
			  AND next_attempt_at_ms <= ?
			  AND attempt_in_round < max_attempts
		)
		SELECT id, version
		FROM due
		WHERE provider_id = ?
		  AND effective_priority = (SELECT MAX(effective_priority) FROM due)
		ORDER BY effective_priority DESC, next_attempt_at_ms, created_at_ms
		LIMIT 1
	`, nowMilliseconds, agingMilliseconds, nowMilliseconds, in.ProviderID).Scan(&id, &oldVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return ClaimedNotification{}, ErrNotFound
	}
	if err != nil {
		return ClaimedNotification{}, fmt.Errorf("select notification to claim: %w", err)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE notifications
		SET status = ?,
		    attempt_in_round = attempt_in_round + 1,
		    total_attempt_count = total_attempt_count + 1,
		    next_attempt_at_ms = NULL,
		    lease_owner = ?,
		    lease_epoch = lease_epoch + 1,
		    lease_until_ms = ?,
		    updated_at_ms = ?,
		    version = version + 1
		WHERE id = ?
		  AND version = ?
		  AND status IN ('PENDING','RETRY_WAIT')
		  AND next_attempt_at_ms <= ?
		  AND attempt_in_round < max_attempts
	`, StatusDelivering, in.WorkerID, in.Now.Add(in.LeaseDuration).UTC().UnixMilli(),
		nowMilliseconds, id, oldVersion, nowMilliseconds)
	if err != nil {
		return ClaimedNotification{}, fmt.Errorf("claim notification: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return ClaimedNotification{}, fmt.Errorf("inspect notification claim: %w", err)
	}
	if rowsAffected != 1 {
		return ClaimedNotification{}, ErrNotFound
	}

	notification, err := getNotification(ctx, tx.QueryRowContext(ctx, notificationSelect+` WHERE id = ?`, id))
	if err != nil {
		return ClaimedNotification{}, err
	}
	attemptID := "att_" + ulid.Make().String()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO delivery_attempts (
			id, notification_id, delivery_round, attempt_in_round,
			total_attempt_no, lease_epoch, worker_id, provider_version,
			status, started_at_ms
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'STARTED', ?)
	`, attemptID, notification.ID, notification.DeliveryRound, notification.AttemptInRound,
		notification.TotalAttemptCount, notification.LeaseEpoch, in.WorkerID,
		in.ProviderVersion, nowMilliseconds); err != nil {
		return ClaimedNotification{}, fmt.Errorf("start delivery attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ClaimedNotification{}, fmt.Errorf("commit notification claim: %w", err)
	}
	return ClaimedNotification{Notification: notification, AttemptID: attemptID}, nil
}

func (s *Store) Complete(ctx context.Context, in CompleteRequest) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin completion: %w", err)
	}
	defer tx.Rollback()

	var attemptInRound int
	var maxAttempts int
	err = tx.QueryRowContext(ctx, `
		SELECT attempt_in_round, max_attempts
		FROM notifications
		WHERE id = ? AND status = ? AND lease_owner = ? AND lease_epoch = ? AND version = ?
	`, in.ID, StatusDelivering, in.WorkerID, in.LeaseEpoch, in.Version).Scan(&attemptInRound, &maxAttempts)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrStaleLease
	}
	if err != nil {
		return fmt.Errorf("read notification completion state: %w", err)
	}

	status := StatusDead
	var nextAttemptAt any
	var succeededAt any
	var deadAt any
	if in.Outcome.Success {
		status = StatusSucceeded
		succeededAt = in.CompletedAt.UTC().UnixMilli()
	} else if in.Outcome.Retryable && attemptInRound < maxAttempts && in.NextAttemptAt != nil {
		status = StatusRetryWait
		nextAttemptAt = in.NextAttemptAt.UTC().UnixMilli()
	} else {
		deadAt = in.CompletedAt.UTC().UnixMilli()
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE notifications
		SET status = ?,
		    next_attempt_at_ms = ?,
		    lease_owner = NULL,
		    lease_until_ms = NULL,
		    version = version + 1,
		    last_error_category = ?,
		    last_http_status = ?,
		    last_error_message = ?,
		    updated_at_ms = ?,
		    succeeded_at_ms = ?,
		    dead_at_ms = ?
		WHERE id = ? AND status = ? AND lease_owner = ? AND lease_epoch = ? AND version = ?
	`, status, nextAttemptAt, nullableErrorCategory(in.Outcome), nullableInt(in.Outcome.HTTPStatus),
		nullableMessage(in.Outcome), in.CompletedAt.UTC().UnixMilli(), succeededAt, deadAt,
		in.ID, StatusDelivering, in.WorkerID, in.LeaseEpoch, in.Version)
	if err != nil {
		return fmt.Errorf("complete notification: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect notification completion: %w", err)
	}
	if rowsAffected != 1 {
		return ErrStaleLease
	}

	attemptStatus := "FAILED"
	if in.Outcome.Success {
		attemptStatus = "SUCCEEDED"
	}
	result, err = tx.ExecContext(ctx, `
		UPDATE delivery_attempts
		SET status = ?,
		    finished_at_ms = ?,
		    latency_ms = MAX(0, ? - started_at_ms),
		    http_status = ?,
		    error_category = ?,
		    error_message = ?,
		    retryable = ?,
		    next_attempt_at_ms = ?
		WHERE id = ? AND notification_id = ? AND status = 'STARTED'
	`, attemptStatus, in.CompletedAt.UTC().UnixMilli(), in.CompletedAt.UTC().UnixMilli(),
		nullableInt(in.Outcome.HTTPStatus), nullableErrorCategory(in.Outcome),
		nullableMessage(in.Outcome), boolInteger(in.Outcome.Retryable), nextAttemptAt,
		in.AttemptID, in.ID)
	if err != nil {
		return fmt.Errorf("complete delivery attempt: %w", err)
	}
	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect delivery attempt completion: %w", err)
	}
	if rowsAffected != 1 {
		return ErrStaleLease
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit notification completion: %w", err)
	}
	return nil
}

func nullableErrorCategory(outcome DeliveryOutcome) any {
	if outcome.Success || outcome.Category == "" || outcome.Category == ErrorNone {
		return nil
	}
	return string(outcome.Category)
}

func nullableInt(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableMessage(outcome DeliveryOutcome) any {
	if outcome.Success || outcome.SafeMessage == "" {
		return nil
	}
	const maximumLength = 512
	if len(outcome.SafeMessage) > maximumLength {
		return outcome.SafeMessage[:maximumLength]
	}
	return outcome.SafeMessage
}

func boolInteger(value bool) int {
	if value {
		return 1
	}
	return 0
}

type rowScanner interface {
	Scan(dest ...any) error
}

func getNotification(_ context.Context, row rowScanner) (Notification, error) {
	var notification Notification
	var payload string
	var nextAttemptAt sql.NullInt64
	var leaseOwner sql.NullString
	var leaseUntil sql.NullInt64
	var lastErrorCategory sql.NullString
	var lastHTTPStatus sql.NullInt64
	var lastErrorMessage sql.NullString
	var occurredAt int64
	var createdAt int64
	var updatedAt int64
	var succeededAt sql.NullInt64
	var deadAt sql.NullInt64

	err := row.Scan(
		&notification.ID,
		&notification.RequestID,
		&notification.CallerID,
		&notification.IdempotencyKey,
		&notification.RequestHash,
		&notification.RequestHashVersion,
		&notification.ProviderID,
		&notification.EventType,
		&notification.Priority,
		&occurredAt,
		&payload,
		&notification.Status,
		&notification.DeliveryRound,
		&notification.AttemptInRound,
		&notification.TotalAttemptCount,
		&notification.MaxAttempts,
		&nextAttemptAt,
		&leaseOwner,
		&notification.LeaseEpoch,
		&leaseUntil,
		&notification.Version,
		&lastErrorCategory,
		&lastHTTPStatus,
		&lastErrorMessage,
		&createdAt,
		&updatedAt,
		&succeededAt,
		&deadAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Notification{}, ErrNotFound
	}
	if err != nil {
		return Notification{}, fmt.Errorf("scan notification: %w", err)
	}

	notification.Payload = []byte(payload)
	notification.OccurredAt = time.UnixMilli(occurredAt).UTC()
	notification.CreatedAt = time.UnixMilli(createdAt).UTC()
	notification.UpdatedAt = time.UnixMilli(updatedAt).UTC()
	notification.LeaseOwner = leaseOwner.String
	notification.LastErrorCategory = ErrorCategory(lastErrorCategory.String)
	notification.LastErrorMessage = lastErrorMessage.String
	notification.NextAttemptAt = nullableTime(nextAttemptAt)
	notification.LeaseUntil = nullableTime(leaseUntil)
	notification.SucceededAt = nullableTime(succeededAt)
	notification.DeadAt = nullableTime(deadAt)
	if lastHTTPStatus.Valid {
		status := int(lastHTTPStatus.Int64)
		notification.LastHTTPStatus = &status
	}
	return notification, nil
}

func nullableTime(value sql.NullInt64) *time.Time {
	if !value.Valid {
		return nil
	}
	parsed := time.UnixMilli(value.Int64).UTC()
	return &parsed
}

const notificationSelect = `
	SELECT
		id, request_id, caller_id, idempotency_key, request_hash,
		request_hash_version, provider_id, event_type, priority,
		occurred_at_ms, payload_json, status, delivery_round,
		attempt_in_round, total_attempt_count, max_attempts,
		next_attempt_at_ms, lease_owner, lease_epoch, lease_until_ms,
		version, last_error_category, last_http_status, last_error_message,
		created_at_ms, updated_at_ms, succeeded_at_ms, dead_at_ms
	FROM notifications
`

const migrationTableSQL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	version INTEGER PRIMARY KEY,
	applied_at_ms INTEGER NOT NULL
) STRICT;
`

const schemaV1SQL = `
CREATE TABLE IF NOT EXISTS notifications (
	id                     TEXT PRIMARY KEY,
	request_id             TEXT NOT NULL,
	caller_id              TEXT NOT NULL,
	idempotency_key        TEXT NOT NULL,
	request_hash           TEXT NOT NULL,
	request_hash_version   INTEGER NOT NULL,
	provider_id            TEXT NOT NULL,
	event_type             TEXT NOT NULL,
	priority               INTEGER NOT NULL DEFAULT 2 CHECK (priority IN (1,2,3)),
	occurred_at_ms         INTEGER NOT NULL,
	payload_json           TEXT NOT NULL CHECK (json_valid(payload_json)),
	status                 TEXT NOT NULL CHECK (
		status IN ('PENDING','DELIVERING','RETRY_WAIT','SUCCEEDED','DEAD')
	),
	delivery_round         INTEGER NOT NULL DEFAULT 1 CHECK (delivery_round >= 1),
	attempt_in_round       INTEGER NOT NULL DEFAULT 0 CHECK (attempt_in_round >= 0),
	total_attempt_count    INTEGER NOT NULL DEFAULT 0 CHECK (total_attempt_count >= 0),
	max_attempts           INTEGER NOT NULL CHECK (max_attempts >= 1),
	next_attempt_at_ms     INTEGER,
	lease_owner            TEXT,
	lease_epoch            INTEGER NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
	lease_until_ms         INTEGER,
	version                INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
	last_error_category    TEXT,
	last_http_status       INTEGER,
	last_error_message     TEXT,
	created_at_ms          INTEGER NOT NULL,
	updated_at_ms          INTEGER NOT NULL,
	succeeded_at_ms        INTEGER,
	dead_at_ms             INTEGER,
	UNIQUE (caller_id, idempotency_key),
	CHECK (
		(status = 'DELIVERING'
		 AND lease_owner IS NOT NULL
		 AND lease_until_ms IS NOT NULL
		 AND next_attempt_at_ms IS NULL)
		OR
		(status <> 'DELIVERING'
		 AND lease_owner IS NULL
		 AND lease_until_ms IS NULL)
	),
	CHECK (
		(status IN ('PENDING','RETRY_WAIT') AND next_attempt_at_ms IS NOT NULL)
		OR
		(status IN ('DELIVERING','SUCCEEDED','DEAD') AND next_attempt_at_ms IS NULL)
	)
) STRICT;

CREATE TABLE IF NOT EXISTS delivery_attempts (
	id                    TEXT PRIMARY KEY,
	notification_id       TEXT NOT NULL,
	delivery_round        INTEGER NOT NULL,
	attempt_in_round      INTEGER NOT NULL,
	total_attempt_no      INTEGER NOT NULL,
	lease_epoch           INTEGER NOT NULL,
	worker_id             TEXT NOT NULL,
	provider_version      TEXT NOT NULL,
	status                TEXT NOT NULL CHECK (
		status IN ('STARTED','SUCCEEDED','FAILED','ABANDONED')
	),
	started_at_ms         INTEGER NOT NULL,
	finished_at_ms        INTEGER,
	latency_ms            INTEGER,
	http_status           INTEGER,
	error_category        TEXT,
	error_message         TEXT,
	retryable             INTEGER CHECK (retryable IN (0,1)),
	next_attempt_at_ms    INTEGER,
	FOREIGN KEY (notification_id) REFERENCES notifications(id),
	UNIQUE (notification_id, total_attempt_no),
	UNIQUE (notification_id, lease_epoch)
) STRICT;

CREATE TABLE IF NOT EXISTS replay_audits (
	id                 TEXT PRIMARY KEY,
	notification_id    TEXT NOT NULL,
	operator_id        TEXT NOT NULL,
	reason             TEXT NOT NULL,
	from_round          INTEGER NOT NULL,
	to_round            INTEGER NOT NULL,
	created_at_ms       INTEGER NOT NULL,
	FOREIGN KEY (notification_id) REFERENCES notifications(id),
	UNIQUE (notification_id, to_round)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_notifications_due
ON notifications(priority DESC, next_attempt_at_ms, created_at_ms, provider_id)
WHERE status IN ('PENDING','RETRY_WAIT');

CREATE INDEX IF NOT EXISTS idx_notifications_expired_lease
ON notifications(lease_until_ms)
WHERE status = 'DELIVERING';

CREATE INDEX IF NOT EXISTS idx_notifications_provider_status
ON notifications(provider_id, status, updated_at_ms);

CREATE INDEX IF NOT EXISTS idx_attempts_notification
ON delivery_attempts(notification_id, total_attempt_no DESC);
`
