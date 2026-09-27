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
