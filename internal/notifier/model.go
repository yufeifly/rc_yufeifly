package notifier

import (
	"encoding/json"
	"errors"
	"time"
)

type Status string

const (
	StatusPending    Status = "PENDING"
	StatusDelivering Status = "DELIVERING"
	StatusRetryWait  Status = "RETRY_WAIT"
	StatusSucceeded  Status = "SUCCEEDED"
	StatusDead       Status = "DEAD"
)

type Priority int

const (
	PriorityLow Priority = iota + 1
	PriorityNormal
	PriorityHigh
)

type ErrorCategory string

const (
	ErrorNone                ErrorCategory = "NONE"
	ErrorDNSError            ErrorCategory = "DNS_ERROR"
	ErrorConnect             ErrorCategory = "CONNECT_ERROR"
	ErrorTransportUnknown    ErrorCategory = "TRANSPORT_UNKNOWN"
	ErrorTimeoutUnknown      ErrorCategory = "TIMEOUT_UNKNOWN"
	ErrorShutdownUnknown     ErrorCategory = "SHUTDOWN_UNKNOWN"
	ErrorTLSConfig           ErrorCategory = "TLS_CONFIG_ERROR"
	ErrorUpstreamRedirect    ErrorCategory = "UPSTREAM_REDIRECT"
	ErrorUpstreamTimeout     ErrorCategory = "UPSTREAM_TIMEOUT"
	ErrorUpstreamTooEarly    ErrorCategory = "UPSTREAM_TOO_EARLY"
	ErrorRateLimited         ErrorCategory = "RATE_LIMITED"
	ErrorUpstream5XX         ErrorCategory = "UPSTREAM_5XX"
	ErrorAuthRejected        ErrorCategory = "AUTH_REJECTED"
	ErrorUpstream4XX         ErrorCategory = "UPSTREAM_4XX"
	ErrorRequestBuild        ErrorCategory = "REQUEST_BUILD_ERROR"
	ErrorProviderConfig      ErrorCategory = "PROVIDER_CONFIG_ERROR"
	ErrorInternal            ErrorCategory = "INTERNAL_ERROR"
	ErrorLeaseExpiredUnknown ErrorCategory = "LEASE_EXPIRED_UNKNOWN"
)

var (
	ErrNotFound            = errors.New("notification not found")
	ErrProviderNotFound    = errors.New("provider not found or disabled")
	ErrInvalidRequest      = errors.New("invalid notification request")
	ErrIdempotencyConflict = errors.New("idempotency key is already used with different content")
	ErrStaleLease          = errors.New("notification lease is stale")
	ErrInvalidState        = errors.New("invalid notification state")
	ErrUnsafeDestination   = errors.New("destination is blocked by network policy")
)

type Notification struct {
	ID                 string
	RequestID          string
	CallerID           string
	IdempotencyKey     string
	RequestHash        string
	RequestHashVersion int
	ProviderID         string
	EventType          string
	Priority           Priority
	OccurredAt         time.Time
	Payload            json.RawMessage
	Status             Status
	DeliveryRound      int
	AttemptInRound     int
	TotalAttemptCount  int
	MaxAttempts        int
	NextAttemptAt      *time.Time
	LeaseOwner         string
	LeaseEpoch         int64
	LeaseUntil         *time.Time
	Version            int64
	LastErrorCategory  ErrorCategory
	LastHTTPStatus     *int
	LastErrorMessage   string
	CreatedAt          time.Time
	UpdatedAt          time.Time
	SucceededAt        *time.Time
	DeadAt             *time.Time
}

type CreateNotification struct {
	ID                 string
	RequestID          string
	CallerID           string
	IdempotencyKey     string
	RequestHash        string
	RequestHashVersion int
	ProviderID         string
	EventType          string
	Priority           Priority
	OccurredAt         time.Time
	Payload            json.RawMessage
	MaxAttempts        int
	CreatedAt          time.Time
}

type CreateResult struct {
	Notification Notification
	Duplicate    bool
}

type ClaimedNotification struct {
	Notification
	AttemptID string
}

type DeliveryOutcome struct {
	Success     bool
	Category    ErrorCategory
	HTTPStatus  *int
	Retryable   bool
	RetryAfter  *time.Duration
	SafeMessage string
}

type DueProvider struct {
	ProviderID        string
	EffectivePriority Priority
	DueAt             time.Time
}

type ClaimRequest struct {
	ProviderID      string
	ProviderVersion string
	WorkerID        string
	Now             time.Time
	LeaseDuration   time.Duration
	AgingInterval   time.Duration
}

type CompleteRequest struct {
	ID            string
	AttemptID     string
	WorkerID      string
	LeaseEpoch    int64
	Version       int64
	Outcome       DeliveryOutcome
	CompletedAt   time.Time
	NextAttemptAt *time.Time
}

type ReplayRequest struct {
	ID         string
	AuditID    string
	OperatorID string
	Reason     string
	Now        time.Time
}
