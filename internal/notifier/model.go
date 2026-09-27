package notifier

import "errors"

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
	ErrIdempotencyConflict = errors.New("idempotency key is already used with different content")
	ErrStaleLease          = errors.New("notification lease is stale")
	ErrInvalidState        = errors.New("invalid notification state")
)
