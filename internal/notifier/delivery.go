package notifier

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func classifyResponse(status int, retryAfter string, now time.Time) DeliveryOutcome {
	outcome := DeliveryOutcome{
		HTTPStatus:  intPointer(status),
		Category:    ErrorUpstream4XX,
		SafeMessage: "upstream rejected the notification",
	}

	switch {
	case status >= 200 && status < 300:
		outcome.Success = true
		outcome.Category = ErrorNone
		outcome.SafeMessage = ""
	case status >= 300 && status < 400:
		outcome.Category = ErrorUpstreamRedirect
		outcome.SafeMessage = "upstream returned a redirect"
	case status == http.StatusRequestTimeout:
		outcome.Category = ErrorUpstreamTimeout
		outcome.Retryable = true
		outcome.SafeMessage = "upstream timed out the request"
	case status == http.StatusTooEarly:
		outcome.Category = ErrorUpstreamTooEarly
		outcome.Retryable = true
		outcome.SafeMessage = "upstream asked to retry the request"
	case status == http.StatusTooManyRequests:
		outcome.Category = ErrorRateLimited
		outcome.Retryable = true
		outcome.SafeMessage = "upstream rate limited the request"
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		outcome.Category = ErrorAuthRejected
		outcome.SafeMessage = "upstream rejected authentication"
	case status >= 500 && status < 600:
		outcome.Category = ErrorUpstream5XX
		outcome.Retryable = true
		outcome.SafeMessage = "upstream returned a server error"
	}

	if outcome.Retryable {
		outcome.RetryAfter = parseRetryAfter(retryAfter, now)
	}
	return outcome
}

func classifyCallError(ctx context.Context, err error, wroteRequest bool) DeliveryOutcome {
	outcome := DeliveryOutcome{
		Category:    ErrorInternal,
		Retryable:   true,
		SafeMessage: "delivery failed",
	}

	if errors.Is(ctx.Err(), context.Canceled) {
		outcome.Category = ErrorShutdownUnknown
		outcome.SafeMessage = "delivery was canceled during shutdown"
		return outcome
	}
	if errors.Is(err, context.DeadlineExceeded) || isTimeoutError(err) {
		outcome.Category = ErrorTimeoutUnknown
		outcome.SafeMessage = "delivery result is unknown after timeout"
		return outcome
	}
	if isTLSError(err) {
		outcome.Category = ErrorTLSConfig
		outcome.Retryable = false
		outcome.SafeMessage = "TLS validation failed"
		return outcome
	}

	var dnsError *net.DNSError
	if errors.As(err, &dnsError) {
		outcome.Category = ErrorDNSError
		outcome.SafeMessage = "provider DNS lookup failed"
		return outcome
	}
	if wroteRequest {
		outcome.Category = ErrorTransportUnknown
		outcome.SafeMessage = "delivery result is unknown after a transport failure"
		return outcome
	}

	var networkError *net.OpError
	if errors.As(err, &networkError) || errors.Is(err, ErrUnsafeDestination) {
		outcome.Category = ErrorConnect
		outcome.SafeMessage = "provider connection failed"
	}
	return outcome
}

func parseRetryAfter(value string, now time.Time) *time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 || seconds > int64((time.Duration(1<<63-1))/time.Second) {
			return nil
		}
		delay := time.Duration(seconds) * time.Second
		return &delay
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return nil
	}
	delay := when.Sub(now)
	if delay < 0 {
		delay = 0
	}
	return &delay
}

func retryDelay(policy RetryConfig, attempt int, random float64) time.Duration {
	if policy.InitialDelay <= 0 {
		return 0
	}
	maxDelay := policy.MaxDelay
	if maxDelay <= 0 {
		maxDelay = policy.InitialDelay
	}

	base := policy.InitialDelay
	for step := 1; step < attempt && base < maxDelay; step++ {
		if base > maxDelay/2 {
			base = maxDelay
			break
		}
		base *= 2
	}
	if base > maxDelay {
		base = maxDelay
	}

	jitter := policy.JitterRatio
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	if random < 0 {
		random = 0
	}
	if random > 1 {
		random = 1
	}
	factor := 1 - jitter + 2*jitter*random
	delay := time.Duration(float64(base) * factor)
	if delay < 0 {
		return 0
	}
	return delay
}

func isTimeoutError(err error) bool {
	var networkError net.Error
	return errors.As(err, &networkError) && networkError.Timeout()
}

func isTLSError(err error) bool {
	var unknownAuthority x509.UnknownAuthorityError
	var hostnameError x509.HostnameError
	var certificateError x509.CertificateInvalidError
	var recordHeaderError tls.RecordHeaderError
	return errors.As(err, &unknownAuthority) ||
		errors.As(err, &hostnameError) ||
		errors.As(err, &certificateError) ||
		errors.As(err, &recordHeaderError)
}

func intPointer(value int) *int {
	return &value
}
