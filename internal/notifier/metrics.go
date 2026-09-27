package notifier

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry *prometheus.Registry

	apiRequests          *prometheus.CounterVec
	apiDuration          *prometheus.HistogramVec
	accepted             *prometheus.CounterVec
	notificationsByState *prometheus.GaugeVec
	oldestDueAge         *prometheus.GaugeVec
	claimed              *prometheus.CounterVec
	deliveryAttempts     *prometheus.CounterVec
	deliveryDuration     *prometheus.HistogramVec
	retries              *prometheus.CounterVec
	dead                 *prometheus.CounterVec
	leaseRecovered       *prometheus.CounterVec
	staleCompletion      *prometheus.CounterVec
	workersInUse         prometheus.Gauge
	workerCapacity       prometheus.Gauge
	providerInFlight     *prometheus.GaugeVec
	providerLimit        *prometheus.GaugeVec
	databaseDuration     *prometheus.HistogramVec
	databaseErrors       *prometheus.CounterVec
}

func NewMetrics(registry *prometheus.Registry, workerCapacity int, providerLimits map[string]int) *Metrics {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}
	metrics := &Metrics{
		registry: registry,
		apiRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifier_api_requests_total",
			Help: "Total API requests.",
		}, []string{"route", "method", "status"}),
		apiDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notifier_api_request_duration_seconds",
			Help:    "API request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method"}),
		accepted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifier_notifications_accepted_total",
			Help: "Total durably accepted notifications.",
		}, []string{"provider", "duplicate"}),
		notificationsByState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "notifier_notifications_by_status",
			Help: "Current notifications grouped by provider and status.",
		}, []string{"provider", "status"}),
		oldestDueAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "notifier_oldest_due_age_seconds",
			Help: "Age of the oldest due notification.",
		}, []string{"provider"}),
		claimed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifier_notifications_claimed_total",
			Help: "Total claimed notifications.",
		}, []string{"provider", "priority"}),
		deliveryAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifier_delivery_attempts_total",
			Help: "Total completed delivery attempts.",
		}, []string{"provider", "outcome", "error_category"}),
		deliveryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notifier_delivery_duration_seconds",
			Help:    "Provider delivery duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"provider", "outcome"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifier_retries_scheduled_total",
			Help: "Total scheduled retries.",
		}, []string{"provider", "error_category"}),
		dead: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifier_dead_total",
			Help: "Total notifications moved to dead state.",
		}, []string{"provider", "error_category"}),
		leaseRecovered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifier_lease_recovered_total",
			Help: "Total expired leases recovered.",
		}, []string{"provider"}),
		staleCompletion: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifier_stale_completion_total",
			Help: "Total stale completion attempts.",
		}, []string{"provider"}),
		workersInUse: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "notifier_workers_in_use",
			Help: "Current number of active workers.",
		}),
		workerCapacity: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "notifier_worker_capacity",
			Help: "Configured worker capacity.",
		}),
		providerInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "notifier_provider_in_flight",
			Help: "Current provider requests in flight.",
		}, []string{"provider"}),
		providerLimit: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "notifier_provider_concurrency_limit",
			Help: "Configured provider concurrency limit.",
		}, []string{"provider"}),
		databaseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notifier_database_operation_duration_seconds",
			Help:    "Database operation duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"operation"}),
		databaseErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifier_database_errors_total",
			Help: "Total database operation errors.",
		}, []string{"operation"}),
	}
	registry.MustRegister(
		metrics.apiRequests,
		metrics.apiDuration,
		metrics.accepted,
		metrics.notificationsByState,
		metrics.oldestDueAge,
		metrics.claimed,
		metrics.deliveryAttempts,
		metrics.deliveryDuration,
		metrics.retries,
		metrics.dead,
		metrics.leaseRecovered,
		metrics.staleCompletion,
		metrics.workersInUse,
		metrics.workerCapacity,
		metrics.providerInFlight,
		metrics.providerLimit,
		metrics.databaseDuration,
		metrics.databaseErrors,
	)
	metrics.workerCapacity.Set(float64(workerCapacity))
	for provider, limit := range providerLimits {
		metrics.providerLimit.WithLabelValues(provider).Set(float64(limit))
	}
	return metrics
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) ObserveAPI(route, method string, status int, elapsed time.Duration) {
	if m == nil {
		return
	}
	m.apiRequests.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
	m.apiDuration.WithLabelValues(route, method).Observe(elapsed.Seconds())
}

func (m *Metrics) ObserveAccepted(provider string, duplicate bool) {
	if m == nil {
		return
	}
	m.accepted.WithLabelValues(provider, strconv.FormatBool(duplicate)).Inc()
}

func (m *Metrics) ObserveClaim(provider string, priority Priority) {
	if m == nil {
		return
	}
	m.claimed.WithLabelValues(provider, priorityLabel(priority)).Inc()
	m.workersInUse.Inc()
	m.providerInFlight.WithLabelValues(provider).Inc()
}

func (m *Metrics) ObserveDelivery(provider string, outcome DeliveryOutcome, elapsed time.Duration) {
	if m == nil {
		return
	}
	result := deliveryOutcomeLabel(outcome)
	category := outcome.Category
	if category == "" {
		category = ErrorNone
	}
	m.deliveryAttempts.WithLabelValues(provider, result, string(category)).Inc()
	m.deliveryDuration.WithLabelValues(provider, result).Observe(elapsed.Seconds())
}

func (m *Metrics) ObserveWorkerReleased(provider string) {
	if m == nil {
		return
	}
	m.workersInUse.Dec()
	m.providerInFlight.WithLabelValues(provider).Dec()
}

func (m *Metrics) ObserveRetry(provider string, category ErrorCategory) {
	if m != nil {
		m.retries.WithLabelValues(provider, string(category)).Inc()
	}
}

func (m *Metrics) ObserveDead(provider string, category ErrorCategory) {
	if m != nil {
		m.dead.WithLabelValues(provider, string(category)).Inc()
	}
}

func (m *Metrics) ObserveLeaseRecovery(provider string) {
	if m != nil {
		m.leaseRecovered.WithLabelValues(provider).Inc()
	}
}

func (m *Metrics) ObserveStaleCompletion(provider string) {
	if m != nil {
		m.staleCompletion.WithLabelValues(provider).Inc()
	}
}

func (m *Metrics) ObserveDatabase(operation string, elapsed time.Duration, err error) {
	if m == nil {
		return
	}
	m.databaseDuration.WithLabelValues(operation).Observe(elapsed.Seconds())
	if err != nil {
		m.databaseErrors.WithLabelValues(operation).Inc()
	}
}

func (m *Metrics) RefreshStore(ctx context.Context, store *Store, now time.Time) error {
	if m == nil {
		return nil
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT provider_id, status, COUNT(*)
		FROM notifications
		GROUP BY provider_id, status
	`)
	if err != nil {
		return fmt.Errorf("query notification status metrics: %w", err)
	}
	m.notificationsByState.Reset()
	for rows.Next() {
		var provider string
		var status Status
		var count int
		if err := rows.Scan(&provider, &status, &count); err != nil {
			rows.Close()
			return fmt.Errorf("scan notification status metrics: %w", err)
		}
		m.notificationsByState.WithLabelValues(provider, string(status)).Set(float64(count))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate notification status metrics: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close notification status metrics: %w", err)
	}

	dueRows, err := store.db.QueryContext(ctx, `
		SELECT provider_id, MIN(next_attempt_at_ms)
		FROM notifications
		WHERE status IN ('PENDING','RETRY_WAIT') AND next_attempt_at_ms <= ?
		GROUP BY provider_id
	`, now.UTC().UnixMilli())
	if err != nil {
		return fmt.Errorf("query oldest due metrics: %w", err)
	}
	defer dueRows.Close()
	m.oldestDueAge.Reset()
	for dueRows.Next() {
		var provider string
		var dueAtMilliseconds int64
		if err := dueRows.Scan(&provider, &dueAtMilliseconds); err != nil {
			return fmt.Errorf("scan oldest due metrics: %w", err)
		}
		age := now.Sub(time.UnixMilli(dueAtMilliseconds))
		if age < 0 {
			age = 0
		}
		m.oldestDueAge.WithLabelValues(provider).Set(age.Seconds())
	}
	if err := dueRows.Err(); err != nil {
		return fmt.Errorf("iterate oldest due metrics: %w", err)
	}
	return nil
}

func deliveryOutcomeLabel(outcome DeliveryOutcome) string {
	if outcome.Success {
		return "success"
	}
	switch outcome.Category {
	case ErrorTransportUnknown, ErrorTimeoutUnknown, ErrorShutdownUnknown, ErrorLeaseExpiredUnknown:
		return "unknown"
	default:
		if outcome.Retryable {
			return "retryable_failure"
		}
		return "permanent_failure"
	}
}

func priorityLabel(priority Priority) string {
	switch priority {
	case PriorityHigh:
		return "HIGH"
	case PriorityNormal:
		return "NORMAL"
	default:
		return "LOW"
	}
}
