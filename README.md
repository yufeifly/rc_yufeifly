# Reliable HTTP Notifier

A single-process Go service that durably accepts notification tasks, schedules them by server-owned priority, and delivers HTTP requests with at-least-once semantics.

## Reliability Model

SQLite is the source of truth. The API returns `202 Accepted` only after the notification transaction commits. A worker claim and its `STARTED` attempt are committed together, while the external HTTP call runs outside the database transaction.

Each claim increments a lease epoch and row version. Completion uses both values as a fencing token, so an expired worker cannot overwrite a newer worker's result. This protects local state, but it cannot undo an external side effect. Every delivery therefore carries the stable notification ID in `X-Notification-Id` and the configured idempotency header; providers should deduplicate on that value.

```text
POST -> SQLite commit -> Scheduler -> Claim + Attempt
                                 -> HTTP provider
                                 -> fenced completion
```

Delivery states are `PENDING`, `DELIVERING`, `RETRY_WAIT`, `SUCCEEDED`, and `DEAD`. Retryable failures use bounded exponential backoff; expired leases become unknown-result attempts and are recovered. An administrator can replay a dead notification without changing its ID or deleting attempt history.

## Run Locally

Requirements: Go 1.25+, Python 3, and `curl`.

```bash
export CRM_BEARER_TOKEN=local-crm-token
export INVENTORY_API_KEY=local-inventory-key
go run ./cmd/mock-provider -address 127.0.0.1:18080
go run ./cmd/notifier -config configs/config.example.yaml
```

The checked-in example binds to loopback and permits HTTP/private addresses for local development only. Production validation requires static-token authentication and rejects those network-policy exceptions.

Docker Compose provides the same local-only setup:

```bash
docker compose up --build
```

It exposes the notifier only on `127.0.0.1:8080` and persists SQLite data in a named volume. Credentials in `docker-compose.yml` are examples, not production secrets.

## API

Create a notification:

```bash
curl -X POST http://127.0.0.1:8080/v1/notifications \
  -H 'Authorization: Bearer local-producer-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "idempotency_key": "order-20260927-1",
    "provider": "local",
    "event_type": "payment.succeeded",
    "payload": {"order_id": "20260927-1", "amount": 19900}
  }'
```

Query status:

```bash
curl http://127.0.0.1:8080/v1/notifications/ntf_ID \
  -H 'Authorization: Bearer local-producer-token'
```

Replay a dead notification:

```bash
curl -X POST http://127.0.0.1:8080/v1/notifications/ntf_ID/replay \
  -H 'Authorization: Bearer local-admin-token' \
  -H 'Content-Type: application/json' \
  -d '{"reason":"provider credentials rotated"}'
```

The idempotency scope is `caller_id + idempotency_key`. Reusing a key with equivalent canonical JSON returns the existing task; reusing it with different effective content returns `409`.

## Priority

Callers cannot submit priority. Configuration maps `event_type` to `HIGH`, `NORMAL`, or `LOW`; the chosen value is persisted at acceptance time.

Waiting tasks age one level per `priority.aging_interval`, capped at `HIGH`. Aging starts at `next_attempt_at`, not creation time, so an old task leaving retry backoff does not jump ahead unfairly. Providers at the highest effective priority are selected round-robin, with global and per-provider concurrency permits reserved before claim.

## Provider Safety

Providers are selected by configured ID; callers cannot supply URLs. Each provider gets an isolated transport and concurrency semaphore. The client disables proxies and redirects, enforces timeouts and TLS 1.2+, and validates resolved addresses at connection time to resist DNS rebinding. Response bodies and credentials are never persisted.

Production should use HTTPS public endpoints, secret environment variables, a local disk or single-node volume for SQLite, and provider-side idempotency.

## Metrics

`GET /metrics` requires an admin token when static authentication is enabled. Labels are intentionally low-cardinality: provider, route, status, outcome, priority, and error category. IDs, callers, URLs, and raw errors are excluded.

API 5xx ratio:

```promql
sum(rate(notifier_api_requests_total{status=~"5.."}[5m]))
/
clamp_min(sum(rate(notifier_api_requests_total[5m])), 1)
```

Provider delivery failure ratio:

```promql
sum by (provider) (
  rate(notifier_delivery_attempts_total{outcome=~"retryable_failure|permanent_failure|unknown"}[5m])
)
/
clamp_min(sum by (provider) (rate(notifier_delivery_attempts_total[5m])), 1)
```

## Verification

```bash
make test
make race
make vet
make build
make demo
make recovery-demo
make benchmark
```

`make demo` verifies acceptance, idempotency, priority, anti-starvation aging, retry, dead letter, replay, metrics, and provider concurrency. `make recovery-demo` kills the notifier during an in-flight request and verifies recovery with a stable notification ID and increased epoch. The benchmark fails below 50 requests/second or above 200 ms P95.

## Operational Boundary

SQLite WAL keeps the MVP self-contained and makes transaction and crash semantics easy to inspect. It is intentionally single-instance and must not be placed on a shared filesystem with uncertain locking. Move to PostgreSQL when multiple active instances or higher write concurrency are required.

An MQ is not used because it would require an outbox, relay, publish confirmation, and a second retry state machine to preserve the same guarantees. Introduce one only when measured database polling or single-node throughput becomes the bottleneck.

See [technical design](docs/TECHNICAL_DESIGN.md), [PRD](docs/PRD.md), and [implementation plan](docs/IMPLEMENTATION_PLAN.md).
