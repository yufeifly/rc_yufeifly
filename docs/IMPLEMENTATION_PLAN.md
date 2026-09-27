# Reliable HTTP Notifier Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a runnable Go MVP that durably accepts HTTP notification tasks, schedules them by server-assigned priority, delivers them with at-least-once semantics, and recovers safely from failures.

**Architecture:** A single Go process hosts the HTTP API, SQLite-backed task store, scheduler, and bounded workers. SQLite is the source of truth; workers use lease epochs and version CAS for safe completion, while stable notification IDs provide the external idempotency key.

**Tech Stack:** Go 1.25+, `net/http`, `database/sql`, `modernc.org/sqlite`, `gopkg.in/yaml.v3`, `oklog/ulid/v2`, Prometheus Go client.

---

## File Map

| File | Responsibility |
|---|---|
| `go.mod`, `go.sum` | Module and pinned dependencies |
| `internal/notifier/model.go` | Domain types, statuses, priorities and errors |
| `internal/notifier/config.go` | YAML loading, secret resolution and validation |
| `internal/notifier/store.go` | SQLite schema, transactions, queries and CAS |
| `internal/notifier/provider.go` | Provider registry, concurrency permit and request construction |
| `internal/notifier/delivery.go` | HTTP execution, outcome classification and retry calculation |
| `internal/notifier/scheduler.go` | Priority aging, Provider fairness, lease recovery and worker dispatch |
| `internal/notifier/service.go` | Create, get and replay use cases |
| `internal/notifier/auth.go` | Static token authentication and authorization |
| `internal/notifier/httpapi.go` | Routes, request validation and response mapping |
| `internal/notifier/metrics.go` | Prometheus metrics |
| `cmd/notifier/main.go` | Process wiring and lifecycle |
| `cmd/mock-provider/main.go` | Deterministic external API simulator |
| `configs/*.yaml` | Local, recovery and Docker examples |
| `scripts/*.sh` | End-to-end, crash recovery and benchmark checks |
| `README.md` | Runbook and design summary |
| `AI_USAGE.md` | AI decisions, rejected suggestions and verification |

---

### Task 1: Bootstrap Domain and Configuration

**Files:**
- Create: `go.mod`
- Create: `internal/notifier/model.go`
- Create: `internal/notifier/config_test.go`
- Create: `internal/notifier/config.go`
- Create: `configs/config.example.yaml`

- [ ] **Step 1: Initialize the module**

```bash
go mod init github.com/yufeifly/rc_yufeifly
go get github.com/oklog/ulid/v2
go get gopkg.in/yaml.v3
go get modernc.org/sqlite
go get github.com/prometheus/client_golang/prometheus
```

- [ ] **Step 2: Write failing configuration and priority tests**

```go
func TestResolvePriorityUsesServerMapping(t *testing.T) {
    cfg := PriorityConfig{
        Default: "NORMAL",
        Events: map[string]string{"payment.succeeded": "HIGH"},
    }
    got, err := ResolvePriority(cfg, "payment.succeeded")
    if err != nil || got != PriorityHigh {
        t.Fatalf("got %v, %v", got, err)
    }
}

func TestLoadConfigRejectsUnknownFields(t *testing.T) {
    // Write a temporary YAML containing an unknown top-level key.
    // LoadConfig must return a non-nil error.
}

func TestProductionRejectsUnsafeNetworkPolicy(t *testing.T) {
    // Production with disabled auth, HTTP, or private IP access must fail.
}
```

- [ ] **Step 3: Verify RED**

Run:

```bash
go test ./internal/notifier -run 'TestResolvePriority|TestLoadConfig|TestProduction' -count=1
```

Expected: compile failure because the configuration and domain APIs do not exist.

- [ ] **Step 4: Implement minimal domain and configuration**

Define:

```go
type Status string
type Priority int
type ErrorCategory string

const (
    PriorityLow Priority = iota + 1
    PriorityNormal
    PriorityHigh
)

func ResolvePriority(cfg PriorityConfig, eventType string) (Priority, error)
func LoadConfig(path string) (Config, error)
func (c Config) Validate() error
func (c *Config) ResolveSecrets() error
```

Use `yaml.Decoder.KnownFields(true)`. Keep secret values in unexported runtime fields and only persist environment variable names in YAML.

- [ ] **Step 5: Verify GREEN and refactor**

```bash
gofmt -w internal/notifier/*.go
go test ./internal/notifier -run 'TestResolvePriority|TestLoadConfig|TestProduction' -count=1
```

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/notifier/model.go internal/notifier/config.go internal/notifier/config_test.go configs/config.example.yaml
git commit -m "feat: add notifier domain and config"
```

---

### Task 2: Add SQLite Store and Idempotent Acceptance

**Files:**
- Create: `internal/notifier/store_test.go`
- Create: `internal/notifier/store.go`

- [ ] **Step 1: Write failing persistence tests**

```go
func TestStoreCreateOrGetIsIdempotent(t *testing.T) {
    store := openTestStore(t)
    first := createFixture("caller-a", "order-1", `{"a":1}`)

    created, err := store.CreateOrGet(context.Background(), first)
    if err != nil || created.Duplicate {
        t.Fatalf("first create: %+v, %v", created, err)
    }

    duplicate, err := store.CreateOrGet(context.Background(), first)
    if err != nil || !duplicate.Duplicate {
        t.Fatalf("duplicate: %+v, %v", duplicate, err)
    }
    if duplicate.Notification.ID != created.Notification.ID {
        t.Fatalf("id changed")
    }
}

func TestStoreRejectsIdempotencyConflict(t *testing.T) {
    // Same caller/key with a different request hash must return
    // ErrIdempotencyConflict.
}
```

- [ ] **Step 2: Verify RED**

```bash
go test ./internal/notifier -run TestStore -count=1
```

Expected: compile failure because `Store` does not exist.

- [ ] **Step 3: Implement schema and CreateOrGet**

Implement:

```go
func OpenStore(ctx context.Context, cfg DatabaseConfig) (*Store, error)
func (s *Store) CreateOrGet(ctx context.Context, in CreateNotification) (CreateResult, error)
func (s *Store) Get(ctx context.Context, id string) (Notification, error)
func (s *Store) GetByIdempotencyKey(ctx context.Context, callerID, key string) (Notification, error)
```

Create `notifications`, `delivery_attempts`, `replay_audits`, indexes and migration metadata exactly as defined in the technical design. Configure WAL, FULL synchronous mode, foreign keys and busy timeout.

- [ ] **Step 4: Add concurrent idempotency test**

```go
func TestConcurrentCreateProducesOneLogicalTask(t *testing.T) {
    // Start multiple goroutines with the same caller/key/hash.
    // Assert every result has the same ID and the table contains one row.
}
```

- [ ] **Step 5: Verify GREEN**

```bash
go test ./internal/notifier -run 'TestStore|TestConcurrentCreate' -count=1
```

- [ ] **Step 6: Commit**

```bash
git add internal/notifier/store.go internal/notifier/store_test.go
git commit -m "feat: persist idempotent notifications"
```

---

### Task 3: Add Service and HTTP Acceptance API

**Files:**
- Create: `internal/notifier/service_test.go`
- Create: `internal/notifier/service.go`
- Create: `internal/notifier/auth.go`
- Create: `internal/notifier/httpapi_test.go`
- Create: `internal/notifier/httpapi.go`

- [ ] **Step 1: Write failing service tests**

```go
func TestCanonicalJSONProducesStableHash(t *testing.T) {
    left := json.RawMessage(`{"amount":100,"id":"o1"}`)
    right := json.RawMessage(`{"id":"o1", "amount":100}`)
    if hashRequest(left) != hashRequest(right) {
        t.Fatal("equivalent JSON must have the same hash")
    }
}

func TestCreateAssignsPriorityFromEventType(t *testing.T) {
    // Submit payment.succeeded without a priority field.
    // Assert persisted priority is HIGH.
}
```

- [ ] **Step 2: Verify RED, then implement Service**

```bash
go test ./internal/notifier -run 'TestCanonicalJSON|TestCreateAssignsPriority' -count=1
```

Implement:

```go
type CreateInput struct {
    RequestID, CallerID, IdempotencyKey, ProviderID, EventType string
    OccurredAt *time.Time
    Payload json.RawMessage
}

func (s *Service) Create(ctx context.Context, in CreateInput) (CreateResult, error)
func (s *Service) Get(ctx context.Context, id, callerID, role string) (Notification, error)
```

- [ ] **Step 3: Write failing HTTP tests**

```go
func TestCreateReturns202OnlyAfterCommit(t *testing.T) {
    // Use a real temporary SQLite database.
    // POST a valid request and assert 202 plus a queryable task.
}

func TestStoreFailureNeverReturns202(t *testing.T) {
    // Close the store before POST and assert 503.
}

func TestProducerCannotReadAnotherCallersTask(t *testing.T) {
    // Submit as caller A, query as caller B, expect 404.
}
```

- [ ] **Step 4: Verify RED, then implement API and auth**

```bash
go test ./internal/notifier -run 'TestCreateReturns|TestStoreFailure|TestProducer' -count=1
```

Routes:

```text
POST /v1/notifications
GET  /v1/notifications/{id}
GET  /health/live
GET  /health/ready
```

Use `http.MaxBytesReader`, `DisallowUnknownFields`, single-document validation, stable error codes, request IDs and constant-time token comparison.

- [ ] **Step 5: Verify GREEN and commit**

```bash
go test ./internal/notifier -count=1
git add internal/notifier/service.go internal/notifier/service_test.go internal/notifier/auth.go internal/notifier/httpapi.go internal/notifier/httpapi_test.go
git commit -m "feat: add notification HTTP API"
```

---

### Task 4: Add Provider Adapters and Hardened HTTP Delivery

**Files:**
- Create: `internal/notifier/delivery_test.go`
- Create: `internal/notifier/provider.go`
- Create: `internal/notifier/delivery.go`

- [ ] **Step 1: Write failing Provider contract tests**

```go
func TestProviderBuildsEnvelopeAndBearerAuth(t *testing.T) {
    // Assert envelope fields, stable notification ID, attempt headers,
    // bearer token and configured headers.
}

func TestProviderBuildsPassthroughAndAPIKey(t *testing.T) {
    // Assert raw payload body and configured API-key header.
}

func TestProviderDoesNotFollowRedirect(t *testing.T) {
    // Upstream returns 302; assert redirect target receives no request.
}

func TestSafeDialRejectsLoopbackInProduction(t *testing.T) {
    // Resolve a loopback target and assert the Dialer rejects it.
}
```

- [ ] **Step 2: Verify RED**

```bash
go test ./internal/notifier -run 'TestProvider|TestSafeDial' -count=1
```

- [ ] **Step 3: Implement Provider Registry and request building**

Implement:

```go
func NewProviderRegistry(cfg Config) (*ProviderRegistry, error)
func (r *ProviderRegistry) Get(id string) (*Provider, bool)
func (p *Provider) TryAcquire() bool
func (p *Provider) Release()
func (p *Provider) Deliver(ctx context.Context, task ClaimedNotification) DeliveryOutcome
```

Each Provider owns an HTTP client, connection pool and bounded semaphore. Disable proxy use and redirects; validate every resolved IP at dial time.

- [ ] **Step 4: Add failing classification tests**

Cover 2xx, 3xx, 400, 401, 403, 408, 425, 429, 5xx, DNS, connection and timeout. Assert response bodies and secrets are never persisted.

- [ ] **Step 5: Implement classification and retry parsing**

```go
func classifyResponse(status int, retryAfter string, now time.Time) DeliveryOutcome
func classifyCallError(ctx context.Context, err error, wroteRequest bool) DeliveryOutcome
func parseRetryAfter(value string, now time.Time) *time.Duration
func retryDelay(policy RetryConfig, attempt int, random float64) time.Duration
```

- [ ] **Step 6: Verify GREEN and commit**

```bash
go test ./internal/notifier -run 'TestProvider|TestSafeDial|TestRetry|TestClassif' -count=1
git add internal/notifier/provider.go internal/notifier/delivery.go internal/notifier/delivery_test.go
git commit -m "feat: add secure provider delivery"
```

---

### Task 5: Add Priority Scheduling, Lease and Fencing

**Files:**
- Create: `internal/notifier/scheduler_test.go`
- Create: `internal/notifier/scheduler.go`
- Modify: `internal/notifier/store.go`
- Modify: `internal/notifier/store_test.go`

- [ ] **Step 1: Write failing priority and aging tests**

```go
func TestHigherPriorityIsClaimedFirst(t *testing.T) {
    // Insert due LOW, NORMAL and HIGH tasks.
    // Claim and assert HIGH is selected.
}

func TestLowPriorityAgesWithoutStarvation(t *testing.T) {
    // Insert LOW due for 2*agingInterval and a newly due HIGH task.
    // Assert old LOW wins the tie by due time.
}

func TestRetryAgingStartsAtNextAttemptTime(t *testing.T) {
    // Old RETRY_WAIT task becomes due now.
    // Assert its age is zero rather than age since creation.
}
```

- [ ] **Step 2: Verify RED**

```bash
go test ./internal/notifier -run 'TestHigherPriority|TestLowPriority|TestRetryAging' -count=1
```

- [ ] **Step 3: Implement effective priority and indexed claim**

```go
func EffectivePriority(original Priority, dueSince, now time.Time, aging time.Duration) Priority
func (s *Store) ListDueProviders(ctx context.Context, now time.Time, aging time.Duration) ([]DueProvider, error)
func (s *Store) ClaimAndStart(ctx context.Context, in ClaimRequest) (ClaimedNotification, error)
```

Persist original priority. Compute effective priority from `next_attempt_at`, cap at HIGH, then order by effective priority, due time and creation time.

- [ ] **Step 4: Write failing fencing test**

```go
func TestExpiredLeaseRejectsOldWorker(t *testing.T) {
    // Worker A claims epoch 1.
    // Recover and let worker B claim epoch 2.
    // Completing with epoch 1 must return ErrStaleLease.
}
```

- [ ] **Step 5: Implement scheduler and CAS completion**

```go
func NewScheduler(cfg Config, store *Store, providers *ProviderRegistry, metrics *Metrics, log *slog.Logger) *Scheduler
func (s *Scheduler) Start(ctx context.Context)
func (s *Scheduler) Shutdown(ctx context.Context) error
func (s *Store) Complete(ctx context.Context, in CompleteRequest) error
```

Reserve global and Provider permits before Claim. Rotate Providers within the highest effective priority tier. Release permits with `defer`.

- [ ] **Step 6: Verify GREEN and commit**

```bash
go test ./internal/notifier -run 'TestHigherPriority|TestLowPriority|TestRetryAging|TestExpiredLease' -count=1
go test -race ./internal/notifier -count=1
git add internal/notifier/scheduler.go internal/notifier/scheduler_test.go internal/notifier/store.go internal/notifier/store_test.go
git commit -m "feat: add priority lease scheduler"
```

---

### Task 6: Add Retry, Recovery, Dead Letter and Replay

**Files:**
- Modify: `internal/notifier/scheduler_test.go`
- Modify: `internal/notifier/store_test.go`
- Modify: `internal/notifier/store.go`
- Modify: `internal/notifier/service.go`
- Modify: `internal/notifier/httpapi.go`

- [ ] **Step 1: Write failing lifecycle tests**

```go
func TestRetryableFailureSchedulesNextAttempt(t *testing.T) {
    // Complete with UPSTREAM_5XX and assert RETRY_WAIT with future time.
}

func TestPermanentFailureBecomesDead(t *testing.T) {
    // Complete with UPSTREAM_4XX and assert DEAD.
}

func TestExpiredStartedAttemptBecomesAbandoned(t *testing.T) {
    // Expire a DELIVERING lease and assert Attempt=ABANDONED.
}

func TestReplayPreservesNotificationIDAndHistory(t *testing.T) {
    // Replay DEAD and assert same ID, incremented round and audit row.
}
```

- [ ] **Step 2: Verify RED**

```bash
go test ./internal/notifier -run 'TestRetryable|TestPermanent|TestExpiredStarted|TestReplay' -count=1
```

- [ ] **Step 3: Implement lifecycle transactions**

```go
func (s *Store) Complete(ctx context.Context, in CompleteRequest) error
func (s *Store) RecoverExpired(ctx context.Context, now time.Time, limit int) (int, error)
func (s *Store) ReplayDead(ctx context.Context, in ReplayRequest) (Notification, error)
func (s *Service) Replay(ctx context.Context, id, operatorID, reason string) (Notification, error)
```

Task and Attempt updates occur in the same transaction. Every completion checks owner, epoch and version.

- [ ] **Step 4: Add Replay route and verify GREEN**

```text
POST /v1/notifications/{id}/replay
```

```bash
go test ./internal/notifier -count=1
go test -race ./internal/notifier -count=1
```

- [ ] **Step 5: Commit**

```bash
git add internal/notifier
git commit -m "feat: add retry recovery and replay"
```

---

### Task 7: Add Metrics and Process Lifecycle

**Files:**
- Create: `internal/notifier/metrics_test.go`
- Create: `internal/notifier/metrics.go`
- Modify: `internal/notifier/httpapi.go`
- Create: `cmd/notifier/main.go`

- [ ] **Step 1: Write failing metrics tests**

```go
func TestMetricsExposeProviderFailureCounters(t *testing.T) {
    // Record success, retryable failure and unknown.
    // Gather and assert bounded provider/outcome/error_category labels.
}

func TestMetricsNeverUseNotificationIDAsLabel(t *testing.T) {
    // Inspect metric descriptors and reject high-cardinality labels.
}
```

- [ ] **Step 2: Verify RED**

```bash
go test ./internal/notifier -run TestMetrics -count=1
```

- [ ] **Step 3: Implement Metrics and `/metrics`**

Use Prometheus CounterVec, HistogramVec and GaugeVec with the exact names in the technical design. Refresh database-derived gauges periodically rather than scanning on scrape.

- [ ] **Step 4: Write failing lifecycle tests**

```go
func TestShutdownStopsClaimingAndDrainsWorkers(t *testing.T) {
    // Block one Provider call, initiate shutdown, release it,
    // then assert shutdown completes and no new task was claimed.
}
```

- [ ] **Step 5: Implement process wiring and graceful shutdown**

Wire config, store, registry, metrics, service, scheduler and HTTP server. Bind the listener before setting readiness. On signal, set not-ready, stop claims, drain workers and close resources.

- [ ] **Step 6: Verify GREEN and commit**

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/...
git add internal/notifier/metrics.go internal/notifier/metrics_test.go internal/notifier/httpapi.go cmd/notifier/main.go
git commit -m "feat: add metrics and lifecycle"
```

---

### Task 8: Add Mock Provider and End-to-End Verification

**Files:**
- Create: `cmd/mock-provider/main.go`
- Create: `scripts/demo.sh`
- Create: `scripts/recovery-demo.sh`
- Create: `scripts/benchmark.sh`
- Create: `scripts/benchmark-ingress.py`
- Create: `configs/config.recovery.yaml`
- Create: `Makefile`

- [ ] **Step 1: Write the executable acceptance scenario**

`scripts/demo.sh` must start both binaries with a temporary database and assert:

```text
health ready
normal success
idempotent duplicate
HIGH before NORMAL before LOW
LOW aging eventually executes
503 -> 503 -> 200
400 -> DEAD
Replay -> SUCCEEDED
metrics expose provider failures
provider concurrency never exceeds limit
```

- [ ] **Step 2: Run and verify RED**

```bash
bash scripts/demo.sh
```

Expected: failure because the mock provider and runnable binaries are incomplete.

- [ ] **Step 3: Implement deterministic Mock Provider**

Support:

```text
GET  /health
POST /always-200
POST /always-400
POST /always-503
POST /fail-twice-then-200
POST /controlled/{key}
PUT  /controlled/{key}?status=200
GET  /records
```

- [ ] **Step 4: Add crash-recovery acceptance**

`scripts/recovery-demo.sh` must:

1. start a delayed Provider;
2. submit a task;
3. wait until the first request is observed;
4. kill notifier with `SIGKILL`;
5. restart against the same SQLite file;
6. wait for lease expiry;
7. assert eventual success, increased epoch and stable notification ID.

- [ ] **Step 5: Verify GREEN and commit**

```bash
make demo
make recovery-demo
git add cmd/mock-provider scripts configs/config.recovery.yaml Makefile
git commit -m "test: add notifier acceptance demos"
```

---

### Task 9: Add Packaging and Final Documentation

**Files:**
- Create: `.gitignore`
- Create: `.dockerignore`
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `.github/workflows/ci.yml`
- Create: `README.md`
- Create: `AI_USAGE.md`

- [ ] **Step 1: Add failing packaging checks**

```bash
test -f README.md
test -f AI_USAGE.md
test -f Dockerfile
test -f docker-compose.yml
```

Expected: failure until the files exist.

- [ ] **Step 2: Add packaging**

Use a multi-stage Dockerfile, non-root runtime user, persistent `/data` volume and Loopback-only host port bindings in Compose. Example credentials must be explicitly marked local-only.

- [ ] **Step 3: Add README**

Document:

- architecture and at-least-once boundary;
- setup and configuration;
- API examples;
- priority and aging behavior;
- metrics and failure-rate queries;
- normal demo, recovery demo and benchmark;
- SQLite and MQ trade-offs.

- [ ] **Step 4: Add AI usage record**

Record:

- AI-assisted PRD and design analysis;
- framework research;
- rejected Asynq/River/Backlite/Machinery adoption;
- human decision to make event priority P0;
- test-first implementation and verification evidence.

- [ ] **Step 5: Run complete quality gate**

```bash
gofmt -w .
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/...
make demo
make recovery-demo
make benchmark
```

Expected: every command exits 0; benchmark meets `>= 50 req/s`, `P95 <= 200 ms`, and all requests return 202.

- [ ] **Step 6: Commit**

```bash
git add .gitignore .dockerignore Dockerfile docker-compose.yml .github README.md AI_USAGE.md
git commit -m "docs: add runbook and delivery assets"
```

---

## Final Verification

- [ ] `git status --short` is empty.
- [ ] `go test ./...` passes.
- [ ] `go test -race ./...` passes.
- [ ] `go vet ./...` passes.
- [ ] `go build ./cmd/...` passes.
- [ ] Normal delivery, retry, dead letter and Replay demos pass.
- [ ] Crash recovery demo proves stable notification ID and increasing epoch.
- [ ] Priority test proves HIGH precedence and LOW anti-starvation.
- [ ] Metrics can derive API and Provider failure rates.
- [ ] No secret appears in logs, API responses or persisted Attempt data.
- [ ] Git history separates requirements, design, implementation, tests and delivery documentation.
