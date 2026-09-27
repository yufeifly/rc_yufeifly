# AI Usage Record

AI assistance was used for requirements analysis, architecture exploration, implementation planning, code generation, and verification. Human review determined the reliability contract and approved the technical design before implementation.

## Decisions

- Event priority was promoted to P0 by explicit human decision.
- The selected design uses SQLite WAL, a bounded in-process scheduler, Provider-specific concurrency, lease epochs, version CAS, stable notification IDs, and at-least-once delivery.
- Priority is assigned by server configuration. Aging prevents starvation without allowing callers to self-declare urgency.
- External HTTP execution remains outside database transactions.

## Framework Research

The design phase considered established queue and workflow projects, including Asynq, River, Backlite, and Machinery. They were not adopted for the MVP:

- Asynq and Machinery introduce Redis or broker operations while application state, replay audit, and fencing still require integration.
- River is strong for PostgreSQL-backed jobs, but PostgreSQL is outside the single-node MVP deployment target.
- Backlite is close to the SQLite deployment model, but the notifier still needs custom Provider fairness, request hardening, attempt evidence, and lease/version fencing.

The project therefore uses standard Go concurrency and explicit SQLite transactions. This is a scoped choice, not a claim that a custom queue is universally preferable.

## Implementation Method

Work proceeded in small Git commits. Each behavior was introduced with a failing test, followed by the minimum implementation and package-level regression checks. Coverage includes:

- strict configuration and secret resolution;
- durable idempotent acceptance;
- canonical request hashing and tenant isolation;
- Provider request contracts and SSRF controls;
- priority aging and starvation prevention;
- atomic claim plus attempt creation;
- lease fencing and stale completion rejection;
- retry, dead letter, recovery, and replay;
- bounded metrics labels and graceful shutdown;
- executable normal, crash-recovery, and benchmark scenarios.

## Verification

The final gate runs:

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

No AI-generated claim is treated as verification evidence unless the corresponding command or executable scenario succeeds.
