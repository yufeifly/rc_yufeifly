#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
MOCK_PORT="${MOCK_PORT:-18280}"
NOTIFIER_PORT="${NOTIFIER_PORT:-18281}"
MOCK_PID=""
NOTIFIER_PID=""

cleanup() {
  [[ -n "$NOTIFIER_PID" ]] && kill "$NOTIFIER_PID" 2>/dev/null || true
  [[ -n "$MOCK_PID" ]] && kill "$MOCK_PID" 2>/dev/null || true
  rm -rf "$TMP"
}
trap cleanup EXIT

cd "$ROOT"
env -u GOROOT go build -o "$TMP/mock-provider" ./cmd/mock-provider
env -u GOROOT go build -o "$TMP/notifier" ./cmd/notifier

cat >"$TMP/config.yaml" <<YAML
environment: development
server:
  address: "127.0.0.1:${NOTIFIER_PORT}"
  max_body_bytes: 262144
  shutdown_grace: 5s
  read_header_timeout: 2s
  read_timeout: 5s
  write_timeout: 5s
  idle_timeout: 30s
database: {path: "$TMP/benchmark.db", busy_timeout: 5s}
scheduler:
  poll_interval: 20ms
  provider_scan_limit: 100
  recovery_batch_size: 100
  lease_duration: 10s
  finalize_retry_budget: 2s
workers: {concurrency: 10}
priority: {default: NORMAL, aging_interval: 30s, events: {}}
retry:
  initial_delay: 100ms
  max_delay: 1s
  jitter_ratio: 0.2
  default_max_attempts: 3
auth:
  mode: static_token
  principals:
    - caller_id: benchmark
      role: producer
      token_env: BENCHMARK_TOKEN
network_policy: {allow_insecure_http: true, allow_private_ip: true}
providers:
  benchmark:
    version: v1
    enabled: true
    method: POST
    url: "http://127.0.0.1:${MOCK_PORT}/always-200"
    body_mode: passthrough
    success_status_codes: [200]
    max_response_bytes: 1024
    auth: {type: none}
    timeout: {connect: 1s, request: 3s}
    retry: {max_attempts: 3}
    max_concurrency: 5
YAML

export BENCHMARK_TOKEN=benchmark-token
"$TMP/mock-provider" -address "127.0.0.1:${MOCK_PORT}" >"$TMP/mock.log" 2>&1 &
MOCK_PID=$!
"$TMP/notifier" -config "$TMP/config.yaml" >"$TMP/notifier.log" 2>&1 &
NOTIFIER_PID=$!

for _ in $(seq 1 100); do
  curl -fsS "http://127.0.0.1:${NOTIFIER_PORT}/health/ready" >/dev/null 2>&1 && break
  sleep 0.05
done
curl -fsS "http://127.0.0.1:${NOTIFIER_PORT}/health/ready" >/dev/null

python3 "$ROOT/scripts/benchmark-ingress.py" \
  --url "http://127.0.0.1:${NOTIFIER_PORT}" \
  --token "$BENCHMARK_TOKEN" \
  --requests "${REQUESTS:-500}" \
  --concurrency "${CONCURRENCY:-20}" \
  --min-rps "${MIN_RPS:-50}" \
  --max-p95-ms "${MAX_P95_MS:-200}"
