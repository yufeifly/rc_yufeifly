#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
MOCK_PORT="${MOCK_PORT:-18180}"
NOTIFIER_PORT="${NOTIFIER_PORT:-18181}"
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
  shutdown_grace: 3s
  read_header_timeout: 2s
  read_timeout: 5s
  write_timeout: 5s
  idle_timeout: 30s
database:
  path: "$TMP/recovery.db"
  busy_timeout: 5s
scheduler:
  poll_interval: 25ms
  provider_scan_limit: 100
  recovery_batch_size: 100
  lease_duration: 1s
  finalize_retry_budget: 500ms
workers: {concurrency: 1}
priority:
  default: NORMAL
  aging_interval: 1s
  events: {}
retry:
  initial_delay: 50ms
  max_delay: 200ms
  jitter_ratio: 0
  default_max_attempts: 3
auth:
  mode: disabled
  principals: []
network_policy:
  allow_insecure_http: true
  allow_private_ip: true
providers:
  delayed:
    version: v1
    enabled: true
    method: POST
    url: "http://127.0.0.1:${MOCK_PORT}/delayed"
    body_mode: envelope
    success_status_codes: [200]
    max_response_bytes: 1024
    auth: {type: none}
    timeout: {connect: 1s, request: 5s}
    retry: {max_attempts: 3}
    max_concurrency: 1
YAML

"$TMP/mock-provider" -address "127.0.0.1:${MOCK_PORT}" -delay 2s >"$TMP/mock.log" 2>&1 &
MOCK_PID=$!
"$TMP/notifier" -config "$TMP/config.yaml" >"$TMP/notifier-first.log" 2>&1 &
NOTIFIER_PID=$!

wait_http() {
  local url="$1"
  for _ in $(seq 1 100); do
    curl -fsS "$url" >/dev/null 2>&1 && return 0
    sleep 0.05
  done
  return 1
}

wait_http "http://127.0.0.1:${MOCK_PORT}/health"
wait_http "http://127.0.0.1:${NOTIFIER_PORT}/health/ready"
notification_id="$(curl -fsS -X POST "http://127.0.0.1:${NOTIFIER_PORT}/v1/notifications" \
  -H "X-Caller-Id: recovery-test" \
  -H "Content-Type: application/json" \
  -d '{"idempotency_key":"crash-1","provider":"delayed","event_type":"recovery.test","payload":{"value":1}}' |
  python3 -c 'import json,sys; print(json.load(sys.stdin)["notification_id"])')"

for _ in $(seq 1 100); do
  count="$(curl -fsS "http://127.0.0.1:${MOCK_PORT}/records" |
    ID="$notification_id" python3 -c 'import json,os,sys; print(sum(r["notification_id"] == os.environ["ID"] for r in json.load(sys.stdin)["records"]))')"
  (( count >= 1 )) && break
  sleep 0.05
done
(( count >= 1 ))

kill -9 "$NOTIFIER_PID"
wait "$NOTIFIER_PID" 2>/dev/null || true
NOTIFIER_PID=""
"$TMP/notifier" -config "$TMP/config.yaml" >"$TMP/notifier-second.log" 2>&1 &
NOTIFIER_PID=$!
wait_http "http://127.0.0.1:${NOTIFIER_PORT}/health/ready"

for _ in $(seq 1 200); do
  status="$(curl -fsS "http://127.0.0.1:${NOTIFIER_PORT}/v1/notifications/${notification_id}" \
    -H "X-Caller-Id: recovery-test" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
  [[ "$status" == "SUCCEEDED" ]] && break
  sleep 0.05
done
[[ "$status" == "SUCCEEDED" ]]

curl -fsS "http://127.0.0.1:${MOCK_PORT}/records" |
  ID="$notification_id" python3 -c 'import json,os,sys; rows=[r for r in json.load(sys.stdin)["records"] if r["notification_id"] == os.environ["ID"]]; assert len(rows) >= 2; assert {r["notification_id"] for r in rows} == {os.environ["ID"]}'

epoch="$(python3 - "$TMP/recovery.db" "$notification_id" <<'PY'
import sqlite3, sys
connection = sqlite3.connect(sys.argv[1])
print(connection.execute("SELECT lease_epoch FROM notifications WHERE id = ?", (sys.argv[2],)).fetchone()[0])
PY
)"
(( epoch >= 2 ))

echo "recovery-demo: PASS"
