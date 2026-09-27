#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
MOCK_PORT="${MOCK_PORT:-18080}"
NOTIFIER_PORT="${NOTIFIER_PORT:-18081}"
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
database:
  path: "$TMP/notifier.db"
  busy_timeout: 5s
scheduler:
  poll_interval: 20ms
  provider_scan_limit: 100
  recovery_batch_size: 100
  lease_duration: 10s
  finalize_retry_budget: 2s
workers:
  concurrency: 4
priority:
  default: NORMAL
  aging_interval: 1s
  events:
    payment.succeeded: HIGH
    crm.sync: NORMAL
    analytics.report: LOW
retry:
  initial_delay: 50ms
  max_delay: 200ms
  jitter_ratio: 0
  default_max_attempts: 4
auth:
  mode: static_token
  principals:
    - caller_id: demo
      role: producer
      token_env: DEMO_PRODUCER_TOKEN
    - caller_id: demo-admin
      role: admin
      token_env: DEMO_ADMIN_TOKEN
network_policy:
  allow_insecure_http: true
  allow_private_ip: true
providers:
  success:
    version: v1
    enabled: true
    method: POST
    url: "http://127.0.0.1:${MOCK_PORT}/always-200"
    body_mode: envelope
    success_status_codes: [200]
    max_response_bytes: 1024
    auth: {type: none}
    timeout: {connect: 1s, request: 3s}
    retry: {max_attempts: 4}
    max_concurrency: 1
  retry:
    version: v1
    enabled: true
    method: POST
    url: "http://127.0.0.1:${MOCK_PORT}/fail-twice-then-200"
    body_mode: envelope
    success_status_codes: [200]
    max_response_bytes: 1024
    auth: {type: none}
    timeout: {connect: 1s, request: 3s}
    retry: {max_attempts: 4}
    max_concurrency: 1
  controlled:
    version: v1
    enabled: true
    method: POST
    url: "http://127.0.0.1:${MOCK_PORT}/controlled/priority"
    body_mode: envelope
    success_status_codes: [200]
    max_response_bytes: 1024
    auth: {type: none}
    timeout: {connect: 1s, request: 8s}
    retry: {max_attempts: 4}
    max_concurrency: 1
YAML

export DEMO_PRODUCER_TOKEN=producer-token
export DEMO_ADMIN_TOKEN=admin-token
"$TMP/mock-provider" -address "127.0.0.1:${MOCK_PORT}" >"$TMP/mock.log" 2>&1 &
MOCK_PID=$!
"$TMP/notifier" -config "$TMP/config.yaml" >"$TMP/notifier.log" 2>&1 &
NOTIFIER_PID=$!

wait_http() {
  local url="$1"
  for _ in $(seq 1 100); do
    curl -fsS "$url" >/dev/null 2>&1 && return 0
    sleep 0.05
  done
  return 1
}

json_field() {
  python3 -c "import json,sys; print(json.load(sys.stdin)$1)"
}

submit() {
  local key="$1" provider="$2" event="$3"
  curl -fsS -X POST "http://127.0.0.1:${NOTIFIER_PORT}/v1/notifications" \
    -H "Authorization: Bearer $DEMO_PRODUCER_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"idempotency_key\":\"$key\",\"provider\":\"$provider\",\"event_type\":\"$event\",\"payload\":{\"key\":\"$key\"}}"
}

status_of() {
  curl -fsS "http://127.0.0.1:${NOTIFIER_PORT}/v1/notifications/$1" \
    -H "Authorization: Bearer $DEMO_PRODUCER_TOKEN" | json_field "['status']"
}

wait_status() {
  local id="$1" want="$2"
  for _ in $(seq 1 200); do
    [[ "$(status_of "$id")" == "$want" ]] && return 0
    sleep 0.05
  done
  echo "timeout waiting for $id to become $want" >&2
  return 1
}

wait_record_count() {
  local path="$1" want="$2"
  for _ in $(seq 1 200); do
    local count
    count="$(curl -fsS "http://127.0.0.1:${MOCK_PORT}/records" | \
      PATH_FILTER="$path" python3 -c 'import json,os,sys; print(sum(r["path"] == os.environ["PATH_FILTER"] for r in json.load(sys.stdin)["records"]))')"
    (( count >= want )) && return 0
    sleep 0.05
  done
  return 1
}

wait_http "http://127.0.0.1:${MOCK_PORT}/health"
wait_http "http://127.0.0.1:${NOTIFIER_PORT}/health/ready"

normal_json="$(submit normal-success success crm.sync)"
normal_id="$(printf '%s' "$normal_json" | json_field "['notification_id']")"
wait_status "$normal_id" SUCCEEDED
duplicate_id="$(submit normal-success success crm.sync | json_field "['notification_id']")"
[[ "$duplicate_id" == "$normal_id" ]]

retry_id="$(submit retry-success retry crm.sync | json_field "['notification_id']")"
wait_status "$retry_id" SUCCEEDED

curl -fsS -X PUT "http://127.0.0.1:${MOCK_PORT}/controlled/priority?status=200&blocked=true" >/dev/null
blocker_id="$(submit priority-blocker controlled payment.succeeded | json_field "['notification_id']")"
wait_record_count /controlled/priority 1
low_id="$(submit priority-low controlled analytics.report | json_field "['notification_id']")"
normal_priority_id="$(submit priority-normal controlled crm.sync | json_field "['notification_id']")"
high_id="$(submit priority-high controlled payment.succeeded | json_field "['notification_id']")"
curl -fsS -X PUT "http://127.0.0.1:${MOCK_PORT}/controlled/priority?status=200&blocked=false" >/dev/null
wait_status "$blocker_id" SUCCEEDED
wait_status "$high_id" SUCCEEDED
wait_status "$normal_priority_id" SUCCEEDED
wait_status "$low_id" SUCCEEDED

ORDER="$(curl -fsS "http://127.0.0.1:${MOCK_PORT}/records" | \
  IDS="$high_id,$normal_priority_id,$low_id" python3 -c 'import json,os,sys; ids=os.environ["IDS"].split(","); rows=json.load(sys.stdin)["records"]; print(",".join(r["notification_id"] for r in rows if r["notification_id"] in ids))')"
[[ "$ORDER" == "$high_id,$normal_priority_id,$low_id" ]]

curl -fsS -X PUT "http://127.0.0.1:${MOCK_PORT}/controlled/priority?status=200&blocked=true" >/dev/null
aging_blocker="$(submit aging-blocker controlled payment.succeeded | json_field "['notification_id']")"
wait_record_count /controlled/priority 5
aged_low="$(submit aging-low controlled analytics.report | json_field "['notification_id']")"
sleep 2.1
new_high="$(submit aging-high controlled payment.succeeded | json_field "['notification_id']")"
curl -fsS -X PUT "http://127.0.0.1:${MOCK_PORT}/controlled/priority?status=200&blocked=false" >/dev/null
wait_status "$aging_blocker" SUCCEEDED
wait_status "$aged_low" SUCCEEDED
wait_status "$new_high" SUCCEEDED
AGING_ORDER="$(curl -fsS "http://127.0.0.1:${MOCK_PORT}/records" | \
  IDS="$aged_low,$new_high" python3 -c 'import json,os,sys; ids=os.environ["IDS"].split(","); rows=json.load(sys.stdin)["records"]; print(",".join(r["notification_id"] for r in rows if r["notification_id"] in ids))')"
[[ "$AGING_ORDER" == "$aged_low,$new_high" ]]

curl -fsS -X PUT "http://127.0.0.1:${MOCK_PORT}/controlled/priority?status=400&blocked=false" >/dev/null
dead_id="$(submit replay-dead controlled crm.sync | json_field "['notification_id']")"
wait_status "$dead_id" DEAD
curl -fsS -X PUT "http://127.0.0.1:${MOCK_PORT}/controlled/priority?status=200&blocked=false" >/dev/null
curl -fsS -X POST "http://127.0.0.1:${NOTIFIER_PORT}/v1/notifications/${dead_id}/replay" \
  -H "Authorization: Bearer $DEMO_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"reason":"provider recovered"}' >/dev/null
wait_status "$dead_id" SUCCEEDED

curl -fsS "http://127.0.0.1:${NOTIFIER_PORT}/metrics" \
  -H "Authorization: Bearer $DEMO_ADMIN_TOKEN" | grep -q "notifier_delivery_attempts_total"
curl -fsS "http://127.0.0.1:${MOCK_PORT}/records" | \
  python3 -c 'import json,sys; data=json.load(sys.stdin); assert data["max_in_flight_by_path"].get("/controlled/priority", 0) <= 1'

echo "demo: PASS"
