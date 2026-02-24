#!/usr/bin/env sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
CONNECTOR_DIR="/infra/debezium/connectors"

wait_for_connect() {
  echo "Waiting for Kafka Connect at ${CONNECT_URL}..."
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "${CONNECT_URL}/" >/dev/null 2>&1; then
      echo "Kafka Connect is ready"
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  echo "Kafka Connect did not become ready in time" >&2
  return 1
}

register_connector() {
  file="$1"
  name=$(basename "$file" .json)

  if grep -Eq '^[[:space:]]*"name"[[:space:]]*:' "$file"; then
    echo "ERROR: $file contains a top-level \"name\" key. Remove it; this script PUTs to /connectors/{name}/config and expects config-only JSON." >&2
    return 1
  fi

  echo "Registering connector ${name} from ${file}"

  resp_file="/tmp/${name}.connect.response.json"
  http_code=$(curl -sS -X PUT \
    -H "Content-Type: application/json" \
    --data @"${file}" \
    -o "$resp_file" \
    -w "%{http_code}" \
    "${CONNECT_URL}/connectors/${name}/config" || true)

  if [ "$http_code" -lt 200 ] || [ "$http_code" -ge 300 ]; then
    echo "ERROR: Kafka Connect returned HTTP $http_code while registering ${name}. Response body:" >&2
    cat "$resp_file" >&2 || true
    echo "" >&2
    return 1
  fi

  cat "$resp_file"
  echo ""

  echo "Waiting for connector ${name} to be RUNNING..."
  j=0
  while [ "$j" -lt 30 ]; do
    status_json=$(curl -fsS "${CONNECT_URL}/connectors/${name}/status" 2>/dev/null || true)
    if echo "$status_json" | grep -q '"state"[[:space:]]*:[[:space:]]*"RUNNING"'; then
      echo "Connector ${name} is RUNNING"
      return 0
    fi
    j=$((j + 1))
    sleep 1
  done

  echo "WARNING: Connector ${name} not RUNNING after timeout. Status was:" >&2
  echo "$status_json" >&2
  return 1
}

wait_for_connect

for file in "${CONNECTOR_DIR}"/*.json; do
  [ -f "$file" ] || continue
  register_connector "$file"
done
