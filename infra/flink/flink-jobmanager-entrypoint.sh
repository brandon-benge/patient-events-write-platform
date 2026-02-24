#!/usr/bin/env bash
set -euo pipefail

CONF="/opt/flink/conf/flink-conf.yaml"
SERVICE_DNS="${FLINK_JOBMANAGER_DNS:-flink-jobmanager}"

# Append FLINK_PROPERTIES (same intent as the official entrypoint)
if [ -n "${FLINK_PROPERTIES:-}" ]; then
  echo "Applying FLINK_PROPERTIES to ${CONF}"
  printf "\n%s\n" "$FLINK_PROPERTIES" >> "$CONF"
fi

# Ensure RPC/REST bind to all interfaces so TaskManagers can reach the JM from other containers.
# The log 'inbound addresses are [pekko.tcp://flink@localhost:6123]' indicates JM is bound/advertised as localhost.
# These settings fix that for docker-compose networking.
ensure_kv() {
  local key="$1" val="$2"
  if grep -Eq "^${key}:" "$CONF"; then
    # Replace existing value
    sed -i "s|^${key}:.*|${key}: ${val}|" "$CONF"
  else
    echo "${key}: ${val}" >> "$CONF"
  fi
}

ensure_kv "jobmanager.bind-host" "0.0.0.0"
ensure_kv "rest.bind-address" "0.0.0.0"
# Ensure advertised addresses resolve on the docker network.
ensure_kv "jobmanager.rpc.address" "$SERVICE_DNS"
ensure_kv "rest.advertised-address" "$SERVICE_DNS"

# Helpful when debugging inside containers
echo "--- Effective Flink bind/advertise settings ---"
grep -E '^(jobmanager\.bind-host|jobmanager\.rpc\.address|rest\.bind-address|rest\.advertised-address):' "$CONF" || true
echo "--------------------------------------------"

echo "Starting Flink JobManager (foreground)..."
exec /docker-entrypoint.sh jobmanager