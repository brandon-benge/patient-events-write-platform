#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:9092}"
TOPIC_DIR="/infra/kafka/topics"

wait_for_kafka() {
  echo "Waiting for Kafka at ${BOOTSTRAP_SERVER}..."
  for _ in $(seq 1 60); do
    if kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" --list >/dev/null 2>&1; then
      echo "Kafka is ready"
      return 0
    fi
    sleep 2
  done
  echo "Kafka did not become ready in time" >&2
  return 1
}

create_topic_from_env() {
  local env_file="$1"
  # shellcheck disable=SC1090
  source "${env_file}"

  if kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" --topic "${TOPIC_NAME}" --describe >/dev/null 2>&1; then
    echo "Topic ${TOPIC_NAME} already exists"
    return 0
  fi

  kafka-topics \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${TOPIC_NAME}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}" \
    --config "retention.ms=${RETENTION_MS}" \
    --config "cleanup.policy=${CLEANUP_POLICY}" \
    --config "min.insync.replicas=${MIN_INSYNC_REPLICAS}"

  echo "Created topic ${TOPIC_NAME}"
}

wait_for_kafka
create_topic_from_env "${TOPIC_DIR}/debezium_phi_patient_versions.env"
create_topic_from_env "${TOPIC_DIR}/phi_patient_versions_deletes.env"
create_topic_from_env "${TOPIC_DIR}/deid_dlq.env"
