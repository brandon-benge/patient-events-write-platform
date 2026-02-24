"""De-ID projection job (Flink streaming application).

Implements section 10.3 contract per ARCHITECTURE.md:
- Consume Debezium CDC envelopes for phi_patient_versions inserts
- Validate payload contract and PHI exclusion (no name/dob)
- Persist immutable deid_patient_versions(de_id, version) to PostgreSQL (De-ID Store)
- Confirm monotonic persisted_version in phi_patient_head via conditional update
- Publish DLQ envelopes to deid_dlq Kafka topic for validation and DB failures

I/O uses Flink connectors:
- FlinkKafkaConsumer for CDC source
- FlinkKafkaProducer for DLQ sink
- FlatMapFunction for sequential DB operations with DLQ routing
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

import psycopg2
import redis
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import FlatMapFunction, RuntimeContext
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types

# Setup logging
DEBUG = os.getenv("DEBUG", "false").lower() == "true"
log = logging.getLogger(__name__)
log_level = logging.DEBUG if DEBUG else logging.INFO
logging.basicConfig(level=log_level, format='%(asctime)s %(levelname)s %(name)s - %(message)s')

CDC_TOPIC = "phi_patient_versions"
DLQ_TOPIC = "deid_dlq"

REQUIRED_FIELDS = {
    "phi_id",
    "de_id",
    "version",
    "event_id",
    "request_hash",
}
FORBIDDEN_FIELDS = {"name", "dob"}


class ContractViolation(ValueError):
    """Raised when CDC payload contract is violated."""


@dataclass(frozen=True)
class ProjectionEvent:
    """De-ID projection event extracted from CDC envelope."""
    phi_id: str
    de_id: str
    version: int
    event_id: str
    request_hash: str
    favorite_color: str | None


@dataclass(frozen=True)
class DeidRecord:
    """De-ID record for deid_patient_versions table."""
    de_id: str
    version: int
    favorite_color: str | None
    event_id: str  # For Redis observability


@dataclass(frozen=True)
class PersistedVersionUpdate:
    """Update for phi_patient_head.persisted_version (monotonic)."""
    phi_id: str
    version: int


def _to_dict(raw_value: str | bytes | Mapping[str, Any]) -> dict[str, Any]:
    """Convert raw record to dict."""
    if isinstance(raw_value, bytes):
        return json.loads(raw_value.decode("utf-8"))
    if isinstance(raw_value, str):
        return json.loads(raw_value)
    if isinstance(raw_value, Mapping):
        return dict(raw_value)
    raise TypeError(f"Unsupported record type: {type(raw_value)!r}")


def parse_debezium_record(raw_value: str | bytes | Mapping[str, Any]) -> ProjectionEvent:
    """Parse Debezium envelope and enforce De-ID projection contract.

    Expected shape contains an AFTER object under either:
    - payload.after (standard Debezium envelope), or
    - after (simplified)

    Validates:
    - Required fields present (phi_id, de_id, version, event_id, request_hash)
    - Forbidden PHI columns absent (name, dob)
    - favorite_color is optional (may be None)
    """
    try:
        envelope = _to_dict(raw_value)
        if DEBUG:
            log.debug(f"Parsing CDC record. Keys in envelope: {list(envelope.keys())}")

        # Extract 'after' from envelope (try nested payload first, then top-level).
        # If the connector uses ExtractNewRecordState, the record is already flattened,
        # so we treat the top-level object as the "after" payload.
        after: Any = None
        payload = envelope.get("payload")
        if isinstance(payload, Mapping):
            after = payload.get("after")
            if DEBUG:
                log.debug(f"Found payload.after. Keys in after: {list(after.keys()) if isinstance(after, Mapping) else 'N/A'}")
        if after is None:
            after = envelope.get("after")
            if DEBUG:
                log.debug(f"Using envelope.after. Keys in after: {list(after.keys()) if isinstance(after, Mapping) else 'N/A'}")
        if after is None:
            after = envelope
            if DEBUG:
                log.debug(f"Using envelope as after. Keys: {list(after.keys())}")

        if not isinstance(after, Mapping):
            raise ContractViolation("Record missing payload.after/after and is not a flat object")

        # Check for forbidden PHI fields
        forbidden = FORBIDDEN_FIELDS.intersection(after.keys())
        if forbidden:
            log.warning(f"Found forbidden fields: {sorted(forbidden)}")
            raise ContractViolation(
                f"CDC payload contains forbidden PHI columns: {sorted(forbidden)}"
            )

        # Check for required fields
        missing = REQUIRED_FIELDS.difference(after.keys())
        if missing:
            log.warning(f"Missing required fields: {sorted(missing)}")
            raise ContractViolation(f"CDC payload missing required fields: {sorted(missing)}")

        event = ProjectionEvent(
            phi_id=str(after["phi_id"]),
            de_id=str(after["de_id"]),
            version=int(after["version"]),
            event_id=str(after["event_id"]),
            request_hash=str(after["request_hash"]),
            favorite_color=(
                None if after.get("favorite_color") is None else str(after.get("favorite_color"))
            ),
        )
        if DEBUG:
            log.debug(f"Successfully parsed: phi_id={event.phi_id}, de_id={event.de_id}, version={event.version}")
        return event
        
    except ContractViolation as e:
        log.warning(f"ContractViolation: {str(e)}")
        raise
    except Exception as e:
        log.error(f"Unexpected error in parse_debezium_record: {type(e).__name__}: {str(e)}", exc_info=True)
        raise


def _best_effort_ids(raw_value: str) -> tuple[str | None, str | None, int | None, str | None]:
    """Best-effort extraction of (phi_id, de_id, version, event_id) from raw CDC message.
    
    Returns (phi_id, de_id, version, event_id) where any can be None if extraction fails.
    """
    try:
        envelope = _to_dict(raw_value)
        payload = envelope.get("payload", {})
        after = payload.get("after") or envelope.get("after") or envelope
        if isinstance(after, Mapping):
            return (
                str(after.get("phi_id")) if after.get("phi_id") is not None else None,
                str(after.get("de_id")) if after.get("de_id") is not None else None,
                int(after.get("version")) if after.get("version") is not None else None,
                str(after.get("event_id")) if after.get("event_id") is not None else None,
            )
    except Exception:
        pass
    return (None, None, None, None)


def _build_dlq_envelope(phi_id: str | None, de_id: str | None, version: int | None,
                        event_id: str | None, failure_phase: str, error: str,
                        raw_message: str) -> str:
    """Build DLQ envelope per ARCHITECTURE.md §16.4.
    
    Returns JSON string with: event_id, phi_id, de_id, version, failure_phase, error, failed_at (ISO-8601 UTC Z), raw_message
    """
    now_utc = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    
    dlq_value = json.dumps({
        "event_id": event_id,
        "phi_id": phi_id,
        "de_id": de_id,
        "version": version,
        "failure_phase": failure_phase,
        "error": error,
        "failed_at": now_utc,
        "raw_message": raw_message,
    })
    
    return dlq_value


class SequentialProcessFunction(FlatMapFunction):
    """Sequential processing with DLQ routing at each failure point.
    
    Processing flow per ARCHITECTURE.md §10.3.1:
    1. Validate contract → fail: emit DLQ (CONTRACT_VIOLATION)
    2. INSERT deid_patient_versions → fail: emit DLQ (DEID_INSERT_FAILED) after max_retries
    3. UPDATE phi_patient_head.persisted_version → fail: emit DLQ (PHI_HEAD_UPDATE_FAILED) after max_retries
    4. UPDATE Redis persisted_at → best effort (no DLQ on failure)
    
    Emits (de_id, dlq_json) for any failure (keyed by de_id), emits nothing on success.
    """
    
    def __init__(self, db_host: str, db_port: int, db_name: str,
                 db_user: str, db_password: str, redis_host: str, redis_port: int,
                 max_retries: int = 3, retry_backoff_ms: int = 50):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.connection = None
        self.redis_client = None
        self.record_count = 0
        self.success_count = 0
        self.dlq_count = 0
    
    def open(self, runtime_context: RuntimeContext):
        """Initialize JDBC and Redis connections."""
        try:
            # Create PostgreSQL connection (psycopg2)
            self.connection = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                dbname=self.db_name,
            )
            self.connection.autocommit = False
            log.info(
                f"Postgres connection initialized: {self.db_host}:{self.db_port}/{self.db_name}"
            )
            
            # Initialize Redis connection
            self.redis_client = redis.Redis(
                host=self.redis_host,
                port=self.redis_port,
                decode_responses=True,
                socket_connect_timeout=1,
                socket_timeout=2,
            )
            log.info(f"Redis connection initialized: {self.redis_host}:{self.redis_port}")
            
        except Exception as e:
            log.error(f"Failed to initialize connections: {e}", exc_info=True)
            raise
    
    def flat_map(self, value: str):
        """Process CDC event with sequential DB operations and DLQ routing."""
        import time

        self.record_count += 1
        raw_value = value  # Keep for DLQ payloads

        if self.record_count % 1000 == 0:
            log.info(f"Sequential process: {self.record_count} processed, "
                     f"{self.success_count} success, {self.dlq_count} DLQ")

        try:
            # Step 1: Validate contract
            event = parse_debezium_record(value)

            # Create De-ID record
            deid_record = DeidRecord(
                de_id=event.de_id,
                version=event.version,
                favorite_color=event.favorite_color,
                event_id=event.event_id,
            )

            # Create persisted_version update
            pv_update = PersistedVersionUpdate(
                phi_id=event.phi_id,
                version=event.version,
            )

        except ContractViolation as e:
            # Contract validation failed - send to DLQ
            phi_id, de_id, version, event_id = _best_effort_ids(raw_value)
            dlq_json = _build_dlq_envelope(
                phi_id=phi_id,
                de_id=de_id,
                version=version,
                event_id=event_id,
                failure_phase="CONTRACT_VIOLATION",
                error=str(e),
                raw_message=raw_value,
            )
            self.dlq_count += 1
            yield dlq_json
            return

        except Exception as e:
            # Unexpected parsing error - send to DLQ
            phi_id, de_id, version, event_id = _best_effort_ids(raw_value)
            dlq_json = _build_dlq_envelope(
                phi_id=phi_id,
                de_id=de_id,
                version=version,
                event_id=event_id,
                failure_phase="CONTRACT_VIOLATION",
                error=str(e),
                raw_message=raw_value,
            )
            self.dlq_count += 1
            yield dlq_json
            return

        # Step 2: INSERT into deid_patient_versions (with retry)
        last_error = None
        step2_ok = False
        for attempt in range(self.max_retries + 1):
            cursor = None
            try:
                cursor = self.connection.cursor()
                cursor.execute(
                    """
                    INSERT INTO deid_patient_versions (de_id, version, favorite_color)
                    VALUES (%s::uuid, %s, %s)
                    ON CONFLICT (de_id, version) DO NOTHING
                    """,
                    (deid_record.de_id, deid_record.version, deid_record.favorite_color),
                )
                rows_inserted = cursor.rowcount

                # Commit Step B (De-ID insert) in its own transaction
                self.connection.commit()

                if DEBUG and rows_inserted > 0:
                    log.debug(
                        f"Inserted de_id={deid_record.de_id}, version={deid_record.version}"
                    )

                step2_ok = True
                break

            except Exception as e:
                last_error = e
                try:
                    self.connection.rollback()
                except Exception:
                    pass

                if attempt < self.max_retries:
                    # Backoff and retry transient errors
                    time.sleep(self.retry_backoff_ms / 1000.0)
                    continue

            finally:
                if cursor:
                    cursor.close()

        if not step2_ok:
            # Max retries exceeded - send to DLQ
            dlq_json = _build_dlq_envelope(
                phi_id=event.phi_id,
                de_id=event.de_id,
                version=event.version,
                event_id=event.event_id,
                failure_phase="DEID_INSERT_FAILED",
                error=str(last_error),
                raw_message=raw_value,
            )
            self.dlq_count += 1
            yield dlq_json
            return

        # Step 3: UPDATE phi_patient_head.persisted_version (with retry)
        last_error = None
        step3_ok = False
        for attempt in range(self.max_retries + 1):
            cursor = None
            try:
                cursor = self.connection.cursor()
                cursor.execute(
                    """
                    UPDATE phi_patient_head
                    SET persisted_version = %s, last_updated_at = NOW()
                    WHERE phi_id = %s::uuid AND persisted_version < %s
                    """,
                    (pv_update.version, pv_update.phi_id, pv_update.version),
                )
                rows_updated = cursor.rowcount

                # Commit Step C (head persisted_version confirmation) in its own transaction
                self.connection.commit()

                if DEBUG and rows_updated > 0:
                    log.debug(
                        f"Updated phi_patient_head phi_id={pv_update.phi_id}, persisted_version={pv_update.version}"
                    )

                step3_ok = True
                break

            except Exception as e:
                last_error = e
                try:
                    self.connection.rollback()
                except Exception:
                    pass

                if attempt < self.max_retries:
                    # Backoff and retry transient errors
                    time.sleep(self.retry_backoff_ms / 1000.0)
                    continue

            finally:
                if cursor:
                    cursor.close()

        if not step3_ok:
            # Max retries exceeded - send to DLQ
            dlq_json = _build_dlq_envelope(
                phi_id=event.phi_id,
                de_id=event.de_id,
                version=event.version,
                event_id=event.event_id,
                failure_phase="PHI_HEAD_UPDATE_FAILED",
                error=str(last_error),
                raw_message=raw_value,
            )
            self.dlq_count += 1
            yield dlq_json
            return

        # Step 4: UPDATE Redis persisted_at (best effort, no DLQ on failure)
        if self.redis_client:
            key = None
            try:
                key = f"idempotency:{deid_record.event_id}"
                value_str = self.redis_client.get(key)
                if value_str:
                    value_data = json.loads(value_str)
                    value_data["persisted_at"] = datetime.now(timezone.utc).isoformat()
                    self.redis_client.set(key, json.dumps(value_data))
                    if DEBUG:
                        log.debug(f"Updated Redis {key} with persisted_at")
            except Exception as e:
                if key:
                    log.warning(f"Redis update failed for key={key}: {type(e).__name__}: {e}")
                else:
                    log.warning(f"Redis update failed for event_id={deid_record.event_id}: {type(e).__name__}: {e}")

        # Success! (no output needed, just count)
        self.success_count += 1
    
    def close(self):
        """Cleanup connections."""
        if self.connection:
            try:
                self.connection.close()
                log.info(f"PostgreSQL connection closed. Final stats: {self.success_count} success, "
                         f"{self.dlq_count} DLQ")
            except Exception as e:
                log.warning(f"Error closing PostgreSQL connection: {e}")


def main() -> None:
    """Flink job entrypoint: consume CDC, validate, persist to DB and DLQ."""
    log.info("Starting deid_projection_job")
    if DEBUG:
        log.debug(f"DEBUG mode enabled")
    
    # Configuration from environment
    kafka_brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    db_host = os.getenv("DB_HOST", "postgres")
    db_port = int(os.getenv("DB_PORT", "5432"))
    db_user = os.getenv("DB_USER", "app")
    db_password = os.getenv("DB_PASSWORD", "app")
    db_name = os.getenv("DB_NAME", "patient_events")
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    
    # Retry settings for transient errors
    max_retries = int(os.getenv("MAX_RETRIES", "3"))
    retry_backoff_ms = int(os.getenv("RETRY_BACKOFF_MS", "50"))
    
    # Parallelism settings (default 4, can override with env)
    parallelism = int(os.getenv("FLINK_PARALLELISM", "4"))
    kafka_source_parallelism = int(os.getenv("KAFKA_SOURCE_PARALLELISM", "6"))
    job_parallelism = int(os.getenv("FLINK_JOB_PARALLELISM", str(parallelism)))

    # Use the higher of the two so the pipeline can actually run at >= 4 when requested.
    effective_parallelism = max(parallelism, job_parallelism)

    log.info(f"Kafka: {kafka_brokers}, DB: {db_host}:{db_port}/{db_name}")
    log.info(f"Parallelism: {parallelism}, Job parallelism: {job_parallelism} (effective={effective_parallelism})")
    log.info(f"Retry settings: max_retries={max_retries}, retry_backoff_ms={retry_backoff_ms}ms")

    # Create Flink streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(effective_parallelism)
    log.info("Flink streaming environment created")

    # --------- Kafka Source (CDC) ---------
    kafka_source_props = {
        "bootstrap.servers": kafka_brokers,
        "group.id": "deid-projection-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    }

    cdc_stream = (
        env.add_source(
            FlinkKafkaConsumer(
                CDC_TOPIC,
                SimpleStringSchema(),
                properties=kafka_source_props,
            )
        )
        .name("phi-cdc-kafka-source")
    )
    cdc_stream.set_parallelism(kafka_source_parallelism)
    log.info(f"Kafka source added for topic: {CDC_TOPIC} with parallelism={kafka_source_parallelism}")

    # --------- Process: Sequential validation and DB operations with DLQ routing ---------
    # Sequential processing function: validates contract, performs DB ops, emits DLQ on failure
    processed_stream = (
        cdc_stream
        .flat_map(
            SequentialProcessFunction(
                db_host=db_host,
                db_port=db_port,
                db_name=db_name,
                db_user=db_user,
                db_password=db_password,
                redis_host=redis_host,
                redis_port=redis_port,
                max_retries=max_retries,
                retry_backoff_ms=retry_backoff_ms,
            ),
            output_type=Types.STRING(),
        )
        .name("sequential-process-flatmap")
    )
    processed_stream.set_parallelism(effective_parallelism)
    log.info("Sequential processing function added (contract validation → deid insert → phi_head update → redis)")

    # --------- Sink: DLQ to Kafka ---------
    # All DLQ messages (contract violations, DB failures, etc.) go to DLQ topic
    (
        processed_stream
        .add_sink(
            FlinkKafkaProducer(
                DLQ_TOPIC,
                SimpleStringSchema(),
                kafka_source_props,
            )
        )
        .name("deid-dlq-kafka-sink")
        .set_parallelism(effective_parallelism)
    )
    log.info("DLQ Kafka sink added")

    # Execute job
    log.info("Executing job...")
    env.execute("deid_projection_job")


if __name__ == "__main__":
    main()
