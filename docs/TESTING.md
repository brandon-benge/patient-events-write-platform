# Testing Guide

This document defines all tests in the repository, what they validate, and how they are executed locally and in GitHub Actions.

## Test Coverage Summary

| Layer | Test | Location | Type |
|---|---|---|---|
| Java API | Request hash determinism/consistency | `services/src/test/java/com/patienteventswriteplatform/patient/RequestHashServiceTest.java` | Unit |
| Java API | Admission + idempotency contract | `services/src/test/java/com/patienteventswriteplatform/patient/AdmissionIdempotencyIntegrationTest.java` | Integration (Testcontainers) |
| Java API | Phase-1 atomic commit + receipt gating | `services/src/test/java/com/patienteventswriteplatform/patient/Phase1AtomicCommitReceiptGatingIntegrationTest.java` | Integration (Testcontainers) |
| Flink | CDC payload contract parsing + PHI exclusion checks | `infra/flink/tests/test_deid_projection_job.py` | Unit |
| Infra/E2E | Infra readiness, purge trigger, CDC→De-ID→persisted_version flow | `infra/scripts/validate_e2e.py` | End-to-end validation script |
| DB Trigger | Purge trigger behavior on persisted_version update | `infra/scripts/test_persisted_version_purge_trigger.py` | Integration script |

## Java API Tests

### 1) RequestHashServiceTest (Unit)
- **File**: `services/src/test/java/com/patienteventswriteplatform/patient/RequestHashServiceTest.java`
- **Validates**:
  - Hash generation is deterministic for identical payloads.
  - Update hash includes `phi_id`.
  - Hash changes when payload changes.

### 2) AdmissionIdempotencyIntegrationTest (Integration)
- **File**: `services/src/test/java/com/patienteventswriteplatform/patient/AdmissionIdempotencyIntegrationTest.java`
- **Validates**:
  - Duplicate `event_id` + same payload returns HTTP 200 and does not create a second PHI commit.
  - Duplicate `event_id` + different payload returns HTTP 409 with no PHI mutation.
  - `PHI_FAILED` receipt with matching hash retries and can return HTTP 202.
- **Runtime requirements**:
  - Docker available (Testcontainers starts Redis/Postgres).
  - `RUN_INTEGRATION_TESTS=true` to enable this class.

### 3) Phase1AtomicCommitReceiptGatingIntegrationTest (Integration)
- **File**: `services/src/test/java/com/patienteventswriteplatform/patient/Phase1AtomicCommitReceiptGatingIntegrationTest.java`
- **Validates**:
  - Create request writes `phi_patient_head` and `phi_patient_versions` for version 1.
  - API does not return 202 when Redis committed receipt update fails.
  - Receipt phase does not regress from `PHI_COMMITTED` to `ADMITTED` on duplicate.
- **Runtime requirements**:
  - Docker available (Testcontainers).
  - `RUN_INTEGRATION_TESTS=true`.

## Flink Tests

### 4) test_deid_projection_job.py (Unit)
- **File**: `infra/flink/tests/test_deid_projection_job.py`
- **Validates**:
  - Debezium envelope parsing (`payload.after`, `after`, and flat formats).
  - Required-field enforcement.
  - Rejection of forbidden PHI columns (`name`, `dob`).
  - Event shape and core constants (e.g., DLQ topic name).

## Infra / End-to-End Validation

### 5) validate_e2e.py (System Validation)
- **File**: `infra/scripts/validate_e2e.py`
- **Validates**:
  - Compose services are up/healthy.
  - Kafka Connect and required connectors exist.
  - Required Kafka topics exist.
  - Flink has TaskManager + running job.
  - Redis connectivity.
  - Purge trigger test is executed.
  - Active CDC→De-ID flow:
    1. create patient via API,
    2. verify CDC topic offset advances,
    3. verify `deid_patient_versions` row exists,
    4. verify `phi_patient_head.persisted_version` advances.
  - Optional DLQ check via `--check-dlq`.

### 6) test_persisted_version_purge_trigger.py (DB Trigger)
- **File**: `infra/scripts/test_persisted_version_purge_trigger.py`
- **Validates**:
  - Updating `persisted_version` deletes older PHI version rows (`version < persisted_version`).
  - Trigger behavior for purge path executes as expected.

## Local Execution Commands

### Java unit tests
```bash
mvn -f services/pom.xml test
```

### Java integration tests only
```bash
RUN_INTEGRATION_TESTS=true mvn -f services/pom.xml \
  -Dtest=AdmissionIdempotencyIntegrationTest,Phase1AtomicCommitReceiptGatingIntegrationTest test
```

### Flink unit tests
```bash
python3 -m unittest -v infra/flink/tests/test_deid_projection_job.py
```

### Full infra/e2e validation (stack must be running)
```bash
python3 infra/scripts/validate_e2e.py --timeout 180
```

### E2E validation including DLQ probe
```bash
python3 infra/scripts/validate_e2e.py --timeout 180 --check-dlq
```

## GitHub Actions Automation

### services tests workflow
- **File**: `.github/workflows/services-tests.yml`
- **Jobs**:
  - `unit-tests`: runs all Java tests with default settings.
  - `integration-tests`: sets `RUN_INTEGRATION_TESTS=true` and runs Java integration classes.

### infra and flink tests workflow
- **File**: `.github/workflows/test.yml`
- **Jobs**:
  - `flink-unit-tests`: runs Flink unit tests.
  - `e2e-compose-validation`: builds API jar, starts compose, runs `validate_e2e.py`, and tears down stack.

## Notes
- Integration tests are intentionally gated by `RUN_INTEGRATION_TESTS=true` so local unit runs do not fail on machines without Docker/Testcontainers.
- E2E script timeout may need tuning if GitHub runners are under heavy load.
