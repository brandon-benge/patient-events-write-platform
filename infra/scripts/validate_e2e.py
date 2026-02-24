#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = ROOT / "infra" / "docker-compose.yml"
REQUIRED_SERVICES = [
    "postgres",
    "redis",
    "kafka",
    "kafka-connect",
    "api",
    "flink-jobmanager",
    "flink-taskmanager",
]

DB_NAME = "patient_events"
DB_USER = "app"

REQUIRED_TABLES = [
    "phi_patient_head",
    "deid_patient_versions",
]

# Update these if your topic names differ.
REQUIRED_TOPICS = [
    "phi_patient_versions",
    "phi_patient_versions_deletes",
    "deid_dlq",
]

# Update this if your connector name differs.
REQUIRED_CONNECTORS = [
    "phi_patient_versions",
    "phi_patient_versions_deletes",
]


def run(cmd: list[str], check: bool = True) -> str:
    result = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True)
    if check and result.returncode != 0:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"Command failed: {' '.join(cmd)}")
    return result.stdout.strip()


def compose_cmd(*args: str) -> list[str]:
    return ["docker", "compose", "-f", str(COMPOSE_FILE), *args]


def ensure_stack_running(timeout_seconds: int = 60) -> None:
    """Validate that the infra stack is already up (and healthy if healthchecks exist)."""
    deadline = time.time() + timeout_seconds

    def get_status() -> dict[str, dict]:
        # `docker compose ps --format json` returns a JSON array of objects that includes
        # Service, Name, State, Health, and other fields.
        raw = run(compose_cmd("ps", "--format", "json"), check=False)
        rows: list[dict] = []
        if raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    rows = parsed
                elif isinstance(parsed, dict):
                    rows = [parsed]
            except Exception:
                # Compose may emit NDJSON (one JSON object per line)
                for ln in raw.splitlines():
                    ln = ln.strip()
                    if not ln:
                        continue
                    try:
                        obj = json.loads(ln)
                        if isinstance(obj, dict):
                            rows.append(obj)
                    except Exception:
                        continue
        out: dict[str, dict] = {}
        for r in rows:
            svc = r.get("Service") or ""
            if svc:
                out[svc] = r
        return out

    while time.time() < deadline:
        status = get_status()

        missing = [s for s in REQUIRED_SERVICES if s not in status]
        if missing:
            time.sleep(2)
            continue

        # Compose reports State like "running".
        not_running = [s for s in REQUIRED_SERVICES if status[s].get("State") != "running"]
        if not_running:
            time.sleep(2)
            continue

        # If a container has a healthcheck, prefer it being healthy.
        unhealthy = []
        for s in REQUIRED_SERVICES:
            h = status[s].get("Health")
            if not h:
                continue
            hl = str(h).strip().lower()
            # Services without healthchecks often report empty/"n/a"; ignore those.
            if hl in ("n/a", "none", ""):
                continue
            # Only fail if a healthcheck exists and it's not healthy.
            if hl != "healthy":
                unhealthy.append(s)
        if unhealthy:
            time.sleep(2)
            continue

        return

    # On failure, print the raw JSON output for debugging.
    raw = run(compose_cmd("ps", "--format", "json"), check=False)
    raise TimeoutError(
        "Infra stack is not fully running/healthy. Raw docker compose JSON output:\n"
        + raw
    )


def validate_postgres_schema(timeout_seconds: int = 60) -> None:
    """Validate Postgres is reachable and required tables exist."""
    deadline = time.time() + timeout_seconds

    def table_exists(table: str) -> bool:
        q = (
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name='{}' LIMIT 1;".format(table)
        )
        out = run(
            compose_cmd(
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                DB_USER,
                "-d",
                DB_NAME,
                "-tA",
                "-c",
                q,
            ),
            check=False,
        )
        return out.strip() == "1"

    # Wait for DB connectivity first
    while time.time() < deadline:
        out = run(
            compose_cmd(
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                DB_USER,
                "-d",
                DB_NAME,
                "-tA",
                "-c",
                "SELECT 1;",
            ),
            check=False,
        )
        if out.strip() == "1":
            break
        time.sleep(2)
    else:
        raise TimeoutError(f"Postgres did not become reachable for db '{DB_NAME}' as user '{DB_USER}'")

    missing = [t for t in REQUIRED_TABLES if not table_exists(t)]
    if missing:
        raise RuntimeError(f"Missing required Postgres tables: {missing}")


def validate_postgres_purge_trigger() -> None:
    """Validate the persisted_version purge trigger works correctly."""
    trigger_test_script = ROOT / "infra" / "scripts" / "test_persisted_version_purge_trigger.py"
    if not trigger_test_script.exists():
        raise RuntimeError(f"Purge trigger test script not found at {trigger_test_script}")
    
    result = subprocess.run(
        [sys.executable, str(trigger_test_script), "--compose-file", str(COMPOSE_FILE)],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    
    if result.returncode != 0:
        raise RuntimeError(
            f"Purge trigger test failed with exit code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )



def _kafka_topics_list_cmd() -> str:
    """Return a shell snippet that lists Kafka topics inside the kafka container."""
    # Try common locations across images.
    return (
        "set -e; "
        "for c in "
        "kafka-topics "
        "/opt/bitnami/kafka/bin/kafka-topics.sh "
        "/usr/bin/kafka-topics "
        "/opt/kafka/bin/kafka-topics.sh; "
        "do "
        "  if command -v $c >/dev/null 2>&1 || [ -x $c ]; then "
        "    exec ${c} --bootstrap-server kafka:9092 --list; "
        "  fi; "
        "done; "
        "echo 'ERROR: kafka-topics CLI not found in kafka container.' >&2; "
        "exit 127"
    )


def validate_kafka_topics(timeout_seconds: int = 60) -> None:
    """Validate required Kafka topics exist."""
    deadline = time.time() + timeout_seconds
    last_err = ""

    while time.time() < deadline:
        try:
            out = run(
                compose_cmd(
                    "exec",
                    "-T",
                    "kafka",
                    "sh",
                    "-lc",
                    _kafka_topics_list_cmd(),
                ),
                check=False,
            )
            if "ERROR: kafka-topics CLI not found" in out or not out.strip():
                last_err = out.strip() or "kafka-topics CLI not found"
                time.sleep(2)
                continue

            topics = {ln.strip() for ln in out.splitlines() if ln.strip()}
            missing = [t for t in REQUIRED_TOPICS if t not in topics]
            if missing:
                raise RuntimeError(f"Missing required Kafka topics: {missing}. Found: {sorted(topics)[:25]}{'...' if len(topics) > 25 else ''}")
            return
        except Exception as exc:
            last_err = str(exc)
            time.sleep(2)

    raise TimeoutError(f"Kafka topics validation did not succeed: {last_err}")


def validate_kafka_connect_connectors(timeout_seconds: int = 60) -> None:
    """Validate required Kafka Connect connectors are registered (e.g., Debezium source)."""
    deadline = time.time() + timeout_seconds
    last_err = ""

    while time.time() < deadline:
        try:
            with urllib.request.urlopen("http://localhost:8083/connectors", timeout=3) as resp:
                if resp.status != 200:
                    last_err = f"HTTP {resp.status}"
                    time.sleep(2)
                    continue
                body = resp.read().decode("utf-8")
                names = set(json.loads(body))

            missing = [c for c in REQUIRED_CONNECTORS if c not in names]
            if missing:
                raise RuntimeError(f"Missing required Kafka Connect connectors: {missing}. Found: {sorted(names)}")
            return
        except Exception as exc:
            last_err = str(exc)
            time.sleep(2)

    raise TimeoutError(f"Kafka Connect connector validation did not succeed: {last_err}")


def validate_flink_job_running(timeout_seconds: int = 60) -> None:
    """Validate at least one TaskManager is connected and at least one job is RUNNING."""
    deadline = time.time() + timeout_seconds

    def get_json(url: str) -> dict:
        with urllib.request.urlopen(url, timeout=3) as resp:
            return json.loads(resp.read().decode("utf-8"))

    # Wait for at least one TaskManager
    while time.time() < deadline:
        try:
            tms = get_json("http://localhost:8081/taskmanagers")
            if int(tms.get("taskmanagers", []) and len(tms.get("taskmanagers", [])) or 0) > 0:
                break
        except Exception:
            pass
        time.sleep(2)
    else:
        raise TimeoutError("No Flink TaskManager connected to JobManager")

    # Wait for a running job
    while time.time() < deadline:
        try:
            jobs = get_json("http://localhost:8081/jobs/overview")
            for j in jobs.get("jobs", []):
                if j.get("state") == "RUNNING":
                    return
        except Exception:
            pass
        time.sleep(2)

    raise TimeoutError("No RUNNING Flink job found (check Flink UI / submitter logs)")


def wait_for_api(timeout_seconds: int = 60) -> None:
    """Only validate that the API port is reachable; do not invoke business endpoints."""
    deadline = time.time() + timeout_seconds
    probe_url = "http://localhost:8080/"
    while time.time() < deadline:
        try:
            req = urllib.request.Request(probe_url, method="GET")
            with urllib.request.urlopen(req, timeout=2) as resp:
                # Any HTTP response means the port is reachable.
                _ = resp.status
                return
        except urllib.error.HTTPError:
            # Non-2xx still proves the server is reachable.
            return
        except Exception:
            time.sleep(2)
    raise TimeoutError("API port 8080 did not become reachable")

def validate_redis_connectivity(timeout_seconds: int = 60) -> None:
    """Validate Redis is reachable via PING command."""
    deadline = time.time() + timeout_seconds
    
    while time.time() < deadline:
        try:
            out = run(
                compose_cmd(
                    "exec",
                    "-T",
                    "redis",
                    "redis-cli",
                    "PING",
                ),
                check=False,
            )
            if out.strip() == "PONG":
                return
        except Exception:
            pass
        time.sleep(2)
    
    raise TimeoutError("Redis did not respond to PING")

def main() -> int:
    parser = argparse.ArgumentParser(description="Validate local infra is up (no start/stop).")
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Seconds to wait for required services to be running/healthy.",
    )
    args = parser.parse_args()

    print("[1/9] Validating docker compose stack is already running")
    ensure_stack_running(timeout_seconds=args.timeout)

    print("[2/9] Validating API port is reachable (no business calls)")
    wait_for_api(timeout_seconds=args.timeout)

    print("[3/9] Validating Kafka Connect REST is reachable")
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen("http://localhost:8083/connectors", timeout=2) as resp:
                if resp.status == 200:
                    break
        except Exception:
            time.sleep(2)
    else:
        raise TimeoutError("Kafka Connect (8083) did not become reachable")

    print("[4/9] Validating Debezium connector is registered in Kafka Connect")
    validate_kafka_connect_connectors(timeout_seconds=args.timeout)

    print("[5/9] Validating Postgres schema + required tables")
    validate_postgres_schema(timeout_seconds=args.timeout)

    print("[6/9] Validating Postgres purge trigger on persisted_version")
    validate_postgres_purge_trigger()

    print("[7/9] Validating Kafka topics exist")
    validate_kafka_topics(timeout_seconds=args.timeout)

    print("[8/9] Validating Flink has a connected TaskManager and a RUNNING job")
    validate_flink_job_running(timeout_seconds=args.timeout)

    print("[9/9] Validating Redis connectivity + warmup")
    validate_redis_connectivity(timeout_seconds=args.timeout)

    print("\nINFRA VALIDATION PASSED")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Infra validation failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
