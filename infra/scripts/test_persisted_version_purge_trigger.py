#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_COMPOSE_FILE = ROOT / "infra" / "docker-compose.yml"
DEBUG = os.environ.get("DEBUG", "").lower() in ("1", "true", "yes")


class ScriptError(RuntimeError):
    pass


def debug_print(*args, **kwargs) -> None:
    if DEBUG:
        print(*args, **kwargs)


def run_compose_psql(compose_file: Path, db_user: str, db_name: str, sql: str) -> str:
    cmd = [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        db_user,
        "-d",
        db_name,
        "-tA",
        "-c",
        sql,
    ]
    result = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True)
    if result.returncode != 0:
        raise ScriptError(
            "psql command failed\n"
            f"SQL: {sql}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )
    return result.stdout.strip()


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def ensure_phi_id_does_not_exist(compose_file: Path, db_user: str, db_name: str, phi_id: str) -> None:
    exists = run_compose_psql(
        compose_file,
        db_user,
        db_name,
        "SELECT COUNT(*) FROM phi_patient_head WHERE phi_id = " + sql_literal(phi_id) + "::uuid;",
    )
    if exists != "0":
        raise ScriptError(f"Generated phi_id unexpectedly already exists: {phi_id}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Integration check for purge trigger on phi_patient_head.persisted_version. "
            "Creates parent/child rows, updates persisted_version, verifies child purge, and cleans up parent."
        )
    )
    parser.add_argument("--db-user", default="app", help="Postgres user (default: app)")
    parser.add_argument("--db-name", default="patient_events", help="Postgres database (default: patient_events)")
    parser.add_argument(
        "--compose-file",
        default=str(DEFAULT_COMPOSE_FILE),
        help=f"Path to docker-compose.yml (default: {DEFAULT_COMPOSE_FILE})",
    )
    args = parser.parse_args()

    compose_file = Path(args.compose_file).resolve()
    db_user = args.db_user
    db_name = args.db_name

    phi_id = str(uuid.uuid4())
    de_id = str(uuid.uuid4())
    event_id = str(uuid.uuid4())

    initial_persisted_version = 1
    updated_persisted_version = 2
    child_version_to_purge = 1

    parent_inserted = False

    try:
        debug_print("[1/6] Ensuring generated phi_id does not already exist...")
        ensure_phi_id_does_not_exist(compose_file, db_user, db_name, phi_id)

        debug_print("[2/6] Inserting parent row into phi_patient_head...")
        run_compose_psql(
            compose_file,
            db_user,
            db_name,
            (
                "INSERT INTO phi_patient_head (phi_id, de_id, current_version, persisted_version) VALUES ("
                f"{sql_literal(phi_id)}::uuid, "
                f"{sql_literal(de_id)}::uuid, "
                "1, "
                f"{initial_persisted_version}"
                ");"
            ),
        )
        parent_inserted = True

        debug_print("[3/6] Inserting child row into phi_patient_versions...")
        run_compose_psql(
            compose_file,
            db_user,
            db_name,
            (
                "INSERT INTO phi_patient_versions "
                "(phi_id, de_id, event_id, request_hash, version, name, dob, favorite_color) VALUES ("
                f"{sql_literal(phi_id)}::uuid, "
                f"{sql_literal(de_id)}::uuid, "
                f"{sql_literal(event_id)}::uuid, "
                f"{sql_literal('trigger-test-' + event_id.replace('-', ''))}, "
                f"{child_version_to_purge}, "
                f"{sql_literal('Trigger Test')}, "
                f"{sql_literal('1990-01-01')}::date, "
                f"{sql_literal('blue')}"
                ");"
            ),
        )

        pre_count = run_compose_psql(
            compose_file,
            db_user,
            db_name,
            (
                "SELECT COUNT(*) FROM phi_patient_versions WHERE "
                f"phi_id = {sql_literal(phi_id)}::uuid AND version = {child_version_to_purge};"
            ),
        )
        if pre_count != "1":
            raise ScriptError(
                "Setup validation failed: expected 1 child row before update, "
                f"found {pre_count}."
            )

        debug_print("[4/6] Updating parent persisted_version to trigger purge...")
        run_compose_psql(
            compose_file,
            db_user,
            db_name,
            (
                "UPDATE phi_patient_head SET persisted_version = "
                f"{updated_persisted_version} "
                "WHERE phi_id = "
                f"{sql_literal(phi_id)}::uuid;"
            ),
        )

        debug_print("[5/6] Validating child row was purged by trigger...")
        post_count = run_compose_psql(
            compose_file,
            db_user,
            db_name,
            (
                "SELECT COUNT(*) FROM phi_patient_versions WHERE "
                f"phi_id = {sql_literal(phi_id)}::uuid AND version = {child_version_to_purge};"
            ),
        )
        if post_count != "0":
            raise ScriptError(
                "Trigger validation failed: expected child row to be deleted, "
                f"but found {post_count} row(s)."
            )

        debug_print("[6/6] Deleting parent row from phi_patient_head...")
        run_compose_psql(
            compose_file,
            db_user,
            db_name,
            "DELETE FROM phi_patient_head WHERE phi_id = " + sql_literal(phi_id) + "::uuid;",
        )
        parent_inserted = False

        debug_print("SUCCESS: purge trigger deleted child versions < persisted_version as expected.")
        return 0

    except ScriptError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        if parent_inserted:
            try:
                run_compose_psql(
                    compose_file,
                    db_user,
                    db_name,
                    "DELETE FROM phi_patient_head WHERE phi_id = " + sql_literal(phi_id) + "::uuid;",
                )
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
