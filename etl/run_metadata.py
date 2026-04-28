from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import duckdb


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_metadata_tables(db_path: Path) -> None:
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_run_log (
                run_id VARCHAR,
                pipeline_name VARCHAR,
                source_type VARCHAR,
                status VARCHAR,
                started_at_utc VARCHAR,
                finished_at_utc VARCHAR,
                duration_seconds DOUBLE,
                error_message VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_table_row_counts (
                run_id VARCHAR,
                table_name VARCHAR,
                row_count BIGINT,
                captured_at_utc VARCHAR
            )
            """
        )
    finally:
        conn.close()


def log_pipeline_run(
    db_path: Path,
    run_id: str,
    pipeline_name: str,
    source_type: str,
    status: str,
    started_at_utc: str,
    finished_at_utc: str,
    duration_seconds: float,
    error_message: str | None,
) -> None:
    ensure_metadata_tables(db_path)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            INSERT INTO etl_run_log
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                pipeline_name,
                source_type,
                status,
                started_at_utc,
                finished_at_utc,
                duration_seconds,
                error_message,
            ],
        )
    finally:
        conn.close()


def capture_table_row_counts(db_path: Path, run_id: str) -> Dict[str, int]:
    tables = ["patients", "encounters", "conditions", "observations", "medications", "claims", "rwe_cohort"]
    captured_at = _utc_now()
    output: Dict[str, int] = {}

    conn = duckdb.connect(str(db_path))
    try:
        ensure_metadata_tables(db_path)
        for table in tables:
            count = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            output[table] = count
            conn.execute(
                """
                INSERT INTO etl_table_row_counts
                VALUES (?, ?, ?, ?)
                """,
                [run_id, table, count, captured_at],
            )
    finally:
        conn.close()

    return output
