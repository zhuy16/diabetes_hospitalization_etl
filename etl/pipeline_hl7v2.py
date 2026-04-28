from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import uuid

from etl.load_duckdb import rebuild_database
from etl.parse_hl7v2 import HL7ParseConfig, parse_hl7v2_to_tables
from etl.run_metadata import capture_table_row_counts, log_pipeline_run


def run_hl7v2_pipeline(repo_root: Path) -> Path:
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    hl7_dir = repo_root / "data" / "raw" / "synthea" / "hl7v2"
    processed_out = repo_root / "data" / "processed" / "from_hl7v2"
    db_path = repo_root / "db" / "clinical.duckdb"
    sql_dir = repo_root / "sql"

    try:
        dataset = parse_hl7v2_to_tables(
            HL7ParseConfig(hl7_dir=hl7_dir, output_dir=processed_out)
        )
        rebuild_database(db_path=db_path, sql_dir=sql_dir, dataset=dataset)
        capture_table_row_counts(db_path, run_id)

        finished_at = datetime.now(timezone.utc)
        log_pipeline_run(
            db_path=db_path,
            run_id=run_id,
            pipeline_name="pipeline_hl7v2",
            source_type="hl7v2",
            status="SUCCESS",
            started_at_utc=started_at.replace(microsecond=0).isoformat(),
            finished_at_utc=finished_at.replace(microsecond=0).isoformat(),
            duration_seconds=(finished_at - started_at).total_seconds(),
            error_message=None,
        )
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        log_pipeline_run(
            db_path=db_path,
            run_id=run_id,
            pipeline_name="pipeline_hl7v2",
            source_type="hl7v2",
            status="FAILED",
            started_at_utc=started_at.replace(microsecond=0).isoformat(),
            finished_at_utc=finished_at.replace(microsecond=0).isoformat(),
            duration_seconds=(finished_at - started_at).total_seconds(),
            error_message=str(exc),
        )
        raise

    return db_path


if __name__ == "__main__":
    here = Path(__file__).resolve().parents[1]
    out = run_hl7v2_pipeline(here)
    print(f"HL7 v2 pipeline complete. DuckDB database: {out}")
