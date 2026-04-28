from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import uuid

from etl.extract_synthea import ExtractConfig, load_synthea_or_demo
from etl.load_duckdb import rebuild_database
from etl.run_metadata import capture_table_row_counts, log_pipeline_run


def run_pipeline(repo_root: Path) -> Path:
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)

    raw_csv_dir = repo_root / "data" / "raw" / "synthea" / "csv"
    diabetes130_csv = repo_root / "data" / "raw" / "diabetes130" / "diabetic_data.csv"
    processed_demo_dir = repo_root / "data" / "processed" / "demo_csv"
    db_path = repo_root / "db" / "clinical.duckdb"
    sql_dir = repo_root / "sql"

    try:
        dataset = load_synthea_or_demo(
            ExtractConfig(
                raw_csv_dir=raw_csv_dir,
                processed_demo_dir=processed_demo_dir,
                diabetes130_csv=diabetes130_csv,
            )
        )
        rebuild_database(db_path=db_path, sql_dir=sql_dir, dataset=dataset)
        capture_table_row_counts(db_path, run_id)

        finished_at = datetime.now(timezone.utc)
        log_pipeline_run(
            db_path=db_path,
            run_id=run_id,
            pipeline_name="pipeline",
            source_type="csv_demo",
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
            pipeline_name="pipeline",
            source_type="csv_demo",
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
    out = run_pipeline(here)
    print(f"Pipeline complete. DuckDB database: {out}")
