from __future__ import annotations

from pathlib import Path
import sys

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from etl.pipeline_hl7v2 import run_hl7v2_pipeline


def main() -> None:
    db_path = run_hl7v2_pipeline(ROOT)

    conn = duckdb.connect(str(db_path))
    try:
        patients = conn.execute("SELECT COUNT(*) FROM patients").fetchone()[0]
        observations = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        cohort = conn.execute("SELECT COUNT(*) FROM rwe_cohort").fetchone()[0]

        assert patients > 0, "HL7 pipeline produced no patients"
        assert observations > 0, "HL7 pipeline produced no observations"
        assert cohort > 0, "HL7 pipeline produced no cohort rows"

        print("HL7 pipeline smoke test passed.")
        print(f"patients={patients} observations={observations} rwe_cohort={cohort}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
