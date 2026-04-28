from __future__ import annotations

from pathlib import Path
import sys

import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from etl.pipeline import run_pipeline


def main() -> None:
    db_path = run_pipeline(ROOT)

    conn = duckdb.connect(str(db_path))
    try:
        checks = {
            "patients": "SELECT COUNT(*) FROM patients",
            "t2d_patients": "SELECT COUNT(*) FROM t2d_patients",
            "sglt2_exposure": "SELECT COUNT(*) FROM sglt2_exposure",
            "hba1c_trajectory": "SELECT COUNT(*) FROM hba1c_trajectory",
            "ckd_risk": "SELECT COUNT(*) FROM ckd_risk",
            "rwe_cohort": "SELECT COUNT(*) FROM rwe_cohort",
        }

        results = {name: conn.execute(sql).fetchone()[0] for name, sql in checks.items()}

        assert results["patients"] > 0, "patients table is empty"
        assert results["t2d_patients"] > 0, "t2d_patients view is empty"
        assert results["hba1c_trajectory"] > 0, "hba1c_trajectory view is empty"
        assert results["ckd_risk"] > 0, "ckd_risk view is empty"

        print("SQL view smoke tests passed.")
        for k, v in results.items():
            print(f"{k}: {v}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
