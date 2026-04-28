from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List
import json

import duckdb


@dataclass
class DQCheckResult:
    check: str
    status: str
    value: str
    details: str


def _conn(db_path: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path))


def _run_scalar(conn: duckdb.DuckDBPyConnection, sql: str):
    return conn.execute(sql).fetchone()[0]


def run_dq_checks(db_path: Path) -> List[DQCheckResult]:
    results: List[DQCheckResult] = []

    conn = _conn(db_path)
    try:
        core_tables = ["patients", "encounters", "conditions", "observations", "medications", "claims"]
        for table in core_tables:
            count = _run_scalar(conn, f"SELECT COUNT(*) FROM {table}")
            status = "PASS" if count > 0 else "WARN"
            results.append(
                DQCheckResult(
                    check=f"row_count_{table}",
                    status=status,
                    value=str(count),
                    details=f"Row count in {table}",
                )
            )

        null_patient_id = _run_scalar(
            conn,
            """
            SELECT
              (SELECT COUNT(*) FROM patients WHERE patient_id IS NULL)
              + (SELECT COUNT(*) FROM encounters WHERE patient_id IS NULL)
              + (SELECT COUNT(*) FROM conditions WHERE patient_id IS NULL)
              + (SELECT COUNT(*) FROM observations WHERE patient_id IS NULL)
              + (SELECT COUNT(*) FROM medications WHERE patient_id IS NULL)
              + (SELECT COUNT(*) FROM claims WHERE patient_id IS NULL)
            """,
        )
        results.append(
            DQCheckResult(
                check="null_patient_ids",
                status="PASS" if null_patient_id == 0 else "FAIL",
                value=str(null_patient_id),
                details="Null patient_id count across core tables",
            )
        )

        known_loinc = _run_scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM observations
            WHERE loinc_code IN ('4548-4', '33914-3', '2160-0')
            """,
        )
        all_loinc = _run_scalar(conn, "SELECT COUNT(*) FROM observations")
        loinc_pct = (known_loinc / all_loinc * 100.0) if all_loinc else 0.0
        results.append(
            DQCheckResult(
                check="known_loinc_coverage_pct",
                status="PASS" if loinc_pct >= 80 else "WARN",
                value=f"{loinc_pct:.2f}",
                details="Percent of observations with expected LOINC codes",
            )
        )

        diabetes_conditions = _run_scalar(
            conn,
            "SELECT COUNT(*) FROM conditions WHERE icd10_code LIKE 'E11%'",
        )
        ckd_conditions = _run_scalar(
            conn,
            "SELECT COUNT(*) FROM conditions WHERE icd10_code LIKE 'N18%'",
        )
        results.append(
            DQCheckResult(
                check="icd10_diabetes_rows",
                status="PASS" if diabetes_conditions > 0 else "WARN",
                value=str(diabetes_conditions),
                details="Conditions mapped to Type 2 diabetes ICD-10 E11%",
            )
        )
        results.append(
            DQCheckResult(
                check="icd10_ckd_rows",
                status="PASS" if ckd_conditions > 0 else "WARN",
                value=str(ckd_conditions),
                details="Conditions mapped to CKD ICD-10 N18%",
            )
        )

        bad_hba1c = _run_scalar(
            conn,
            """
            SELECT COUNT(*)
            FROM observations
            WHERE loinc_code = '4548-4' AND (value < 3 OR value > 20)
            """,
        )
        results.append(
            DQCheckResult(
                check="hba1c_out_of_expected_range",
                status="PASS" if bad_hba1c == 0 else "WARN",
                value=str(bad_hba1c),
                details="HbA1c rows outside rough expected range [3,20]",
            )
        )
    finally:
        conn.close()

    return results


def write_dq_report(results: List[DQCheckResult], report_dir: Path) -> Dict[str, str]:
    report_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "total_checks": len(results),
        "pass": sum(1 for r in results if r.status == "PASS"),
        "warn": sum(1 for r in results if r.status == "WARN"),
        "fail": sum(1 for r in results if r.status == "FAIL"),
    }

    json_path = report_dir / "dq_report.json"
    md_path = report_dir / "dq_report.md"

    payload = {
        "summary": summary,
        "checks": [r.__dict__ for r in results],
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# Data Quality Report",
        "",
        f"- Total checks: {summary['total_checks']}",
        f"- PASS: {summary['pass']}",
        f"- WARN: {summary['warn']}",
        f"- FAIL: {summary['fail']}",
        "",
        "| Check | Status | Value | Details |",
        "|---|---|---:|---|",
    ]

    for row in results:
        lines.append(f"| {row.check} | {row.status} | {row.value} | {row.details} |")

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return {"json": str(json_path), "md": str(md_path)}


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    db = root / "db" / "clinical.duckdb"
    results = run_dq_checks(db)
    out = write_dq_report(results, root / "data" / "processed" / "reports")
    print(f"DQ checks complete. Reports: {out['md']} and {out['json']}")
