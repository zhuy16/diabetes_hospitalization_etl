from __future__ import annotations

from pathlib import Path
from typing import Dict

import duckdb
import pandas as pd

from etl.normalize_codes import normalize_icd10, normalize_loinc, normalize_rxnorm


def _read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _standardize(df_map: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    conditions = df_map["conditions"].copy()
    if not conditions.empty:
        conditions.loc[:, "icd10_code"] = conditions["icd10_code"].map(normalize_icd10)

    observations = df_map["observations"].copy()
    if not observations.empty:
        observations.loc[:, "loinc_code"] = observations["loinc_code"].map(normalize_loinc)

    medications = df_map["medications"].copy()
    if not medications.empty:
        medications["rxnorm_code"] = medications.apply(
            lambda r: normalize_rxnorm(r.get("rxnorm_code"), r.get("drug_name")), axis=1
        )

    output = dict(df_map)
    output["conditions"] = conditions
    output["observations"] = observations
    output["medications"] = medications
    return output


def rebuild_database(
    db_path: Path,
    sql_dir: Path,
    dataset: Dict[str, pd.DataFrame],
) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))

    tables = ["patients", "encounters", "conditions", "observations", "medications", "claims"]
    for table in tables:
        conn.execute(f"DROP TABLE IF EXISTS {table}")

    conn.execute(_read_sql(sql_dir / "schema.sql"))

    standardized = _standardize(dataset)

    for table in tables:
        df = standardized.get(table, pd.DataFrame())
        conn.register("tmp_df", df)
        conn.execute(f"INSERT INTO {table} SELECT * FROM tmp_df")
        conn.unregister("tmp_df")

    for view_file in [
        "views_t2d.sql",
        "views_exposure.sql",
        "views_labs.sql",
        "views_risk.sql",
        "views_final_cohort.sql",
    ]:
        conn.execute(_read_sql(sql_dir / view_file))

    conn.close()
