from __future__ import annotations

from pathlib import Path

import pandas as pd


def _fmt_date(value: str) -> str:
    if not value:
        return ""
    return str(value).replace("-", "")


def _safe(value) -> str:
    if pd.isna(value):
        return ""
    return str(value)


def _gender_hl7(gender: str) -> str:
    g = str(gender).upper().strip()
    if g == "M":
        return "M"
    if g == "F":
        return "F"
    return "U"


def build_sample_hl7v2(repo_root: Path, patient_limit: int = 2) -> None:
    raw_csv_dir = repo_root / "data" / "raw" / "synthea" / "csv"
    demo_csv_dir = repo_root / "data" / "processed" / "demo_csv"
    csv_dir = raw_csv_dir if (raw_csv_dir / "patients.csv").exists() else demo_csv_dir
    out_dir = repo_root / "data" / "raw" / "synthea" / "hl7v2"
    out_dir.mkdir(parents=True, exist_ok=True)

    patients = pd.read_csv(csv_dir / "patients.csv")
    encounters = pd.read_csv(csv_dir / "encounters.csv")
    conditions = pd.read_csv(csv_dir / "conditions.csv")
    observations = pd.read_csv(csv_dir / "observations.csv")
    medications = pd.read_csv(csv_dir / "medications.csv")
    claims = pd.read_csv(csv_dir / "claims.csv")

    selected_ids = patients.head(patient_limit)["patient_id"].tolist()

    for patient_id in selected_ids:
        p = patients[patients["patient_id"] == patient_id].iloc[0]
        p_enc = encounters[encounters["patient_id"] == patient_id]
        p_con = conditions[conditions["patient_id"] == patient_id]
        p_obs = observations[observations["patient_id"] == patient_id]
        p_med = medications[medications["patient_id"] == patient_id]
        p_clm = claims[claims["patient_id"] == patient_id]

        lines: list[str] = []

        lines.append(
            "MSH|^~\\&|SYNTHETIC|DIABETES_ETL|ANALYTICS|DEMO|"
            "20260428100000||ADT^A01|MSG" + patient_id + "|P|2.5"
        )
        lines.append(
            "PID|1||"
            + patient_id
            + "||"
            + patient_id
            + "^DEMO||"
            + _fmt_date(_safe(p["birth_date"]))
            + "|"
            + _gender_hl7(_safe(p["gender"]))
            + "|||^^"
            + _safe(p["state"])
            + "^"
            + _safe(p["zip"])
        )

        for idx, row in p_enc.iterrows():
            visit_num = idx + 1
            lines.append(
                "PV1|"
                + str(visit_num)
                + "|"
                + _safe(row["encounter_type"]).upper()
                + "|||"
                + _safe(row["provider_id"])
                + "||||||||||||"
                + _safe(row["encounter_id"])
            )

        for i, row in enumerate(p_con.itertuples(index=False), start=1):
            lines.append(
                "DG1|"
                + str(i)
                + "||"
                + _safe(row.icd10_code)
                + "^"
                + _safe(row.icd10_description)
                + "^I10||"
                + _fmt_date(_safe(row.onset_date))
            )

        for i, row in enumerate(p_obs.itertuples(index=False), start=1):
            lines.append(
                "OBX|"
                + str(i)
                + "|NM|"
                + _safe(row.loinc_code)
                + "^"
                + _safe(row.loinc_description)
                + "^LN||"
                + _safe(row.value)
                + "|"
                + _safe(row.unit)
                + "|||||F|||"
                + _fmt_date(_safe(row.observation_date))
            )

        for i, row in enumerate(p_med.itertuples(index=False), start=1):
            lines.append(
                "RXE|"
                + str(i)
                + "|"
                + _safe(row.dosage)
                + "|"
                + _safe(row.rxnorm_code)
                + "^"
                + _safe(row.drug_name)
                + "^RXNORM||"
                + _fmt_date(_safe(row.start_date))
                + "|"
                + _fmt_date(_safe(row.stop_date))
            )

        for i, row in enumerate(p_clm.itertuples(index=False), start=1):
            lines.append(
                "FT1|"
                + str(i)
                + "||"
                + _fmt_date(_safe(row.claim_date))
                + "||CG|"
                + _safe(row.cpt_code)
                + "^CPT|||1|"
                + _safe(row.amount_billed)
                + "||"
                + _safe(row.payer)
                + "|"
                + _safe(row.icd10_primary)
            )

        (out_dir / f"{patient_id}.hl7").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    build_sample_hl7v2(root)
    print("Generated sample HL7 v2 messages in data/raw/synthea/hl7v2")
