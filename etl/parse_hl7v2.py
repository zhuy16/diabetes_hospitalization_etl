from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import pandas as pd


@dataclass
class HL7ParseConfig:
    hl7_dir: Path
    output_dir: Path


@dataclass
class HL7ParseStats:
    files_processed: int = 0
    malformed_segments: int = 0
    unknown_segments: int = 0
    skipped_rows_without_patient: int = 0


def _safe(parts: List[str], idx: int) -> str:
    return parts[idx] if idx < len(parts) else ""


def _to_iso(yyyymmdd: str) -> str | None:
    value = (yyyymmdd or "").strip()
    if len(value) != 8 or not value.isdigit():
        return None
    return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"


def _split_coding(coded_value: str) -> tuple[str | None, str | None]:
    chunks = (coded_value or "").split("^")
    code = chunks[0].strip() if chunks and chunks[0].strip() else None
    desc = chunks[1].strip() if len(chunks) > 1 and chunks[1].strip() else None
    return code, desc


def _with_schema(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    for col in columns:
        if col not in df.columns:
            df[col] = None
    return df[columns]


def parse_hl7v2_to_tables(config: HL7ParseConfig) -> Dict[str, pd.DataFrame]:
    if not config.hl7_dir.exists():
        raise FileNotFoundError(f"HL7 directory not found: {config.hl7_dir}")

    hl7_files = sorted(config.hl7_dir.glob("*.hl7"))
    if not hl7_files:
        raise FileNotFoundError(f"No .hl7 files found in: {config.hl7_dir}")

    stats = HL7ParseStats()
    patient_rows = []
    encounter_rows = []
    condition_rows = []
    observation_rows = []
    medication_rows = []
    claim_rows = []

    for hl7_file in hl7_files:
        stats.files_processed += 1
        lines = [line.strip() for line in hl7_file.read_text(encoding="utf-8").splitlines() if line.strip()]

        patient_id = None
        first_encounter_id = None
        condition_idx = 0
        observation_idx = 0
        medication_idx = 0
        claim_idx = 0

        for line in lines:
            parts = line.split("|")
            seg = _safe(parts, 0)

            if not seg:
                stats.malformed_segments += 1
                continue

            if seg == "PID":
                patient_id = _safe(parts, 3).strip() or hl7_file.stem
                birth_date = _to_iso(_safe(parts, 7))
                gender = _safe(parts, 8).strip()
                address = _safe(parts, 11).split("^")
                state = address[2].strip() if len(address) > 2 and address[2].strip() else None
                zip_code = address[3].strip() if len(address) > 3 and address[3].strip() else None

                patient_rows.append(
                    {
                        "patient_id": patient_id,
                        "birth_date": birth_date,
                        "gender": gender,
                        "race": None,
                        "ethnicity": None,
                        "state": state,
                        "zip": zip_code,
                    }
                )

            elif seg == "PV1":
                if not patient_id:
                    stats.skipped_rows_without_patient += 1
                    continue
                encounter_id = next((x for x in reversed(parts) if x.strip()), "")
                encounter_id = encounter_id.strip() or f"{patient_id}_ENC"
                if first_encounter_id is None:
                    first_encounter_id = encounter_id

                encounter_rows.append(
                    {
                        "encounter_id": encounter_id,
                        "patient_id": patient_id,
                        "encounter_date": None,
                        "encounter_type": _safe(parts, 2).strip().lower() or None,
                        "provider_id": _safe(parts, 5).strip() or None,
                        "payer": None,
                        "total_cost": None,
                    }
                )

            elif seg == "DG1":
                if not patient_id:
                    stats.skipped_rows_without_patient += 1
                    continue
                condition_idx += 1
                code, desc = _split_coding(_safe(parts, 3))
                onset_date = _to_iso(_safe(parts, 5))

                condition_rows.append(
                    {
                        "condition_id": f"{patient_id}_DG1_{condition_idx:03d}",
                        "patient_id": patient_id,
                        "encounter_id": first_encounter_id,
                        "icd10_code": code,
                        "icd10_description": desc,
                        "onset_date": onset_date,
                        "resolution_date": None,
                    }
                )

            elif seg == "OBX":
                if not patient_id:
                    stats.skipped_rows_without_patient += 1
                    continue
                observation_idx += 1
                code, desc = _split_coding(_safe(parts, 3))
                obs_date = _to_iso(_safe(parts, 14))

                observation_rows.append(
                    {
                        "observation_id": f"{patient_id}_OBX_{observation_idx:03d}",
                        "patient_id": patient_id,
                        "encounter_id": first_encounter_id,
                        "loinc_code": code,
                        "loinc_description": desc,
                        "value": pd.to_numeric(_safe(parts, 5), errors="coerce"),
                        "unit": _safe(parts, 6).strip() or None,
                        "observation_date": obs_date,
                    }
                )

            elif seg == "RXE":
                if not patient_id:
                    stats.skipped_rows_without_patient += 1
                    continue
                medication_idx += 1
                code, desc = _split_coding(_safe(parts, 3))
                start_date = _to_iso(_safe(parts, 5))
                stop_date = _to_iso(_safe(parts, 6))

                medication_rows.append(
                    {
                        "medication_id": f"{patient_id}_RXE_{medication_idx:03d}",
                        "patient_id": patient_id,
                        "encounter_id": first_encounter_id,
                        "rxnorm_code": code,
                        "ndc_code": None,
                        "drug_name": desc,
                        "start_date": start_date,
                        "stop_date": stop_date,
                        "dosage": _safe(parts, 2).strip() or None,
                    }
                )

            elif seg == "FT1":
                if not patient_id:
                    stats.skipped_rows_without_patient += 1
                    continue
                claim_idx += 1
                cpt_code, _ = _split_coding(_safe(parts, 6))
                claim_date = _to_iso(_safe(parts, 3))

                claim_rows.append(
                    {
                        "claim_id": f"{patient_id}_FT1_{claim_idx:03d}",
                        "patient_id": patient_id,
                        "encounter_id": first_encounter_id,
                        "claim_date": claim_date,
                        "cpt_code": cpt_code,
                        "icd10_primary": _safe(parts, 13).strip() or None,
                        "payer": _safe(parts, 12).strip() or None,
                        "amount_billed": pd.to_numeric(_safe(parts, 10), errors="coerce"),
                        "amount_paid": None,
                    }
                )

            elif seg != "MSH":
                stats.unknown_segments += 1

    # Deduplicate patient and encounter rows by ids to keep load deterministic.
    patients = pd.DataFrame(patient_rows)
    if not patients.empty:
        patients = patients.drop_duplicates(subset=["patient_id"], keep="last")
    patients = _with_schema(
        patients,
        ["patient_id", "birth_date", "gender", "race", "ethnicity", "state", "zip"],
    )

    encounters = pd.DataFrame(encounter_rows)
    if not encounters.empty:
        encounters = encounters.drop_duplicates(subset=["encounter_id"], keep="last")
    encounters = _with_schema(
        encounters,
        [
            "encounter_id",
            "patient_id",
            "encounter_date",
            "encounter_type",
            "provider_id",
            "payer",
            "total_cost",
        ],
    )

    tables = {
        "patients": patients,
        "encounters": encounters,
        "conditions": _with_schema(
            pd.DataFrame(condition_rows),
            [
                "condition_id",
                "patient_id",
                "encounter_id",
                "icd10_code",
                "icd10_description",
                "onset_date",
                "resolution_date",
            ],
        ),
        "observations": _with_schema(
            pd.DataFrame(observation_rows),
            [
                "observation_id",
                "patient_id",
                "encounter_id",
                "loinc_code",
                "loinc_description",
                "value",
                "unit",
                "observation_date",
            ],
        ),
        "medications": _with_schema(
            pd.DataFrame(medication_rows),
            [
                "medication_id",
                "patient_id",
                "encounter_id",
                "rxnorm_code",
                "ndc_code",
                "drug_name",
                "start_date",
                "stop_date",
                "dosage",
            ],
        ),
        "claims": _with_schema(
            pd.DataFrame(claim_rows),
            [
                "claim_id",
                "patient_id",
                "encounter_id",
                "claim_date",
                "cpt_code",
                "icd10_primary",
                "payer",
                "amount_billed",
                "amount_paid",
            ],
        ),
    }

    config.output_dir.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        df.to_csv(config.output_dir / f"{name}.csv", index=False)

    summary = {
        "files_processed": stats.files_processed,
        "malformed_segments": stats.malformed_segments,
        "unknown_segments": stats.unknown_segments,
        "skipped_rows_without_patient": stats.skipped_rows_without_patient,
    }
    pd.DataFrame([summary]).to_csv(config.output_dir / "parse_summary.csv", index=False)

    return tables


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    out = root / "data" / "processed" / "from_hl7v2"
    parse_hl7v2_to_tables(
        HL7ParseConfig(
            hl7_dir=root / "data" / "raw" / "synthea" / "hl7v2",
            output_dir=out,
        )
    )
    print(f"Parsed HL7 v2 files into: {out}")
