from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd

from etl.normalize_codes import CREATININE_LOINC, EGFR_LOINC, HBA1C_LOINC


MEDICATION_COLUMNS = [
    "metformin",
    "repaglinide",
    "nateglinide",
    "chlorpropamide",
    "glimepiride",
    "acetohexamide",
    "glipizide",
    "glyburide",
    "tolbutamide",
    "pioglitazone",
    "rosiglitazone",
    "acarbose",
    "miglitol",
    "troglitazone",
    "tolazamide",
    "examide",
    "citoglipton",
    "insulin",
    "glyburide-metformin",
    "glipizide-metformin",
    "glimepiride-pioglitazone",
    "metformin-rosiglitazone",
    "metformin-pioglitazone",
]


def load_diabetes130_to_canonical(csv_path: Path) -> Dict[str, pd.DataFrame]:
    source = pd.read_csv(csv_path, na_values=["?", "Unknown/Invalid"], low_memory=False)
    source = source.sort_values(["patient_nbr", "encounter_id"]).reset_index(drop=True)

    encounter_dates = _build_encounter_dates(source)
    source = source.assign(encounter_date=encounter_dates)

    patients = _build_patients(source)
    encounters = _build_encounters(source)
    conditions = _build_conditions(source)
    observations = _build_observations(source)
    medications = _build_medications(source)
    claims = _build_claims(source)

    return {
        "patients": patients,
        "encounters": encounters,
        "conditions": conditions,
        "observations": observations,
        "medications": medications,
        "claims": claims,
    }


def _build_encounter_dates(source: pd.DataFrame) -> pd.Series:
    today = date.today()
    dates: list[str] = []
    for patient_id, group in source.groupby("patient_nbr", sort=False):
        patient_offset = int(str(patient_id)) % 23
        total = len(group)
        for index in range(total):
            days_ago = 28 + patient_offset + (total - index - 1) * 75
            dates.append((today - timedelta(days=days_ago)).isoformat())
    return pd.Series(dates, index=source.index)


def _build_patients(source: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for patient_id, group in source.groupby("patient_nbr", sort=False):
        first = group.iloc[0]
        rows.append(
            {
                "patient_id": str(patient_id),
                "birth_date": _birth_date_from_age_bucket(first.get("age")),
                "gender": _clean_text(first.get("gender")) or "Unknown",
                "race": _clean_text(first.get("race")) or "Unknown",
                "ethnicity": None,
                "state": "Unknown",
                "zip": None,
            }
        )
    return pd.DataFrame(rows)


def _build_encounters(source: pd.DataFrame) -> pd.DataFrame:
    encounter_type_map = {
        "1": "emergency",
        "2": "urgent",
        "3": "elective",
        "4": "newborn",
        "5": "other",
        "6": "other",
        "7": "trauma",
        "8": "other",
    }
    rows = []
    for _, row in source.iterrows():
        payer = _clean_text(row.get("payer_code")) or "Unknown"
        provider = _clean_text(row.get("medical_specialty")) or "Unknown"
        estimated_cost = (
            float(row.get("time_in_hospital", 0) or 0) * 850
            + float(row.get("num_lab_procedures", 0) or 0) * 14
            + float(row.get("num_procedures", 0) or 0) * 120
            + float(row.get("num_medications", 0) or 0) * 9
        )
        encounter_type = encounter_type_map.get(str(row.get("admission_type_id")), "inpatient")
        rows.append(
            {
                "encounter_id": str(row["encounter_id"]),
                "patient_id": str(row["patient_nbr"]),
                "encounter_date": row["encounter_date"],
                "encounter_type": encounter_type,
                "provider_id": provider[:64],
                "payer": payer,
                "total_cost": round(estimated_cost, 2),
            }
        )
    return pd.DataFrame(rows)


def _build_conditions(source: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in source.iterrows():
        diagnosis_values = [row.get("diag_1"), row.get("diag_2"), row.get("diag_3")]
        for position, raw_code in enumerate(diagnosis_values, start=1):
            code = _clean_text(raw_code)
            if not code:
                continue
            mapped = _map_diag_to_icd10(code)
            rows.append(
                {
                    "condition_id": f"{row['encounter_id']}_DX{position}",
                    "patient_id": str(row["patient_nbr"]),
                    "encounter_id": str(row["encounter_id"]),
                    "icd10_code": mapped,
                    "icd10_description": _condition_description(mapped, code),
                    "onset_date": row["encounter_date"],
                    "resolution_date": None,
                }
            )
    return pd.DataFrame(rows)


def _build_observations(source: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in source.iterrows():
        patient_id = str(row["patient_nbr"])
        encounter_id = str(row["encounter_id"])
        encounter_date = row["encounter_date"]

        a1c_value = _map_a1c_result(row.get("A1Cresult"))
        if a1c_value is not None:
            rows.append(
                {
                    "observation_id": f"{encounter_id}_A1C",
                    "patient_id": patient_id,
                    "encounter_id": encounter_id,
                    "loinc_code": HBA1C_LOINC,
                    "loinc_description": "Hemoglobin A1c/Hemoglobin.total in Blood",
                    "value": a1c_value,
                    "unit": "%",
                    "observation_date": encounter_date,
                }
            )

        egfr_value = _derive_egfr_from_diags([row.get("diag_1"), row.get("diag_2"), row.get("diag_3")])
        if egfr_value is not None:
            rows.append(
                {
                    "observation_id": f"{encounter_id}_EGFR",
                    "patient_id": patient_id,
                    "encounter_id": encounter_id,
                    "loinc_code": EGFR_LOINC,
                    "loinc_description": "Glomerular filtration rate/1.73 sq M predicted",
                    "value": egfr_value,
                    "unit": "mL/min/1.73m2",
                    "observation_date": encounter_date,
                }
            )
            rows.append(
                {
                    "observation_id": f"{encounter_id}_CR",
                    "patient_id": patient_id,
                    "encounter_id": encounter_id,
                    "loinc_code": CREATININE_LOINC,
                    "loinc_description": "Creatinine [Mass/volume] in Serum or Plasma",
                    "value": round(max(0.6, 4.2 - (egfr_value / 25.0)), 2),
                    "unit": "mg/dL",
                    "observation_date": encounter_date,
                }
            )
    return pd.DataFrame(rows)


def _build_medications(source: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in source.iterrows():
        patient_id = str(row["patient_nbr"])
        encounter_id = str(row["encounter_id"])
        start_date = row["encounter_date"]
        stop_date = (pd.to_datetime(start_date) + pd.Timedelta(days=30)).date().isoformat()
        for med_col in MEDICATION_COLUMNS:
            status = _clean_text(row.get(med_col))
            if status in (None, "No"):
                continue
            rows.append(
                {
                    "medication_id": f"{encounter_id}_{med_col}",
                    "patient_id": patient_id,
                    "encounter_id": encounter_id,
                    "rxnorm_code": None,
                    "ndc_code": None,
                    "drug_name": med_col.replace("-", " "),
                    "start_date": start_date,
                    "stop_date": stop_date,
                    "dosage": status,
                }
            )
    return pd.DataFrame(rows)


def _build_claims(source: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in source.iterrows():
        encounter_id = str(row["encounter_id"])
        primary_diag = _map_diag_to_icd10(_clean_text(row.get("diag_1")) or "E11.9")
        billed = (
            float(row.get("time_in_hospital", 0) or 0) * 900
            + float(row.get("num_lab_procedures", 0) or 0) * 20
            + float(row.get("num_procedures", 0) or 0) * 180
        )
        paid = billed * 0.78
        rows.append(
            {
                "claim_id": f"CL_{encounter_id}",
                "patient_id": str(row["patient_nbr"]),
                "encounter_id": encounter_id,
                "claim_date": row["encounter_date"],
                "cpt_code": "99223",
                "icd10_primary": primary_diag,
                "payer": _clean_text(row.get("payer_code")) or "Unknown",
                "amount_billed": round(billed, 2),
                "amount_paid": round(paid, 2),
            }
        )
    return pd.DataFrame(rows)


def _birth_date_from_age_bucket(age_bucket: object) -> Optional[str]:
    value = _clean_text(age_bucket)
    if not value:
        return None
    digits = [int(part) for part in value.replace("[", "").replace(")", "").split(",") if part.strip().isdigit()]
    if len(digits) != 2:
        return None
    midpoint = int(round((digits[0] + digits[1]) / 2.0))
    return date.today().replace(month=1, day=1, year=max(1920, date.today().year - midpoint)).isoformat()


def _map_a1c_result(value: object) -> Optional[float]:
    mapping = {
        "Norm": 5.6,
        ">7": 7.6,
        ">8": 8.9,
    }
    return mapping.get(_clean_text(value))


def _derive_egfr_from_diags(diags: Iterable[object]) -> Optional[float]:
    stage_map = {
        "585": 45.0,
        "585.1": 92.0,
        "585.2": 72.0,
        "585.3": 48.0,
        "585.4": 24.0,
        "585.5": 14.0,
        "585.6": 9.0,
        "585.9": 42.0,
        "403": 48.0,
        "404": 28.0,
        "586": 35.0,
    }
    for diag in diags:
        cleaned = _clean_text(diag)
        if not cleaned:
            continue
        for prefix, egfr in stage_map.items():
            if cleaned.startswith(prefix):
                return egfr
    return None


def _map_diag_to_icd10(code: str) -> str:
    if code.startswith("250"):
        return "E11.9"
    if code.startswith("585.1"):
        return "N18.1"
    if code.startswith("585.2"):
        return "N18.2"
    if code.startswith("585.3"):
        return "N18.3"
    if code.startswith("585.4"):
        return "N18.4"
    if code.startswith("585.5"):
        return "N18.5"
    if code.startswith("585.6"):
        return "N18.6"
    if code.startswith("585") or code.startswith("586") or code.startswith("403") or code.startswith("404"):
        return "N18.9"
    if code.startswith("401"):
        return "I10"
    if code.startswith("414"):
        return "I25.10"
    if code.startswith("428"):
        return "I50.9"
    if code.startswith("272"):
        return "E78.5"
    if code.startswith("584"):
        return "N17.9"
    return code


def _condition_description(mapped_code: str, source_code: str) -> str:
    descriptions = {
        "E11.9": "Type 2 diabetes mellitus",
        "N18.1": "Chronic kidney disease, stage 1",
        "N18.2": "Chronic kidney disease, stage 2",
        "N18.3": "Chronic kidney disease, stage 3",
        "N18.4": "Chronic kidney disease, stage 4",
        "N18.5": "Chronic kidney disease, stage 5",
        "N18.6": "End stage renal disease",
        "N18.9": "Chronic kidney disease, unspecified",
        "I10": "Essential hypertension",
        "I25.10": "Atherosclerotic heart disease",
        "I50.9": "Heart failure, unspecified",
        "E78.5": "Hyperlipidemia, unspecified",
        "N17.9": "Acute kidney failure, unspecified",
    }
    return descriptions.get(mapped_code, f"Source diagnosis {source_code}")


def _clean_text(value: object) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text == "?":
        return None
    return text