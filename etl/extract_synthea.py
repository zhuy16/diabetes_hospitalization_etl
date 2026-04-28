from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import random
from typing import Dict

import pandas as pd

from etl.normalize_codes import CREATININE_LOINC, EGFR_LOINC, HBA1C_LOINC, SGLT2_RXNORM


@dataclass
class ExtractConfig:
    raw_csv_dir: Path
    processed_demo_dir: Path
    seed: int = 7
    patient_count: int = 300


def _read_if_exists(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def load_synthea_or_demo(config: ExtractConfig) -> Dict[str, pd.DataFrame]:
    patients = _read_if_exists(config.raw_csv_dir / "patients.csv")
    encounters = _read_if_exists(config.raw_csv_dir / "encounters.csv")
    conditions = _read_if_exists(config.raw_csv_dir / "conditions.csv")
    observations = _read_if_exists(config.raw_csv_dir / "observations.csv")
    medications = _read_if_exists(config.raw_csv_dir / "medications.csv")
    claims = _read_if_exists(config.raw_csv_dir / "claims.csv")

    if all(not df.empty for df in [patients, encounters, conditions, observations, medications]):
        return {
            "patients": patients,
            "encounters": encounters,
            "conditions": conditions,
            "observations": observations,
            "medications": medications,
            "claims": claims,
        }

    demo = _generate_demo_dataset(config)
    _persist_demo_dataset(config.processed_demo_dir, demo)
    return demo


def _persist_demo_dataset(output_dir: Path, dataset: Dict[str, pd.DataFrame]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, df in dataset.items():
        df.to_csv(output_dir / f"{name}.csv", index=False)


def _generate_demo_dataset(config: ExtractConfig) -> Dict[str, pd.DataFrame]:
    random.seed(config.seed)

    today = date.today()
    states = ["MA", "CA", "TX", "NY"]
    races = ["White", "Black", "Asian", "Other"]
    genders = ["M", "F"]

    patients_rows = []
    encounters_rows = []
    conditions_rows = []
    observations_rows = []
    medications_rows = []
    claims_rows = []

    sglt2_codes = list(SGLT2_RXNORM.keys())

    for i in range(1, config.patient_count + 1):
        pid = f"P{i:05d}"
        birth_year = random.randint(1945, 2005)
        birth_date = date(birth_year, random.randint(1, 12), random.randint(1, 28))
        patients_rows.append(
            {
                "patient_id": pid,
                "birth_date": birth_date.isoformat(),
                "gender": random.choice(genders),
                "race": random.choice(races),
                "ethnicity": "Not Hispanic or Latino",
                "state": random.choice(states),
                "zip": str(random.randint(10000, 99999)),
            }
        )

        encounter_count = random.randint(2, 7)
        has_t2d = random.random() < 0.58
        has_ckd = has_t2d and random.random() < 0.28
        on_sglt2 = has_t2d and random.random() < 0.42

        # Create chronologically ordered encounters so longitudinal labs look realistic.
        encounter_days_ago = sorted(random.sample(range(10, 541), k=encounter_count), reverse=True)

        # HbA1c is a rolling 2-3 month average and should evolve gradually.
        hba1c_current = None
        monthly_hba1c_drift = 0.0
        if has_t2d:
            hba1c_current = random.uniform(6.8, 10.2)
            if on_sglt2:
                monthly_hba1c_drift = random.uniform(-0.28, -0.06)
            else:
                monthly_hba1c_drift = random.uniform(-0.05, 0.12)

        previous_enc_date = None

        for e, days_ago in enumerate(encounter_days_ago):
            eid = f"E{i:05d}_{e:02d}"
            enc_date = today - timedelta(days=days_ago)
            encounters_rows.append(
                {
                    "encounter_id": eid,
                    "patient_id": pid,
                    "encounter_date": enc_date.isoformat(),
                    "encounter_type": random.choice(["outpatient", "inpatient", "ed"]),
                    "provider_id": f"PR{random.randint(1, 50):03d}",
                    "payer": random.choice(["Medicare", "Medicaid", "Commercial"]),
                    "total_cost": round(random.uniform(90, 12000), 2),
                }
            )

            if has_t2d and e == 0:
                conditions_rows.append(
                    {
                        "condition_id": f"C{i:05d}_T2D",
                        "patient_id": pid,
                        "encounter_id": eid,
                        "icd10_code": random.choice(["E11.9", "E11.65", "E11.22"]),
                        "icd10_description": "Type 2 diabetes mellitus",
                        "onset_date": (enc_date - timedelta(days=300)).isoformat(),
                        "resolution_date": None,
                    }
                )

            if has_ckd and e in (0, 1):
                conditions_rows.append(
                    {
                        "condition_id": f"C{i:05d}_CKD_{e}",
                        "patient_id": pid,
                        "encounter_id": eid,
                        "icd10_code": random.choice(["N18.2", "N18.3", "N18.4"]),
                        "icd10_description": "Chronic kidney disease",
                        "onset_date": (enc_date - timedelta(days=160)).isoformat(),
                        "resolution_date": None,
                    }
                )

            # HbA1c
            if has_t2d:
                if previous_enc_date is None:
                    days_since_prev = 90
                else:
                    days_since_prev = max(14, (enc_date - previous_enc_date).days)

                # Progress slowly: drift by elapsed months + mild measurement/process noise.
                elapsed_months = days_since_prev / 30.0
                noise = random.uniform(-0.12, 0.12)
                hba1c_current = hba1c_current + (monthly_hba1c_drift * elapsed_months) + noise
                hba1c_current = min(12.0, max(5.8, hba1c_current))
                hba1c_value = round(hba1c_current, 1)
                observations_rows.append(
                    {
                        "observation_id": f"O{i:05d}_{e:02d}_A1C",
                        "patient_id": pid,
                        "encounter_id": eid,
                        "loinc_code": HBA1C_LOINC,
                        "loinc_description": "Hemoglobin A1c/Hemoglobin.total in Blood",
                        "value": hba1c_value,
                        "unit": "%",
                        "observation_date": enc_date.isoformat(),
                    }
                )

            # eGFR + creatinine
            egfr_value = round(random.uniform(25, 110), 1)
            creatinine_value = round(random.uniform(0.5, 3.1), 2)
            observations_rows.extend(
                [
                    {
                        "observation_id": f"O{i:05d}_{e:02d}_EGFR",
                        "patient_id": pid,
                        "encounter_id": eid,
                        "loinc_code": EGFR_LOINC,
                        "loinc_description": "Glomerular filtration rate/1.73 sq M predicted",
                        "value": egfr_value,
                        "unit": "mL/min/1.73m2",
                        "observation_date": enc_date.isoformat(),
                    },
                    {
                        "observation_id": f"O{i:05d}_{e:02d}_CR",
                        "patient_id": pid,
                        "encounter_id": eid,
                        "loinc_code": CREATININE_LOINC,
                        "loinc_description": "Creatinine [Mass/volume] in Serum or Plasma",
                        "value": creatinine_value,
                        "unit": "mg/dL",
                        "observation_date": enc_date.isoformat(),
                    },
                ]
            )

            claims_rows.append(
                {
                    "claim_id": f"CL{i:05d}_{e:02d}",
                    "patient_id": pid,
                    "encounter_id": eid,
                    "claim_date": enc_date.isoformat(),
                    "cpt_code": random.choice(["99213", "99214", "83036", "80053"]),
                    "icd10_primary": "E11.9" if has_t2d else random.choice(["I10", "E78.5", "J06.9"]),
                    "payer": random.choice(["Medicare", "Medicaid", "Commercial"]),
                    "amount_billed": round(random.uniform(110, 2800), 2),
                    "amount_paid": round(random.uniform(80, 2300), 2),
                }
            )

            previous_enc_date = enc_date

        if on_sglt2:
            first_encounter = f"E{i:05d}_00"
            start_date = today - timedelta(days=random.randint(60, 400))
            rxnorm = random.choice(sglt2_codes)
            medications_rows.append(
                {
                    "medication_id": f"M{i:05d}_SGLT2",
                    "patient_id": pid,
                    "encounter_id": first_encounter,
                    "rxnorm_code": rxnorm,
                    "ndc_code": None,
                    "drug_name": SGLT2_RXNORM[rxnorm],
                    "start_date": start_date.isoformat(),
                    "stop_date": (start_date + timedelta(days=90)).isoformat(),
                    "dosage": random.choice(["10 mg", "25 mg", "100 mg"]),
                }
            )

    return {
        "patients": pd.DataFrame(patients_rows),
        "encounters": pd.DataFrame(encounters_rows),
        "conditions": pd.DataFrame(conditions_rows),
        "observations": pd.DataFrame(observations_rows),
        "medications": pd.DataFrame(medications_rows),
        "claims": pd.DataFrame(claims_rows),
    }
