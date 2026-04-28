from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def _obs_value(value, unit):
    return {
        "valueQuantity": {
            "value": float(value) if pd.notna(value) else None,
            "unit": unit or "",
            "system": "http://unitsofmeasure.org",
            "code": unit or "",
        }
    }


def build_sample_fhir(repo_root: Path, patient_limit: int = 2) -> None:
    raw_csv_dir = repo_root / "data" / "raw" / "synthea" / "csv"
    demo_csv_dir = repo_root / "data" / "processed" / "demo_csv"
    csv_dir = raw_csv_dir if (raw_csv_dir / "patients.csv").exists() else demo_csv_dir
    out_dir = repo_root / "data" / "raw" / "synthea" / "fhir"
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

        entries = []

        entries.append(
            {
                "resource": {
                    "resourceType": "Patient",
                    "id": str(patient_id),
                    "gender": "male" if str(p["gender"]).upper() == "M" else "female",
                    "birthDate": str(p["birth_date"]),
                    "address": [
                        {
                            "state": str(p["state"]),
                            "postalCode": str(p["zip"]),
                        }
                    ],
                    "extension": [
                        {
                            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                            "valueString": str(p["race"]),
                        },
                        {
                            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                            "valueString": str(p["ethnicity"]),
                        },
                    ],
                }
            }
        )

        for _, row in p_enc.iterrows():
            entries.append(
                {
                    "resource": {
                        "resourceType": "Encounter",
                        "id": str(row["encounter_id"]),
                        "subject": {"reference": f"Patient/{patient_id}"},
                        "class": {"code": str(row["encounter_type"])},
                        "period": {"start": str(row["encounter_date"])},
                    }
                }
            )

        for _, row in p_con.iterrows():
            entries.append(
                {
                    "resource": {
                        "resourceType": "Condition",
                        "id": str(row["condition_id"]),
                        "subject": {"reference": f"Patient/{patient_id}"},
                        "encounter": {"reference": f"Encounter/{row['encounter_id']}"},
                        "code": {
                            "coding": [
                                {
                                    "system": "http://hl7.org/fhir/sid/icd-10-cm",
                                    "code": str(row["icd10_code"]),
                                    "display": str(row["icd10_description"]),
                                }
                            ]
                        },
                        "onsetDateTime": str(row["onset_date"]),
                    }
                }
            )

        for _, row in p_obs.iterrows():
            entries.append(
                {
                    "resource": {
                        "resourceType": "Observation",
                        "id": str(row["observation_id"]),
                        "status": "final",
                        "subject": {"reference": f"Patient/{patient_id}"},
                        "encounter": {"reference": f"Encounter/{row['encounter_id']}"},
                        "code": {
                            "coding": [
                                {
                                    "system": "http://loinc.org",
                                    "code": str(row["loinc_code"]),
                                    "display": str(row["loinc_description"]),
                                }
                            ]
                        },
                        **_obs_value(row["value"], row["unit"]),
                        "effectiveDateTime": str(row["observation_date"]),
                    }
                }
            )

        for _, row in p_med.iterrows():
            entries.append(
                {
                    "resource": {
                        "resourceType": "MedicationRequest",
                        "id": str(row["medication_id"]),
                        "subject": {"reference": f"Patient/{patient_id}"},
                        "encounter": {"reference": f"Encounter/{row['encounter_id']}"},
                        "medicationCodeableConcept": {
                            "coding": [
                                {
                                    "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                                    "code": str(row["rxnorm_code"]),
                                    "display": str(row["drug_name"]),
                                }
                            ]
                        },
                        "authoredOn": str(row["start_date"]),
                    }
                }
            )

        for _, row in p_clm.iterrows():
            entries.append(
                {
                    "resource": {
                        "resourceType": "Claim",
                        "id": str(row["claim_id"]),
                        "patient": {"reference": f"Patient/{patient_id}"},
                        "created": str(row["claim_date"]),
                        "diagnosis": [
                            {
                                "diagnosisCodeableConcept": {
                                    "coding": [
                                        {
                                            "system": "http://hl7.org/fhir/sid/icd-10-cm",
                                            "code": str(row["icd10_primary"]),
                                        }
                                    ]
                                }
                            }
                        ],
                    }
                }
            )

        bundle = {
            "resourceType": "Bundle",
            "type": "collection",
            "entry": entries,
        }

        (out_dir / f"{patient_id}.bundle.json").write_text(
            json.dumps(bundle, indent=2), encoding="utf-8"
        )


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    build_sample_fhir(root)
    print("Generated sample FHIR bundles in data/raw/synthea/fhir")
