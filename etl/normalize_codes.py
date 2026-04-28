from __future__ import annotations

from typing import Optional

DIABETES_CODES_PREFIX = "E11"
CKD_CODES_PREFIX = "N18"

HBA1C_LOINC = "4548-4"
EGFR_LOINC = "33914-3"
CREATININE_LOINC = "2160-0"

SGLT2_RXNORM = {
    "2200644": "empagliflozin",
    "1545149": "canagliflozin",
    "1488574": "dapagliflozin",
}

SGLT2_NAME_TO_RXNORM = {v: k for k, v in SGLT2_RXNORM.items()}


def normalize_icd10(code: Optional[str]) -> Optional[str]:
    if code is None:
        return None
    cleaned = code.strip().upper()
    if not cleaned:
        return None
    return cleaned


def normalize_loinc(code: Optional[str]) -> Optional[str]:
    if code is None:
        return None
    cleaned = code.strip()
    return cleaned if cleaned else None


def normalize_rxnorm(rxnorm_code: Optional[str], drug_name: Optional[str]) -> Optional[str]:
    if rxnorm_code:
        return rxnorm_code.strip()
    if not drug_name:
        return None
    return SGLT2_NAME_TO_RXNORM.get(drug_name.lower().strip())


def condition_bucket(icd10_code: Optional[str]) -> str:
    if not icd10_code:
        return "unknown"
    if icd10_code.startswith(DIABETES_CODES_PREFIX):
        return "t2d"
    if icd10_code.startswith(CKD_CODES_PREFIX):
        return "ckd"
    return "other"
