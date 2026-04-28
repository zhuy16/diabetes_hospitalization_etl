from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd


class MimicAdapterStub:
    """Placeholder for future MIMIC-IV integration.

    This keeps the architecture interview-ready by showing where MIMIC tables
    (admissions, diagnoses_icd, labevents, prescriptions) map into canonical ETL tables.
    """

    def __init__(self, mimic_root: Path) -> None:
        self.mimic_root = mimic_root

    def extract(self) -> Dict[str, pd.DataFrame]:
        raise NotImplementedError(
            "MIMIC-IV adapter is intentionally a stub for the MVP. "
            "Implement mapping from MIMIC core/hosp tables to canonical tables."
        )
