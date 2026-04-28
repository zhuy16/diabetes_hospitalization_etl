# Demo Runbook (Phase II)

## Goal

Show an end-to-end healthcare ETL + SQL analytics + AI query workflow in under 8 minutes.

## Demo flow

1. Raw healthcare message inputs
- Open `data/raw/synthea/hl7v2/P00001.hl7`
- Explain key segments: MSH, PID, PV1, DG1, OBX, RXE, FT1.

2. Transform into canonical tables
- Run: `bash scripts/run_phase2.sh`
- Show parsed outputs: `data/processed/from_hl7v2/*.csv`
- Show DQ outputs: `data/processed/reports/dq_report.md`

3. SQL cohort modeling
- Open `sql/views_t2d.sql`, `sql/views_labs.sql`, and `sql/views_risk.sql`
- Highlight ICD-10, LOINC, RxNorm logic and window functions.

4. Analytics consumption
- Run dashboard: `streamlit run dashboard/app.py`
- Show filters and risk distribution.

5. AI-assisted analysis in dashboard
- Run dashboard: `streamlit run dashboard/app.py`
- In **Ask the Cohort (Natural Language)**, ask:
  "How many diabetic patients are high CKD risk without SGLT2 exposure?"

## Talking points for reviewers

- Canonical healthcare coding systems used directly in ETL and analytics.
- Message-level HL7 ingestion with structured SQL warehouse outputs.
- Data quality reporting with explicit PASS/WARN/FAIL checks.
- Reproducible local run scripts and modular pipeline components.
- Clinical interpretation caveat: synthetic HbA1c can appear noisy (rapid 7-11 swings), but real HbA1c usually changes gradually over months because it reflects an approximately 3-month glycemic average.
