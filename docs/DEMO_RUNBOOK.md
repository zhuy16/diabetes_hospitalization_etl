# Demo Runbook

## Goal

Show an end-to-end healthcare ETL + SQL analytics + AI query + interactive cohort dashboard workflow in under 10 minutes.

## Demo flow

1. Raw healthcare message inputs
- Open `data/raw/synthea/hl7v2/P00001.hl7`
- Explain key segments: MSH, PID, PV1, DG1, OBX, RXE, FT1.

2. Transform into canonical tables
- Run: `bash scripts/run_phase2.sh` (uses synthetic/default source path)
- Show parsed outputs: `data/processed/from_hl7v2/*.csv`
- Show DQ outputs: `data/processed/reports/dq_report.md`

3. SQL cohort modeling
- Open `sql/views_t2d.sql`, `sql/views_labs.sql`, and `sql/views_risk.sql`
- Highlight ICD-10, LOINC, RxNorm logic and window functions.

4. Interactive analytics dashboard
- Run: `streamlit run dashboard/app.py`
- Left sidebar defines cohort:
  - CKD Risk filter (multi-select)
  - Diabetes Drug filter (multi-select)
  - Minimum HbA1c threshold slider
  - Optional: Natural language cohort restriction (e.g., "high-risk patients not on SGLT2")
- Left panel: Trajectory visualization (select Y-axis from any numeric field) + Distribution pie chart (select category from any categorical field)
- Right panel: Cohort Distribution insight boxplot (select value and category)
- All panels apply the same cohort restriction.

5. AI-assisted natural language query (optional)
- In sidebar "Optional Custom Cohort (NLQ)" section, ask a question that returns `patient_id`:
  - Example: "How many diabetic patients are high CKD risk without SGLT2 exposure?"
  - The LLM converts it to a SQL query against `rwe_cohort`, `ckd_risk`, etc.
  - Click "Apply Cohort" to restrict all visualizations to that subset.

Optional dataset switch for the same demo structure:
- Synthetic/default: `make run-synthetic`
- Diabetes130: `make run-diabetes130`

## Talking points for reviewers

- Canonical healthcare coding systems used directly in ETL and analytics.
- Message-level HL7 ingestion with structured SQL warehouse outputs.
- Data quality reporting with explicit PASS/WARN/FAIL checks.
- Reproducible local run scripts and modular pipeline components.
- Cohort-first interactive dashboard: sidebar defines cohort, all panels enforce it. Dataset-agnostic axis selection supports exploration of any data source using the same canonical schema.
- Natural language cohort restriction: write SQL without knowing SQL. Safety constraints via SELECT-only enforcement.
- Clinical interpretation caveat: synthetic HbA1c can appear noisy (rapid 7–11 swings), but real HbA1c usually changes gradually over months because it reflects an approximately 3-month glycemic average.
