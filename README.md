# ClinicalCohort AI - Healthcare SQL ETL Demo

This project is a portfolio-ready healthcare ETL and analytics prototype focused on a canonical RWE question:

- Identify Type 2 Diabetes (ICD-10 E11%) patients
- Find SGLT2 treatment exposure (RxNorm concepts)
- Track HbA1c trajectory (LOINC 4548-4)
- Stratify CKD risk using eGFR (LOINC 33914-3) and CKD diagnoses (ICD-10 N18%)

## Why this project

This demonstrates healthcare data engineering depth with domain coding systems and practical SQL analytics.

## Architecture

Raw HL7/FHIR (or synthetic fallback)
  -> ETL extraction and normalization
  -> DuckDB canonical schema (patients/encounters/conditions/observations/medications/claims)
  -> SQL cohort views (t2d, exposure, labs, risk, final cohort)
  -> Streamlit dashboard + optional Anthropic text-to-SQL CLI

## Dashboard snapshot

The dashboard reads from the final DuckDB cohort view and lets you slice the synthetic population by CKD risk, SGLT2 exposure, and HbA1c threshold.

![ClinicalCohort AI dashboard](docs/dashboard.png)

## Project structure

- `data/raw/synthea/fhir/` sample raw FHIR bundles
- `data/raw/synthea/hl7v2/` sample raw HL7 v2 messages
- `data/processed/demo_csv/` synthetic tabular source used for current MVP ETL loader
- `db/clinical.duckdb` local analytical database
- `etl/` extraction, normalization, loading, and pipeline entrypoint
- `sql/` schema and analytic views
- `agent/` text-to-SQL prompt and CLI
- `dashboard/app.py` Streamlit app
- `tests/test_sql_views.py` smoke checks
- `scripts/run_pipeline.sh` one-command pipeline + validation
- `scripts/run_phase2.sh` Phase II workflow (HL7 -> DuckDB -> DQ -> tests)
- `scripts/run_phase3.sh` Phase III workflow (robustness test + phase2 + metadata)
- `etl/data_quality.py` data-quality checks and report generator
- `etl/run_metadata.py` ETL run log and table-row-count capture
- `docs/INTERVIEW_DEMO.md` recruiter-facing demo flow
- `docs/PHASE3.md` production-style polish summary

## Quick start

1. Create and activate a virtual environment.
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Optional: add `.env` from `.env.example` for Anthropic agent.
4. Run pipeline and checks:
   - `bash scripts/run_pipeline.sh`
5. Launch dashboard:
   - `streamlit run dashboard/app.py`

HL7 v2 ingestion path:

1. Ensure raw HL7 files exist in `data/raw/synthea/hl7v2/`.
2. Parse HL7 and load DuckDB via:
   - `python -m etl.pipeline_hl7v2`
3. Parsed tabular outputs are written to:
   - `data/processed/from_hl7v2/`

Phase II full run:

- `bash scripts/run_phase2.sh`
- This generates/refreshes:
   - `data/processed/from_hl7v2/*.csv`
   - `data/processed/from_hl7v2/parse_summary.csv`
   - `data/processed/reports/dq_report.md`
   - `data/processed/reports/dq_report.json`

Phase III full run:

- `bash scripts/run_phase3.sh`
- Adds pipeline observability tables in DuckDB:
   - `etl_run_log`
   - `etl_table_row_counts`
- Includes parser robustness testing:
   - `tests/test_parse_hl7v2_parser.py`

## Synthea input

For this MVP, normalized tabular inputs are stored in `data/processed/demo_csv/`.

If you have Synthea CSV exports, place them in `data/processed/demo_csv/` with names:

- `patients.csv`
- `encounters.csv`
- `conditions.csv`
- `observations.csv`
- `medications.csv`
- `claims.csv` (optional)

If files are missing, ETL auto-generates deterministic demo data so the project remains runnable.
Generated fallback CSVs are saved to `data/processed/demo_csv/` for inspection and ad hoc analysis.

FHIR note:

- Synthea can export both CSV and FHIR. This MVP ETL loads CSV for speed.
- Sample raw FHIR bundles are available in `data/raw/synthea/fhir/` (generated from demo data) so you can inspect HL7 FHIR resource structure.
- To regenerate those examples, run: `python -m etl.generate_sample_fhir`

HL7 v2 note:

- Sample raw HL7 v2 messages are available in `data/raw/synthea/hl7v2/`.
- These files include common segments used in ETL parsing demos: `MSH`, `PID`, `PV1`, `DG1`, `OBX`, `RXE`, `FT1`.
- To regenerate those examples, run: `python -m etl.generate_sample_hl7v2`
- To parse HL7 files into processed tabular outputs only, run: `python -m etl.parse_hl7v2`

## Example SQL questions

- How many T2D patients are on SGLT2 therapy?
- Show average HbA1c by month in the last 12 months.
- List high CKD risk patients without SGLT2 exposure.

## Agent usage

- `python -m agent.text_to_sql`
- Ask a question in natural language.
- The tool generates read-only SQL and executes it on DuckDB.

## What this demonstrates for hiring

- Healthcare code systems: ICD-10, LOINC, RxNorm
- ETL design and canonical modeling
- SQL depth: cohort logic, joins, and window functions
- Lightweight AI-assisted analytics workflow
- Runnable local prototype for interviews
- Data quality discipline (explicit PASS/WARN/FAIL checks)
- HL7 segment parsing with deterministic canonical output schemas
- ETL run observability and auditability (per-run metadata + row counts)
