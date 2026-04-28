# Full Technical Reference

This is the detailed reference documentation. For a quick overview, see [README.md](../README.md).

---

## Project Goals

This project is a portfolio-ready healthcare ETL and analytics prototype focused on a canonical RWE question:

- Identify Type 2 Diabetes (ICD-10 E11%) patients
- Find SGLT2 treatment exposure (RxNorm concepts)
- Track HbA1c trajectory (LOINC 4548-4)
- Stratify CKD risk using eGFR (LOINC 33914-3) and CKD diagnoses (ICD-10 N18%)

Demonstrates healthcare data engineering depth with domain coding systems and practical SQL analytics.

---

## Architecture

```
Raw source (synthetic/demo, Diabetes130, CSV, HL7/FHIR)
  → ETL extraction and normalization
  → DuckDB canonical schema (patients/encounters/conditions/observations/medications/claims)
  → SQL cohort views (t2d, exposure, labs, risk, final cohort)
  → Streamlit dashboard + optional Anthropic text-to-SQL CLI
```

---

## Project Structure

### Data Layer

- `data/raw/synthea/fhir/` — Sample raw FHIR bundles
- `data/raw/synthea/hl7v2/` — Sample raw HL7 v2 messages
- `data/raw/synthea/csv/` — Drop real CSVs here
- `data/raw/diabetes130/` — Optional UCI Diabetes130 source
- `data/processed/demo_csv/` — Synthetic tabular source for ETL loader
- `data/processed/from_hl7v2/` — Tables parsed from HL7
- `data/processed/reports/` — DQ reports
- `db/clinical.duckdb` — Local analytical database

### ETL Layer

- `etl/pipeline.py` — Main entry point (CSV path)
- `etl/pipeline_hl7v2.py` — HL7 v2 path
- `etl/extract_synthea.py` — Load raw CSVs or generate demo data
- `etl/load_duckdb.py` — Insert DataFrames into DuckDB + run SQL views
- `etl/parse_hl7v2.py` — Parse HL7 v2 segments into DataFrames
- `etl/data_quality.py` — DQ checks (PASS/WARN/FAIL)
- `etl/run_metadata.py` — ETL audit logging
- `etl/normalize_codes.py` — Code constants (ICD-10, LOINC, RxNorm)
- `etl/generate_sample_fhir.py` — Generate sample FHIR bundles
- `etl/generate_sample_hl7v2.py` — Generate sample HL7 messages
- `etl/mimic_adapter_stub.py` — Stub interface for MIMIC-IV

### SQL Layer

- `sql/schema.sql` — CREATE TABLE statements (6 canonical tables)
- `sql/views_t2d.sql` — T2D patients (ICD E11.*)
- `sql/views_exposure.sql` — SGLT2 exposure
- `sql/views_labs.sql` — HbA1c trajectory with window functions
- `sql/views_risk.sql` — CKD risk (eGFR-based)
- `sql/views_final_cohort.sql` — RWE cohort (final join)

### Application Layer

- `dashboard/app.py` — Streamlit dashboard with cohort-first UX:
  - **Left sidebar**: Multi-select CKD Risk, Diabetes Drug filters; HbA1c threshold slider; optional NLQ-based cohort restriction (sidebar queries `rwe_cohort`, `ckd_risk` tables via Claude text-to-SQL)
  - **Left panel**: Trajectory chart (selectable Y-axis across all numeric columns) + distribution pie (selectable categorical dimension)
  - **Right panel**: Value-vs-category boxplot (selectable value and category; statistical group comparison)
  - **Dataset-agnostic**: Same visualization code and selectbox logic works for any data source (synthetic, Diabetes130, or custom CSV) mapped to canonical schema
  - **Cohort-uniform**: All charts and tables enforce the same sidebar-defined cohort filter; custom NLQ cohort acts as additional patient_id restriction
- `agent/text_to_sql.py` — CLI text-to-SQL agent (Anthropic)
- `agent/prompt_template.txt` — System prompt

### Testing & DevOps

- `tests/test_sql_views.py` — SQL smoke tests
- `tests/test_hl7_pipeline.py` — HL7 pipeline test
- `tests/test_parse_hl7v2_parser.py` — HL7 parser robustness test
- `.github/workflows/ci.yml` — GitHub Actions CI
- `Makefile` — Build and run targets
- `scripts/run_pipeline.sh` — One-command pipeline
- `scripts/run_phase2.sh` — Phase II (HL7 → DQ)
- `scripts/run_phase3.sh` — Phase III (audit logging + tests)

---

## Data Formats

### Canonical Tables

Six tables defined in `sql/schema.sql`. Every pipeline (CSV, HL7, FHIR, MIMIC) must produce DataFrames matching these columns exactly.

| Table | Key columns | Code system |
|---|---|---|
| `patients` | patient_id, birth_date, gender, race | — |
| `encounters` | encounter_id, patient_id, encounter_date, encounter_type | — |
| `conditions` | patient_id, icd10_code, onset_date | **ICD-10-CM** |
| `observations` | patient_id, loinc_code, value, unit, observation_date | **LOINC** |
| `medications` | patient_id, rxnorm_code, drug_name, start_date | **RxNorm** |
| `claims` | patient_id, cpt_code, icd10_primary, amount_billed | **CPT / ICD-10** |

### CSV Source

For CSV-based ETL:
1. Name files: `patients.csv`, `encounters.csv`, `conditions.csv`, `observations.csv`, `medications.csv`, `claims.csv`
2. Place in: `data/raw/synthea/csv/`
3. Run with synthetic/default source selection (`make run-synthetic`)

### Dataset Selection

Source is explicit via `ETL_SOURCE` to keep analytics semantics clear:

```bash
# Default behavior (synthetic/demo or raw CSV in data/raw/synthea/csv)
make run-synthetic

# Optional Diabetes130 mapping path
make run-diabetes130

# Equivalent direct command
ETL_SOURCE=synthetic .venv/bin/python -m etl.pipeline
ETL_SOURCE=diabetes130 .venv/bin/python -m etl.pipeline
```

### HL7 v2

- Pipe-delimited segment files
- Common segments: `MSH`, `PID`, `PV1`, `DG1`, `OBX`, `RXE`, `FT1`
- Parsed by `etl/parse_hl7v2.py`
- Output: canonical tables in `data/processed/from_hl7v2/`

### FHIR Bundles

- Sample bundles in `data/raw/synthea/fhir/`
- Currently read-only examples
- To integrate: create `etl/load_fhir.py` that parses Bundle resources into canonical DataFrames

---

## Workflows

### Phase 1: CSV Pipeline

```bash
bash scripts/run_pipeline.sh
# or explicitly
bash scripts/run_pipeline.sh synthetic
```

- Loads/generates CSV data
- Uses `ETL_SOURCE` to choose source (`synthetic` default, `diabetes130` optional)
- Builds DuckDB tables
- Creates SQL views
- Auto-generates demo data if needed

### Phase 2: Data Quality

```bash
bash scripts/run_phase2.sh
```

- Runs Phase 1
- Executes DQ checks
- Generates `dq_report.md` and `dq_report.json`
- Parses HL7 files (if present) and generates parse summary

### Phase 3: Production Hardening

```bash
bash scripts/run_phase3.sh
```

- Runs Phase 2
- Logs ETL metadata into DuckDB (`etl_run_log`, `etl_table_row_counts`)
- Runs parser robustness tests
- CI/CD ready

---

## Example Queries

### Agent (Natural Language)

```bash
python -m agent.text_to_sql
> How many T2D patients are on SGLT2 therapy?
> Show average HbA1c by month in the last 12 months.
> List high CKD risk patients without SGLT2 exposure.
```

### Direct SQL

```sql
SELECT COUNT(DISTINCT patient_id) FROM t2d_patients;

SELECT 
  observation_date, 
  AVG(hba1c) 
FROM hba1c_trajectory 
GROUP BY 1 
ORDER BY 1;

SELECT * FROM rwe_cohort 
WHERE ckd_risk_level = 'HIGH' 
  AND sglt2_drug IS NULL;
```

---

## Customization

| Goal | File |
|---|---|
| Change cohort definition | `sql/views_t2d.sql` — change `E11%` to any ICD prefix |
| Add new drug class | `etl/normalize_codes.py` + `sql/views_exposure.sql` |
| Add new lab outcome | `sql/views_labs.sql` |
| Change CKD thresholds | `sql/views_risk.sql` — CASE WHEN values |
| Add dashboard panels | `dashboard/app.py` — add `st.subheader` + `conn.execute` blocks |
| Change agent behavior | `agent/prompt_template.txt` + `agent/text_to_sql.py` |
| Add code mappings | `etl/normalize_codes.py` |

---

## Caveats vs Real Data

| Aspect | This demo | Real data |
|---|---|---|
| **Patient source** | Synthea synthetic | EHR export, claims, MIMIC |
| **HbA1c** | Gradual model, ~0.2 change/step | 2–3 month average, slow changes |
| **Drug exposure** | Binary (on/off) | Includes dose, gaps, switches |
| **ICD codes** | Clean, well-formed | ICD-9, malformed, missing |
| **LOINC codes** | Always `4548-4` | Free text, local codes, mapping tables |
| **RxNorm** | SGLT2 only | Full drug lookup required |
| **eGFR/CKD** | Min value, count | Time series, GFR staging, CKD-EPI |
| **Claims** | Synthetic amounts | Contractual adjustments, DRG |
| **Time window** | Last 12 months (hard-coded) | Parameterized study windows |
| **Missing data** | Very low (<5%) | 20–40% on labs |
| **De-identification** | N/A (synthetic) | Safe Harbor / Expert Determination |

---

## Testing

```bash
make test               # Run pytest suite
make phase3             # Include robustness test
```

Includes:
- SQL view smoke tests
- HL7 pipeline integration test
- HL7 parser robustness (malformed segments, unknowns)

---

## What This Demonstrates

- **Healthcare coding systems**: ICD-10, LOINC, RxNorm normalization
- **ETL design**: Canonical modeling, schema independence
- **SQL depth**: Cohort logic, joins, window functions
- **Data quality**: PASS/WARN/FAIL discipline
- **LLM integration**: Text-to-SQL with safety constraints
- **Analytics UI**: Streamlit filters, charts, tables
- **DevOps**: Makefile, CI/CD, versioned metadata
- **Parsing**: HL7 v2 segment extraction and error handling
