
<br />
# ClinicalCohort AI — Longitudinal Healthcare Analytics

![ClinicalCohort AI dashboard](docs/dashboard.png)


ClinicalCohort AI is a robust, extensible analytics platform for longitudinal clinical and observational healthcare data, centered around four core functionalities:

- **Patient trajectory visualization:** Explore how key metrics (e.g., lab values) change over time for individual patients or cohorts.
- **Feature value distribution:** Analyze the distribution of any numeric feature across different patient categories (e.g., drug exposure, risk strata).
- **Patient category distribution:** Instantly see how patients are distributed across categorical variables (e.g., risk levels, drug groups).
- **Natural language to SQL (NLQ-to-SQL) via LLM:** Define and filter cohorts or generate custom analytics by asking questions in plain English, powered by a large language model.

---

## Purpose

ClinicalCohort AI is designed for:
- Healthcare data scientists and analysts
- Clinical informatics teams
- Researchers working with EHR, claims, or observational datasets

It provides a reproducible, extensible framework for:
- Building and validating patient cohorts
- Running complex cohort-based analytics
- Visualizing trends, risk, and outcomes

While the platform implements many best practices for data quality, modular ETL, and flexible analytics, it is intended primarily for research, prototyping, and advanced analytics—not as a turnkey production system.

---

## How It Works

**Input:**
- Raw healthcare data (CSV, HL7 v2, FHIR bundles, or demo data)
- Canonical schema: patients, encounters, conditions, observations, medications, claims

**Processing:**
- ETL pipeline extracts, normalizes, and loads data into DuckDB
- Data quality checks and audit logging
- SQL views define cohorts, risk strata, and metrics
- Text-to-SQL agent (NLQ) translates natural language to safe SQL queries

**Output:**
- Interactive Streamlit dashboard for cohort exploration
- Statistical visualizations (trajectories, distributions, group comparisons)
- Data quality reports
- Exportable cohort tables and metrics

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the ETL pipeline with demo data
make run-synthetic

# 3. Launch the dashboard
streamlit run dashboard/app.py

# 4. (Optional) Ask questions via CLI
python -m agent.text_to_sql
```

---


## Core Features

- **Trajectory visualization:** Plot patient-level or cohort-level metric trends over time, with subgroup overlays and sampling controls.
- **Distribution analysis:** Box plots and statistical summaries of feature values across any categorical variable.
- **Category breakdowns:** Pie charts and counts for patient groups by risk, drug, or any other category.
- **NLQ-to-SQL agent:** Use natural language to define cohorts, filter data, or generate custom analytics—no SQL required.
- **Cohort filters:** Sidebar controls for risk, exposure, thresholds, and custom NLQ cohort restriction.
- **Dataset-agnostic:** Works with any data mapped to the canonical schema.
- **Robust ETL:** Handles ICD-10, LOINC, RxNorm; normalization and logging.
- **Data quality:** Built-in checks and reports.
- **Extensible:** Add new metrics, risk models, or cohort logic via SQL and Python.

---

## Limitations

- Not a substitute for validated clinical decision support
- CKD risk and trend timing are proxies, not validated clinical labels (see dashboard caveats)
- Requires data mapped to canonical schema for full functionality
- NLQ agent is SELECT-only and may not support all SQL constructs
- See [docs/PHASE3.md](docs/PHASE3.md) for production caveats and hardening notes

---

## How to Use Your Own Data

1. Format your data as CSV, HL7 v2, or FHIR bundles matching the canonical schema (see [docs/REPO_WALKTHROUGH.md](docs/REPO_WALKTHROUGH.md))
2. Place files in the appropriate `data/raw/` subfolder
3. Run the ETL pipeline (see Makefile targets or [DETAILED_README.md](docs/DETAILED_README.md))
4. Launch the dashboard or use the CLI agent

---

## Documentation & Specifications

- **[REPO_WALKTHROUGH.md](docs/REPO_WALKTHROUGH.md):** Folder map, entry points, data formats, how to plug in new datasets
- **[DEMO_RUNBOOK.md](docs/DEMO_RUNBOOK.md):** Step-by-step demo walkthrough
- **[PHASE3.md](docs/PHASE3.md):** Production hardening, HL7 parsing, metadata logging, testing
- **[DETAILED_README.md](docs/DETAILED_README.md):** Full technical details and advanced usage

---

## Example Use Cases

- Build and analyze diabetes or CKD cohorts from EHR/claims data
- Compare outcomes by drug exposure, risk strata, or custom cohort definitions
- Prototype new cohort logic or risk models for research

---

## Setup (First Time)

```bash
git clone https://github.com/zhuy16/ClinicalCohort-AI.git
cd ClinicalCohort-AI
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# Optional: add Anthropic API key for text-to-SQL agent
cp .env.example .env
# Edit .env and add your ANTHROPIC_API_KEY
make run-synthetic
make dashboard
```

---

## For More Details

See the [docs/](docs/) folder for full specifications, advanced configuration, and extension guides.

```bash
# Synthetic/demo source (default)
make run-synthetic

# Optional Diabetes130 source
make run-diabetes130

# Equivalent environment-variable form
ETL_SOURCE=synthetic .venv/bin/python -m etl.pipeline
ETL_SOURCE=diabetes130 .venv/bin/python -m etl.pipeline
```

- `ETL_SOURCE=synthetic` (default) uses generated demo data.
- `ETL_SOURCE=diabetes130` uses UCI Diabetes130 data mapped to the canonical schema.

---

## Scenarios

### Scenario 1: Synthetic Demo (0 setup)

```bash
make run-synthetic    # Auto-generates demo CSV data
make dashboard        # Interactive filters + charts
```

**What it shows**: Immediate gratification. Pipeline works. Dashboard is real. You can explore filtering by risk strata, exposure status, and threshold constraints, plus optional NLQ cohort restriction.

---

### Scenario 2: Your Own CSV Data

You have claims/EHR CSVs from Kaggle or a hospital extract. Want to ask the same questions.

**Steps**:
1. Shape your CSVs to match the canonical schema (6 tables: patients, encounters, conditions, observations, medications, claims)
2. Drop them into `data/raw/synthea/csv/`
3. Run: `make run-synthetic`
4. Ask away: `python -m agent.text_to_sql`

**Files to customize**:
- `etl/extract_synthea.py`: Add a CSV loader for your schema if column names differ
- `etl/normalize_codes.py`: Map your ICD/LOINC/RxNorm codes to the expected values
- `sql/views_t2d.sql`: Change the cohort logic from the default diabetes example to any ICD prefix/condition you care about

---

### Scenario 3: HL7 v2 or FHIR Data

You have raw HL7 segments or FHIR bundles. Want to ingest those instead of CSVs.

**HL7 path**:
```bash
# Ensure raw files exist in data/raw/synthea/hl7v2/
make run-hl7          # Parses HL7 → canonical tables → DuckDB
make dashboard
```

**FHIR path** (stub):
- Create `etl/load_fhir.py` that parses FHIR resources into the 6 canonical DataFrames
- Wire it into `etl/pipeline.py` the same way HL7 is wired

---

### Scenario 4: Different Clinical Question

Your stakeholder says: *"We care about readmission risk in diabetic CKD patients. Can you build that?"*

**Adapt in minutes**:
1. Edit `sql/views_t2d.sql` to filter your cohort definition (e.g., add comorbidity logic)
2. Edit `sql/views_risk.sql` to define your risk buckets (not just eGFR, maybe add hospitalization counts)
3. Add a new SQL view `sql/views_readmission.sql` with your metric
4. Run: `make run-synthetic`
5. Ask: `python -m agent.text_to_sql`  
   > *"What's the average readmission rate for patients in the HIGH risk bucket?"*

---

## Use This As

- **Portfolio piece**: "I built a text-to-SQL system for healthcare analytics"
- **Professional demo**: "Let me show you real ETL, SQL cohort logic, and a working LLM integration"
- **Template**: Copy this structure, swap your data, ask your questions
- **Learning**: Study canonical modeling, healthcare code systems, DuckDB + SQL views + Streamlit

---

## Key Skills on Display

| Skill | Where |
|---|---|
| ETL pipeline design | `etl/pipeline.py` + `etl/extract_synthea.py` |
| Healthcare code systems (ICD-10, LOINC, RxNorm) | `etl/normalize_codes.py` + views |
| SQL cohort logic, window functions, joins | `sql/views_*.sql` |
| Data quality discipline | `etl/data_quality.py` reports |
| LLM integration (Anthropic) | `agent/text_to_sql.py` |
| Analytics UI (Streamlit) | `dashboard/app.py` |
| DevOps (Makefile, CI/CD, tests) | `Makefile`, `.github/workflows/ci.yml` |

---

## Detailed Docs

- **[REPO_WALKTHROUGH.md](docs/REPO_WALKTHROUGH.md)** — Folder map, entry points, data formats, how to plug in new datasets
- **[DEMO_RUNBOOK.md](docs/DEMO_RUNBOOK.md)** — Script for walking a reviewer through the demo
- **[PHASE3.md](docs/PHASE3.md)** — Production hardening details (HL7 parsing robustness, metadata logging, testing)
- **[DETAILED_README.md](docs/DETAILED_README.md)** — Full project structure and technical details

---

## Quick Reference

| I want to… | Run this |
|---|---|
| Run synthetic/default pipeline | `make run` or `make run-synthetic` |
| Run Diabetes130 pipeline | `make run-diabetes130` or `ETL_SOURCE=diabetes130 .venv/bin/python -m etl.pipeline` |
| HL7 ingestion | `make run-hl7` or `python -m etl.pipeline_hl7v2` |
| Ask questions via CLI | `python -m agent.text_to_sql` |
| Ask questions in dashboard | `streamlit run dashboard/app.py` then use **Optional Custom Cohort (NLQ)** in the sidebar |
| Explore dashboard | `streamlit run dashboard/app.py` |
| Check data quality | `make dq` |
| Run tests | `make test` |
| See all phases | `make phase3` |

---

## Setup (First Time)

```bash
# Clone and enter repo
git clone https://github.com/zhuy16/ClinicalCohort-AI.git
cd ClinicalCohort-AI

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Optional: add Anthropic API key for text-to-SQL agent
cp .env.example .env
# Edit .env: add your ANTHROPIC_API_KEY

# Run the demo
make run-synthetic
make dashboard
```

---

## What's Next

- Integrate real data from your source (CSV, FHIR, HL7, MIMIC-IV)
- Extend the cohort definition or add new clinical questions
- Deploy the dashboard to Streamlit Cloud
- Add more sophisticated risk models or outcome metrics
