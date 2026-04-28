# ClinicalCohort AI — Healthcare Data to Insights

Ask questions about patient data in plain English. Get SQL queries, results, and interactive dashboards.

![ClinicalCohort AI dashboard](docs/dashboard.png)

---

## 30-Second Demo

```bash
# Setup (first time only)
pip install -r requirements.txt
make run

# Ask a question
python -m agent.text_to_sql
> How many patients with high CKD risk are on SGLT2 treatment?

# Or explore visually
streamlit run dashboard/app.py
```

---

## What This Shows

✅ **Real healthcare ETL**: ICD-10, LOINC, RxNorm code systems. Canonical schema. DuckDB analytical database.  
✅ **SQL + AI**: Text-to-SQL agent using Claude. SELECT-only safety enforcement. Query against live cohort views.  
✅ **End-to-end pipeline**: Raw data → Extract/normalize → Load tables → Build SQL views → Query/visualize.  
✅ **Production polish**: Data quality checks. ETL audit logging. Comprehensive test suite. CI/CD ready.  

---

## Scenarios

### Scenario 1: Synthetic Demo (0 setup)

```bash
make run              # Auto-generates demo CSV data
make dashboard        # Interactive filters + charts
```

**What it shows**: Immediate gratification. Pipeline works. Dashboard is real. You can explore filtering by CKD risk, SGLT2 drug, HbA1c thresholds.

---

### Scenario 2: Your Own CSV Data

You have claims/EHR CSVs from Kaggle or a hospital extract. Want to ask the same questions.

**Steps**:
1. Shape your CSVs to match the canonical schema (6 tables: patients, encounters, conditions, observations, medications, claims)
2. Drop them into `data/raw/synthea/csv/`
3. Run: `make run`
4. Ask away: `python -m agent.text_to_sql`

**Files to customize**:
- `etl/extract_synthea.py`: Add a CSV loader for your schema if column names differ
- `etl/normalize_codes.py`: Map your ICD/LOINC/RxNorm codes to the expected values
- `sql/views_t2d.sql`: Change the cohort from "T2D (E11%)" to any ICD prefix you care about

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
4. Run: `make run`
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
| Run full pipeline | `make run` or `bash scripts/run_pipeline.sh` |
| HL7 ingestion | `make run-hl7` or `python -m etl.pipeline_hl7v2` |
| Ask questions via CLI | `python -m agent.text_to_sql` |
| Ask questions in dashboard | `streamlit run dashboard/app.py` then use **Ask the Cohort (Natural Language)** |
| Explore dashboard | `streamlit run dashboard/app.py` |
| Check data quality | `make dq` |
| Run tests | `make test` |
| See all phases | `make phase3` |

---

## Setup (First Time)

```bash
# Clone and enter repo
git clone https://github.com/zhuy16/diabetes_hospitalization_etl.git
cd diabetes_hospitalization_etl

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Optional: add Anthropic API key for text-to-SQL agent
cp .env.example .env
# Edit .env: add your ANTHROPIC_API_KEY

# Run the demo
make run
make dashboard
```

---

## What's Next

- Integrate real data from your source (CSV, FHIR, HL7, MIMIC-IV)
- Extend the cohort definition or add new clinical questions
- Deploy the dashboard to Streamlit Cloud
- Add more sophisticated risk models or outcome metrics
