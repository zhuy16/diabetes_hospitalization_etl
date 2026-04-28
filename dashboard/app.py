from __future__ import annotations

from pathlib import Path
import os
import sys

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from agent.text_to_sql import ask


def _repo_root() -> Path:
    return REPO_ROOT


def _connect() -> duckdb.DuckDBPyConnection:
    db_path = os.getenv("DUCKDB_PATH", str(_repo_root() / "db" / "clinical.duckdb"))
    return duckdb.connect(db_path)


st.set_page_config(page_title="ClinicalCohort AI", layout="wide")
st.title("ClinicalCohort AI - RWE Dashboard")

conn = _connect()

risk_options = [
    row[0]
    for row in conn.execute(
        "SELECT DISTINCT ckd_risk_level FROM ckd_risk WHERE ckd_risk_level IS NOT NULL ORDER BY 1"
    ).fetchall()
]
_NONE_LABEL = "None (no diabetes drug)"
drug_options = [_NONE_LABEL] + [
    row[0]
    for row in conn.execute(
        "SELECT DISTINCT sglt2_drug FROM rwe_cohort WHERE sglt2_drug IS NOT NULL ORDER BY 1"
    ).fetchall()
]

known_sglt2 = {"empagliflozin", "canagliflozin", "dapagliflozin"}
observed_drugs = [d.lower() for d in drug_options if d != _NONE_LABEL]
sglt2_share = (
    sum(1 for d in observed_drugs if d in known_sglt2) / len(observed_drugs)
    if observed_drugs
    else 0.0
)
is_diabetes130_mode = bool(observed_drugs) and sglt2_share < 0.5

st.sidebar.header("Filters")
selected_risk = st.sidebar.multiselect("CKD Risk", options=risk_options, default=risk_options)
selected_drug = st.sidebar.multiselect("Diabetes Drug", options=drug_options, default=drug_options)
min_hba1c = st.sidebar.slider("Minimum HbA1c", min_value=5.0, max_value=12.0, value=5.0, step=0.1)

risk_filter = "1=1"
if selected_risk:
    safe_risks = "', '".join(v.replace("'", "''") for v in selected_risk)
    risk_filter = f"coalesce(c.ckd_risk_level, 'UNKNOWN') IN ('{safe_risks}')"

# Build drug filter that correctly handles patients with no observed diabetes drug exposure.
drug_filter = "1=1"
if selected_drug:
    include_none = _NONE_LABEL in selected_drug
    named_drugs = [d for d in selected_drug if d != _NONE_LABEL]
    if named_drugs and include_none:
        safe_drugs = "', '".join(d.replace("'", "''") for d in named_drugs)
        drug_filter = f"(r.sglt2_drug IN ('{safe_drugs}') OR r.sglt2_drug IS NULL)"
    elif named_drugs:
        safe_drugs = "', '".join(d.replace("'", "''") for d in named_drugs)
        drug_filter = f"r.sglt2_drug IN ('{safe_drugs}')"
    elif include_none:
        drug_filter = "r.sglt2_drug IS NULL"
    else:
        drug_filter = "1=0"  # nothing selected → no results

metrics = conn.execute(
    f"""
    SELECT
      COUNT(DISTINCT r.patient_id) AS t2d_patients,
      COUNT(DISTINCT CASE WHEN r.sglt2_drug IS NOT NULL THEN r.patient_id END) AS on_sglt2,
      COUNT(DISTINCT CASE WHEN c.ckd_risk_level = 'HIGH' THEN r.patient_id END) AS high_ckd,
      ROUND(AVG(r.hba1c), 2) AS avg_hba1c
    FROM rwe_cohort r
    LEFT JOIN ckd_risk c ON r.patient_id = c.patient_id
    WHERE {risk_filter}
      AND {drug_filter}
      AND coalesce(r.hba1c, 0) >= {min_hba1c}
    """
).fetchdf()

c1, c2, c3, c4 = st.columns(4)
c1.metric("T2D Patients", int(metrics.loc[0, "t2d_patients"]))
c2.metric("On Diabetes Drug", int(metrics.loc[0, "on_sglt2"]))
c3.metric("High CKD Signal", int(metrics.loc[0, "high_ckd"]))
c4.metric("Avg HbA1c", float(metrics.loc[0, "avg_hba1c"] or 0.0))

explore_col, ask_col = st.columns([1.35, 1])

with explore_col:
    st.subheader("Population Exploration")
    st.caption("These charts reflect the cohort selected by the sidebar filters on the left.")

    if is_diabetes130_mode:
        st.markdown("**Average HbA1c by Primary Diabetes Drug**")
        hba1c_by_drug = conn.execute(
            f"""
            SELECT
                coalesce(r.sglt2_drug, 'No Diabetes Drug') AS primary_drug,
                ROUND(AVG(r.hba1c), 2) AS avg_hba1c,
                COUNT(DISTINCT r.patient_id) AS patients
            FROM rwe_cohort r
            LEFT JOIN ckd_risk c ON r.patient_id = c.patient_id
            WHERE {risk_filter}
              AND {drug_filter}
              AND coalesce(r.hba1c, 0) >= {min_hba1c}
            GROUP BY 1
            HAVING COUNT(*) > 0
            ORDER BY patients DESC
            LIMIT 12
            """
        ).fetchdf()
        if hba1c_by_drug.empty:
            st.info("No drug-level HbA1c rows found.")
        else:
            fig = px.bar(hba1c_by_drug, x="primary_drug", y="avg_hba1c", color="patients")
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("**HbA1c Trajectory**")
    a1c = conn.execute(
                f"""
                SELECT r.observation_date, AVG(r.hba1c) AS avg_hba1c
                FROM rwe_cohort r
                LEFT JOIN ckd_risk c ON r.patient_id = c.patient_id
                WHERE {risk_filter}
                    AND {drug_filter}
                    AND coalesce(r.hba1c, 0) >= {min_hba1c}
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchdf()
    if a1c.empty:
        st.info("No HbA1c data found.")
    else:
        fig = px.line(a1c, x="observation_date", y="avg_hba1c", markers=True)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("**CKD Risk Distribution**")
    risk = conn.execute(
                f"""
                SELECT c.ckd_risk_level, COUNT(DISTINCT r.patient_id) AS patients
                FROM rwe_cohort r
                LEFT JOIN ckd_risk c ON r.patient_id = c.patient_id
                WHERE {risk_filter}
                    AND {drug_filter}
                    AND coalesce(r.hba1c, 0) >= {min_hba1c}
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchdf()
    if risk.empty:
        st.info("No CKD risk rows found.")
    else:
        fig = px.pie(risk, names="ckd_risk_level", values="patients")
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("**Cohort Preview**")
    preview = conn.execute(
        f"""
                SELECT
                    patient_id,
                    sglt2_drug AS primary_drug,
                    hba1c AS latest_hba1c,
                    ckd_risk_level AS kidney_risk
        FROM rwe_cohort
        WHERE {drug_filter.replace('r.', '')}
          AND {risk_filter.replace('c.', '')}
          AND coalesce(hba1c, 0) >= {min_hba1c}
        ORDER BY patient_id
        LIMIT 30
        """
    ).fetchdf()
    st.dataframe(preview, use_container_width=True)

with ask_col:
    st.subheader("Ask the Cohort")
    st.caption("This panel generates SQL and refreshes the result table for each new question. It does not change the exploration charts on the left.")
    with st.expander("What data is available and what can I ask?", expanded=False):
        st.markdown(
            """
            **Available data views**
            - `rwe_cohort`: patient-level cohort with drug exposure, HbA1c, CKD risk fields
            - `t2d_patients`: Type 2 diabetes cohort (ICD-10 E11%)
            - `sglt2_exposure`: first observed diabetes medication exposure for each patient in the current dataset
            - `hba1c_trajectory`: HbA1c time series and month-over-month change
            - `ckd_risk`: eGFR-driven risk buckets (HIGH/MEDIUM/LOW)

            **Good question types**
            - Counts and rates by risk bucket or drug
            - Trend questions over time (HbA1c by month)
            - Cohort slicing (e.g., high-risk patients not on a diabetes drug)
            - Top-N summaries (e.g., most common CKD strata)
            """
        )

    question = st.text_area(
        "Ask a question",
        placeholder="Example: How many HIGH CKD risk patients are not on a diabetes drug?",
        height=100,
    )

    if st.button("Run Question", type="primary"):
        q = question.strip()
        if not q:
            st.warning("Please enter a question first.")
        else:
            try:
                generated_sql, result_df = ask(q)
                st.session_state["nlq_sql"] = generated_sql
                st.session_state["nlq_result"] = result_df
            except Exception as exc:
                st.error(f"Could not run question: {exc}")

    if "nlq_sql" in st.session_state and "nlq_result" in st.session_state:
        st.caption("Generated SQL")
        st.code(st.session_state["nlq_sql"], language="sql")
        st.caption("Question Result")
        st.dataframe(st.session_state["nlq_result"], use_container_width=True)
        st.caption("This result updates for each question you run.")
    else:
        st.info("Run a question to generate SQL and see the result table here.")

st.caption("Filters apply to metrics and charts. CKD risk comes from the ckd_risk view. Drug exposure reflects the first observed diabetes medication for each patient.")
st.info(
    "Clinical caveat: when using Diabetes 130, CKD risk and dates are mapped proxies derived from available columns. "
    "Interpret trend timing and kidney risk buckets as exploratory signals, not validated clinical labels."
)

conn.close()
