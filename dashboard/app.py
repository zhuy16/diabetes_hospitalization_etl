from __future__ import annotations

from pathlib import Path
import os

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


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
_NONE_LABEL = "None (not on SGLT2)"
drug_options = [_NONE_LABEL] + [
    row[0]
    for row in conn.execute(
        "SELECT DISTINCT sglt2_drug FROM rwe_cohort WHERE sglt2_drug IS NOT NULL ORDER BY 1"
    ).fetchall()
]

st.sidebar.header("Filters")
selected_risk = st.sidebar.multiselect("CKD Risk", options=risk_options, default=risk_options)
selected_drug = st.sidebar.multiselect("SGLT2 Drug", options=drug_options, default=drug_options)
min_hba1c = st.sidebar.slider("Minimum HbA1c", min_value=5.0, max_value=12.0, value=5.0, step=0.1)

risk_filter = "1=1"
if selected_risk:
    safe_risks = "', '".join(v.replace("'", "''") for v in selected_risk)
    risk_filter = f"coalesce(c.ckd_risk_level, 'UNKNOWN') IN ('{safe_risks}')"

# Build drug filter that correctly handles patients not on any SGLT2 (NULL sglt2_drug).
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
c2.metric("On SGLT2", int(metrics.loc[0, "on_sglt2"]))
c3.metric("High CKD Risk", int(metrics.loc[0, "high_ckd"]))
c4.metric("Avg HbA1c", float(metrics.loc[0, "avg_hba1c"] or 0.0))

left, right = st.columns(2)

with left:
    st.subheader("HbA1c Trajectory")
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

with right:
    st.subheader("CKD Risk Distribution")
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

st.subheader("Cohort Preview")
preview = conn.execute(
    f"""
    SELECT patient_id, sglt2_drug, hba1c, ckd_risk_level
    FROM rwe_cohort
    WHERE {drug_filter.replace('r.', '')}
      AND {risk_filter.replace('c.', '')}
      AND coalesce(hba1c, 0) >= {min_hba1c}
    ORDER BY patient_id
    LIMIT 30
    """
).fetchdf()
st.dataframe(preview, use_container_width=True)

st.caption("Filters apply to metrics and charts. CKD risk comes from the ckd_risk view.")
st.info(
    "Clinical caveat: synthetic HbA1c in this demo can fluctuate more rapidly than real biology. "
    "In real patients, HbA1c is a 2-3 month glycemic average and usually changes gradually over months, "
    "not week to week."
)

conn.close()
