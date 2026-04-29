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

st.markdown(
    """
    <style>
    :root {
        --app-bg-top: #f4f7fb;
        --app-bg-bottom: #eef3f8;
        --panel-bg: #ffffff;
        --panel-border: #c8d3df;
        --text-main: #12202f;
        --text-muted: #4d5d70;
        --accent: #0f6a9a;
    }

    [data-testid="stAppViewContainer"] {
        background: linear-gradient(180deg, var(--app-bg-top) 0%, var(--app-bg-bottom) 100%);
    }

    .main .block-container,
    [data-testid="stAppViewBlockContainer"],
    [data-testid="stMainBlockContainer"] {
        max-width: min(1800px, 98vw);
        padding-left: 1rem;
        padding-right: 1rem;
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #f6f9fc 0%, #edf3f9 100%);
        border-right: 1px solid #d2dce7;
    }

    [data-testid="stSidebarUserContent"] {
        padding: 0.4rem 0.35rem 0.8rem 0.35rem;
    }

    [data-testid="stSidebar"] * {
        color: var(--text-main);
    }

    [data-testid="stSidebar"] .stMarkdown h3 {
        color: #0e3853;
        letter-spacing: 0.02em;
        font-weight: 700;
    }

    .main h1, .main h2, .main h3 {
        color: var(--text-main);
    }

    .main p, .main label, .main span, .main li {
        color: var(--text-main);
    }

    [data-testid="stVerticalBlockBorderWrapper"] {
        background: var(--panel-bg);
        border: 1px solid var(--panel-border);
        border-radius: 14px;
        box-shadow: 0 4px 16px rgba(16, 34, 52, 0.07);
        margin: 0 0.2rem 0.75rem 0.2rem;
        padding: 0.7rem 0.85rem 0.85rem 0.85rem;
    }

    [data-testid="stVerticalBlockBorderWrapper"] [data-testid="stVerticalBlockBorderWrapper"] {
        background: transparent;
        border: 0;
        box-shadow: none;
        margin: 0;
        padding: 0;
    }

    [data-testid="stHorizontalBlock"]:has(> [data-testid="column"]:nth-child(2)):not(:has(> [data-testid="column"]:nth-child(3))) > [data-testid="column"]:first-child {
        border-right: 1px solid #c3d0dc;
        padding-right: 0.75rem;
    }

    [data-testid="stHorizontalBlock"]:has(> [data-testid="column"]:nth-child(2)):not(:has(> [data-testid="column"]:nth-child(3))) > [data-testid="column"]:nth-child(2) {
        padding-left: 0.75rem;
    }

    [data-testid="stSidebar"] [data-testid="stVerticalBlockBorderWrapper"] {
        margin: 0.2rem 0.45rem 0.95rem 0.45rem;
        padding: 0.55rem 0.65rem 0.75rem 0.65rem;
    }

    [data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid #ccd8e3;
        border-radius: 12px;
        padding: 0.55rem 0.7rem;
        box-shadow: 0 2px 10px rgba(10, 29, 46, 0.06);
    }

    [data-testid="stMetricLabel"] {
        color: var(--text-muted);
        font-weight: 600;
    }

    [data-testid="stMetricValue"] {
        color: #0e3853;
        font-weight: 700;
    }

    div[data-baseweb="select"] > div,
    [data-baseweb="input"] > div,
    .stSlider {
        background: #ffffff;
        border-radius: 10px;
    }

    .stAlert {
        border-radius: 10px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

px.defaults.template = "plotly_white"
px.defaults.color_discrete_sequence = ["#0f6a9a", "#3f8fb5", "#73a9bf", "#3d5a80", "#7aa6c2", "#4f7f9f"]

conn = _connect()

st.sidebar.markdown("### ClinicalCohort AI - RWE Dashboard")
st.sidebar.caption("Longitudinal cohort analytics workspace")
st.sidebar.markdown("---")

risk_options = [
    row[0]
    for row in conn.execute(
        "SELECT DISTINCT ckd_risk_level FROM ckd_risk WHERE ckd_risk_level IS NOT NULL ORDER BY 1"
    ).fetchall()
]
_NONE_LABEL = "None"
drug_options = [_NONE_LABEL] + [
    row[0]
    for row in conn.execute(
        "SELECT DISTINCT sglt2_drug FROM rwe_cohort WHERE sglt2_drug IS NOT NULL ORDER BY 1"
    ).fetchall()
]

risk_options = [
    row[0]
    for row in conn.execute(
        "SELECT DISTINCT ckd_risk_level FROM ckd_risk ORDER BY 1"
    ).fetchall()
]

st.sidebar.header("Filters")
selected_risk = st.sidebar.multiselect("Risk Strata", options=risk_options, default=risk_options)
selected_drug = st.sidebar.multiselect("Exposure Status", options=drug_options, default=drug_options)
min_threshold = st.sidebar.slider("Minimum Threshold", min_value=0.0, max_value=20.0, value=0.0, step=0.1)
min_metric = min_threshold

# Keep optional dataset-specific block disabled unless explicitly reintroduced.
is_diabetes130_mode = False

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

st.sidebar.markdown("---")
st.sidebar.subheader("Optional Custom Cohort (NLQ)")
use_custom_cohort = st.sidebar.checkbox(
    "Use custom cohort restriction",
    value=st.session_state.get("use_custom_cohort", False),
    help="When enabled, the dashboard is restricted to patient_id values returned by the NLQ query below.",
)
st.session_state["use_custom_cohort"] = use_custom_cohort

if use_custom_cohort:
    st.sidebar.caption("Write a question that returns a `patient_id` column.")
    cohort_question = st.sidebar.text_area(
        "Custom cohort question",
        key="cohort_question",
        placeholder="Example: List patient_id for HIGH CKD risk patients not on diabetes drug",
        height=90,
    )

    if st.sidebar.button("Apply Cohort", key="apply_custom_cohort"):
        q = cohort_question.strip()
        if not q:
            st.sidebar.warning("Please enter a cohort question first.")
        else:
            try:
                generated_sql, result_df = ask(q)
                st.session_state["cohort_sql"] = generated_sql
                st.session_state["cohort_result"] = result_df
                if "patient_id" not in result_df.columns:
                    st.sidebar.error("Query result must include a `patient_id` column to define cohort.")
                    st.session_state.pop("cohort_patient_df", None)
                else:
                    cohort_patient_df = result_df[["patient_id"]].dropna().astype(str).drop_duplicates()
                    st.session_state["cohort_patient_df"] = cohort_patient_df
                    st.sidebar.success(f"Custom cohort applied: {len(cohort_patient_df)} patients")
            except Exception as exc:
                st.sidebar.error(f"Could not apply cohort query: {exc}")

    if st.sidebar.button("Clear Cohort", key="clear_custom_cohort"):
        st.session_state.pop("cohort_sql", None)
        st.session_state.pop("cohort_result", None)
        st.session_state.pop("cohort_patient_df", None)

st.sidebar.markdown("---")
st.sidebar.caption(
    "**Clinical coding** — Observations use LOINC codes (e.g., 4548-4 for HbA1c, 33914-3 for eGFR). "
    "Conditions use ICD-10 codes (e.g., E11.% for Type 2 Diabetes, N18.% for CKD). "
    "Medications use RxNorm codes. **Available views** — `rwe_cohort` (patient_id, sglt2_drug, hba1c, ckd_risk_level, …) · "
    "`hba1c_trajectory` (patient_id, observation_month, hba1c, hba1c_change) · "
    "`ckd_risk` (patient_id, egfr, ckd_risk_level) · "
    "`sglt2_exposure` (patient_id, drug_name, start_date)"
)

cohort_filter = "1=1"
if st.session_state.get("use_custom_cohort", False):
    cohort_patient_df = st.session_state.get("cohort_patient_df")
    if isinstance(cohort_patient_df, pd.DataFrame) and not cohort_patient_df.empty:
        conn.register("custom_cohort_patients", cohort_patient_df)
        cohort_filter = "r.patient_id IN (SELECT patient_id FROM custom_cohort_patients)"

metrics = conn.execute(
    f"""
    SELECT
      COUNT(DISTINCT r.patient_id) AS cohort_size,
      COUNT(DISTINCT CASE WHEN r.sglt2_drug IS NOT NULL THEN r.patient_id END) AS with_drug_exposure,
      COUNT(DISTINCT CASE WHEN c.ckd_risk_level = 'HIGH' THEN r.patient_id END) AS high_risk,
      ROUND(AVG(r.hba1c), 2) AS avg_metric
    FROM rwe_cohort r
    LEFT JOIN ckd_risk c ON r.patient_id = c.patient_id
    WHERE {risk_filter}
      AND {drug_filter}
      AND {cohort_filter}
      AND coalesce(r.hba1c, 0) >= {min_metric}
    """
).fetchdf()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Cohort Size", int(metrics.loc[0, "cohort_size"]))
c2.metric("With Exposure", int(metrics.loc[0, "with_drug_exposure"]))
c3.metric("High Risk", int(metrics.loc[0, "high_risk"]))
c4.metric("Avg Metric", float(metrics.loc[0, "avg_metric"] or 0.0))

# Schema introspection — shared by both panels
_schema_cols = conn.execute(
    """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'main'
      AND table_name = 'rwe_cohort'
    ORDER BY ordinal_position
    """
).fetchdf()
_numeric_markers = ("INT", "DOUBLE", "DECIMAL", "REAL", "FLOAT")
value_candidates = []
category_candidates = []
for _, _row in _schema_cols.iterrows():
    _col = str(_row["column_name"])
    _dtype = str(_row["data_type"]).upper()
    if any(m in _dtype for m in _numeric_markers):
        value_candidates.append(_col)
    if "VARCHAR" in _dtype and _col != "patient_id":
        category_candidates.append(_col)
if "birth_date" in _schema_cols["column_name"].tolist():
    value_candidates.append("age_years")
pretty_names = {
    "hba1c": "HbA1c",
    "hba1c_change": "HbA1c Change",
    "lowest_egfr": "Lowest eGFR",
    "age_years": "Age (years)",
    "sglt2_drug": "Primary Diabetes Drug",
    "ckd_risk_level": "Kidney Risk Level",
    "gender": "Gender",
}

st.subheader("Cohort Distribution")
st.caption("These charts reflect the cohort selected by the sidebar filters on the left.")

explore_col, ask_col = st.columns([1, 1], gap="small")
left_panel = explore_col.container(border=True)
right_panel = ask_col.container(border=True)

# Add a visible left border to the right panel for clear separation
with ask_col:
    st.markdown(
        '<style>'
        '[data-testid="stVerticalBlockBorderWrapper"] { border-left: 2px solid #b0b9c6 !important; margin-left: -1px; padding-left: 1.5rem !important; }'
        '</style>',
        unsafe_allow_html=True,
    )

with left_panel:
    st.markdown("**Trajectory over time**")
    if is_diabetes130_mode:
        st.markdown("**Average HbA1c by Primary Diabetes Drug**")
        hba1c_by_drug = conn.execute(
            f"""
            SELECT
                coalesce(r.sglt2_drug, 'No Drug') AS exposure_group,
                ROUND(AVG(r.hba1c), 2) AS avg_metric,
                COUNT(DISTINCT r.patient_id) AS patients
            FROM rwe_cohort r
            LEFT JOIN ckd_risk c ON r.patient_id = c.patient_id
            WHERE {risk_filter}
              AND {drug_filter}
              AND {cohort_filter}
              AND coalesce(r.hba1c, 0) >= {min_metric}
            GROUP BY 1
            HAVING COUNT(*) > 0
            ORDER BY patients DESC
            LIMIT 12
            """
        ).fetchdf()
        if hba1c_by_drug.empty:
            st.info("No exposure-level data found.")
        else:
            fig = px.bar(hba1c_by_drug, x="exposure_group", y="avg_metric", color="patients")
            st.plotly_chart(fig, use_container_width=True)

    # Left panel trajectory Y-axis selector
    traj_y_col = st.selectbox(
        "Trajectory Y-axis",
        options=value_candidates if value_candidates else ["hba1c"],
        index=value_candidates.index("hba1c") if "hba1c" in value_candidates else 0,
        format_func=lambda x: pretty_names.get(x, x),
        key="traj_y_col",
    )
    subgroup_options = {
        "Primary Drug": "coalesce(r.sglt2_drug, 'No Diabetes Drug')",
        "Kidney Risk": "coalesce(c.ckd_risk_level, 'UNKNOWN')",
        "Gender": "coalesce(r.gender, 'UNKNOWN')",
    }
    selected_subgroup = st.selectbox(
        "Line pattern subgroup",
        options=list(subgroup_options.keys()),
        index=0,
        key="traj_subgroup",
    )
    sampled_patients = st.slider(
        "Sample patient trajectories",
        min_value=3,
        max_value=20,
        value=8,
        key="traj_sample_patients",
    )
    subgroup_expr = subgroup_options[selected_subgroup]

    if traj_y_col == "age_years":
        _traj_y_expr = "CAST(date_diff('year', r.birth_date, current_date) AS DOUBLE)"
    else:
        _traj_y_expr = f"r.{traj_y_col}"

    patient_lines = conn.execute(
        f"""
        WITH filtered AS (
            SELECT
                r.patient_id,
                r.observation_date,
                {_traj_y_expr} AS traj_metric,
                {subgroup_expr} AS subgroup
            FROM rwe_cohort r
            LEFT JOIN ckd_risk c ON r.patient_id = c.patient_id
            WHERE {risk_filter}
              AND {drug_filter}
              AND {cohort_filter}
              AND coalesce(r.hba1c, 0) >= {min_metric}
              AND traj_metric IS NOT NULL
        ),
        patient_rank AS (
            SELECT
                patient_id,
                COUNT(*) AS points_per_patient,
                MAX(observation_date) AS last_seen
            FROM filtered
            GROUP BY 1
            ORDER BY points_per_patient DESC, last_seen DESC, patient_id
            LIMIT {sampled_patients}
        )
        SELECT
            f.patient_id,
            f.observation_date,
            f.traj_metric,
            f.subgroup
        FROM filtered f
        JOIN patient_rank p ON f.patient_id = p.patient_id
        ORDER BY f.patient_id, f.observation_date
        """
    ).fetchdf()
    if patient_lines.empty:
        st.info("No HbA1c data found.")
    else:
        fig = px.line(
            patient_lines,
            x="observation_date",
            y="traj_metric",
            color="patient_id",
            line_dash="subgroup",
            markers=True,
            labels={
                "patient_id": "Patient",
                "traj_metric": pretty_names.get(traj_y_col, traj_y_col),
                "subgroup": selected_subgroup,
            },
        )
        fig.update_traces(marker={"size": 7, "opacity": 0.75})

        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            "Each colored line is one patient trajectory. Line pattern (solid/dashed/dotted) encodes the selected subgroup."
        )

    dist_cat_col = st.selectbox(
        "Distribution by",
        options=category_candidates if category_candidates else ["ckd_risk_level"],
        index=category_candidates.index("ckd_risk_level") if "ckd_risk_level" in category_candidates else 0,
        format_func=lambda x: pretty_names.get(x, x),
        key="dist_cat_col",
    )
    st.markdown("**Cohort Distribution**")
    _dist_cat_expr = f"coalesce(CAST(r.{dist_cat_col} AS VARCHAR), 'UNKNOWN')"
    risk = conn.execute(
        f"""
        SELECT {_dist_cat_expr} AS category, COUNT(DISTINCT r.patient_id) AS patients
        FROM rwe_cohort r
        LEFT JOIN ckd_risk c ON r.patient_id = c.patient_id
        WHERE {risk_filter}
          AND {drug_filter}
          AND {cohort_filter}
          AND coalesce(r.hba1c, 0) >= {min_metric}
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchdf()
    if risk.empty:
        st.info("No distribution rows found.")
    else:
        fig = px.pie(risk, names="category", values="patients",
                     labels={"category": pretty_names.get(dist_cat_col, dist_cat_col)})
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("**Cohort Preview**")
    preview = conn.execute(
        f"""
        SELECT
            r.patient_id,
            r.sglt2_drug AS primary_drug,
            r.hba1c AS latest_hba1c,
            c.ckd_risk_level AS kidney_risk
        FROM rwe_cohort r
        LEFT JOIN ckd_risk c ON r.patient_id = c.patient_id
        WHERE {risk_filter}
          AND {drug_filter}
          AND {cohort_filter}
          AND coalesce(r.hba1c, 0) >= {min_metric}
        ORDER BY r.patient_id
        LIMIT 30
        """
    ).fetchdf()
    st.dataframe(preview, use_container_width=True)
    st.caption(
        "LOINC codes: 4548-4 (HbA1c). OMOP standard tables: patient, observation, condition_occurrence. "
        "Data source: canonical RWE cohort with standard healthcare terminologies."
    )

with right_panel:
    st.markdown("**Categorical differences**")

    if not value_candidates or not category_candidates:
        st.info("No compatible value/category columns found in rwe_cohort for this visualization.")
    else:

        selected_value = st.selectbox(
            "Value to plot",
            options=value_candidates,
            index=value_candidates.index("hba1c") if "hba1c" in value_candidates else 0,
            format_func=lambda x: pretty_names.get(x, x),
        )
        selected_category = st.selectbox(
            "Categorical variable",
            options=category_candidates,
            index=category_candidates.index("sglt2_drug") if "sglt2_drug" in category_candidates else 0,
            format_func=lambda x: pretty_names.get(x, x),
        )
        max_groups = st.slider("Max groups to display", min_value=2, max_value=6, value=4)

        if selected_value == "age_years":
            value_expr = "CAST(date_diff('year', r.birth_date, current_date) AS DOUBLE)"
        else:
            value_expr = f"CAST(r.{selected_value} AS DOUBLE)"

        category_expr = f"coalesce(CAST(r.{selected_category} AS VARCHAR), 'UNKNOWN')"

        comparison_df = conn.execute(
            f"""
            WITH filtered AS (
                SELECT
                    r.patient_id,
                    r.observation_date,
                    {value_expr} AS metric_value,
                    {category_expr} AS category
                FROM rwe_cohort r
                LEFT JOIN ckd_risk c ON r.patient_id = c.patient_id
                WHERE {risk_filter}
                  AND {drug_filter}
                  AND {cohort_filter}
                  AND coalesce(r.hba1c, 0) >= {min_metric}
            ),
            latest_per_patient AS (
                SELECT
                    patient_id,
                    category,
                    metric_value,
                    ROW_NUMBER() OVER (
                        PARTITION BY patient_id
                        ORDER BY observation_date DESC NULLS LAST
                    ) AS rn
                FROM filtered
            ),
            base AS (
                SELECT patient_id, category, metric_value
                FROM latest_per_patient
                WHERE rn = 1
                  AND metric_value IS NOT NULL
            ),
            top_categories AS (
                SELECT category, COUNT(*) AS patients
                FROM base
                GROUP BY 1
                ORDER BY patients DESC
                LIMIT {max_groups}
            )
            SELECT b.category, b.metric_value
            FROM base b
            JOIN top_categories t ON b.category = t.category
            """
        ).fetchdf()

        if comparison_df.empty:
            st.info("No eligible rows for this comparison under current filters.")
        else:
            categories_present = comparison_df["category"].nunique()
            if categories_present < 2:
                st.info("Only one group is present with current filters. Broaden filters or pick another category.")

            fig = px.box(
                comparison_df,
                x="category",
                y="metric_value",
                color="category",
                points="all",
                labels={
                    "category": pretty_names.get(selected_category, selected_category),
                    "metric_value": pretty_names.get(selected_value, selected_value),
                },
            )
            fig.update_traces(jitter=0.35, pointpos=0.0, marker={"size": 5, "opacity": 0.55})
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

            median_df = (
                comparison_df.groupby("category", as_index=False)
                .agg(median_value=("metric_value", "median"), patients=("metric_value", "count"))
                .sort_values("median_value", ascending=False)
            )
            if len(median_df) >= 2:
                top = median_df.iloc[0]
                bottom = median_df.iloc[-1]
                median_diff = float(top["median_value"] - bottom["median_value"])
                metric_label = pretty_names.get(selected_value, selected_value)
                st.caption(
                    f"Insight: median {metric_label} is {median_diff:.2f} higher in '{top['category']}' "
                    f"vs '{bottom['category']}' (n={int(top['patients'])} vs n={int(bottom['patients'])})."
                )
            else:
                only = median_df.iloc[0]
                metric_label = pretty_names.get(selected_value, selected_value)
                st.caption(
                    f"Insight: one group available under current filters. "
                    f"Median {metric_label} is {float(only['median_value']):.2f} (n={int(only['patients'])})."
                )

    if st.session_state.get("use_custom_cohort", False):
        cohort_patient_df = st.session_state.get("cohort_patient_df")
        if isinstance(cohort_patient_df, pd.DataFrame) and not cohort_patient_df.empty:
            st.success(f"Custom NLQ cohort active: {len(cohort_patient_df)} patients")
            if "cohort_sql" in st.session_state:
                with st.expander("Custom cohort SQL", expanded=False):
                    st.code(st.session_state["cohort_sql"], language="sql")

st.caption("All visualizations use the same cohort definition from the sidebar filters and optional custom NLQ cohort restriction. CKD risk comes from ckd_risk; drug exposure reflects the first observed diabetes medication per patient.")
st.info(
    "Clinical caveat: when using Diabetes 130, CKD risk and dates are mapped proxies derived from available columns. "
    "Interpret trend timing and kidney risk buckets as exploratory signals, not validated clinical labels."
)

conn.close()
