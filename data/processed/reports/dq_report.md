# Data Quality Report

- Total checks: 11
- PASS: 11
- WARN: 0
- FAIL: 0

| Check | Status | Value | Details |
|---|---|---:|---|
| row_count_patients | PASS | 71518 | Row count in patients |
| row_count_encounters | PASS | 101766 | Row count in encounters |
| row_count_conditions | PASS | 303496 | Row count in conditions |
| row_count_observations | PASS | 34882 | Row count in observations |
| row_count_medications | PASS | 120054 | Row count in medications |
| row_count_claims | PASS | 101766 | Row count in claims |
| null_patient_ids | PASS | 0 | Null patient_id count across core tables |
| known_loinc_coverage_pct | PASS | 100.00 | Percent of observations with expected LOINC codes |
| icd10_diabetes_rows | PASS | 38708 | Conditions mapped to Type 2 diabetes ICD-10 E11% |
| icd10_ckd_rows | PASS | 10266 | Conditions mapped to CKD ICD-10 N18% |
| hba1c_out_of_expected_range | PASS | 0 | HbA1c rows outside rough expected range [3,20] |
