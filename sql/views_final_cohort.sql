CREATE OR REPLACE VIEW rwe_cohort AS
SELECT
    p.patient_id,
    p.gender,
    p.birth_date,
    s.drug_name AS sglt2_drug,
    s.start_date AS treatment_start,
    h.observation_date,
    h.hba1c,
    h.hba1c_change,
    r.lowest_egfr,
    r.ckd_risk_level
FROM t2d_patients t
JOIN patients p ON t.patient_id = p.patient_id
LEFT JOIN sglt2_exposure s ON t.patient_id = s.patient_id
LEFT JOIN hba1c_trajectory h ON t.patient_id = h.patient_id
LEFT JOIN ckd_risk r ON t.patient_id = r.patient_id;
