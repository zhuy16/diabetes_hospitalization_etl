CREATE OR REPLACE VIEW ckd_risk AS
SELECT
    p.patient_id,
    MIN(CASE WHEN o.loinc_code = '33914-3' THEN o.value END) AS lowest_egfr,
    COUNT(CASE WHEN c.icd10_code LIKE 'N18%' THEN 1 END) AS ckd_diagnosis_count,
    CASE
        WHEN MIN(CASE WHEN o.loinc_code = '33914-3' THEN o.value END) < 30 THEN 'HIGH'
        WHEN MIN(CASE WHEN o.loinc_code = '33914-3' THEN o.value END) < 60 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS ckd_risk_level
FROM t2d_patients p
LEFT JOIN observations o ON p.patient_id = o.patient_id
LEFT JOIN conditions c ON p.patient_id = c.patient_id
GROUP BY p.patient_id;
