CREATE OR REPLACE VIEW hba1c_trajectory AS
SELECT
    o.patient_id,
    o.observation_date,
    o.value AS hba1c,
    LAG(o.value) OVER (
        PARTITION BY o.patient_id
        ORDER BY o.observation_date
    ) AS previous_hba1c,
    o.value - LAG(o.value) OVER (
        PARTITION BY o.patient_id
        ORDER BY o.observation_date
    ) AS hba1c_change
FROM observations o
WHERE o.loinc_code = '4548-4'
  AND o.observation_date >= CURRENT_DATE - INTERVAL '12 months';
