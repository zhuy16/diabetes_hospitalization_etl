CREATE OR REPLACE VIEW t2d_patients AS
SELECT DISTINCT patient_id
FROM conditions
WHERE icd10_code LIKE 'E11%';
