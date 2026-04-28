CREATE OR REPLACE VIEW sglt2_exposure AS
SELECT
    patient_id,
    drug_name,
    rxnorm_code,
    start_date,
    stop_date
FROM medications
WHERE lower(drug_name) IN ('empagliflozin', 'canagliflozin', 'dapagliflozin')
   OR rxnorm_code IN ('2200644', '1545149', '1488574');
