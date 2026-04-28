CREATE OR REPLACE VIEW sglt2_exposure AS
WITH ranked_medications AS (
    SELECT
        patient_id,
        drug_name,
        rxnorm_code,
        start_date,
        stop_date,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id
            ORDER BY start_date NULLS LAST, medication_id
        ) AS medication_rank
    FROM medications
    WHERE drug_name IS NOT NULL
)
SELECT
    patient_id,
    drug_name,
    rxnorm_code,
    start_date,
    stop_date
FROM ranked_medications
WHERE medication_rank = 1;
