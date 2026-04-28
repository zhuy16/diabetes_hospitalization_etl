CREATE TABLE IF NOT EXISTS patients (
    patient_id VARCHAR PRIMARY KEY,
    birth_date DATE,
    gender VARCHAR,
    race VARCHAR,
    ethnicity VARCHAR,
    state VARCHAR,
    zip VARCHAR
);

CREATE TABLE IF NOT EXISTS encounters (
    encounter_id VARCHAR PRIMARY KEY,
    patient_id VARCHAR,
    encounter_date DATE,
    encounter_type VARCHAR,
    provider_id VARCHAR,
    payer VARCHAR,
    total_cost DOUBLE
);

CREATE TABLE IF NOT EXISTS conditions (
    condition_id VARCHAR PRIMARY KEY,
    patient_id VARCHAR,
    encounter_id VARCHAR,
    icd10_code VARCHAR,
    icd10_description VARCHAR,
    onset_date DATE,
    resolution_date DATE
);

CREATE TABLE IF NOT EXISTS observations (
    observation_id VARCHAR PRIMARY KEY,
    patient_id VARCHAR,
    encounter_id VARCHAR,
    loinc_code VARCHAR,
    loinc_description VARCHAR,
    value DOUBLE,
    unit VARCHAR,
    observation_date DATE
);

CREATE TABLE IF NOT EXISTS medications (
    medication_id VARCHAR PRIMARY KEY,
    patient_id VARCHAR,
    encounter_id VARCHAR,
    rxnorm_code VARCHAR,
    ndc_code VARCHAR,
    drug_name VARCHAR,
    start_date DATE,
    stop_date DATE,
    dosage VARCHAR
);

CREATE TABLE IF NOT EXISTS claims (
    claim_id VARCHAR PRIMARY KEY,
    patient_id VARCHAR,
    encounter_id VARCHAR,
    claim_date DATE,
    cpt_code VARCHAR,
    icd10_primary VARCHAR,
    payer VARCHAR,
    amount_billed DOUBLE,
    amount_paid DOUBLE
);
