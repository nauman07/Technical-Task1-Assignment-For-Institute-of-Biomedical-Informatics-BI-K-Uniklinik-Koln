-- -----------------------------------------------------------------------------
-- Reset (safe for reruns in a disposable/dev environment)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS data_quality_log;
DROP TABLE IF EXISTS diagnoses;
DROP TABLE IF EXISTS encounters;
DROP TABLE IF EXISTS patients;

-- -----------------------------------------------------------------------------
-- Patients: normalized demographics + metric units
-- -----------------------------------------------------------------------------
CREATE TABLE patients (
    patient_id   VARCHAR(50) PRIMARY KEY,
    given_name   VARCHAR(100),
    family_name  VARCHAR(100),
    sex          VARCHAR(1),                -- 'M','F','U'
    dob          DATE,
    height_cm    NUMERIC(6,2),
    weight_kg    NUMERIC(6,2)
);

-- -----------------------------------------------------------------------------
-- Encounters: admits/discharges with time zones (mixed inputs normalized)
-- -----------------------------------------------------------------------------
CREATE TABLE encounters (
    encounter_id    VARCHAR(50) PRIMARY KEY,
    patient_id      VARCHAR(50) REFERENCES patients(patient_id),
    admit_dt        TIMESTAMPTZ,
    discharge_dt    TIMESTAMPTZ,
    encounter_type  VARCHAR(30),
    source_file     VARCHAR(255)
);

CREATE INDEX idx_encounters_patient ON encounters(patient_id);

-- -----------------------------------------------------------------------------
-- Diagnoses: from diagnoses.xml (ICD-10 codes with primary flag & timestamp)
-- -----------------------------------------------------------------------------
CREATE TABLE diagnoses (
    diagnosis_id  BIGSERIAL PRIMARY KEY,
    encounter_id  VARCHAR(50) REFERENCES encounters(encounter_id),
    code          VARCHAR(20),
    system        VARCHAR(50),
    is_primary    BOOLEAN,
    recorded_at   TIMESTAMPTZ
);

CREATE INDEX idx_diagnoses_encounter ON diagnoses(encounter_id);

-- -----------------------------------------------------------------------------
-- Data quality/audit log (free-form reasons, original values preserved)
-- -----------------------------------------------------------------------------
CREATE TABLE data_quality_log (
    log_id        BIGSERIAL PRIMARY KEY,
    ts            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    file_name     VARCHAR(255) NOT NULL,
    row_id        VARCHAR(100),
    column_name   VARCHAR(100),
    value_seen    TEXT,
    reason        TEXT NOT NULL
);

CREATE INDEX idx_dq_file_ts ON data_quality_log(file_name, ts);
