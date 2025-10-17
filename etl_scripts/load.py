import os
from typing import Dict

import pandas as pd
from sqlalchemy import text

# Import separated modules and shared utilities
from extract import extract_data
from transform import transform_data
from typing import Optional, Dict, Any, List
from utils import (
    mk_engine, ensure_schema, load_df, fetch_existing_keys,
    dq, logger, LOAD_MODE, DQ_BUFFER, 
    PATIENTS_CSV, ENCOUNTERS_CSV, DIAGNOSES_XML
)

def apply_referential_integrity(
    data: Dict[str, pd.DataFrame], 
    engine: Optional[Any] = None
) -> Dict[str, pd.DataFrame]:
    """
    Enforces referential integrity checks between dataframes before loading:
    1. Encounters must reference a Patient.
    2. Diagnoses must reference an Encounter (either in the batch or in the DB).
    """
    logger.info("--- Applying Referential Integrity Checks ---")
    patients = data["patients"].copy()
    encounters = data["encounters"].copy()
    diagnoses_df = data["diagnoses"].copy()
    
    # 1. Encounters -> Patients
    valid_pat_ids = set(patients["patient_id"].dropna().unique())
    mask_fk_ok = encounters["patient_id"].isin(valid_pat_ids)
    
    dropped_fk = int((~mask_fk_ok).sum())
    if dropped_fk > 0:
        fn = os.path.basename(ENCOUNTERS_CSV)
        dq(fn, "*", "patient_id", str(dropped_fk), "Encounters dropped: no matching patient in current batch.")
        # Log specific rows dropped
        for _, r in encounters.loc[~mask_fk_ok, ["encounter_id","patient_id"]].iterrows():
            dq(fn, r["encounter_id"], "patient_id", r["patient_id"], "No matching patient; encounter excluded to satisfy FK.")
    encounters = encounters[mask_fk_ok]
    logger.info(f"Encounters after Patient FK check: {len(encounters)} rows remaining.")

    # 2. Diagnoses -> Encounters
    valid_enc_ids = set(encounters["encounter_id"].dropna().unique())
    
    # In append/upsert modes, include already-existing encounters from DB
    if LOAD_MODE in {"append", "upsert"} and engine:
        db_enc_ids = fetch_existing_keys(engine, "encounters", "encounter_id")
        valid_enc_ids |= db_enc_ids
        logger.info(f"Included {len(db_enc_ids)} existing encounter IDs from DB for FK check.")

    mask_diag_fk = diagnoses_df["encounter_id"].isin(valid_enc_ids)
    dropped_diag_fk = int((~mask_diag_fk).sum())
    
    if dropped_diag_fk > 0:
        fn = os.path.basename(DIAGNOSES_XML)
        dq(fn, "*", "encounter_id", str(dropped_diag_fk), "Diagnoses dropped: no matching encounter (FK).")
        # Log specific rows dropped
        for _, r in diagnoses_df.loc[~mask_diag_fk, ["encounter_id","code"]].iterrows():
            dq(fn, r["encounter_id"], "encounter_id", r["encounter_id"], "No matching encounter; diagnosis excluded to satisfy FK.")
    diagnoses_df = diagnoses_df[mask_diag_fk]
    logger.info(f"Diagnoses after Encounter FK check: {len(diagnoses_df)} rows remaining.")

    data["encounters"] = encounters
    data["diagnoses"] = diagnoses_df
    return data


def handle_load(engine: Any, data: Dict[str, pd.DataFrame]):
    """Handles data loading into the database based on the LOAD_MODE."""
    patients = data["patients"]
    encounters = data["encounters"]
    diagnoses_df = data["diagnoses"]

    logger.info(f"--- Starting Data Load ({LOAD_MODE.upper()} mode) ---")

    if LOAD_MODE == "truncate":
        # Truncate tables in FK-dependent order (CASCADE handles diagnoses if specified)
        with engine.begin() as cxn:
            cxn.execute(text('TRUNCATE TABLE "diagnoses" RESTART IDENTITY CASCADE;'))
            cxn.execute(text('TRUNCATE TABLE "encounters" RESTART IDENTITY CASCADE;'))
            cxn.execute(text('TRUNCATE TABLE "patients" RESTART IDENTITY CASCADE;'))
        logger.info("TRUNCATE completed.")
        
        # In truncate mode, load all rows
        load_df(engine, patients, "patients")
        load_df(engine, encounters, "encounters")
        load_df(engine, diagnoses_df, "diagnoses")

    elif LOAD_MODE in {"append", "upsert"}:
        # In append mode, filter out existing PKs to prevent database errors
        if LOAD_MODE == "append":
            # Load existing keys from the database
            existing_pat_ids = fetch_existing_keys(engine, "patients", "patient_id")
            existing_enc_ids = fetch_existing_keys(engine, "encounters", "encounter_id")
            
            # Filter DataFrames to only include new records
            patients_new = patients[~patients["patient_id"].isin(existing_pat_ids)]
            encounters_new = encounters[~encounters["encounter_id"].isin(existing_enc_ids)]
            
            if len(patients_new) < len(patients):
                dq(os.path.basename(PATIENTS_CSV), "*", "patient_id", str(len(patients) - len(patients_new)), "Append mode: skipped existing patients.")
            if len(encounters_new) < len(encounters):
                dq(os.path.basename(ENCOUNTERS_CSV), "*", "encounter_id", str(len(encounters) - len(encounters_new)), "Append mode: skipped existing encounters.")
            
            patients, encounters = patients_new, encounters_new

        # Load data in FK-safe order: patients -> encounters -> diagnoses
        load_df(engine, patients, "patients")
        load_df(engine, encounters, "encounters")
        load_df(engine, diagnoses_df, "diagnoses")

    else:
        raise ValueError(f"Unsupported LOAD_MODE: {LOAD_MODE}")

def run_etl():
    """The main ETL orchestration function."""
    try:
        # E - Extract
        raw_data = extract_data()

        # T - Transform
        transformed_data = transform_data(raw_data)

        # DB Setup (needed for full FK check)
        engine = mk_engine()
        ensure_schema(engine)

        # Integrity Check (dependent on transformed data and existing DB keys)
        final_data = apply_referential_integrity(transformed_data, engine)

        # L - Load
        handle_load(engine, final_data)

        # Flush DQ log
        if DQ_BUFFER:
            dq_df = pd.DataFrame(DQ_BUFFER, columns=["file_name","row_id","column_name","value_seen","reason"])
            load_df(engine, dq_df, "data_quality_log")
            
        logger.info("ETL completed successfully. 🎉")

    except FileNotFoundError as e:
        logger.error(f"ETL failed. Missing file: {e}")
    except ValueError as e:
        logger.error(f"ETL failed due to configuration or schema error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during ETL: {e}", exc_info=True)


if __name__ == "__main__":
    run_etl()