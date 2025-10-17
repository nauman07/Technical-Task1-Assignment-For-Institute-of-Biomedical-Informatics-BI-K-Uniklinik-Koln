import os
from typing import Dict

import pandas as pd

# Import shared utilities and configuration
from utils import (
    PATIENTS_CSV, ENCOUNTERS_CSV, DIAGNOSES_XML,
    clean_str, sanitize_text, titlecase_or_none, normalize_sex, 
    parse_datetime_any, to_height_cm, to_weight_kg, dq, logger
)

def transform_patients(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the raw patients DataFrame:
    - Sanitizes and title-cases names.
    - Normalizes sex.
    - Parses and validates date of birth.
    - Converts height/weight to cm/kg with unit logging.
    - Handles full-row and PK-level deduplication.
    """
    file_name = os.path.basename(PATIENTS_CSV)
    out = pd.DataFrame()
    
    # --- 1. ID and Name Cleaning/Normalization ---
    # patient_id is critical for DQ logging; attempt to clean first.
    temp_pid_series = df["patient_id"].map(lambda v: sanitize_text(v, 50, file_name, str(v), "patient_id"))
    
    # Apply text sanitation and title-casing
    out["patient_id"]  = temp_pid_series
    out["given_name"]  = df.get("given_name", pd.Series([None]*len(df))).map(
        lambda v: sanitize_text(v, 100, file_name, "", "given_name")
    ).map(titlecase_or_none)
    out["family_name"] = df.get("family_name", pd.Series([None]*len(df))).map(
        lambda v: sanitize_text(v, 100, file_name, "", "family_name")
    ).map(titlecase_or_none)

    # --- 2. Sex Normalization ---
    temp_sex = df.get("sex", pd.Series([None]*len(df))).map(clean_str)
    # Apply row-wise function to log DQ events with patient_id context
    out["sex"] = [normalize_sex(s, file_name, pid or str(i)) 
                  for i, (s, pid) in enumerate(zip(temp_sex, out["patient_id"]))]

    # --- 3. Date of Birth Parsing ---
    dob_series = []
    for i, row in df.iterrows():
        raw_pid = out.loc[i, "patient_id"] or clean_str(row.get("patient_id")) # Use cleaned PID if available
        dob = clean_str(row.get("date_of_birth")) or clean_str(row.get("dob"))
        ts = parse_datetime_any(dob, file_name, raw_pid or str(i), "date_of_birth")
        dob_series.append(ts.date() if ts is not None else None)
        # Log if patient_id was missing in the original row
        if raw_pid is None:
             dq(file_name, str(i), "patient_id", None, "Missing critical key; row will be dropped.")
    out["dob"] = dob_series

    # --- 4. Biometric Conversion ---
    out["height_cm"] = [to_height_cm(clean_str(df.loc[i].get("height")), file_name, out.loc[i, "patient_id"] or str(i)) 
                        for i in df.index]
    out["weight_kg"] = [to_weight_kg(clean_str(df.loc[i].get("weight")), file_name, out.loc[i, "patient_id"] or str(i)) 
                        for i in df.index]

    # --- 5. Deduplication and PK Check ---
    before_full = len(out)
    out = out.drop_duplicates(keep="first")
    if len(out) < before_full:
        dq(file_name, "*", None, str(before_full - len(out)), "Dropped exact duplicate patient rows (full-row match).")

    # Drop rows missing critical Primary Key
    before = len(out)
    missing_pk_mask = out["patient_id"].isna()
    if missing_pk_mask.any():
        for idx in out.loc[missing_pk_mask].index:
            dq(file_name, str(idx), "patient_id", None, "Missing critical key; row dropped.")
    out = out.dropna(subset=["patient_id"])

    # PK-level dedupe (keep last)
    before_pk = len(out)
    dup_ids = out.loc[out["patient_id"].duplicated(keep=False), "patient_id"].unique()
    for pid in dup_ids:
        dq(file_name, pid, "patient_id", pid, "Duplicate patient_id in batch; keeping last, dropping others.")
    out = out.drop_duplicates(subset=["patient_id"], keep="last")
    
    logger.info(f"patients: pruned {before - len(out)} rows (missing PK) and {before_pk - len(out)} PK dups (net).")
    return out


def transform_encounters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the raw encounters DataFrame:
    - Sanitizes and normalizes text fields (IDs, type, source_file).
    - Parses and validates admit/discharge dates (including chronology check).
    - Handles deduplication.
    """
    file_name = os.path.basename(ENCOUNTERS_CSV)
    out = pd.DataFrame()

    # --- 1. Text Field Sanitation ---
    out["encounter_id"]   = df["encounter_id"].map(lambda v: sanitize_text(v, 50, file_name, str(v), "encounter_id"))
    out["patient_id"]     = df["patient_id"].map(lambda v: sanitize_text(v, 50, file_name, str(v), "patient_id"))
    
    # Encounter type sanitation and upper-casing
    out["encounter_type"] = df.get("encounter_type", pd.Series([None]*len(df))).map(
        lambda s: sanitize_text(s, 30, file_name, "", "encounter_type")
    ).map(lambda s: s.upper() if s else None)
    
    out["source_file"]    = df.get("source_file", pd.Series([None]*len(df))).map(
        lambda s: sanitize_text(s, 255, file_name, "", "source_file")
    )

    # --- 2. Datetime Parsing and Validation ---
    admit, disch = [], []
    for i, row in df.iterrows():
        # Get ID for DQ logging; use row index if ID is missing
        rid = out.loc[i, "encounter_id"] or clean_str(row.get("encounter_id")) or str(i)
        
        ad = parse_datetime_any(clean_str(row.get("admit_dt")), file_name, rid, "admit_dt")
        dc = parse_datetime_any(clean_str(row.get("discharge_dt")), file_name, rid, "discharge_dt")
        
        # Chronology check: discharge before admit
        if ad is not None and dc is not None and dc < ad:
            dq(file_name, rid, "discharge_dt", f"{dc} < admit {ad}", "Discharge before admit; kept values (potential data error).")
        
        admit.append(ad); disch.append(dc)
        
        # Log if encounter_id was missing in the original row
        if clean_str(row.get("encounter_id")) is None:
             dq(file_name, str(i), "encounter_id", None, "Missing critical key; row will be dropped.")
             
    out["admit_dt"]     = admit
    out["discharge_dt"] = disch

    # --- 3. Deduplication and PK Check ---
    before_full = len(out)
    out = out.drop_duplicates(keep="first")
    if len(out) < before_full:
        dq(file_name, "*", None, str(before_full - len(out)), "Dropped exact duplicate encounter rows (full-row match).")

    # Drop rows missing critical Primary Key
    before = len(out)
    missing_pk_mask = out["encounter_id"].isna()
    if missing_pk_mask.any():
        for idx in out.loc[missing_pk_mask].index:
            dq(file_name, str(idx), "encounter_id", None, "Missing critical key; row dropped.")
    out = out.dropna(subset=["encounter_id"])

    # PK-level dedupe (keep last)
    before_pk = len(out)
    dup_ids = out.loc[out["encounter_id"].duplicated(keep=False), "encounter_id"].unique()
    for eid in dup_ids:
        dq(file_name, eid, "encounter_id", eid, "Duplicate encounter_id in batch; keeping last, dropping others.")
    out = out.drop_duplicates(subset=["encounter_id"], keep="last")
    
    logger.info(f"encounters: pruned {before - len(out)} rows (missing PK) and {before_pk - len(out)} PK dups (net).")
    return out


def transform_diagnoses(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the raw diagnoses DataFrame:
    - Sanitizes and validates fields (encounter_id, code, system).
    - Converts is_primary text to boolean.
    - Parses recorded_at timestamp.
    - Drops rows missing critical keys (encounter_id or code).
    """
    file_name = os.path.basename(DIAGNOSES_XML)
    rows = []
    
    for i, row in df.iterrows():
        # Clean ID fields first for use in DQ logging
        enc = row["encounter_id"]
        safe_enc_id = sanitize_text(enc, 50, file_name, enc or str(i), "encounter_id")
        code = row["code"]
        
        # --- Critical Check (must have a valid encounter ID and code) ---
        crit_missing = []
        if not safe_enc_id: 
            crit_missing.append("encounterId")
        if not code or not clean_str(code): # check if code is empty after cleaning
            crit_missing.append("code")
            
        if crit_missing:
            dq(file_name, safe_enc_id or str(i), ",".join(crit_missing), row["recorded_at"], "Missing critical field(s); row skipped.")
            continue

        # --- Sanitation & Normalization ---
        # Width & normalization logging for other fields
        code_s   = sanitize_text(code, 20, file_name, safe_enc_id, "code")
        # Default system to ICD-10 if missing
        system_s = sanitize_text(row["system"] or "ICD-10", 50, file_name, safe_enc_id, "system")

        # Convert is_primary text (e.g., 'true') to boolean
        is_primary = None
        if row["is_primary_text"] is not None:
            is_primary = row["is_primary_text"].strip().lower() == "true"
            
        # Parse datetime
        recorded_at = parse_datetime_any(row["recorded_at"], file_name, safe_enc_id, "recordedAt")

        # --- Append Transformed Row ---
        rows.append({
            "encounter_id": safe_enc_id,
            "code": code_s,
            "system": system_s,
            "is_primary": is_primary,
            "recorded_at": recorded_at
        })
        
    # Re-build DataFrame from list of dictionaries for efficiency
    out = pd.DataFrame(rows, columns=["encounter_id","code","system","is_primary","recorded_at"])
    logger.info(f"diagnoses: {len(out)} rows successfully transformed.")
    return out


def transform_data(raw_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """Main transformation function."""
    logger.info("--- Starting Data Transformation ---")

    patients_transformed = transform_patients(raw_data["patients_raw"])
    encounters_transformed = transform_encounters(raw_data["encounters_raw"])
    diagnoses_transformed = transform_diagnoses(raw_data["diagnoses_raw"])

    return {
        "patients": patients_transformed,
        "encounters": encounters_transformed,
        "diagnoses": diagnoses_transformed
    }

if __name__ == "__main__":
    logger.error("This module is intended to be run by 'load.py'.")