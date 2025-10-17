import os
from typing import List, Dict, Any

import pandas as pd
import xml.etree.ElementTree as ET

# Import shared utilities and configuration
from utils import (
    PATIENTS_CSV, ENCOUNTERS_CSV, DIAGNOSES_XML,
    EXPECTED_PATIENT_COLS, EXPECTED_ENCOUNTER_COLS,
    dq, logger, clean_str
)

def _is_header_like(cells: list[str]) -> bool:
    """Check if a list of cells looks like the encounter header."""
    header = [c.lower() for c in cells]
    return header == [c.lower() for c in EXPECTED_ENCOUNTER_COLS] or \
           ("encounter_id" in header and "patient_id" in header)

def _normalize_cells(cells: list[str]) -> list[str]:
    """Pads or truncates a list of cells to match the expected encounter column count."""
    cells = [c.strip() for c in cells]
    expected_len = len(EXPECTED_ENCOUNTER_COLS)
    if len(cells) < expected_len:
        # Pad with empty strings
        cells = cells + ["" for _ in range(expected_len - len(cells))]
    elif len(cells) > expected_len:
        # Truncate
        cells = cells[:expected_len]
    return cells

def read_encounters_csv(path: str) -> pd.DataFrame:
    """
    Reads the encounters CSV, handling:
    1. Semicolon delimiters (converts to comma).
    2. Mid-file header rows (drops them).
    3. Missing required columns (raises error).
    """
    file_name = os.path.basename(path)
    logger.info(f"Starting read for {file_name}...")

    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    if not lines:
        return pd.DataFrame(columns=EXPECTED_ENCOUNTER_COLS)

    fixed_lines = [lines[0]]  # Keep original header
    for i, line in enumerate(lines[1:], start=2):
        raw = line.rstrip("\n")
        
        # Semicolon delimiter fix
        if ";" in raw and (raw.count(";") >= 1):
            cells = _normalize_cells(raw.split(";"))
            if _is_header_like(cells):
                dq(file_name, f"line-{i}", None, raw, "Dropped inner header row (semicolon variant).")
                continue
            dq(file_name, f"line-{i}", None, raw, "Semicolon delimiters detected; converted to commas.")
            fixed_lines.append(",".join(cells) + "\n")
            continue

        # Drop duplicate header rows (comma-separated variant)
        csv_cells = [c.strip() for c in raw.split(",")]
        if _is_header_like(csv_cells):
            dq(file_name, f"line-{i}", None, raw, "Dropped inner header row (comma variant).")
            continue

        fixed_lines.append(line)

    # Write to a temporary file for pandas to read consistently
    tmp = path + ".fixed.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.writelines(fixed_lines)

    # Read the temporary file
    df = pd.read_csv(tmp, dtype=str).applymap(lambda v: v.strip() if isinstance(v, str) else v)

    # Final column check
    missing = [c for c in EXPECTED_ENCOUNTER_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"{path} missing columns after fix: {missing}")
    
    # Drop any header-like leftovers that survived
    mask_headerish = (
        df["encounter_id"].str.lower().eq("encounter_id") |
        df["patient_id"].str.lower().eq("patient_id")
    )
    if mask_headerish.any():
        n = int(mask_headerish.sum())
        dq(file_name, "*", None, str(n), "Dropped header-like rows after parse.")
        df = df[~mask_headerish]

    os.remove(tmp) # Clean up temporary file
    return df

def read_csv_required(path: str, expected_cols: List[str]) -> pd.DataFrame:
    """Reads a CSV file, ensuring all expected columns are present."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Missing file: {path}")
    
    # Read all columns as string for transformation
    df = pd.read_csv(path, dtype=str).applymap(lambda v: v.strip() if isinstance(v, str) else v)
    
    # Check for missing columns
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"{path} missing required columns: {missing}")
    
    return df

def parse_diagnoses_xml(xml_path: str) -> pd.DataFrame:
    """Parses the diagnoses XML file into a DataFrame."""
    if not os.path.exists(xml_path):
        raise FileNotFoundError(f"Missing file: {xml_path}")

    logger.info(f"Starting XML parse for {os.path.basename(xml_path)}...")
    
    # Define XML namespace for XPath queries
    ns = {"d": "http://example.org/diagnosis"}
    tree = ET.parse(xml_path)
    root = tree.getroot()

    rows: List[Dict[str, Any]] = []
    file_name = os.path.basename(xml_path)
    
    # Iterate over all Diagnosis elements
    for node in root.findall("d:Diagnosis", ns):
        # Extract fields using XPath
        raw_data = {
            "encounter_id": node.findtext("d:encounterId", default=None, namespaces=ns),
            "code_el": node.find("d:code", ns),
            "is_primary_text": node.findtext("d:isPrimary", default=None, namespaces=ns),
            "recorded_at": node.findtext("d:recordedAt", default=None, namespaces=ns)
        }
        
        # Handle code and system extraction from the 'code' element
        code = raw_data["code_el"].text.strip() if (raw_data["code_el"] is not None and raw_data["code_el"].text) else None
        system = raw_data["code_el"].get("system").strip() if (raw_data["code_el"] is not None and raw_data["code_el"].get("system")) else None
        
        # Collect raw strings/elements to be transformed later
        rows.append({
            "encounter_id": raw_data["encounter_id"],
            "code": code,
            "system": system,
            "is_primary_text": raw_data["is_primary_text"],
            "recorded_at": raw_data["recorded_at"],
            # Include the file name for later DQ logging
            "source_file": file_name 
        })

    return pd.DataFrame(rows)

def extract_data() -> Dict[str, pd.DataFrame]:
    """Main extraction function to read all source files."""
    logger.info("--- Starting Data Extraction ---")
    
    # 1. Read Patients CSV
    patients_raw = read_csv_required(PATIENTS_CSV, EXPECTED_PATIENT_COLS)
    logger.info(f"Extracted {len(patients_raw)} rows from {os.path.basename(PATIENTS_CSV)}.")

    # 2. Read Encounters CSV (with robust parsing)
    encounters_raw = read_encounters_csv(ENCOUNTERS_CSV)
    logger.info(f"Extracted {len(encounters_raw)} rows from {os.path.basename(ENCOUNTERS_CSV)}.")

    # 3. Parse Diagnoses XML
    diagnoses_raw = parse_diagnoses_xml(DIAGNOSES_XML)
    logger.info(f"Extracted {len(diagnoses_raw)} rows from {os.path.basename(DIAGNOSES_XML)}.")

    return {
        "patients_raw": patients_raw,
        "encounters_raw": encounters_raw,
        "diagnoses_raw": diagnoses_raw
    }

if __name__ == "__main__":
    # Example usage for testing
    try:
        data = extract_data()
        print("\nExtracted DataFrames:")
        for name, df in data.items():
            print(f"- {name}: {len(df)} rows, columns: {list(df.columns)}")
    except FileNotFoundError as e:
        logger.error(e)
    except ValueError as e:
        logger.error(f"Data schema error: {e}")