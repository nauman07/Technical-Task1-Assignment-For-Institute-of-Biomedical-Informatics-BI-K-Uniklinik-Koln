# utils.py

import os
import re
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

import pandas as pd
from dateutil import parser as dtparser
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# -----------------------------------------------------------------------------
# Configuration (env-first; defaults)
# -----------------------------------------------------------------------------
DATA_DIR        = os.getenv("DATA_DIR", "data")
SCRIPT_DIR      = os.getenv("SCRIPT_DIR", "etl_scripts")
PATIENTS_CSV    = os.getenv("PATIENTS_CSV", os.path.join(DATA_DIR, "patients.csv"))
ENCOUNTERS_CSV  = os.getenv("ENCOUNTERS_CSV", os.path.join(DATA_DIR, "encounters.csv"))
DIAGNOSES_XML   = os.getenv("DIAGNOSES_XML", os.path.join(DATA_DIR, "diagnoses.xml"))
SCHEMA_SQL      = os.getenv("SCHEMA_SQL", os.path.join(SCRIPT_DIR, "database_schema.sql"))

DB_HOST         = os.getenv("DB_HOST", "db")
DB_PORT         = int(os.getenv("DB_PORT", "5432"))
DB_NAME         = os.getenv("DB_NAME", "patient_db")
DB_USER         = os.getenv("DB_USER", "user")
DB_PASSWORD     = os.getenv("DB_PASSWORD", "password")

# Fail fast or skip invalid rows?
STRICT_DATES     = bool(int(os.getenv("STRICT_DATES", "0")))    # 1 to drop invalid dates, 0 to set NULL
MAX_FUTURE_YEARS = int(os.getenv("MAX_FUTURE_YEARS", "3"))      # guardrail for future timestamps
LOAD_MODE        = os.getenv("LOAD_MODE", "upsert").lower()     # append | truncate | upsert

# Expected columns for data validation
EXPECTED_PATIENT_COLS = ["patient_id","given_name","family_name","date_of_birth","sex","height","weight"]
EXPECTED_ENCOUNTER_COLS = ["encounter_id","patient_id","admit_dt","discharge_dt","encounter_type","source_file"]

# Logging setup
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# Global buffer for Data Quality events
DQ_BUFFER: List[Dict[str, Any]] = []

# Regexes for cleaning and parsing
_ctrl_re = re.compile(r"[\x00-\x1F\x7F]")        # control chars
_ws_re = re.compile(r"\s+")                     # collapse whitespace runs
_height_cm_re   = re.compile(r"^\s*(?P<num>[\d\.]+)\s*(?:cm)?\s*$", re.I)
_height_in_re   = re.compile(r"^\s*(?P<num>[\d\.]+)\s*(?:in|inch|inches|\")\s*$", re.I)
_height_ftin_re = re.compile(
    r"""^\s*
        (?P<ft>\d+(?:\.\d+)?)       # 5 or 5.5
        \s*(?:ft|feet|')\s*
        (?:
            (?P<inch>\d+(?:\.\d+)?)\s*(?:in|inch|inches|")?
        )?
        \s*$""",
    re.I | re.X
)
_weight_re = re.compile(r"^\s*(?P<num>[\d\.]+)\s*(?P<Unit>kg|lb)?\s*$", re.I)

# -----------------------------------------------------------------------------
# DQ Logging & Basic Cleaning Helpers
# -----------------------------------------------------------------------------

def dq(file_name: str, row_id: Optional[str], column: Optional[str], value_seen: Optional[str], reason: str):
    """Append a data-quality event (later bulk-inserted) and log a warning."""
    DQ_BUFFER.append({
        "file_name": file_name,
        "row_id": None if row_id is None else str(row_id),
        "column_name": column,
        "value_seen": None if value_seen is None else str(value_seen),
        "reason": reason
    })
    logger.warning(f"DQ | {file_name} | row={row_id} | col={column} | {reason} | seen={value_seen}")

def clean_str(x) -> Optional[str]:
    """Basic string cleaning: strip whitespace and return None for Na/empty string."""
    if pd.isna(x):
        return None
    s = str(x).strip()
    return s if s != "" else None

# -----------------------------------------------------------------------------
# DB Utils
# -----------------------------------------------------------------------------

def mk_engine() -> Engine:
    """Create a SQLAlchemy engine for the PostgreSQL database."""
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, pool_pre_ping=True)

def ensure_schema(engine: Engine):
    """Execute the schema SQL script to ensure tables exist."""
    if not os.path.exists(SCHEMA_SQL):
        raise FileNotFoundError(f"Schema SQL not found: {SCHEMA_SQL}")
    with engine.begin() as cxn, open(SCHEMA_SQL, "r", encoding="utf-8") as f:
        # Use a transaction for DDL to ensure atomicity
        cxn.execute(text(f.read()))
    logger.info("Database schema ensured.")

def load_df(engine: Engine, df: pd.DataFrame, table: str):
    """Load a DataFrame to a specified database table using 'append' mode."""
    if df.empty:
        logger.info(f"Skip load: {table} (0 rows).")
        return
    # Use pandas to_sql for efficient bulk insertion
    df.to_sql(table, engine, if_exists="append", index=False, method="multi", chunksize=1000)
    logger.info(f"Loaded {len(df)} rows into {table}.")

def fetch_existing_keys(engine, table: str, key_col: str) -> set:
    """Fetch all primary keys from a table in the database."""
    sql = f'SELECT "{key_col}" FROM "{table}";'
    with engine.begin() as cxn:
        rows = cxn.execute(text(sql)).fetchall()
    return {r[0] for r in rows}

# -----------------------------------------------------------------------------
# Transformation Helpers (used in transform.py)
# -----------------------------------------------------------------------------

def sanitize_text(val: Any, max_len: int, file_name: str, row_id: str, col: str) -> Optional[str]:
    """Trim, remove control chars, collapse whitespace, truncate to max_len; log truncation."""
    if pd.isna(val):
        dq(file_name, row_id, col, None, "Missing value; set NULL.")
        return None
    s = str(val)
    if _ctrl_re.search(s):
        dq(file_name, row_id, col, s, "Control characters removed.")
    s = _ctrl_re.sub("", s).strip()
    s = _ws_re.sub(" ", s)
    if len(s) > max_len:
        dq(file_name, row_id, col, s, f"Value length {len(s)} exceeds {max_len}; truncated.")
        s = s[:max_len]
    if s == "":
        dq(file_name, row_id, col, "", "Empty string after cleaning; set NULL.")
        return None
    return s

def titlecase_or_none(s: Optional[str]) -> Optional[str]:
    """Apply title case to a string or return None if input is None."""
    return None if s is None else s.strip().title()

def normalize_sex(s: Optional[str], file_name: str, row_id: str) -> Optional[str]:
    """Normalize sex to 'M', 'F', or 'U' (Unknown), logging non-standard values."""
    if s is None:
        dq(file_name, row_id, "sex", None, "Missing sex; set to 'U'.")
        return "U"
    c = s.strip().upper()
    if c in {"M", "F"}:
        return c
    dq(file_name, row_id, "sex", s, "Unknown sex value; normalized to 'U'.")
    return "U"

def parse_datetime_any(s: Optional[str], file_name: str, row_id: str, col: str) -> Optional[pd.Timestamp]:
    """Robustly parse a datetime string, validate against future cutoff, and return UTC Timestamp."""
    if s is None:
        dq(file_name, row_id, col, None, "Missing datetime; set NULL.")
        return None
    raw = str(s).strip()
    try:
        dt = dtparser.parse(raw)
        future_cutoff = datetime.now(timezone.utc).replace(microsecond=0)
        # Assume UTC if no timezone info is present
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        
        # Check for unrealistic future dates
        if (dt - future_cutoff).days > 365 * MAX_FUTURE_YEARS:
            dq(file_name, row_id, col, raw, f"Unrealistic future datetime (> {MAX_FUTURE_YEARS} years). Set NULL.")
            return None
        return pd.to_datetime(dt)
    except Exception:
        dq(file_name, row_id, col, raw, "Invalid datetime format; set NULL.")
        return None

def to_height_cm(val: Optional[str], file_name: str, row_id: str) -> Optional[float]:
    """Convert various height formats (cm, in, ft+in, unitless number) to centimeters (cm)."""
    if val is None:
        dq(file_name, row_id, "height", None, "Missing height; set NULL.")
        return None
    s = str(val).strip()

    # 1. Foot/Inch format (e.g., 5'10" or 5.5 ft 2 in)
    m = _height_ftin_re.match(s)
    if m:
        ft = float(m.group("ft")); inch = float(m.group("inch")) if m.group("inch") else 0.0
        cm = ft * 30.48 + inch * 2.54
        dq(file_name, row_id, "height", s, f"Converted height ft+in→cm: {s} → {round(cm,2)}")
        if 30 <= cm <= 272: return round(cm, 2) # Range check (30cm to 272cm)
        dq(file_name, row_id, "height", s, "Implausible height after ft+in conversion; set NULL.")
        return None

    # 2. Inches format (e.g., 70 in)
    m = _height_in_re.match(s)
    if m:
        inches = float(m.group("num"))
        cm = inches * 2.54
        dq(file_name, row_id, "height", s, f"Converted height in→cm: {s} → {round(cm,2)}")
        if 30 <= cm <= 272: return round(cm, 2)
        dq(file_name, row_id, "height", s, "Implausible height after inches conversion; set NULL.")
        return None

    # 3. Centimeters or Unitless Number (e.g., 178 cm or 178)
    m = _height_cm_re.match(s)
    if m:
        cm = float(m.group("num"))
        if 30 <= cm <= 272:
            if "cm" not in s.lower():
                dq(file_name, row_id, "height", s, f"Assumed centimeters for unitless value; kept as {round(cm,2)} cm.")
            return round(cm, 2)
        dq(file_name, row_id, "height", s, "Implausible cm value for height; set NULL.")
        return None

    dq(file_name, row_id, "height", s, "Unrecognized height format; set NULL.")
    return None

def to_weight_kg(s: Optional[str], file_name: str, row_id: str) -> Optional[float]:
    """Convert various weight formats (kg, lb, unitless number) to kilograms (kg)."""
    if s is None:
        dq(file_name, row_id, "weight", None, "Missing weight; set NULL.")
        return None
    m = _weight_re.match(str(s))
    if not m:
        dq(file_name, row_id, "weight", s, "Unrecognized weight format; set NULL.")
        return None
    
    val = float(m.group("num"))
    unit = m.group("Unit").lower() if m.group("Unit") else None
    
    if unit == "lb":
        kg = round(val * 0.45359237, 2)
        dq(file_name, row_id, "weight", str(s), f"Converted weight lb→kg: {s} → {kg}")
        return kg
    
    # Assumed or explicit KG
    if unit in (None, "kg"):
        if unit is None:
            dq(file_name, row_id, "weight", str(s), f"Assumed kilograms for unitless value; kept as {val} kg.")
        # Range check (2kg to 635kg - 1400lbs)
        if not (2 <= val <= 635):
            dq(file_name, row_id, "weight", str(s), "Implausible kg value for weight; set NULL.")
            return None
        return round(val, 2)
    
    dq(file_name, row_id, "weight", str(s), "Unknown weight unit; set NULL.")
    return None