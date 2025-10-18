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
    """Making null value in sex as 'U' (Unknown). and logging it."""
    if s is None:
        dq(file_name, row_id, "sex", None, "Missing sex; set to 'U'.")
        return "U"
    else:
        c = s.strip().upper()
        return c

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

# --- Plausible Ranges for Cross-Unit Guessing ---
# Typical adult human range (e.g., from 1 ft to 8 ft 11 in)
HEIGHT_CM_MIN, HEIGHT_CM_MAX = 30.0, 272.0 # 1 ft to 8 ft 11 in
HEIGHT_IN_MIN, HEIGHT_IN_MAX = 12.0, 107.0 # 1 ft to 8 ft 11 in
# Typical adult human range (e.g., from 4 lbs to 1400 lbs)
WEIGHT_KG_MIN, WEIGHT_KG_MAX = 2.0, 635.0  # ~4.4 lbs to 1400 lbs
WEIGHT_LB_MIN, WEIGHT_LB_MAX = 4.4, 1400.0 # 4.4 lbs to 1400 lbs

# --- Helper function for unitless number check ---
def _try_float(s: str) -> Optional[float]:
    """Safely convert a string to a float if it is a simple number."""
    try:
        if not re.match(r'^\s*[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?\s*$', s):
            return None
        return float(s)
    except ValueError:
        return None


def to_height_cm(val: Optional[str], file_name: str, row_id: str) -> Optional[float]:
    """Convert various height formats (cm, in, ft+in, unitless number) to centimeters (cm) 
    with improved unitless guessing based on plausible ranges."""
    if val is None:
        dq(file_name, row_id, "height", None, "Missing height; set NULL.")
        return None
    s = str(val).strip()

    # 1. Foot/Inch format
    m = _height_ftin_re.match(s)
    if m:
        ft = float(m.group("ft")); inch = float(m.group("inch")) if m.group("inch") else 0.0
        cm = ft * 30.48 + inch * 2.54
        dq(file_name, row_id, "height", s, f"Converted height ft+in→cm: {s} → {round(cm,2)}")
        if HEIGHT_CM_MIN <= cm <= HEIGHT_CM_MAX: return round(cm, 2)
        dq(file_name, row_id, "height", s, "Implausible height after ft+in conversion; set NULL.")
        return None

    # 2. Inches format
    m = _height_in_re.match(s)
    if m:
        inches = float(m.group("num"))
        cm = inches * 2.54
        dq(file_name, row_id, "height", s, f"Converted height in→cm: {s} → {round(cm,2)}")
        if HEIGHT_CM_MIN <= cm <= HEIGHT_CM_MAX: return round(cm, 2)
        dq(file_name, row_id, "height", s, "Implausible height after inches conversion; set NULL.")
        return None

    # 3. Centimeters format (explicit 'cm' unit)
    m = _height_cm_re.match(s)
    if m and any(unit in s.lower() for unit in ["cm", "centimeters"]):
        cm = float(m.group("num"))
        if HEIGHT_CM_MIN <= cm <= HEIGHT_CM_MAX:
            return round(cm, 2)
        dq(file_name, row_id, "height", s, "Implausible explicit cm value for height; set NULL.")
        return None
    
    # 4. Unitless Number - NEW HEURISTIC
    unitless_val = _try_float(s)
    if unitless_val is not None:
        
        # Check if it fits the plausible CM range (default assumption)
        if HEIGHT_CM_MIN <= unitless_val <= HEIGHT_CM_MAX:
            dq(file_name, row_id, "height", s, f"Assumed centimeters (in CM range); kept as {round(unitless_val,2)} cm.")
            return round(unitless_val, 2)
        
        # Check if it fits the plausible INCH range
        if HEIGHT_IN_MIN <= unitless_val <= HEIGHT_IN_MAX:
            cm_guessed = round(unitless_val * 2.54, 2)
            dq(file_name, row_id, "height", s, f"Assumed inches (in IN range); converted to {cm_guessed} cm.")
            return cm_guessed

        # If it doesn't fit a plausible range for either, it's implausible
        dq(file_name, row_id, "height", s, "Implausible unitless value for height (not in CM or IN range); set NULL.")
        return None

    dq(file_name, row_id, "height", s, "Unrecognized height format; set NULL.")
    return None


def to_weight_kg(s: Optional[str], file_name: str, row_id: str) -> Optional[float]:
    """Convert various weight formats (kg, lb, unitless number) to kilograms (kg)
    with improved unitless guessing based on plausible ranges."""
    if s is None:
        dq(file_name, row_id, "weight", None, "Missing weight; set NULL.")
        return None
    
    m = _weight_re.match(str(s))
    
    # 1. Explicit Unit (kg or lb)
    if m:
        val = float(m.group("num"))
        unit = m.group("Unit").lower() if m.group("Unit") else None
        
        if unit == "lb":
            kg = round(val * 0.45359237, 2)
            if WEIGHT_KG_MIN <= kg <= WEIGHT_KG_MAX:
                dq(file_name, row_id, "weight", str(s), f"Converted weight lb→kg: {s} → {kg}")
                return kg
            dq(file_name, row_id, "weight", str(s), "Implausible weight after lb→kg conversion; set NULL.")
            return None
        
        # Explicit KG
        if unit == "kg":
            if WEIGHT_KG_MIN <= val <= WEIGHT_KG_MAX:
                return round(val, 2)
            dq(file_name, row_id, "weight", str(s), "Implausible explicit kg value for weight; set NULL.")
            return None
    
    # 2. Unitless Number - NEW HEURISTIC
    unitless_val = _try_float(str(s))
    if unitless_val is not None:
        
        # Check if it fits the plausible KG range (default assumption)
        if WEIGHT_KG_MIN <= unitless_val <= WEIGHT_KG_MAX:
            dq(file_name, row_id, "weight", str(s), f"Assumed kilograms (in KG range); kept as {round(unitless_val, 2)} kg.")
            return round(unitless_val, 2)
        
        # Check if it fits the plausible LB range
        if WEIGHT_LB_MIN <= unitless_val <= WEIGHT_LB_MAX:
            kg_guessed = round(unitless_val * 0.45359237, 2)
            dq(file_name, row_id, "weight", str(s), f"Assumed pounds (in LB range); converted to {kg_guessed} kg.")
            return kg_guessed

        # If it doesn't fit a plausible range for either, it's implausible
        dq(file_name, row_id, "weight", str(s), "Implausible unitless value for weight (not in KG or LB range); set NULL.")
        return None
    
    # Falls through if no explicit unit matched and not a simple number
    dq(file_name, row_id, "weight", s, "Unrecognized weight format; set NULL.")
    return None
