# ETL Pipeline with Data Quality Controls

This repository contains a containerized Python based Extract, Transform, and Load (ETL) pipeline designed to process patient data from multiple file formats including CSV and XML, clean the data with quality checks, load it into a PostgreSQL database, and display the results via an interactive Streamlit dashboard.

## What does this pipeline do?
* Extraction

    * CSV: patients.csv, encounters.csv with a fixer for lines that accidentally use ; instead of ,, and inner header-row detection/removal.

    * XML: diagnoses.xml with a proper namespace-aware parser.

* Transformation

    * Heights: parses cm, in, ft/in (5'11", 5 ft 11 in, 5.5 ft) → centimeters.

    * Weights: parses kg, lb → kilograms.

    * Dates: accepts mixed formats; sets timezones to UTC when missing; rejects unrealistic future timestamps.

    * Sanitization: removes control chars, trims, collapses whitespace, and truncates to schema lengths (with DQ logs for truncations).

    * Normalization: names → title case, sex values normalized (M, F, or U); encounter types uppercased.

    * De-duplication

        * Full-row duplicates dropped (exact repeats).

        * PK-level duplicates dropped keeping the last (log shows which were dropped).

    * Chronology check: logs if discharge_dt < admit_dt.

    * Header-row scrubs: drops stray header rows appearing mid-file (comma or semicolon variants).

* Loading

    * FK-safe order: patients → encounters → diagnoses.

    * Append / Truncate / Upsert-configurable behavior (see Load Modes).

    * FK filters (pre-load): encounters referencing unknown patients are dropped (logged); diagnoses referencing unknown encounters are dropped (logged).

* Data Quality logging

    * Every non-trivial transformation or decision is logged to data_quality_log (e.g., “Converted height ft+in→cm: 5'11" → 180.34” or “Append mode: skipped existing patients.”).

    * You get a durable audit trail in the DB.

* Dashboard

    * Visualizes data distributions (sex, top diagnosis codes, length-of-stay).

    * Presents DQ metrics and the full data_quality_log.

    * Uses robust groupby(...).size().reset_index(name="count") patterns to avoid column-name drift.

## 🚀 Setup and Running the Pipeline

This solution is designed to run end-to-end with minimal setup using **Docker Compose**.

### Prerequisites

You must have **Docker** and **Docker Compose** installed on your system.

### 1. Clone the Repository

```bash
git clone <your-github-repo-link>
cd etl-patient-data
```

### 2. Run
```bash
docker compose up --build
```

## Repository Structure
```
etl-patient-data/
├──  docker-compose.yml
├──  Dockerfile
├──  README.md
├──  data/
│   ├── patients.csv
│   ├── encounters.csv
│   └── diagnoses.xml
├── etl_scripts/
│   ├── etl_pipeline.py
|   ├── exctract.py
|   ├── load.py
|   ├── transform.py
|   ├── utils.py
│   └── database_schema.sql
└── dashboard/
    └── app.py
```

## Architecture & Files
| Path                               | Purpose                                                                      |
| ---------------------------------- | ---------------------------------------------------------------------------- |
| `etl_scripts/load.py`      | Executes the entire pipeline, handling setup, sequencing, integrity checks, and error logging. Connects to the database, ensures the schema exists, enforces foreign key relationships (batch + DB), and performs the final data insertion based on LOAD_MODE.                                    |
| `etl_scripts/extract.py`      |Reads raw data from CSV and XML files, applying file-level robustness (e.g., handling semicolons, inner headers).                                   |
| `etl_scripts/transform.py`      | Takes raw data, applies data quality rules, cleaning, type conversion, validation, normalization, and deduplication.                                   |
| `etl_scripts/utils.py`      | Contains shared configurations, database connection logic, and low-level data quality helpers (dq, sanitize_text, etc.).                                   |
| `etl_scripts/database_schema.sql`  | Tables & indexes: `patients`, `encounters`, `diagnoses`, `data_quality_log`. |
| `dashboard/app.py`                 | Streamlit dashboard for data & DQ visualization.                             |
| `data/`                            | Input files directory.                                                       |
| `Dockerfile`, `docker-compose.yml` | Containerization & orchestration.                                            |

## Database schema

* patients: patient_id (PK), given_name, family_name, sex, dob, height_cm, weight_kg

* encounters: encounter_id (PK), patient_id (FK), admit_dt, discharge_dt, encounter_type, source_file

* diagnoses: diagnosis_id (PK), encounter_id (FK), code, system, is_primary, recorded_at

* data_quality_log: log_id, ts, file_name, row_id, column_name, value_seen, reason

## Load Modes

* truncate - Full refresh. Truncates tables (with RESTART IDENTITY) then loads current batch.

* append - Inserts only new PKs (anti-join vs DB). Existing keys are skipped (logged).

* upsert - Recommended for iterative loads. If you swapped in the upsert functions, existing keys are updated instead of skipped. (The current script appends; you can re-enable the upsert loaders if desired.)

Can be set via environment: LOAD_MODE={truncate|append|upsert}

## Data Quality Checks

The pipeline adds a row to data_quality_log whenever it makes or detects a non-trivial decision:

* Sanitization

    * Removed control characters

    * Collapsed whitespace

    * Truncated values exceeding schema lengths (with original length)

    * Empty-after-clean → set NULL

* Missing values

    * PKs (rows dropped)

    * Sex missing/unknown → normalized to U

    * Height/Weight/DOB missing → set NULL

* Units & conversions

    * Height: ft+in → cm, in → cm, assumed cm for unitless numeric

    * Weight: lb → kg, assumed kg for unitless numeric

    * Implausible values (height not in 30–272 cm, weight not in 2–635 kg) → set NULL

* Dates

    * Invalid formats → NULL

    * Future dates beyond MAX_FUTURE_YEARS → NULL

    * Missing → NULL

* Chronology

    * discharge_dt < admit_dt (logged; values kept unless you choose to null)

* Delimiters / headers

    * Semicolon-delimited lines converted to commas

    * Inner header rows (comma/semicolon variants) dropped

* Duplicates

    * Exact full-row duplicates dropped

    * PK-level duplicates dropped (kept last)

* FK filters

    * Encounters dropped if patient missing

    * Diagnoses dropped if encounter missing

* Append anti-join

    * Existing patients/encounters skipped in append mode (logged)

## Dashboard

Open http://localhost:8501.

* Data Quality & Inconsistency Log
Pie chart by source, reasons table, and the full data_quality_log grid.

* Data Distribution

    * Patients: sex distribution, age histogram, height/weight histograms

    * Encounters: type distribution, length-of-stay histogram, rows per source file

    * Diagnoses: top 10 codes, primary vs secondary split

* Raw Data Viewer
    * Browse any table directly.