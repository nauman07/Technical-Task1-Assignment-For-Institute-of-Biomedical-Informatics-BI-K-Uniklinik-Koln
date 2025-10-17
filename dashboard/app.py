import os
from datetime import date

import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2

# - 1) DB config (env-driven, matches ETL)
DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "patient_db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

st.set_page_config(page_title="ETL Data Quality Dashboard", layout="wide")
st.title("Patient ETL • Data Quality & Distribution")

# - Helpers ---------------------------------
def get_db_connection():
    try:
        return psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return None

@st.cache_data(show_spinner=False, ttl=60)
def fetch_table(table_name: str) -> pd.DataFrame:
    conn = get_db_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql(f'SELECT * FROM "{table_name}";', conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return pd.DataFrame()

def safe_age(dob: pd.Timestamp) -> float | None:
    try:
        if pd.isna(dob):
            return None
        b = pd.to_datetime(dob).date()
        today = date.today()
        return (today - b).days / 365.25
    except Exception:
        return None

def safe_los(admit, discharge) -> float | None:
    try:
        if pd.isna(admit) or pd.isna(discharge):
            return None
        delta = pd.to_datetime(discharge) - pd.to_datetime(admit)
        return delta.total_seconds() / 3600.0  # hours
    except Exception:
        return None

# - 2) Load data ------------------------------─
df_patients   = fetch_table("patients")          # patient_id, given_name, family_name, sex, dob, height_cm, weight_kg
df_encounters = fetch_table("encounters")        # encounter_id, patient_id, admit_dt, discharge_dt, encounter_type, source_file
df_diagnoses  = fetch_table("diagnoses")         # diagnosis_id, encounter_id, code, system, is_primary, recorded_at
df_dq         = fetch_table("data_quality_log")  # log_id, ts, file_name, row_id, column_name, value_seen, reason

if df_patients.empty and df_encounters.empty and df_diagnoses.empty:
    st.warning("No data has been loaded into the database yet.")
    st.stop()

# - 3) Data Quality Overview ------------------------─
st.header("1) Data Quality & Inconsistency Log")

if df_dq.empty:
    st.success("No data quality inconsistencies were logged during the ETL process.")
else:
    top_kpis = st.columns(3)
    top_kpis[0].metric("Total Logged Issues", len(df_dq))
    top_kpis[1].metric("Files With Issues", df_dq["file_name"].nunique())
    top_kpis[2].metric("Unique Reasons", df_dq["reason"].nunique())

    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Issues by Source File")
        issues_by_file = (
            df_dq.groupby("file_name", dropna=False).size().reset_index(name="count")
        )
        fig1 = px.pie(issues_by_file, names="file_name", values="count", hole=0.35)
        st.plotly_chart(fig1, use_container_width=True)

    with c2:
        st.subheader("Reasons for Inconsistency")
        reasons = (
            df_dq.groupby("reason", dropna=False).size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        st.dataframe(reasons, use_container_width=True, height=320)

    st.subheader("Detailed Quality Log")
    st.dataframe(df_dq.sort_values("ts", ascending=False), use_container_width=True, height=320)

st.markdown("---")

# - 4) Data Distribution --------------------------─
st.header("2) Data Distribution Analysis")

table_choice = st.selectbox("Select table:", ["patients", "encounters", "diagnoses"])

# Patients
if table_choice == "patients" and not df_patients.empty:
    st.subheader("Patients")

    # Sex distribution (stable naming)
    if "sex" in df_patients.columns:
        sex_counts = (
            df_patients.assign(sex=df_patients["sex"].fillna("U"))
            .groupby("sex", dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        st.plotly_chart(
            px.bar(sex_counts, x="sex", y="count", title="Sex Distribution"),
            use_container_width=True,
        )

    # Age distribution
    if "dob" in df_patients.columns:
        ages = df_patients["dob"].apply(safe_age).dropna()
        if not ages.empty:
            st.plotly_chart(
                px.histogram(ages, nbins=20, title="Age Distribution (years)").update_xaxes(title="Age (years)"),
                use_container_width=True,
            )

    # Height/Weight
    hw_cols = st.columns(2)
    if "height_cm" in df_patients.columns and df_patients["height_cm"].notna().any():
        hw_cols[0].plotly_chart(
            px.histogram(df_patients, x="height_cm", nbins=20, title="Height (cm)"),
            use_container_width=True,
        )
    if "weight_kg" in df_patients.columns and df_patients["weight_kg"].notna().any():
        hw_cols[1].plotly_chart(
            px.histogram(df_patients, x="weight_kg", nbins=20, title="Weight (kg)"),
            use_container_width=True,
        )

# Encounters
elif table_choice == "encounters" and not df_encounters.empty:
    st.subheader("Encounters")

    # Encounter type distribution 
    if "encounter_type" in df_encounters.columns:
        type_counts = (
            df_encounters.assign(encounter_type=df_encounters["encounter_type"].fillna("UNKNOWN"))
            .groupby("encounter_type", dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        st.plotly_chart(
            px.bar(type_counts, x="encounter_type", y="count", title="Encounter Type Distribution"),
            use_container_width=True,
        )

    # Length of stay (hours)
    if {"admit_dt", "discharge_dt"}.issubset(df_encounters.columns):
        los_hours = df_encounters.apply(
            lambda r: safe_los(r.get("admit_dt"), r.get("discharge_dt")), axis=1
        ).dropna()
        if not los_hours.empty:
            st.plotly_chart(
                px.histogram(los_hours, nbins=30, title="Length of Stay (hours)").update_xaxes(title="Hours"),
                use_container_width=True,
            )

    # Source file counts 
    if "source_file" in df_encounters.columns:
        src_counts = (
            df_encounters.assign(source_file=df_encounters["source_file"].fillna("UNKNOWN"))
            .groupby("source_file", dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        st.plotly_chart(
            px.bar(src_counts, x="source_file", y="count", title="Rows per Source File"),
            use_container_width=True,
        )

# Diagnoses
elif table_choice == "diagnoses" and not df_diagnoses.empty:
    st.subheader("Diagnoses")

    # Top codes
    if "code" in df_diagnoses.columns:
        top_codes = (
            df_diagnoses.dropna(subset=["code"])
            .groupby("code")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
            .head(10)
        )
        st.plotly_chart(
            px.bar(top_codes, x="code", y="count", title="Top 10 Diagnosis Codes"),
            use_container_width=True,
        )

    # Primary vs secondary 
    if "is_primary" in df_diagnoses.columns:
        prim_counts = (
            df_diagnoses.assign(primary=df_diagnoses["is_primary"].fillna(False).map({True: "Primary", False: "Secondary"}))
            .groupby("primary")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        st.plotly_chart(
            px.pie(prim_counts, names="primary", values="count", hole=0.35, title="Primary vs Secondary"),
            use_container_width=True,
        )

st.markdown("---")

# - 5) Raw Data Viewer ---------------------------─
st.header("3) Raw Data Viewer")

raw_choice = st.selectbox(
    "Choose a table to view:",
    ["patients", "encounters", "diagnoses", "data_quality_log"],
)

if raw_choice == "patients":
    st.dataframe(df_patients, height=320, use_container_width=True)
elif raw_choice == "encounters":
    st.dataframe(df_encounters, height=320, use_container_width=True)
elif raw_choice == "diagnoses":
    st.dataframe(df_diagnoses, height=320, use_container_width=True)
else:
    st.dataframe(df_dq, height=320, use_container_width=True)
