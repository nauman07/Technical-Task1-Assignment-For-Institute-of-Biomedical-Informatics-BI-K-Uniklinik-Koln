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

table_choice = st.selectbox("Select table:", ["patients", "encounters", "diagnoses"], key="dist_table_select")

# --- Patients Distribution Analysis ---
if table_choice == "patients" and not df_patients.empty:
    st.subheader("Patients Distribution")
    
    # Filtering Sidebar for Patients
    st.sidebar.header("Filter Patients Data")
    
    # Sex Filter
    available_sexes = df_patients["sex"].fillna("U").unique().tolist()
    selected_sexes = st.sidebar.multiselect(
        "Filter by Sex:",
        options=available_sexes,
        default=available_sexes,
        key="sex_filter"
    )
    
    # Apply Sex Filter
    df_patients_filtered = df_patients.assign(sex=df_patients["sex"].fillna("U"))
    df_patients_filtered = df_patients_filtered[df_patients_filtered["sex"].isin(selected_sexes)]
    
    st.info(f"Displaying {len(df_patients_filtered)} patients based on filters.")
    
    c1, c2 = st.columns(2)
    
    with c1:
        # Sex distribution (stable naming) - Use Plotly's bright 'Plotly' template
        if "sex" in df_patients_filtered.columns:
            sex_counts = (
                df_patients_filtered
                .groupby("sex", dropna=False)
                .size()
                .reset_index(name="count")
                .sort_values("count", ascending=False)
            )
            fig_sex = px.bar(
                sex_counts,
                x="sex",
                y="count",
                title="Sex Distribution",
                color="sex", # Add color for liveliness
                template="plotly" # Use a modern template
            )
            fig_sex.update_layout(xaxis_title="Sex (U=Unknown)", yaxis_title="Count")
            st.plotly_chart(fig_sex, use_container_width=True)

    with c2:
        # Age distribution
        if "dob" in df_patients_filtered.columns:
            ages = df_patients_filtered["dob"].apply(safe_age).dropna()
            if not ages.empty:
                # Add a filter for the number of bins
                n_bins = st.slider("Age Histogram Bins:", min_value=10, max_value=50, value=20, step=5, key="age_bins")
                fig_age = px.histogram(
                    ages,
                    nbins=n_bins, # Use dynamic bin count
                    title="Age Distribution (years)",
                    template="plotly",
                    marginal="box" # Add a box plot for summary stats
                ).update_xaxes(title="Age (years)")
                st.plotly_chart(fig_age, use_container_width=True)

    # Height/Weight
    hw_cols = st.columns(2)
    if "height_cm" in df_patients_filtered.columns and df_patients_filtered["height_cm"].notna().any():
        hw_cols[0].plotly_chart(
            px.histogram(
                df_patients_filtered.dropna(subset=["height_cm"]),
                x="height_cm", 
                nbins=20, 
                title="Height (cm)",
                template="plotly",
                color_discrete_sequence=px.colors.qualitative.Pastel # Change color scheme
            ),
            use_container_width=True,
        )
    if "weight_kg" in df_patients_filtered.columns and df_patients_filtered["weight_kg"].notna().any():
        hw_cols[1].plotly_chart(
            px.histogram(
                df_patients_filtered.dropna(subset=["weight_kg"]), 
                x="weight_kg", 
                nbins=20, 
                title="Weight (kg)",
                template="plotly",
                color_discrete_sequence=px.colors.qualitative.D3 # Change color scheme
            ),
            use_container_width=True,
        )

# --- Encounters Distribution Analysis ---
elif table_choice == "encounters" and not df_encounters.empty:
    st.subheader("Encounters Distribution ")
    
    # Filtering Sidebar for Encounters
    st.sidebar.header("Filter Encounters Data")

    # Encounter Type Filter
    available_types = df_encounters["encounter_type"].fillna("UNKNOWN").unique().tolist()
    selected_types = st.sidebar.multiselect(
        "Filter by Encounter Type:",
        options=available_types,
        default=available_types,
        key="type_filter"
    )

    # Source File Filter
    available_sources = df_encounters["source_file"].fillna("UNKNOWN").unique().tolist()
    selected_sources = st.sidebar.multiselect(
        "Filter by Source File:",
        options=available_sources,
        default=available_sources,
        key="source_filter"
    )

    # Apply Filters
    df_encounters_filtered = df_encounters.assign(
        encounter_type=df_encounters["encounter_type"].fillna("UNKNOWN"),
        source_file=df_encounters["source_file"].fillna("UNKNOWN")
    )
    df_encounters_filtered = df_encounters_filtered[
        df_encounters_filtered["encounter_type"].isin(selected_types) &
        df_encounters_filtered["source_file"].isin(selected_sources)
    ]
    
    st.info(f"Displaying {len(df_encounters_filtered)} encounters based on filters.")
    
    # Encounter type distribution 
    if "encounter_type" in df_encounters_filtered.columns:
        type_counts = (
            df_encounters_filtered
            .groupby("encounter_type", dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        fig_type = px.bar(
            type_counts, 
            x="encounter_type", 
            y="count", 
            title="Encounter Type Distribution",
            color="encounter_type", # Color by type
            template="seaborn" # A different lively template
        )
        fig_type.update_layout(xaxis_title="Encounter Type", yaxis_title="Count")
        st.plotly_chart(fig_type, use_container_width=True)

    c1, c2 = st.columns(2)
    
    with c1:
        # Length of stay (hours)
        if {"admit_dt", "discharge_dt"}.issubset(df_encounters_filtered.columns):
            los_hours = df_encounters_filtered.apply(
                lambda r: safe_los(r.get("admit_dt"), r.get("discharge_dt")), axis=1
            ).dropna()
            if not los_hours.empty:
                # Add a filter for max LOS for better visualization
                max_los = los_hours.quantile(st.slider("Max LOS Quantile:", min_value=0.5, max_value=1.0, value=0.99, step=0.01, key="los_quantile"))
                los_filtered = los_hours[los_hours <= max_los]
                
                fig_los = px.histogram(
                    los_filtered, 
                    nbins=30, 
                    title=f"Length of Stay (hours) - Max: {max_los:.2f}h",
                    template="plotly_dark", # Use a dark theme for contrast
                ).update_xaxes(title="Hours")
                st.plotly_chart(fig_los, use_container_width=True)

    with c2:
        # Source file counts 
        if "source_file" in df_encounters_filtered.columns:
            src_counts = (
                df_encounters_filtered
                .groupby("source_file", dropna=False)
                .size()
                .reset_index(name="count")
                .sort_values("count", ascending=False)
            )
            fig_src = px.bar(
                src_counts, 
                x="source_file", 
                y="count", 
                title="Rows per Source File",
                color_discrete_sequence=["#FF69B4"] # Pink for liveliness
            )
            fig_src.update_layout(xaxis_title="Source File", yaxis_title="Count")
            st.plotly_chart(fig_src, use_container_width=True)

# --- Diagnoses Distribution Analysis ---
elif table_choice == "diagnoses" and not df_diagnoses.empty:
    st.subheader("Diagnoses Distribution")
    
    # Filtering Sidebar for Diagnoses
    st.sidebar.header("Filter Diagnoses Data")

    # Primary/Secondary Filter
    is_primary_map = {True: "Primary", False: "Secondary", None: "Unknown"}
    available_primaries = ["Primary", "Secondary", "Unknown"]
    selected_primaries = st.sidebar.multiselect(
        "Filter by Primary/Secondary Status:",
        options=available_primaries,
        default=available_primaries,
        format_func=lambda x: x,
        key="primary_filter"
    )
    
    # Apply Filter
    df_diagnoses_filtered = df_diagnoses.assign(
        primary=df_diagnoses["is_primary"].map({True: "Primary", False: "Secondary"}).fillna("Unknown")
    )
    df_diagnoses_filtered = df_diagnoses_filtered[df_diagnoses_filtered["primary"].isin(selected_primaries)]
    
    st.info(f"Displaying {len(df_diagnoses_filtered)} diagnoses based on filters.")
    
    c1, c2 = st.columns(2)
    
    with c1:
        # Top codes
        if "code" in df_diagnoses_filtered.columns:
            top_n = st.slider("Top N Codes:", min_value=5, max_value=25, value=10, step=5, key="top_codes_n")
            top_codes = (
                df_diagnoses_filtered.dropna(subset=["code"])
                .groupby("code")
                .size()
                .reset_index(name="count")
                .sort_values("count", ascending=False)
                .head(top_n)
            )
            fig_top_codes = px.bar(
                top_codes, 
                x="count", 
                y="code", # Horizontal bar chart is better for labels
                orientation='h',
                title=f"Top {top_n} Diagnosis Codes",
                template="ggplot2", # Another visually distinct template
                color="count", # Color intensity by count
                color_continuous_scale=px.colors.sequential.Viridis
            )
            fig_top_codes.update_layout(yaxis={'categoryorder': 'total ascending'}) # Order ascending for visual flow
            st.plotly_chart(fig_top_codes, use_container_width=True)

    with c2:
        # Primary vs secondary 
        if "is_primary" in df_diagnoses_filtered.columns:
            prim_counts = (
                df_diagnoses_filtered
                .groupby("primary")
                .size()
                .reset_index(name="count")
                .sort_values("count", ascending=False)
            )
            fig_prim = px.pie(
                prim_counts, 
                names="primary", 
                values="count", 
                hole=0.4, # Slightly larger hole
                title="Primary vs Secondary Status",
                template="none", # Minimal template for pie
                color_discrete_sequence=px.colors.qualitative.Vivid # Bright color scheme
            )
            fig_prim.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_prim, use_container_width=True)

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
