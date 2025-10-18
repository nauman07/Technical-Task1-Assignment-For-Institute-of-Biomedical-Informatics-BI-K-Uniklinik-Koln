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
st.header("1) Data Quality & Inconsistency Log ")

# Ensure 'ts' is a proper datetime object for filtering
df_dq['ts'] = pd.to_datetime(df_dq['ts'], errors='coerce')

if df_dq.empty:
    st.success("No data quality inconsistencies were logged during the ETL process.")
else:
    # 1. Sidebar Filters for Data Quality Log (NEW)
    st.sidebar.header("Filter Quality Log")
    
    # 1.1 File Name Filter
    all_files = df_dq["file_name"].unique().tolist()
    selected_files = st.sidebar.multiselect(
        "Filter by Source File:",
        options=all_files,
        default=all_files,
        key="dq_file_filter"
    )

    # 1.2 Timestamp Date Range Picker (Non-Slider Filter)
    # Use .dt.tz_convert(None) to drop the timezone before getting date for picker min/max
    # Streamlit's date input does not handle timezone-aware dates well.
    date_col = df_dq['ts'].dt.tz_convert(None).dt.date.dropna() 
    
    if not date_col.empty:
        min_date = date_col.min()
        max_date = date_col.max()
        
        st.sidebar.markdown("---")
        st.sidebar.subheader("Filter by Date Range:")
        
        start_date_filter = st.sidebar.date_input(
            "Start Date:",
            value=min_date,
            min_value=min_date,
            max_value=max_date,
            key="dq_start_date"
        )
        
        end_date_filter = st.sidebar.date_input(
            "End Date:",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
            key="dq_end_date"
        )
        
        # --- FIX APPLIED HERE ---
        # 1. Convert date inputs to timezone-naive datetime objects
        start_ts_naive = pd.to_datetime(start_date_filter)
        end_ts_naive = pd.to_datetime(end_date_filter) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        
        # 2. Localize the naive timestamps to UTC to match the df_dq['ts'] column
        start_ts = start_ts_naive.tz_localize('UTC')
        end_ts = end_ts_naive.tz_localize('UTC')
    else:
        # If no dates exist, use the full (empty) range
        start_ts, end_ts = df_dq['ts'].min(), df_dq['ts'].max()


    # Apply Filters (This line now works because start_ts and end_ts are UTC-aware Timestamps)
    df_dq_filtered = df_dq[
        (df_dq["file_name"].isin(selected_files)) &
        (df_dq["ts"] >= start_ts) &
        (df_dq["ts"] <= end_ts)
    ]
    
    st.info(f"Displaying **{len(df_dq_filtered)}** issues from a total of {len(df_dq)} in the selected range.")

    # 2. Key Performance Indicators (KPIs)
    top_kpis = st.columns(3)
    top_kpis[0].metric("Total Logged Issues", len(df_dq_filtered))
    top_kpis[1].metric("Files With Issues", df_dq_filtered["file_name"].nunique())
    top_kpis[2].metric("Unique Reasons", df_dq_filtered["reason"].nunique())

    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Issues by Source File (Filtered)")
        issues_by_file = (
            df_dq_filtered.groupby("file_name", dropna=False).size().reset_index(name="count")
        )
        # Use a more modern Plotly template
        fig1 = px.pie(
            issues_by_file, 
            names="file_name", 
            values="count", 
            hole=0.4, # Larger hole for donut chart
            title="Distribution by File",
            template="plotly"
        )
        fig1.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig1, use_container_width=True)

    with c2:
        st.subheader("Top Reasons for Inconsistency (Filtered)")
        reasons = (
            df_dq_filtered.groupby("reason", dropna=False).size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
            .head(10) # Show top 10 reasons
        )
        # Use a bar chart for better comparison
        fig2 = px.bar(
            reasons, 
            x="count", 
            y="reason", 
            orientation='h',
            title="Top 10 Inconsistency Reasons",
            template="ggplot2"
        )
        fig2.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig2, use_container_width=True)
        
    st.subheader("Detailed Quality Log (Filtered)")
    # Show the full filtered dataframe
    # To display nicely, we can temporarily remove the timezone for display purposes
    df_display = df_dq_filtered.copy()
    df_display['ts'] = df_display['ts'].dt.tz_convert(None) 
    st.dataframe(df_display.sort_values("ts", ascending=False), use_container_width=True, height=320)

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

    # Make sure date columns are datetime objects before filtering
    df_encounters['admit_dt'] = pd.to_datetime(df_encounters['admit_dt'], errors='coerce')
    df_encounters['discharge_dt'] = pd.to_datetime(df_encounters['discharge_dt'], errors='coerce')

    # Filtering Sidebar for Encounters
    st.sidebar.header("Filter Encounters Data")

    # 1. Encounter Type Filter
    available_types = df_encounters["encounter_type"].fillna("UNKNOWN").unique().tolist()
    selected_types = st.sidebar.multiselect(
        "Filter by Encounter Type:",
        options=available_types,
        default=available_types,
        key="type_filter"
    )

    # 2. Source File Filter
    available_sources = df_encounters["source_file"].fillna("UNKNOWN").unique().tolist()
    selected_sources = st.sidebar.multiselect(
        "Filter by Source File:",
        options=available_sources,
        default=available_sources,
        key="source_filter"
    )
    
    date_col = df_encounters["admit_dt"].dt.tz_convert(None).dt.date.dropna() 
    
    start_date_filter = None
    end_date_filter = None
    
    if not date_col.empty:
        min_date = date_col.min()
        max_date = date_col.max()
        
        # Streamlit slider provides naive datetime.date objects
        date_range = st.sidebar.slider(
            "Filter by Admission Date Range:",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, max_date),
            format="YYYY-MM-DD",
            key="admit_date_range"
        )
        start_date_filter, end_date_filter = date_range[0], date_range[1]
        
        # --- FIX: Convert naive filter dates to UTC-aware Timestamps ---
        # Start date is the beginning of the day (00:00:00)
        start_date = pd.to_datetime(start_date_filter).tz_localize('UTC')
        # End date is the end of the day (23:59:59.999...)
        end_date = (pd.to_datetime(end_date_filter) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)).tz_localize('UTC')
    else:
        start_date, end_date = None, None # Keep filter variables as None if no data

    # Apply Filters
    df_encounters_filtered = df_encounters.assign(
        encounter_type=df_encounters["encounter_type"].fillna("UNKNOWN"),
        source_file=df_encounters["source_file"].fillna("UNKNOWN")
    ).copy() 

    df_encounters_filtered = df_encounters_filtered[
        df_encounters_filtered["encounter_type"].isin(selected_types) &
        df_encounters_filtered["source_file"].isin(selected_sources)
    ]

    # Apply Date Filter
    if start_date is not None and end_date is not None:
        # This comparison now works because 'admit_dt' and the filter variables 
        # (start_date, end_date) are both timezone-aware (UTC).
        df_encounters_filtered = df_encounters_filtered[
            (df_encounters_filtered['admit_dt'] >= start_date) & 
            (df_encounters_filtered['admit_dt'] <= end_date)
        ]

    st.info(f"Displaying {len(df_encounters_filtered)} encounters based on filters.")

    # --- Distribution Plots within the filtered range ---
    
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
        # Admissions and Discharges Over Time 
        if start_date is not None and end_date is not None:
            st.subheader("Admissions & Discharges Over Time")
            
            # Count admissions per day (Normalize to date after filtering)
            admissions = df_encounters_filtered['admit_dt'].dt.normalize().value_counts().rename('Admissions')
            # Count discharges per day
            discharges = df_encounters_filtered['discharge_dt'].dt.normalize().value_counts().rename('Discharges')
            
            # Combine and fill missing days with 0
            ts_data = pd.concat([admissions, discharges], axis=1).fillna(0)
            ts_data.index.name = 'Date'
            
            # To plot correctly, convert the index to be timezone-naive for Plotly
            ts_data.index = ts_data.index.tz_convert(None) 
            ts_data = ts_data.reset_index()

            # Plot using Plotly Express
            fig_ts = px.line(
                ts_data, 
                x='Date', 
                y=['Admissions', 'Discharges'], 
                title='Daily Patient Flow',
                template='plotly',
                markers=True # Add markers for better visibility
            )
            st.plotly_chart(fig_ts, use_container_width=True)

        # Length of stay (hours) - Moved to C1 bottom
        if {"admit_dt", "discharge_dt"}.issubset(df_encounters_filtered.columns):
            los_hours = df_encounters_filtered.apply(
                lambda r: safe_los(r.get("admit_dt"), r.get("discharge_dt")), axis=1
            ).dropna()
            if not los_hours.empty:
                # Max LOS Quantile filter (still available for the filtered data)
                max_los = los_hours.quantile(st.slider("Max LOS Quantile:", min_value=0.5, max_value=1.0, value=0.99, step=0.01, key="los_quantile_filtered"))
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
