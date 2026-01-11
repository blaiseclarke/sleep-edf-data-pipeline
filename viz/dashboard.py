import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# --- Configuration ---
st.set_page_config(page_title="Sleep Data Viewer", layout="wide")

DB_PATH = os.getenv("DB_PATH", "data/sleep_data.db")

# --- Data loading ---
@st.cache_data
def get_subjects():
    """Fetch list of available subjects."""
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("SELECT DISTINCT SUBJECT_ID FROM SLEEP_EPOCHS ORDER BY SUBJECT_ID").df()
    con.close()
    return df["SUBJECT_ID"].tolist()

@st.cache_data
def load_subject_data(subject_id):
    """Fetch all data for a single subject."""
    con = duckdb.connect(DB_PATH, read_only=True)
    query = """
        SELECT * 
        FROM SLEEP_EPOCHS 
        WHERE SUBJECT_ID = ?
        ORDER BY EPOCH_IDX
    """
    df = con.execute(query, [subject_id]).df()
    con.close()
    return df

st.title("Sleep-EDF Data Viewer")

# Sidebar
subject_list = get_subjects()
selected_subject = st.sidebar.selectbox("Subject ID", subject_list)

if selected_subject is None:
    st.text("No data found.")
    st.stop()

# Load subject data
df = load_subject_data(selected_subject)

# Calculate metrics
total_epochs = len(df)
tst_pages = df[df["STAGE"] != "W"]
tst_minutes = (len(tst_pages) * 30) / 60

# Sleep architecture
counts = df["STAGE"].value_counts()
total_sleep_epochs = len(tst_pages)

if total_sleep_epochs > 0:
    deep_pct = (counts.get("N3", 0) / total_sleep_epochs) * 100
    light_pct = ((counts.get("N1", 0) + counts.get("N2", 0)) / total_sleep_epochs) * 100
    rem_pct = (counts.get("REM", 0) / total_sleep_epochs) * 100
else:
    deep_pct = light_pct = rem_pct = 0.0

# Awakenings (bouts of Wake after sleep onset)
df["prev_stage"] = df["STAGE"].shift(1)
awakenings = len(df[(df["STAGE"] == "W") & (df["prev_stage"] != "W") & (df["prev_stage"].notna())])

c1, c2, c3, c4, c5 = st.columns(5)

c1.metric("Total Sleep Time", f"{tst_minutes:.1f} min")
c2.metric("Awakenings", awakenings)
c3.metric("Deep Sleep %", f"{deep_pct:.1f}%")
c4.metric("Light Sleep %", f"{light_pct:.1f}%")
c5.metric("REM Sleep %", f"{rem_pct:.1f}%")

# --- Temporal analysis ---
st.subheader("Sleep Staging")

# Map stages to numbers for plotting
stage_map = {"W": 0, "REM": 1, "N1": 2, "N2": 3, "N3": 4, "MOVE": 0, "NAN": 0}

df["stage_num"] = df["STAGE"].map(stage_map)
df["time_min"] = df.index * 0.5

fig_hypno = go.Figure()
fig_hypno.add_trace(go.Scatter(
    x=df["time_min"], 
    y=df["stage_num"], 
    mode='lines',
    line_shape='hv',
    name="Stage",
    line=dict(color='black', width=1)
))

fig_hypno.update_layout(
    yaxis=dict(
        tickmode='array',
        tickvals=[0, 1, 2, 3, 4],
        ticktext=["Wake", "REM", "N1", "N2", "N3"],
        autorange="reversed",
        showgrid=True,
        gridcolor='lightgrey'
    ),
    xaxis_title="Time (Minutes)",
    xaxis=dict(showgrid=True, gridcolor='lightgrey'),
    plot_bgcolor='white',
    height=300,
    margin=dict(l=0, r=0, t=20, b=0)
)
st.plotly_chart(fig_hypno, use_container_width=True)

# --- Spectral analysis ---
c_left, c_right = st.columns(2)

with c_left:
    st.subheader("Average Band Power (dB)")
    
    power_cols = ["DELTA_POWER", "THETA_POWER", "ALPHA_POWER", "SIGMA_POWER", "BETA_POWER"]
    avg_powers = df[power_cols].mean().reset_index()
    avg_powers.columns = ["Band", "dB"]
    
    # Simple grey bars
    fig_bar = px.bar(
        avg_powers, 
        x='Band', 
        y='dB',
        text_auto='.1f'
    )
    fig_bar.update_traces(marker_color='slategrey')
    fig_bar.update_layout(
        plot_bgcolor='white',
        xaxis=dict(showgrid=True, gridcolor='lightgrey'),
        yaxis=dict(showgrid=True, gridcolor='lightgrey')
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with c_right:
    st.subheader("Stage Distribution")
    
    stage_counts = df["STAGE"].value_counts().reset_index()
    stage_counts.columns = ["Stage", "Count"]
    
    # Simple table instead of chart
    st.dataframe(stage_counts, use_container_width=True, hide_index=True)
