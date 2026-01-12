import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# Configuration
st.set_page_config(page_title="Sleep Data Viewer", layout="wide")

DB_PATH = os.getenv("DB_PATH", "data/sleep_data.db")

# Data loading
@st.cache_data
def get_subjects():
    """Fetch list of available subjects from the summary mart."""
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        df = con.execute("SELECT subject_id FROM sleep_summary ORDER BY subject_id").df()
        return df["subject_id"].tolist()
    except duckdb.CatalogException:
        st.error("dbt models not found. Run `dbt run` first.")
        return []
    finally:
        con.close()

@st.cache_data
def load_analysis_data(subject_id):
    """
    Fetch data from dbt models:
    1. Summary Metrics 
    2. Epoch Metrics
    """
    con = duckdb.connect(DB_PATH, read_only=True)
    
    # Get high-level summary
    summary_query = """
        SELECT * 
        FROM sleep_summary 
        WHERE subject_id = ?
    """
    summary_df = con.execute(summary_query, [subject_id]).df()
    
    # Get epoch data
    epoch_query = """
        SELECT 
            epoch_idx, 
            sleep_stage, 
            is_stage_transition
        FROM sleep_metrics 
        WHERE subject_id = ?
        ORDER BY epoch_idx
    """
    epoch_df = con.execute(epoch_query, [subject_id]).df()
    
    con.close()
    return summary_df, epoch_df

st.title("Sleep-EDF Data Viewer")

# Sidebar
subject_list = get_subjects()

if not subject_list:
    st.warning("No subjects found in analytics tables.")
    st.stop()

selected_subject = st.sidebar.selectbox("Subject ID", subject_list)

# Load data
summary_row, epoch_df = load_analysis_data(selected_subject)

if summary_row.empty:
    st.error(f"No summary data found for Subject {selected_subject}")
    st.stop()

metrics = summary_row.iloc[0]

c1, c2, c3, c4, c5 = st.columns(5)

c1.metric("Total Sleep Time", f"{metrics['total_sleep_minutes']:.1f} min")
c2.metric("Awakenings", int(metrics['number_of_awakenings']))
c3.metric("Deep Sleep %", f"{metrics['deep_sleep_percentage'] * 100:.1f}%")
c4.metric("Light Sleep %", f"{metrics['light_sleep_percentage'] * 100:.1f}%")
c5.metric("REM Sleep %", f"{metrics['rem_sleep_percentage'] * 100:.1f}%")

# Temporal analysis
st.subheader("Sleep Staging")

# Map stages to numbers for plotting
stage_map = {"W": 0, "REM": 1, "N1": 2, "N2": 3, "N3": 4, "MOVE": 0, "NAN": 0}

epoch_df["stage_num"] = epoch_df["sleep_stage"].map(stage_map)
epoch_df["time_min"] = epoch_df["epoch_idx"] * 0.5  # 30s epochs

fig_hypno = go.Figure()
fig_hypno.add_trace(go.Scatter(
    x=epoch_df["time_min"], 
    y=epoch_df["stage_num"], 
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

# Spectral analysis
c_left, c_right = st.columns(2)

with c_left:
    st.subheader("Average Band Power (dB)")
    
    # We query the pre-calculated averages from the Mart
    power_data = {
        "Delta": metrics["avg_delta_power"],
        "Theta": metrics["avg_theta_power"],
        "Alpha": metrics["avg_alpha_power"],
        "Sigma": metrics["avg_sigma_power"],
        "Beta":  metrics["avg_beta_power"]
    }
    
    df_power = pd.DataFrame(list(power_data.items()), columns=["Band", "dB"])
    
    fig_bar = px.bar(
        df_power, 
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
    
    # Create distribution table from summary metrics
    dist_data = {
        "Stage": ["Deep (N3)", "Light (N1/N2)", "REM"],
        "Minutes": [
            metrics["deep_sleep_minutes"], 
            metrics["light_sleep_minutes"], 
            metrics["rem_sleep_minutes"]
        ],
        "Percentage": [
            f"{metrics['deep_sleep_percentage']*100:.1f}%",
            f"{metrics['light_sleep_percentage']*100:.1f}%",
            f"{metrics['rem_sleep_percentage']*100:.1f}%"
        ]
    }
    
    st.dataframe(pd.DataFrame(dist_data), use_container_width=True, hide_index=True)
