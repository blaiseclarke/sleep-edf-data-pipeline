import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# --- Configuration & setup ---
st.set_page_config(
    page_title="Sleep Analysis Dashboard",
    page_icon="💤",
    layout="wide",
    initial_sidebar_state="expanded",
)   

DB_PATH = os.getenv("DB_PATH", "data/sleep_data.db")

# --- Database connection ---
@st.cache_resource
def get_connection():
    """Establishes a connection to DuckDB."""
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        return conn
    except Exception as e:
        st.error(f"Failed to connect to database at {DB_PATH}: {e}")
        return None

conn = get_connection()

# --- Data loading ---
@st.cache_data
def get_subject_ids():
    """Fetches list of available subject IDs."""
    if not conn:
        return []
    try:
        df = conn.execute("SELECT DISTINCT subject_id FROM sleep_summary ORDER BY subject_id").fetchdf()
        return df["subject_id"].tolist()
    except Exception as e:
        st.error(f"Error fetching subjects: {e}")
        return []

@st.cache_data
def get_subject_summary(subject_id):
    """Fetches summary metrics for a specific subject."""
    if not conn:
        return None
    query = """
    SELECT 
        total_recording_minutes,
        number_of_awakenings,
        deep_sleep_percentage,
        light_sleep_percentage,
        rem_sleep_percentage
    FROM sleep_summary
    WHERE subject_id = ?
    """
    try:
        df = conn.execute(query, [subject_id]).fetchdf()
        return df.iloc[0] if not df.empty else None
    except Exception as e:
        st.error(f"Error fetching summary for subject {subject_id}: {e}")
        return None

@st.cache_data
def get_subject_features(subject_id):
    """Fetches time-series features for a specific subject."""
    if not conn:
        return pd.DataFrame()
    query = """
    SELECT 
        epoch_id,
        sleep_stage,
        delta_beta_ratio_z,
        theta_alpha_ratio_z
    FROM sleep_features
    WHERE subject_id = ?
    ORDER BY epoch_id
    """
    try:
        df = conn.execute(query, [subject_id]).fetchdf()
        
        # Infers transition logic via change detection
        # Models epoch ID as integer index (0-150) for plotting
        if not df.empty:
            # Check if epoch_id is consistent, otherwise reset index
            df = df.reset_index(drop=True)
            df['epoch_idx'] = df.index
            
            # Identify transitions
            df['prev_stage'] = df['sleep_stage'].shift(1)
            df['is_stage_transition'] = (df['sleep_stage'] != df['prev_stage']) & (df['prev_stage'].notna())
            
        return df
    except Exception as e:
        st.error(f"Error fetching features for subject {subject_id}: {e}")
        return pd.DataFrame()

# --- App layout ---

# Sidebar
st.sidebar.title("Sleep Analysis 🔬")
subject_ids = get_subject_ids()

if not subject_ids:
    st.sidebar.warning("No data found in database.")
    st.stop()

selected_subject = st.sidebar.selectbox("Select Subject ID", subject_ids)

# Main dashboard
st.title(f"Sleep Report: Subject {selected_subject}")

# Fetch data
summary = get_subject_summary(selected_subject)
features = get_subject_features(selected_subject)

if summary is None or features.empty:
    st.info("No data for this subject.")
    st.stop()

# Columns
col1, col2 = st.columns([1, 1])

# --- Column 1: Sleep Architecture ---
with col1:
    st.header("Sleep Architecture")
    
    # Creates dataframe for chart
    arch_data = pd.DataFrame({
        "Stage": ["Deep Sleep (N3)", "Light Sleep (N1/N2)", "REM Sleep"],
        "Percentage": [
            summary["deep_sleep_percentage"],
            summary["light_sleep_percentage"],
            summary["rem_sleep_percentage"]
        ]
    })
    
    # Normalizes if user data isn't perfectly 100% (handling potential small drift)
    # Requirement asks for raw percentages, so plots raw values
    
    fig_arch = px.bar(
        arch_data, 
        x="Stage", 
        y="Percentage", 
        color="Stage",
        color_discrete_map={
            "Deep Sleep (N3)": "#003f5c",
            "Light Sleep (N1/N2)": "#bc5090", 
            "REM Sleep": "#ffa600"
        },
        range_y=[0, 100],
        title="Sleep Stage Distribution"
    )
    fig_arch.update_layout(showlegend=False)
    st.plotly_chart(fig_arch, use_container_width=True)
    
    # Metrics
    m1, m2 = st.columns(2)
    m1.metric("Awakenings", f"{int(summary['number_of_awakenings'])}")
    m2.metric("Total Recording", f"{summary['total_recording_minutes']:.1f} min")


# --- Column 2: Sleep stability ---
with col2:
    st.header("Sleep stability")
    
    # Sleep hypnogram / transitions
    # Maps stages to categorical values for visualization.
    # Uses a step chart style to visualize discrete stage changes over time.
    
    # Define color map for stages
    stage_colors = {
        "W": "gray",
        "N1": "lightblue",
        "N2": "blue",
        "N3": "darkblue",
        "REM": "purple",
        "MOVE": "black",
        "NAN": "red"
    }

    fig_hypno = go.Figure()
    
    # Use a scatter plot with "hv" line shape to create a step chart look
    # Plotly handles categorical strings on the Y axis automatically.
    fig_hypno.add_trace(go.Scatter(
        x=features["epoch_idx"],
        y=features["sleep_stage"],
        mode="lines+markers",
        line_shape="hv",
        name="Stage",
        line=dict(color="black", width=1),
        marker=dict(size=4)
    ))
    
    # Highlight transitions
    transitions = features[features['is_stage_transition']]
    if not transitions.empty:
        fig_hypno.add_trace(go.Scatter(
            x=transitions["epoch_idx"],
            y=transitions["sleep_stage"],
            mode="markers",
            name="Transition",
            marker=dict(color="red", symbol="x", size=8)
        ))

    # Fix y-axis order to be scientific (W at top, N3 at bottom)
    fig_hypno.update_yaxes(categoryorder="array", categoryarray=["W", "REM", "N1", "N2", "N3"])
    fig_hypno.update_layout(
        title="Sleep Hypnogram & Transitions",
        xaxis_title="Epoch (30s)",
        yaxis_title="Stage",
        height=300
    )
    st.plotly_chart(fig_hypno, use_container_width=True)
    
    # Power ratios (z-scored)
    fig_power = go.Figure()
    
    fig_power.add_trace(go.Scatter(
        x=features["epoch_idx"],
        y=features["delta_beta_ratio_z"],
        mode="lines",
        name="Delta/Beta (Z)",
        line=dict(color="teal")
    ))
    
    fig_power.add_trace(go.Scatter(
        x=features["epoch_idx"],
        y=features["theta_alpha_ratio_z"],
        mode="lines",
        name="Theta/Alpha (Z)",
        line=dict(color="orange")
    ))
    
    fig_power.update_layout(
        title="Normalized Power Ratios (Z-Score)",
        xaxis_title="Epoch (30s)",
        yaxis_title="Z-Score",
        height=300
    )
    st.plotly_chart(fig_power, use_container_width=True)

