import os

import duckdb
import pandas as pd
import streamlit as st

# Sibling modules: Streamlit puts the script's own directory on sys.path, so these
# resolve both under `streamlit run viz/dashboard.py` and `python viz/dashboard.py`.
from charts import BANDS, architecture_figure, band_power_figure, hypnogram_figure
from theme import PLOTLY_CONFIG, palette

DB_PATH = os.getenv("DB_PATH", "data/sleep_data.db")


st.set_page_config(
    page_title="Sleep-EDF",
    layout="wide",
    initial_sidebar_state="expanded",
)


def active_theme() -> str:
    """
    The viewer's light/dark choice. Available from Streamlit 1.52; anything older
    falls back to light rather than raising.
    """
    try:
        return "dark" if st.context.theme.get("type") == "dark" else "light"
    except Exception:
        return "light"


MODE = active_theme()
COLOURS = palette(MODE)

# Typography and chrome. Numerals are monospaced and tabular so figures line up
# and digits do not jitter between subjects. Focus rings are restored at high
# contrast because the default is easy to lose against this surface.
st.markdown(
    f"""
    <style>
      :root {{
        --ink-muted: {COLOURS["ink_muted"]};
        --rule: {COLOURS["grid"]};
        --accent: {COLOURS["series"]};
      }}
      [data-testid="stMetricValue"], [data-testid="stMetricLabel"] {{
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas,
                     "Liberation Mono", monospace;
        font-variant-numeric: tabular-nums;
        font-feature-settings: "tnum";
      }}
      [data-testid="stMetricValue"] {{ font-size: 1.45rem; font-weight: 600; }}
      [data-testid="stMetricLabel"] p {{
        font-size: 0.72rem;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: var(--ink-muted);
      }}
      .sleep-rule {{
        border-top: 1px solid var(--rule);
        margin: 1.4rem 0 0.5rem;
      }}
      .sleep-section {{
        font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
        font-size: 0.78rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: var(--ink-muted);
        margin: 0 0 0.15rem;
      }}
      /* Keyboard focus must stay obvious on every interactive element. */
      *:focus-visible {{
        outline: 2px solid var(--accent) !important;
        outline-offset: 2px !important;
      }}
      @media (prefers-reduced-motion: reduce) {{
        *, *::before, *::after {{
          animation-duration: 0.01ms !important;
          transition-duration: 0.01ms !important;
        }}
      }}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data
def get_subjects():
    """Available subjects, ordered numerically."""
    connection = duckdb.connect(DB_PATH, read_only=True)
    try:
        frame = connection.execute(
            "SELECT subject_id FROM sleep_summary ORDER BY subject_id"
        ).df()
        return frame["subject_id"].tolist()
    except duckdb.CatalogException:
        st.error("dbt models not found. Run `dbt build` first.")
        return []
    finally:
        connection.close()


@st.cache_data
def load_analysis_data(subject_id):
    """Summary row and per-epoch rows for one subject."""
    connection = duckdb.connect(DB_PATH, read_only=True)
    try:
        summary = connection.execute(
            "SELECT * FROM sleep_summary WHERE subject_id = ?", [subject_id]
        ).df()
        epochs = connection.execute(
            """
            SELECT
                epoch_idx,
                sleep_stage,
                is_stage_transition,
                is_in_sleep_period
            FROM sleep_metrics
            WHERE subject_id = ?
            ORDER BY epoch_idx
            """,
            [subject_id],
        ).df()
        return summary, epochs
    finally:
        connection.close()


def section(title: str, description: str) -> None:
    """A hairline rule, a small-caps heading, and a plain-language summary."""
    st.markdown('<div class="sleep-rule"></div>', unsafe_allow_html=True)
    st.markdown(f'<p class="sleep-section">{title}</p>', unsafe_allow_html=True)
    st.markdown(f"##### {description}")


st.markdown("### Sleep-EDF")
st.caption(
    "Sleep architecture from overnight polysomnography, PhysioNet Sleep-EDF age "
    "study. Reads the dbt marts directly from DuckDB."
)

subject_list = get_subjects()
if not subject_list:
    st.warning("No subjects found in the analytics tables.")
    st.stop()

selected_subject = st.sidebar.selectbox(
    "Subject", subject_list, help="Each subject is one overnight recording."
)
high_contrast = st.sidebar.toggle(
    "Pattern fills",
    value=False,
    help=(
        "Adds a hatch to each segment of the sleep architecture bar, so the three "
        "parts stay distinguishable without relying on colour."
    ),
)
st.sidebar.caption(f"Theme: {MODE}. Charts follow your light/dark setting.")

summary_row, epoch_df = load_analysis_data(selected_subject)
if summary_row.empty:
    st.error(f"No summary data for subject {selected_subject}.")
    st.stop()

metrics = summary_row.iloc[0]
sleep_period = epoch_df[epoch_df["is_in_sleep_period"]].copy()
if sleep_period.empty:
    st.warning(f"No scored sleep for subject {selected_subject}.")
    st.stop()

tiles = st.columns(6)
tiles[0].metric("Total sleep", f"{metrics['total_sleep_minutes'] / 60:.1f} h")
tiles[1].metric("Efficiency", f"{metrics['sleep_efficiency'] * 100:.0f}%")
tiles[2].metric("Awakenings", f"{int(metrics['number_of_awakenings'])}")
tiles[3].metric("Deep (N3)", f"{metrics['deep_sleep_percentage'] * 100:.0f}%")
tiles[4].metric("Light (N1+N2)", f"{metrics['light_sleep_percentage'] * 100:.0f}%")
tiles[5].metric("REM", f"{metrics['rem_sleep_percentage'] * 100:.0f}%")

st.caption(
    f"Sleep period {metrics['sleep_period_minutes'] / 60:.1f} h, of which "
    f"{metrics['waso_minutes']:.0f} min awake after onset, within a "
    f"{metrics['total_recording_minutes'] / 60:.1f} h recording. Percentages are "
    "shares of total sleep time."
)

onset_idx = int(sleep_period["epoch_idx"].min())

section(
    "Hypnogram",
    f"Subject {selected_subject} moved through "
    f"{int(metrics['number_of_awakenings'])} awakenings across "
    f"{metrics['sleep_period_minutes'] / 60:.1f} hours in bed.",
)
st.plotly_chart(
    hypnogram_figure(sleep_period, onset_idx, COLOURS),
    config=PLOTLY_CONFIG,
)
with st.expander("Hypnogram as a table"):
    st.dataframe(
        pd.DataFrame(
            {
                "Minutes after onset": (sleep_period["epoch_idx"] - onset_idx) * 0.5,
                "Stage": sleep_period["sleep_stage"],
            }
        ),
        hide_index=True,
        height=260,
    )

left, right = st.columns(2)

with left:
    section(
        "Sleep architecture",
        f"{metrics['deep_sleep_percentage'] * 100:.0f}% of sleep was deep (N3).",
    )
    st.plotly_chart(
        architecture_figure(metrics, high_contrast, COLOURS),
        config=PLOTLY_CONFIG,
    )
    with st.expander("Architecture as a table"):
        st.dataframe(
            pd.DataFrame(
                {
                    "Stage": ["Deep (N3)", "Light (N1+N2)", "REM"],
                    "Minutes": [
                        round(metrics["deep_sleep_minutes"]),
                        round(metrics["light_sleep_minutes"]),
                        round(metrics["rem_sleep_minutes"]),
                    ],
                    "Share of sleep": [
                        f"{metrics['deep_sleep_percentage'] * 100:.1f}%",
                        f"{metrics['light_sleep_percentage'] * 100:.1f}%",
                        f"{metrics['rem_sleep_percentage'] * 100:.1f}%",
                    ],
                }
            ),
            hide_index=True,
        )

with right:
    strongest = max(BANDS, key=lambda band: float(metrics[band[1]]))
    section(
        "Spectral power",
        f"{strongest[0]} ({strongest[2]}) dominates at "
        f"{float(metrics[strongest[1]]):.1f} dB.",
    )
    st.plotly_chart(band_power_figure(metrics, COLOURS), config=PLOTLY_CONFIG)
    with st.expander("Spectral power as a table"):
        st.dataframe(
            pd.DataFrame(
                {
                    "Band": [name for name, _, _ in BANDS],
                    "Range": [span for _, _, span in BANDS],
                    "Mean power (dB)": [
                        round(float(metrics[column]), 2) for _, column, _ in BANDS
                    ],
                }
            ),
            hide_index=True,
        )

st.markdown('<div class="sleep-rule"></div>', unsafe_allow_html=True)
with st.expander("How these numbers are derived"):
    st.markdown(
        """
Sleep-EDF recordings run for roughly 22 hours and span an entire day, and many
subjects nap. Aggregating over the whole recording therefore describes the day
rather than the night: it reports 22 hours of "time in bed", counts every
afternoon transition into wake as an awakening, and averages band power across
hours of ordinary wakefulness.

Each recording is split into sleep episodes wherever a continuous wake bout runs
longer than 60 minutes, and the episode containing the most sleep is kept. Every
metric above except the recording length is scoped to that window. The dataset
carries no lights-off annotation, so this window is the closest available proxy
for time in bed, and sleep onset latency is deliberately not reported: measured
from the start of a recording that begins mid-afternoon, it would be meaningless.

Power is the mean of a Welch periodogram over the two EEG derivations, in
decibels, so negative values are ordinary in the faster bands.

Every chart has a table view, colour is never the only channel carrying meaning,
and the palettes are checked against colour-vision-deficiency simulation in both
light and dark modes rather than picked by eye.
        """
    )
