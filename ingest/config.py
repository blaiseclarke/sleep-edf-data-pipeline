import os
from dotenv import load_dotenv
from pathlib import Path
from mne.datasets.sleep_physionet.age import fetch_data as fetch_age_data
from mne.datasets.sleep_physionet.temazepam import fetch_data as fetch_telemetry_data

# Load environment variables
load_dotenv()

# Configuration
STARTING_SUBJECT = int(os.getenv("STARTING_SUBJECT", 0))
ENDING_SUBJECT = int(os.getenv("ENDING_SUBJECT", 10))
RECORDING = int(os.getenv("RECORDING", 1))
EPOCH_LENGTH = float(os.getenv("EPOCH_LENGTH", 30.0))  # seconds
DB_PATH = os.getenv("DB_PATH", "data/sleep_data.db")
STAGING_DIR = Path(os.getenv("STAGING_DIR", "data/staging"))
STUDY = os.getenv("STUDY", "age").lower()  # Options: age, telemetry

# Shared mapping
SLEEP_STAGE_MAP = {
    "Sleep stage W": "W",
    "Sleep stage 1": "N1",
    "Sleep stage 2": "N2",
    "Sleep stage 3": "N3",
    "Sleep stage 4": "N3",
    "Sleep stage R": "REM",
    "Movement time": "MOVE",
    "Sleep stage ?": "NAN",
}


def fetch_data(subjects, recording):
    """
    Fetches raw Sleep-EDF data files from PhysioNet.

    Args:
        subjects (list[int]): Which subjects to download (ex. [1, 2]).
        recording (list[int]): Which recording session to grab (1 is the default, 2 is often the fallout).

    Returns:
        list[list[str]]: A list of [psg_path, hypnogram_path] pairs for each subject.
    """
    if STUDY == "telemetry":
        return fetch_telemetry_data(subjects=subjects, on_missing="warn")
    return fetch_age_data(subjects=subjects, recording=recording, on_missing="warn")
