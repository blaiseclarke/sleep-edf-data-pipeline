import os
import logging
import numpy as np
import pandas as pd
import mne
from dotenv import load_dotenv
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
STUDY = os.getenv("STUDY", "age").lower()  # Options: age, telemetry


def fetch_data(subjects, recording):
    """
    Fetches raw Sleep-EDF data files from PhysioNet.

    Args:
        subjects (list[int]): List of subject IDs to fetch.
        recording (list[int]): List of recording indices (1 or 2).

    Returns:
        list[list[str]]: A list of [psg_path, hypnogram_path] pairs for each subject.
    """
    if STUDY == "telemetry":
        return fetch_telemetry_data(
            subjects=subjects, recording=recording, on_missing="warn"
        )
    return fetch_age_data(subjects=subjects, recording=recording, on_missing="warn")


# Mapping
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_subject(subject_id):
    """
    Orchestrates the extraction, signal processing, and feature engineering for a single subject.

    Logic:
    1. Fetches raw data (with automatic fallback to Recording 2 if Recording 1 is missing).
    2. Loads EDF files using MNE.
    3. Standardizes channel names.
    4. Segments data into 30s epochs.
    5. Computes Power Spectral Density (PSD) for 5 frequency bands.
    6. Returns a structured DataFrame.

    Args:
        subject_id (int): The unique identifier of the subject.

    Returns:
        pd.DataFrame or None: A DataFrame containing epoch-level features, or None if no data found.
    """
    filepaths = fetch_data(subjects=[subject_id], recording=[RECORDING])
    if not filepaths:
        logger.warning(
            f"Subject {subject_id} missing recording {RECORDING}. Attempting fallback to recording {RECORDING + 1}."
        )
        filepaths = fetch_data(subjects=[subject_id], recording=[RECORDING + 1])

    if not filepaths:
        return None

    psg_file = filepaths[0][0]
    hypnogram_file = filepaths[0][1]

    # Loading raw data
    raw = mne.io.read_raw_edf(psg_file, preload=True, verbose=False)
    annotations = mne.read_annotations(hypnogram_file)
    raw.set_annotations(annotations, emit_warning=False)

    # Renaming channel names to match our schema.
    # Standardizes channel names to ensure consistency across different studies.
    mapping = {
        "EEG Fpz-Cz": "EEG",
        "EEG Pz-Oz": "EEG2",
        "EOG horizontal": "EOG",
        "EMG submental": "EMG",
    }
    map = {k: v for k, v in mapping.items() if k in raw.ch_names}
    raw.rename_channels(map)

    # Building epochs
    # Slices the continuous signal into 30-second windows (standard for sleep scoring).
    events, event_id = mne.events_from_annotations(
        raw, event_id=None, chunk_duration=EPOCH_LENGTH
    )

    tmax = 30.0 - 1.0 / raw.info["sfreq"]
    epochs = mne.Epochs(
        raw=raw,
        events=events,
        event_id=event_id,
        tmin=0.0,
        tmax=tmax,
        baseline=None,
        verbose=False,
        preload=True,
        on_missing="ignore",
    )

    # Calculating power across bands
    spectrum = epochs.compute_psd(picks=["eeg"])
    psd, frequencies = spectrum.get_data(return_freqs=True)

    # Create dataframe
    df = pd.DataFrame()
    df["epoch_idx"] = range(len(epochs))
    df["subject_id"] = subject_id
    df["sleep_stage_label"] = epochs.events[:, 2]

    # Map integers back to strings
    inverse_map = {v: k for k, v in event_id.items()}
    df["sleep_stage_label"] = df["sleep_stage_label"].map(inverse_map)

    # Cleaning sleep stage names
    df["stage"] = df["sleep_stage_label"].apply(lambda x: SLEEP_STAGE_MAP.get(x, x))

    # Add power to dataframe
    df["delta_power"] = calculate_band_power(psd, frequencies, 0.5, 4)
    df["theta_power"] = calculate_band_power(psd, frequencies, 4, 8)
    df["alpha_power"] = calculate_band_power(psd, frequencies, 8, 12)
    df["sigma_power"] = calculate_band_power(psd, frequencies, 12, 16)
    df["beta_power"] = calculate_band_power(psd, frequencies, 16, 30)

    columns = [
        "subject_id",
        "epoch_idx",
        "stage",
        "delta_power",
        "theta_power",
        "alpha_power",
        "sigma_power",
        "beta_power",
    ]
    return df[columns]


def calculate_band_power(psd, frequencies, fmin, fmax):
    """
    Calculates the mean power within a specific frequency band.

    Args:
        psd (np.ndarray): Power Spectral Density array (epochs, channels, freqs).
        frequencies (np.ndarray): Array of frequencies corresponding to PSD.
        fmin (float): Lower bound of the frequency band.
        fmax (float): Upper bound of the frequency band.

    Returns:
        np.ndarray: Mean power for the band across all epochs/channels.
    """
    idx = np.logical_and(frequencies >= fmin, frequencies <= fmax)

    power = psd[:, :, idx].mean(axis=(1, 2)) * 1e12
    return power


def main():
    all_subjects = []

    for id in range(STARTING_SUBJECT, ENDING_SUBJECT + 1):
        try:
            subject = process_subject(id)
            if subject is not None:
                all_subjects.append(subject)
        except Exception as e:
            print(f"Error processing subject {id}: {e}")

    if all_subjects:
        final_df = pd.concat(all_subjects, ignore_index=True)
        # Export data to output file
        output = "sleep_data_debug.csv"
        final_df.to_csv(output, index=False)
        logger.info(
            f"Processed {len(final_df)} epochs from {len(all_subjects)} subjects."
        )
    else:
        logger.info("No data processed.")


if __name__ == "__main__":
    main()
