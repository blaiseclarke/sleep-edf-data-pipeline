import logging
import os

import mne
import numpy as np
import pandas as pd
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
        subjects (list[int]): Which subjects to download (e.g., [1, 2]).
        recording (list[int]): Which recording session to grab (1 is the baseline, 2 is often the fallout).

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
        # Sometimes Recording 1 is corrupt or missing. Fallback to Recording 2
        # to ensure *some* data is retrieved for this subject, rather than failing entirely.
        logger.warning(
            f"Subject {subject_id} missing recording {RECORDING}. Attempting fallback to recording {RECORDING + 1}."
        )
        filepaths = fetch_data(subjects=[subject_id], recording=[RECORDING + 1])

    if not filepaths:
        return None

    psg_file = filepaths[0][0]
    hypnogram_file = filepaths[0][1]

    # Loading raw data
    raw = mne.io.read_raw_edf(psg_file, preload=False, verbose=False)
    annotations = mne.read_annotations(hypnogram_file)
    raw.set_annotations(annotations, emit_warning=False)

    # Rename channels to standard names so downstream analysis doesn't care if
    # the original file used "EEG Fpz-Cz" or just "Fpz".
    # Aim for a unified schema: EEG, EOG, EMG.
    mapping = {
        "EEG Fpz-Cz": "EEG",
        "EEG Pz-Oz": "EEG2",
        "EOG horizontal": "EOG",
        "EMG submental": "EMG",
    }
    map = {k: v for k, v in mapping.items() if k in raw.ch_names}
    raw.rename_channels(map)

    # Identify sleep stages from the hypnogram.
    # Chunk the data into 30s epochs because that's the clinical standard for sleep scoring.
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

    # Calculate Power Spectral Density (PSD) using Welch's Method.
    # Use a 2.56s window which gives ~0.39 Hz frequency resolution.
    # This is fine-grained enough to separate Delta (0.5-4Hz) from Theta (4-8Hz).
    spectrum = epochs.compute_psd(
        method="welch", picks=["eeg"], fmin=0.5, fmax=30.0, n_fft=256, n_per_seg=256
    )
    psd, frequencies = spectrum.get_data(return_freqs=True)

    # Creates dataframe
    df = pd.DataFrame()
    df["epoch_idx"] = range(len(epochs))
    df["subject_id"] = subject_id
    df["sleep_stage_label"] = epochs.events[:, 2]

    # Maps integers back to strings
    inverse_map = {v: k for k, v in event_id.items()}
    df["sleep_stage_label"] = df["sleep_stage_label"].map(inverse_map)

    # Cleans sleep stage names
    df["stage"] = df["sleep_stage_label"].apply(lambda x: SLEEP_STAGE_MAP.get(x, x))

    # Extract power for specific frequency bands.
    # Calculate Total Power (Area Under the Curve) and convert it to dB.
    # This keeps values in a manageable range (e.g., 30-60 dB) rather than massive uV^2 numbers.
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
    Calculates the mean power within a specific frequency band in dB.

    Methodology:
    1. Select frequency bins that fall within the requested band (e.g., 8-12 Hz for Alpha).
    2. Integrate the density (sum * resolution) to get Total Power (uV^2).
    3. Convert to Decibels (dB) using 10 * log10(Power).

    Args:
        psd (np.ndarray): Power Spectral Density array (epochs, channels, freqs).
        frequencies (np.ndarray): Array of frequencies corresponding to PSD.
        fmin (float): Lower bound of the frequency band.
        fmax (float): Upper bound of the frequency band.

    Returns:
        np.ndarray: Mean power (dB) for the band across all epochs (averaged across channels).
    """
    idx = np.logical_and(frequencies >= fmin, frequencies <= fmax)

    # Frequency resolution (bin width)
    freq_res = frequencies[1] - frequencies[0]

    # Integration: Sum of density * frequency resolution (dx).
    # This converts "Density" (uV^2/Hz) back into physical "Power" (uV^2).
    # Multiply by 1e12 to convert Volts^2 to microVolts^2 (more human readable).
    band_power = psd[:, :, idx].sum(axis=2) * freq_res * 1e12

    # Avoid log(0) - though unlikely with real EEG
    band_power = np.maximum(band_power, 1e-10)

    # Coversion to Decibels (dB)
    band_power_db = 10 * np.log10(band_power)

    # Average the power across all selected EEG channels (Fpz-Cz and Pz-Oz).
    # In a more complex study, Frontal and Occipital might be analyzed separately,
    # but for this portfolio summary, a global average is sufficient.
    return band_power_db.mean(axis=1)


def main():
    all_subjects = []

    for id in range(STARTING_SUBJECT, ENDING_SUBJECT + 1):
        try:
            subject = process_subject(id)
            if subject is not None:
                all_subjects.append(subject)
        except Exception as e:
            logger.error(f"Error processing subject {id}: {e}")

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
