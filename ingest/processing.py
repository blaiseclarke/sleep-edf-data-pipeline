from typing import Dict, Generator, cast

import mne
import numpy as np
import pandas as pd

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


def batch_process_file(
    subject_id: int, psg_path: str, hypno_path: str, batch_size: int = 100
) -> Generator[pd.DataFrame, None, None]:
    """
    Generator that streams EEG data from disk, processes it in chunks,
    and yields small DataFrames. Memory usage remains constant.
    """

    # 1. Lazy Load (Metadata only)
    raw = mne.io.read_raw_edf(psg_path, preload=False, verbose=False)

    # 2. Standardize Channels
    mapping = {
        "EEG Fpz-Cz": "EEG",
        "EEG Pz-Oz": "EEG2",
        "EOG horizontal": "EOG",
        "EMG submental": "EMG",
    }
    actual_map = {k: v for k, v in mapping.items() if k in raw.ch_names}
    raw.rename_channels(actual_map)

    # 3. Load Annotations
    annotations = mne.read_annotations(hypno_path)
    raw.set_annotations(annotations, emit_warning=False)

    events, event_id = mne.events_from_annotations(
        raw, chunk_duration=30.0, verbose=False
    )

    # 4. Lazy Epochs
    epochs = mne.Epochs(
        raw=raw,
        events=events,
        event_id=event_id,
        tmin=0.0,
        tmax=30.0,
        baseline=None,
        preload=False,
        on_missing="ignore",
        verbose=False,
    )

    total_epochs = len(epochs)

    # 5. Generator Loop
    for start_idx in range(0, total_epochs, batch_size):
        end_idx = min(start_idx + batch_size, total_epochs)

        # LOAD BATCH
        batch_epochs = epochs[start_idx:end_idx]
        batch_epochs.load_data()

        # TRANSFORM
        # MNE stub bug: fmin/fmax are floats, but stubs sometimes demand int.
        spectrum = batch_epochs.compute_psd(
            method="welch",
            fmin=0.5,  # type: ignore
            fmax=30.0,
            verbose=False,
        )
        psd, freqs = spectrum.get_data(return_freqs=True)

        # FORMAT
        df_batch = _features_to_dataframe(
            psd=psd,
            freqs=freqs,
            epochs=batch_epochs,
            subject_id=subject_id,
            event_id=event_id,
            start_index=start_idx,
        )

        yield df_batch


def _features_to_dataframe(
    psd: np.ndarray,
    freqs: np.ndarray,
    epochs: mne.Epochs,
    subject_id: int,
    event_id: Dict[str, int],
    start_index: int,
) -> pd.DataFrame:
    df = pd.DataFrame()
    batch_length = len(epochs)

    # Ensure epoch_idx is continuous
    df["epoch_idx"] = range(start_index, start_index + batch_length)
    df["subject_id"] = subject_id

    # Extract labels
    df["sleep_stage_label"] = epochs.events[:, 2]

    # Map integers back to strings
    inverse_map = {v: k for k, v in event_id.items()}
    # Pandas stub bug: .map() accepts dicts, but strict typing misses this overload
    df["sleep_stage_label"] = df["sleep_stage_label"].map(inverse_map)  # type: ignore

    df["stage"] = df["sleep_stage_label"].apply(lambda x: SLEEP_STAGE_MAP.get(x, x))

    # Power Calculation
    df["delta_power"] = calculate_band_power(psd, freqs, 0.5, 4)
    df["theta_power"] = calculate_band_power(psd, freqs, 4, 8)
    df["alpha_power"] = calculate_band_power(psd, freqs, 8, 12)
    df["sigma_power"] = calculate_band_power(psd, freqs, 12, 16)
    df["beta_power"] = calculate_band_power(psd, freqs, 16, 30)

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

    # Cast to silence "Series | DataFrame" ambiguity
    return cast(pd.DataFrame, df[columns])


def calculate_band_power(
    psd: np.ndarray, frequencies: np.ndarray, fmin: float, fmax: float
) -> np.ndarray:
    idx = np.logical_and(frequencies >= fmin, frequencies <= fmax)
    freq_res = frequencies[1] - frequencies[0]
    band_power = psd[:, :, idx].sum(axis=2) * freq_res * 1e12
    band_power = np.maximum(band_power, 1e-10)
    band_power_db = 10 * np.log10(band_power)
    return band_power_db.mean(axis=1)
