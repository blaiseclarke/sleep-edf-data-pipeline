import logging
from collections.abc import Generator
from typing import cast

import mne
import numpy as np
import numpy.typing as npt
import pandas as pd

from ingest.config import SLEEP_STAGE_MAP

logger = logging.getLogger(__name__)


def batch_process_file(
    subject_id: int, psg_path: str, hypno_path: str, batch_size: int = 100
) -> Generator[pd.DataFrame, None, None]:
    """
    Generator that streams EEG data from disk, processes it in chunks,
    and yields small DataFrames. Memory usage remains constant.
    """

    # Lazy loading EDF
    raw = mne.io.read_raw_edf(psg_path, preload=False, verbose=None)

    # Standardize channels
    mapping = {
        "EEG Fpz-Cz": "EEG",
        "EEG Pz-Oz": "EEG2",
        "EOG horizontal": "EOG",
        "EMG submental": "EMG",
    }
    actual_map = {k: v for k, v in mapping.items() if k in raw.ch_names}
    raw.rename_channels(actual_map)

    # The EDF reader labels every channel in these files as EEG, respiration
    # and rectal temperature included. Correcting the types lets MNE select
    # real EEG for the spectral work instead of relying on name matching.
    channel_types = {
        "EOG": "eog",
        "EMG": "emg",
        "Resp oro-nasal": "resp",
        "Temp rectal": "temperature",
        "Event marker": "misc",
    }
    # Retyping temperature and the event marker moves them off volts, which is
    # the point rather than a problem, so don't warn once per subject about it.
    raw.set_channel_types(
        {k: v for k, v in channel_types.items() if k in raw.ch_names},
        on_unit_change="ignore",
        verbose=False,
    )

    # Load annotations
    annotations = mne.read_annotations(hypno_path)
    raw.set_annotations(annotations, emit_warning=False)

    events, event_id = mne.events_from_annotations(
        raw, chunk_duration=30.0, verbose=False
    )

    # Lazy epochs
    epochs = mne.Epochs(
        raw=raw,
        events=events,
        event_id=event_id,
        tmin=0.0,
        tmax=30.0 - 1.0 / raw.info["sfreq"],
        baseline=None,
        preload=False,
        on_missing="ignore",
        verbose=False,
    )

    total_epochs = len(events)

    # Only EEG carries the band power we extract, so restrict the PSD to it
    # rather than transforming all seven channels and discarding five.
    if len(mne.pick_types(raw.info, eeg=True)) > 0:
        psd_picks = "eeg"
    else:
        # Fall back to every data channel rather than failing outright
        logger.warning(
            "Subject %d: no EEG channels found, computing power across all channels",
            subject_id,
        )
        psd_picks = None

    # Generator loop
    for start_idx in range(0, total_epochs, batch_size):
        end_idx = min(start_idx + batch_size, total_epochs)

        # Load batch
        batch_epochs = epochs[start_idx:end_idx]
        batch_epochs.load_data()

        # Transform
        # MNE bug: fmin/fmax are floats, but stubs sometimes demand int
        spectrum = batch_epochs.compute_psd(
            method="welch",
            fmin=0.5,  # type: ignore
            fmax=30.0,
            picks=psd_picks,
            verbose=False,
        )
        psd, freqs = spectrum.get_data(return_freqs=True)

        # Format
        # Channel names come from the spectrum, not the epochs, so that they
        # always line up with the channel axis of `psd`
        df_batch = _features_to_dataframe(
            psd=psd,
            freqs=freqs,
            epochs=batch_epochs,
            ch_names=spectrum.ch_names,
            subject_id=subject_id,
            event_id=event_id,
            start_index=start_idx,
        )

        yield df_batch


def _features_to_dataframe(
    psd: npt.NDArray[np.float64],
    freqs: npt.NDArray[np.float64],
    epochs: mne.Epochs,
    ch_names: list[str],
    subject_id: int,
    event_id: dict[str, int],
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

    df["stage"] = df["sleep_stage_label"].apply(lambda x: SLEEP_STAGE_MAP.get(x, "NAN"))

    # Power calculation
    df["delta_power"] = calculate_band_power(psd, freqs, ch_names, 0.5, 4)
    df["theta_power"] = calculate_band_power(psd, freqs, ch_names, 4, 8)
    df["alpha_power"] = calculate_band_power(psd, freqs, ch_names, 8, 12)
    df["sigma_power"] = calculate_band_power(psd, freqs, ch_names, 12, 16)
    df["beta_power"] = calculate_band_power(psd, freqs, ch_names, 16, 30)

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

    # Drop epochs that are not valid sleep stages (ex. Movement time, Unscored)
    # This ensures only W, N1, N2, N3, REM are passed to validation and downstream analysis
    initial_count = len(df)
    df = df[~df["stage"].isin(["MOVE", "NAN"])].copy()
    dropped = initial_count - len(df)
    if dropped > 0:
        logger.info(
            "Subject %d: dropped %d/%d invalid epochs (%.1f%%)",
            subject_id,
            dropped,
            initial_count,
            100 * dropped / initial_count,
        )

    # Cast to silence Series/DataFrame ambiguity
    return cast(pd.DataFrame, df[columns])


def calculate_band_power(
    psd: npt.NDArray[np.float64],
    freqs: npt.NDArray[np.float64],
    ch_names: list[str],
    fmin: float,
    fmax: float,
) -> npt.NDArray[np.float64]:
    # Filter channels (EEG only)
    # Look for "EEG" in the name ("EEG Fpz-Cz", "EEG Pz-Oz"). `ch_names` must
    # describe the channel axis of `psd`. The caller normally picks EEG before
    # computing the PSD, so this is a no-op guard on that path and does the real
    # selecting only when the pick fell back to all channels.
    eeg_indices = [i for i, name in enumerate(ch_names) if "EEG" in name]

    if not eeg_indices:
        # Fallback: if no EEG found, take everything (prevent crash)
        eeg_indices = list(range(len(ch_names)))

    # Select only EEG channels from the PSD tensor
    # psd shape: (n_epochs, n_channels, n_freqs) -> (n_epochs, n_eeg, n_freqs)
    psd_eeg = psd[:, eeg_indices, :]

    # Select frequencies
    idx = np.logical_and(freqs >= fmin, freqs <= fmax)
    freq_res = freqs[1] - freqs[0]

    # Integrate (sum)
    band_power = psd_eeg[:, :, idx].sum(axis=2) * freq_res * 1e12
    band_power = np.maximum(band_power, 1e-10)

    # Average absolute power across EEG channels FIRST
    avg_power = band_power.mean(axis=1)
    avg_power = np.maximum(avg_power, 1e-10)

    # Convert the averaged power to decibels
    return 10 * np.log10(avg_power)
