import pytest
import numpy as np
import pandas as pd
from pandera.errors import SchemaError
from ingest.processing import calculate_band_power
from validators import SleepSchema

# 1 Hz resolution from DC to 30 Hz, so the delta band (0.5-4 Hz) covers
# exactly the bins at 1, 2, 3 and 4 Hz
FREQS = np.arange(0, 31, 1.0)
DELTA_BINS = 4


def _expected_db(psd_value, n_bins=DELTA_BINS):
    """Mirror of the band power maths for a flat PSD, in decibels."""
    return 10 * np.log10(n_bins * psd_value * 1e12)


def test_valid_sleep_epoch():
    """
    Ensures data sample that meets constraints is successfully validated.
    """

    data = {
        "subject_id": [1],
        "epoch_idx": [100],
        "stage": ["N2"],
        "delta_power": [15.5],
        "theta_power": [14.2],
        "alpha_power": [8.0],
        "sigma_power": [1.2],
        "beta_power": [2.5],
    }

    df = pd.DataFrame(data)
    validated_df = SleepSchema.validate(df)
    assert validated_df["subject_id"].iloc[0] == 1
    assert validated_df["stage"].iloc[0] == "N2"


def test_nan_power_validation():
    """
    Ensures Pandera schema raises a SchemaError when a NaN power value is seen.
    Negative values are allowed (dB), but NaNs indicate a calculation failure.
    """

    data = {
        "subject_id": [1],
        "epoch_idx": [100],
        "stage": ["W"],
        "delta_power": [float("nan")],
        "theta_power": [14.2],
        "alpha_power": [8.0],
        "sigma_power": [1.2],
        "beta_power": [2.5],
    }

    df = pd.DataFrame(data)
    with pytest.raises(SchemaError):
        SleepSchema.validate(df)


def test_invalid_stage_label():
    """
    Confirms that the sleep stage validation rejects unsupported sleep stage labels.
    """

    data = {
        "subject_id": [1],
        "epoch_idx": [100],
        "stage": ["SLEEPING"],
        "delta_power": [15.5],
        "theta_power": [14.2],
        "alpha_power": [8.0],
        "sigma_power": [1.2],
        "beta_power": [2.5],
    }

    df = pd.DataFrame(data)
    with pytest.raises(SchemaError):
        SleepSchema.validate(df)


def test_negative_values_allowed():
    """
    Ensures that negative values (common in dB power) are accepted by the schema.
    """

    data = {
        "subject_id": [1],
        "epoch_idx": [100],
        "stage": ["N2"],
        "delta_power": [-5.5],
        "theta_power": [-2.0],
        "alpha_power": [8.0],
        "sigma_power": [1.2],
        "beta_power": [2.5],
    }

    df = pd.DataFrame(data)
    validated_df = SleepSchema.validate(df)
    assert validated_df["delta_power"].iloc[0] == -5.5


def test_band_power_uses_only_eeg_channels():
    """
    Non-EEG channels must not contribute to band power, even when their power
    dwarfs the EEG. ch_names describes the channel axis of psd, so a mismatch
    here would silently integrate respiration or temperature into the EEG bands.
    """
    psd = np.zeros((1, 2, len(FREQS)))
    psd[0, 0, :] = 1e-12  # EEG
    psd[0, 1, :] = 1e-6  # EOG, six orders of magnitude larger

    result = calculate_band_power(psd, FREQS, ["EEG", "EOG"], 0.5, 4)

    assert result.shape == (1,)
    assert result[0] == pytest.approx(_expected_db(1e-12), abs=1e-6)


def test_band_power_averages_across_eeg_channels():
    """Both EEG channels are averaged before the decibel conversion."""
    psd = np.zeros((1, 3, len(FREQS)))
    psd[0, 0, :] = 1e-12  # EEG
    psd[0, 1, :] = 3e-12  # EEG2
    psd[0, 2, :] = 1e-4  # EMG, must be excluded

    result = calculate_band_power(psd, FREQS, ["EEG", "EEG2", "EMG"], 0.5, 4)

    # Averaged in linear power, not in decibels
    assert result[0] == pytest.approx(_expected_db(2e-12), abs=1e-6)


def test_band_power_ignores_frequencies_outside_the_band():
    """Power parked outside the requested band must not leak into it."""
    psd = np.zeros((1, 1, len(FREQS)))
    psd[0, 0, :] = 1e-12
    psd[0, 0, FREQS > 10] = 1e-3  # large power well above the delta band

    result = calculate_band_power(psd, FREQS, ["EEG"], 0.5, 4)

    assert result[0] == pytest.approx(_expected_db(1e-12), abs=1e-6)


def test_band_power_falls_back_when_no_eeg_channel_present():
    """
    With no channel named EEG the function averages everything rather than
    crashing, which keeps an unexpected montage from failing the whole subject.
    """
    psd = np.zeros((1, 2, len(FREQS)))
    psd[0, 0, :] = 1e-12
    psd[0, 1, :] = 3e-12

    result = calculate_band_power(psd, FREQS, ["Fpz-Cz", "Pz-Oz"], 0.5, 4)

    assert result[0] == pytest.approx(_expected_db(2e-12), abs=1e-6)


def test_band_power_is_finite_for_silent_channels():
    """An all-zero channel is clamped rather than producing -inf from log10(0)."""
    psd = np.zeros((2, 1, len(FREQS)))

    result = calculate_band_power(psd, FREQS, ["EEG"], 0.5, 4)

    assert np.isfinite(result).all()
    assert result.shape == (2,)
