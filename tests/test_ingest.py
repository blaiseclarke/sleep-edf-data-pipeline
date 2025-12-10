import pytest
from pydantic import ValidationError
from schemas import SleepEpoch

def test_valid_sleep_epoch():
    """
    Testing for valid data in SleepEpoch.
    """

    data = {
        "subject_id": 1,
        "epoch_idx": 100,
        "stage": "N2",
        "delta_power": 15.5,
        "theta_power": 14.2,
        "alpha_power": 8.0,
        "sigma_power": 1.2,
        "beta_power": 2.5
    }

    epoch = SleepEpoch(**data)
    assert epoch.subject_id == 1
    assert epoch.stage == "N2"

def test_negative_power_validation():
    """
    Testing for negative band power.
    """

    data = {
        "subject_id": 1,
        "epoch_idx": 100,
        "stage": "W",
        "delta_power": -5,
        "theta_power": 14.2,
        "alpha_power": 8.0,
        "sigma_power": 1.2,
        "beta_power": 2.5
    }

    with pytest.raises(ValidationError) as excinfo:
        SleepEpoch(**data)

    assert "Input should be greater than 0" in str(excinfo.value)

def test_invalid_stage_label():
    """
    Testing for invalid sleep stage label.
    """

    data = {
        "subject_id": 1,
        "epoch_idx": 100,
        "stage": "SLEEPING",
        "delta_power": -5,
        "theta_power": 14.2,
        "alpha_power": 8.0,
        "sigma_power": 1.2,
        "beta_power": 2.5
    }

    with pytest.raises(ValidationError):
        SleepEpoch(**data)