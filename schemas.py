from pydantic import BaseModel, Field, field_validator


class SleepEpoch(BaseModel):
    """
    Defines the data contract for a single sleep epoch.

    This schema enforces data types, sleep stage labels, and that all power
    bands must be positive.
    """

    subject_id: int
    epoch_idx: int
    stage: str = Field(..., pattern="^(W|N1|N2|N3|REM|MOVE|NAN)$")

    # Power bands, must be positive
    delta_power: float = Field(..., ge=0)
    theta_power: float = Field(..., ge=0)
    alpha_power: float = Field(..., ge=0)
    sigma_power: float = Field(..., ge=0)
    beta_power: float = Field(..., ge=0)

    @field_validator(
        "delta_power", "theta_power", "alpha_power", "sigma_power", "beta_power"
    )
    @classmethod
    def check_positive(cls, v: float) -> float:
        if v < 0:
            raise ValueError("Power values must be non-negative")
        return v
