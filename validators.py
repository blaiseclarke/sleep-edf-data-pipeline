import pandera.pandas as pa

# Use Pandera to define a contract for the data
# If any row violates these rules (ex. a "Sleep stage 5" which doesn't exist),
# the entire subject is rejected
SleepSchema = pa.DataFrameSchema(
    {
        "subject_id": pa.Column(int),
        "epoch_idx": pa.Column(int),
        # Validate sleep stages
        # Enforce a strict list of allowed strings
        # Everything must be normalized to ["N1", "N2", "N3", "REM", "W"] for the dashboard
        "stage": pa.Column(
            str, checks=pa.Check.isin(["W", "N1", "N2", "N3", "REM"])
        ),
        "delta_power": pa.Column(float),
        "theta_power": pa.Column(float),
        "alpha_power": pa.Column(float),
        "sigma_power": pa.Column(float),
        "beta_power": pa.Column(float),
    }
)
