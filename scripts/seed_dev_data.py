"""
Write a small synthetic SLEEP_EPOCHS table so the dbt models can be built and
tested without downloading the real Sleep-EDF recordings.

The shape matters more than the values. Each synthetic subject gets a daytime
nap separated from the night by a long wake bout, because that is exactly what
the sleep-period detection in `sleep_metrics` has to cope with -- seeding a
clean uninterrupted night would let a regression through unnoticed.

Deterministic: the same seed produces the same table on every run.
"""

import argparse
import logging
import random

from warehouse.factory import get_warehouse_client

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

EPOCHS_PER_MINUTE = 2

# Rough decibel centres per band for each stage, so the marts produce numbers in
# the same range as real recordings (including negative values in fast bands).
BAND_CENTRES = {
    #        delta  theta  alpha  sigma   beta
    "W": (5.0, 4.0, 8.0, 1.0, 2.0),
    "N1": (10.0, 9.0, 5.0, 1.5, 0.5),
    "N2": (15.0, 10.0, 4.0, 5.0, -1.0),
    "N3": (22.0, 12.0, 3.0, 3.0, -3.0),
    "REM": (11.0, 11.0, 4.5, 1.0, 0.0),
}


def _night(rng):
    """A plausible stage sequence: a nap, a long gap, then the main sleep."""
    stages = []

    stages += ["W"] * (90 * EPOCHS_PER_MINUTE)  # afternoon
    stages += ["N1", "N2", "N2", "N1"] * (5 * EPOCHS_PER_MINUTE)  # nap
    stages += ["W"] * (120 * EPOCHS_PER_MINUTE)  # evening, breaks the episode

    for _ in range(4):  # four sleep cycles
        stages += ["N1"] * (5 * EPOCHS_PER_MINUTE)
        stages += ["N2"] * (25 * EPOCHS_PER_MINUTE)
        stages += ["N3"] * (rng.randint(10, 25) * EPOCHS_PER_MINUTE)
        stages += ["N2"] * (10 * EPOCHS_PER_MINUTE)
        stages += ["REM"] * (rng.randint(10, 20) * EPOCHS_PER_MINUTE)
        stages += ["W"] * (rng.randint(1, 4) * EPOCHS_PER_MINUTE)  # brief arousal

    stages += ["W"] * (60 * EPOCHS_PER_MINUTE)  # morning
    return stages


def build_rows(subjects, seed=0):
    rng = random.Random(seed)
    rows = []
    for subject_id in range(subjects):
        for epoch_idx, stage in enumerate(_night(rng)):
            powers = [
                round(centre + rng.uniform(-1.5, 1.5), 4)
                for centre in BAND_CENTRES[stage]
            ]
            rows.append((subject_id, epoch_idx, stage, *powers))
    return rows


def seed(subjects=3, seed_value=0):
    client = get_warehouse_client()
    client.ensure_tables_exist()

    rows = build_rows(subjects, seed_value)

    # Written through the client's own connection settings rather than a second
    # DuckDB handle, so this works for whichever warehouse is configured.
    import duckdb

    connection = duckdb.connect(client.db_path)
    try:
        connection.execute("DELETE FROM SLEEP_EPOCHS")
        connection.executemany(
            """
            INSERT INTO SLEEP_EPOCHS
                (SUBJECT_ID, EPOCH_IDX, STAGE, DELTA_POWER, THETA_POWER,
                 ALPHA_POWER, SIGMA_POWER, BETA_POWER)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    finally:
        connection.close()

    logger.info("Seeded %d epochs across %d subjects.", len(rows), subjects)
    return len(rows)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subjects", type=int, default=3)
    parser.add_argument("--seed", type=int, default=0)
    arguments = parser.parse_args()
    seed(arguments.subjects, arguments.seed)
