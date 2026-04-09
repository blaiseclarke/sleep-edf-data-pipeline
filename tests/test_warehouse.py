import pytest
import pandas as pd
import duckdb
from unittest.mock import patch
from warehouse.duckdb_client import DuckDBClient


@pytest.fixture
def duckdb_client(tmp_path):
    """Creates a DuckDBClient with a temporary database."""
    db_file = str(tmp_path / "test.db")
    return DuckDBClient(db_path=db_file)


@pytest.fixture
def staging_with_data(tmp_path):
    """Creates a staging directory with valid parquet data."""
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()

    df = pd.DataFrame(
        {
            "SUBJECT_ID": [1, 1],
            "EPOCH_IDX": [0, 1],
            "STAGE": ["W", "N1"],
            "DELTA_POWER": [10.5, 20.1],
            "THETA_POWER": [5.2, 4.8],
            "ALPHA_POWER": [2.1, 3.2],
            "SIGMA_POWER": [1.5, 1.8],
            "BETA_POWER": [0.8, 0.9],
        }
    )
    df.to_parquet(staging_dir / "part_0.parquet", index=False)
    return str(staging_dir)


def test_load_epochs_success(duckdb_client, staging_with_data):
    """Verifies that valid parquet data loads correctly."""
    duckdb_client.load_epochs(staging_with_data, subject_id=1)

    conn = duckdb.connect(duckdb_client.db_path)
    result = conn.execute("SELECT COUNT(*) FROM SLEEP_EPOCHS").fetchone()
    conn.close()

    assert result[0] == 2


def test_load_epochs_missing_path(duckdb_client):
    """Verifies that a missing staging path raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError, match="does not exist"):
        duckdb_client.load_epochs("/nonexistent/path", subject_id=1)


def test_load_epochs_empty_directory(duckdb_client, tmp_path):
    """Verifies that a directory with no parquet files raises FileNotFoundError."""
    empty_dir = tmp_path / "empty_staging"
    empty_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="No parquet files"):
        duckdb_client.load_epochs(str(empty_dir), subject_id=1)


def test_load_epochs_overwrite(duckdb_client, staging_with_data):
    """Verifies that overwrite=True replaces existing data for the subject."""
    duckdb_client.load_epochs(staging_with_data, subject_id=1)
    duckdb_client.load_epochs(staging_with_data, subject_id=1, overwrite=True)

    conn = duckdb.connect(duckdb_client.db_path)
    result = conn.execute("SELECT COUNT(*) FROM SLEEP_EPOCHS").fetchone()
    conn.close()

    assert result[0] == 2  # Should still be 2, not 4


def test_load_epochs_no_overwrite_appends(duckdb_client, staging_with_data):
    """Verifies that overwrite=False appends data instead of replacing it."""
    duckdb_client.load_epochs(staging_with_data, subject_id=1)
    duckdb_client.load_epochs(staging_with_data, subject_id=1, overwrite=False)

    conn = duckdb.connect(duckdb_client.db_path)
    result = conn.execute("SELECT COUNT(*) FROM SLEEP_EPOCHS").fetchone()
    conn.close()

    assert result[0] == 4  # 2 original + 2 appended


def test_load_epochs_rollback_on_failure(duckdb_client, staging_with_data, tmp_path):
    """Verifies that data is preserved if INSERT fails during an overwrite."""
    # Load initial data
    duckdb_client.load_epochs(staging_with_data, subject_id=1)

    # Create a staging dir with a corrupt parquet file to trigger INSERT failure
    bad_staging = tmp_path / "bad_staging"
    bad_staging.mkdir()
    (bad_staging / "part_0.parquet").write_text("not a parquet file")

    with pytest.raises(Exception):
        duckdb_client.load_epochs(str(bad_staging), subject_id=1, overwrite=True)

    # Original data should still be intact due to transaction rollback
    conn = duckdb.connect(duckdb_client.db_path)
    result = conn.execute("SELECT COUNT(*) FROM SLEEP_EPOCHS").fetchone()
    conn.close()

    assert result[0] == 2


def test_log_ingestion_error(duckdb_client):
    """Verifies that ingestion errors are logged correctly."""
    duckdb_client.log_ingestion_error(
        subject_id=1,
        error_type="TestError",
        error_message="Something went wrong",
    )

    conn = duckdb.connect(duckdb_client.db_path)
    result = conn.execute("SELECT * FROM INGESTION_ERRORS").fetchall()
    conn.close()

    assert len(result) == 1
    assert result[0][1] == 1  # SUBJECT_ID
    assert result[0][2] == "TestError"


def test_snowflake_missing_role_raises():
    """Verifies that SnowflakeClient raises when SNOWFLAKE_ROLE is not set."""
    env = {
        "SNOWFLAKE_USER": "user",
        "SNOWFLAKE_PASSWORD": "pass",
        "SNOWFLAKE_ACCOUNT": "acct",
    }
    with patch.dict("os.environ", env, clear=True):
        with pytest.raises(ValueError, match="SNOWFLAKE_ROLE"):
            from warehouse.snowflake_client import SnowflakeClient

            SnowflakeClient()
