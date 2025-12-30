import pytest
import pandas as pd
import duckdb
from unittest.mock import patch
from pipeline import run_ingestion_pipeline
from warehouse.duckdb_client import DuckDBClient
from scripts.setup_db import setup_database


@pytest.fixture
def integration_db(tmp_path, monkeypatch):
    """
    Sets up a isolated, temporary DuckDB database for the integration test.
    """
    db_file = str(tmp_path / "integration_sleep.db")

    # Patch the setup script and DuckDBClient init to use this temporary path
    monkeypatch.setattr("scripts.setup_db.DB_PATH", db_file)

    # Initialize the schema in the fresh database
    setup_database()

    # Ensure the pipeline's DuckDBClient instance uses this test database
    monkeypatch.setattr(
        "pipeline.DuckDBClient", lambda **kwargs: DuckDBClient(db_path=db_file)
    )

    return db_file


def test_pipeline_parallel_ingestion_integration(integration_db):
    """
    End-to-End test of the ingestion pipeline:
    Extraction (Mocked) -> Validation (Pandera) -> Load (DuckDB).
    """
    # 1. Create a valid mock DataFrame
    mock_df = pd.DataFrame(
        {
            "subject_id": [1, 1],
            "epoch_idx": [0, 1],
            "stage": ["W", "N1"],
            "delta_power": [10.5, 20.1],
            "theta_power": [5.2, 4.8],
            "alpha_power": [2.1, 3.2],
            "sigma_power": [1.5, 1.8],
            "beta_power": [0.8, 0.9],
        }
    )

    # 2. Patch core extraction and flow control variables
    with patch("pipeline.extract_subject_data") as mock_extract:
        mock_extract.return_value = mock_df

        # Control the flow to only process a single subject (ID 1)
        with patch("pipeline.STARTING_SUBJECT", 1), patch("pipeline.ENDING_SUBJECT", 1):
            # 3. Execute the full Prefect flow
            run_ingestion_pipeline()

    # 4. Verify results in DuckDB
    connection = duckdb.connect(integration_db)
    result_df = connection.execute("SELECT * FROM SLEEP_EPOCHS ORDER BY EPOCH_IDX").df()
    connection.close()

    # Assertions
    assert len(result_df) == 2, "Should have loaded exactly 2 rows"
    assert result_df["SUBJECT_ID"].unique()[0] == 1, "Subject ID should match mock"
    assert result_df["STAGE"].tolist() == ["W", "N1"], "Stages should match mock"
    assert result_df["DELTA_POWER"].iloc[1] == 20.1, (
        "Power values should be persisted correctly"
    )
