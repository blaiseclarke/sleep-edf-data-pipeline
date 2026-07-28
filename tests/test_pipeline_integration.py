from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest

from pipeline import run_ingestion_pipeline
from scripts.setup_db import setup_database


@pytest.fixture
def integration_db(tmp_path, monkeypatch):
    """
    Sets up a isolated, temporary DuckDB database for the integration test.
    """
    db_file = str(tmp_path / "integration_sleep.db")

    # Patch the source of truth for DB_PATH. setup_database() now goes through
    # the warehouse factory, and DuckDBClient reads this at construction time.
    monkeypatch.setattr("ingest.config.DB_PATH", db_file)
    monkeypatch.setenv("WAREHOUSE_TYPE", "duckdb")

    # Initialize the schema in the fresh database
    setup_database()

    return db_file


def test_pipeline_parallel_ingestion_integration(integration_db, tmp_path):
    """
    End-to-End test of the ingestion pipeline:
    Extraction (Mocked) -> Validation (Pandera) -> Load (DuckDB).
    """
    # Create a valid mock DataFrame
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

    # Patch extract_to_parquet (the new generator-based task)
    with patch("pipeline.extract_to_parquet") as mock_extract:
        # Prepare staging area
        staging_dir = tmp_path / "staging_mock"
        staging_dir.mkdir()
        # Write mock data to Parquet so the loader can find it
        mock_df.to_parquet(staging_dir / "batch_1.parquet")

        # Mock the Prefect task `.map()` return value
        # The pipeline expects a list of futures, one per subject
        mock_future = MagicMock()
        mock_future.result.return_value = {
            "subject_id": 1,
            "path": str(staging_dir),
            "error": None,
        }
        mock_extract.map.return_value = [mock_future]

        # Control the flow to only process a single subject (ID 1)
        # Mock fetch_data and run_dbt_transformations to avoid real network calls and subprocess execution
        with (
            patch("pipeline.STARTING_SUBJECT", 1),
            patch("pipeline.ENDING_SUBJECT", 1),
            patch("pipeline.fetch_data", return_value=[]),
            patch("pipeline.run_dbt_transformations"),
        ):
            # Execute the full Prefect flow
            run_ingestion_pipeline()

    # Verify results in DuckDB
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


def test_raised_extraction_failures_reach_the_error_warehouse(integration_db, tmp_path):
    """
    A task that raises (corrupt EDF, exhausted fetch retries, load error) used
    to reach only the Prefect log. It must land in INGESTION_ERRORS with the
    exception type and stack trace, while the healthy subjects still load.
    """
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

    with patch("pipeline.extract_to_parquet") as mock_extract:
        staging_dir = tmp_path / "staging_mock"
        staging_dir.mkdir()
        mock_df.to_parquet(staging_dir / "batch_1.parquet")

        ok_future = MagicMock()
        ok_future.result.return_value = {
            "subject_id": 1,
            "path": str(staging_dir),
            "error": None,
        }
        bad_future = MagicMock()
        bad_future.result.side_effect = RuntimeError("corrupt EDF header")
        mock_extract.map.return_value = [ok_future, bad_future]

        with (
            patch("pipeline.STARTING_SUBJECT", 1),
            patch("pipeline.ENDING_SUBJECT", 2),
            patch("pipeline.fetch_data", return_value=[]),
            patch("pipeline.run_dbt_transformations"),
        ):
            run_ingestion_pipeline()

    connection = duckdb.connect(integration_db)
    loaded = connection.execute(
        "SELECT DISTINCT SUBJECT_ID FROM SLEEP_EPOCHS"
    ).fetchall()
    errors = connection.execute(
        "SELECT SUBJECT_ID, ERROR_TYPE, ERROR_MESSAGE, STACK_TRACE "
        "FROM INGESTION_ERRORS"
    ).fetchall()
    connection.close()

    assert [row[0] for row in loaded] == [1], "The healthy subject still loads"
    assert len(errors) == 1, "The raised failure must reach INGESTION_ERRORS"
    subject_id, error_type, error_message, stack_trace = errors[0]
    assert subject_id == 2
    assert error_type == "RuntimeError"
    assert "corrupt EDF header" in error_message
    assert stack_trace is not None and "RuntimeError" in stack_trace


def test_error_dicts_keep_their_type_and_stack_trace(integration_db):
    """
    The flow used to flatten every failure to error_type="ExtractionFailed" and
    never pass a stack trace, so validation failures were misclassified and
    STACK_TRACE was permanently NULL.
    """
    with patch("pipeline.extract_to_parquet") as mock_extract:
        failed_future = MagicMock()
        failed_future.result.return_value = {
            "subject_id": 1,
            "path": None,
            "error": {
                "type": "SchemaError",
                "message": "bad row",
                "stack_trace": "Traceback ... SchemaErrors",
            },
        }
        mock_extract.map.return_value = [failed_future]

        with (
            patch("pipeline.STARTING_SUBJECT", 1),
            patch("pipeline.ENDING_SUBJECT", 1),
            patch("pipeline.fetch_data", return_value=[]),
            patch("pipeline.run_dbt_transformations"),
        ):
            # The only subject failed, so the flow refuses to run dbt.
            with pytest.raises(RuntimeError, match="All subjects failed"):
                run_ingestion_pipeline()

    connection = duckdb.connect(integration_db)
    errors = connection.execute(
        "SELECT ERROR_TYPE, ERROR_MESSAGE, STACK_TRACE "
        "FROM INGESTION_ERRORS WHERE SUBJECT_ID = 1"
    ).fetchall()
    connection.close()

    assert errors == [("SchemaError", "bad row", "Traceback ... SchemaErrors")]
