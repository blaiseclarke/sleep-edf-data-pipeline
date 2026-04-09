import pytest
from unittest.mock import patch
from pipeline import run_ingestion_pipeline


def test_invalid_subject_range_raises():
    """Verifies that STARTING_SUBJECT > ENDING_SUBJECT raises ValueError."""
    with patch("pipeline.STARTING_SUBJECT", 10), patch("pipeline.ENDING_SUBJECT", 5):
        with pytest.raises(ValueError, match="must be <="):
            run_ingestion_pipeline()


def test_staging_cleanup_only_removes_parquet(tmp_path, monkeypatch):
    """Verifies that extract_to_parquet only removes .parquet files, not the whole directory."""
    staging_dir = tmp_path / "staging"
    monkeypatch.setattr("pipeline.STAGING_DIR", staging_dir)

    # Pre-create the subject directory with a parquet file and a non-parquet file
    subject_dir = staging_dir / "subject_0"
    subject_dir.mkdir(parents=True)
    (subject_dir / "old_part.parquet").write_bytes(b"old data")
    (subject_dir / "metadata.json").write_text('{"note": "keep me"}')

    # Import after monkeypatching so the task picks up the patched STAGING_DIR
    from pipeline import extract_to_parquet

    # Mock fetch_data to return no files (triggers early return)
    # Also mock get_run_logger since there's no active Prefect context
    import logging

    with (
        patch("pipeline.fetch_data", return_value=[]),
        patch("pipeline.get_run_logger", return_value=logging.getLogger("test")),
    ):
        result = extract_to_parquet.fn(subject_id=0)

    # The parquet file should be gone, but the json should survive
    assert not (subject_dir / "old_part.parquet").exists()
    assert (subject_dir / "metadata.json").exists()
    assert result["error"] == "No files found"


def test_config_telemetry_passes_recording():
    """Verifies that fetch_data forwards the recording parameter for the telemetry study."""
    with (
        patch("ingest.config.STUDY", "telemetry"),
        patch("ingest.config.fetch_telemetry_data") as mock_fetch,
    ):
        mock_fetch.return_value = []
        from ingest.config import fetch_data

        fetch_data(subjects=[0], recording=[1])

        mock_fetch.assert_called_once_with(
            subjects=[0], recording=[1], on_missing="warn"
        )
