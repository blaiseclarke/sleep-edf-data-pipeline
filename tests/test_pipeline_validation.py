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


class _FakeProcess:
    """Stands in for subprocess.Popen: iterable stdout plus a return code."""

    def __init__(self, returncode=0, lines=()):
        self.returncode = returncode
        self.stdout = iter(lines)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


def _fake_popen(recorded, failing_subcommand=None, lines=()):
    """Popen replacement that records argv and can fail one subcommand."""

    def factory(argv, **kwargs):
        recorded.append(argv)
        failed = failing_subcommand is not None and argv[1] == failing_subcommand
        return _FakeProcess(returncode=1 if failed else 0, lines=lines)

    return factory


def _invoke_dbt_task(recorded, monkeypatch, warehouse_type="duckdb", **popen_kwargs):
    import logging
    import pipeline

    monkeypatch.setenv("WAREHOUSE_TYPE", warehouse_type)
    with (
        patch("pipeline.subprocess.Popen", _fake_popen(recorded, **popen_kwargs)),
        patch("pipeline.get_run_logger", return_value=logging.getLogger("test")),
    ):
        pipeline.run_dbt_transformations.fn()


def test_dbt_uses_build_rather_than_run_then_test(monkeypatch):
    """
    `build` interleaves each model with its tests, so a failing test blocks its
    dependents. `run` followed by `test` materialises everything first and only
    then notices, leaving bad data in the marts.
    """
    recorded = []
    _invoke_dbt_task(recorded, monkeypatch)

    subcommands = [argv[1] for argv in recorded]
    assert subcommands == ["deps", "build"]
    assert "run" not in subcommands
    assert "test" not in subcommands


def test_dbt_targets_duckdb_by_default(monkeypatch):
    recorded = []
    _invoke_dbt_task(recorded, monkeypatch, warehouse_type="duckdb")

    build = next(argv for argv in recorded if argv[1] == "build")
    assert build[build.index("--target") + 1] == "dev_duckdb"


def test_dbt_targets_snowflake_when_configured(monkeypatch):
    recorded = []
    _invoke_dbt_task(recorded, monkeypatch, warehouse_type="snowflake")

    build = next(argv for argv in recorded if argv[1] == "build")
    assert build[build.index("--target") + 1] == "dev"


def test_dbt_build_failure_raises_with_the_command(monkeypatch):
    """A failed build must abort the flow, naming what failed."""
    recorded = []
    with pytest.raises(RuntimeError, match=r"dbt build .*failed with exit code 1"):
        _invoke_dbt_task(recorded, monkeypatch, failing_subcommand="build")


def test_dbt_deps_failure_skips_the_build(monkeypatch):
    """If packages cannot be installed there is no point attempting a build."""
    recorded = []
    with pytest.raises(RuntimeError, match="dbt deps"):
        _invoke_dbt_task(recorded, monkeypatch, failing_subcommand="deps")

    assert [argv[1] for argv in recorded] == ["deps"]


def test_dbt_output_is_streamed_to_the_logger(monkeypatch, caplog):
    """
    Output is relayed line by line rather than withheld until the command ends,
    so a long build reports progress and a failure surfaces its own diagnostics.
    """
    import logging

    recorded = []
    with caplog.at_level(logging.INFO, logger="test"):
        _invoke_dbt_task(
            recorded,
            monkeypatch,
            lines=["1 of 4 OK created view model main.staging\n", "\n", "Done.\n"],
        )

    messages = [record.message for record in caplog.records]
    assert "1 of 4 OK created view model main.staging" in messages
    assert "Done." in messages
    # Blank lines are dropped rather than logged as empty records
    assert "" not in messages
