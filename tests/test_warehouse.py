import re
import pytest
import pandas as pd
import duckdb
from unittest.mock import MagicMock, patch
from warehouse.duckdb_client import DuckDBClient

SNOWFLAKE_ENV = {
    "SNOWFLAKE_USER": "user",
    "SNOWFLAKE_PASSWORD": "pass",
    "SNOWFLAKE_ACCOUNT": "acct",
    "SNOWFLAKE_ROLE": "role",
    "SNOWFLAKE_WAREHOUSE": "wh",
    "SNOWFLAKE_DATABASE": "db",
    "SNOWFLAKE_SCHEMA": "sch",
}


def _columns_in_ddl(statement):
    """Pull column names out of a CREATE TABLE body, in declaration order."""
    body = statement[statement.index("(") + 1 : statement.rindex(")")]
    return [
        match.group(1)
        for line in body.splitlines()
        if (match := re.match(r"\s*([A-Z_][A-Z0-9_]*)\s+\S", line))
    ]


@pytest.fixture
def snowflake_client():
    """A SnowflakeClient wired to a mock connector, plus the shared cursor."""
    with patch.dict("os.environ", SNOWFLAKE_ENV, clear=True):
        from warehouse.snowflake_client import SnowflakeClient

        client = SnowflakeClient()

    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value = cursor

    with patch.object(client, "_get_connection", return_value=connection):
        yield client, cursor, connection


def _executed(cursor):
    """Every SQL string passed to cursor.execute, in order."""
    return [call.args[0] for call in cursor.execute.call_args_list]


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


def test_snowflake_ensure_tables_exist_creates_both_tables(snowflake_client):
    """
    Without this the Snowflake path could not run at all: load_epochs COPYs into
    SLEEP_EPOCHS and log_ingestion_error inserts into INGESTION_ERRORS, but
    nothing ever created either table.
    """
    client, cursor, connection = snowflake_client

    client.ensure_tables_exist()

    statements = _executed(cursor)
    assert len(statements) == 2
    for statement, table in zip(statements, ["SLEEP_EPOCHS", "INGESTION_ERRORS"]):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in statement

    # Idempotent, so re-running setup must never drop anything
    assert not any("DROP" in s.upper() for s in statements)

    cursor.close.assert_called_once()
    connection.close.assert_called_once()


def test_snowflake_schema_matches_duckdb(snowflake_client, tmp_path):
    """
    The two warehouses declare their DDL separately, so guard against drift.
    dbt reads SLEEP_EPOCHS through one set of models regardless of backend, and
    the Snowflake COPY relies on MATCH_BY_COLUMN_NAME, so the column names have
    to agree.
    """
    client, cursor, _ = snowflake_client
    client.ensure_tables_exist()
    snowflake_ddl = {
        table: _columns_in_ddl(statement)
        for table, statement in zip(
            ["SLEEP_EPOCHS", "INGESTION_ERRORS"], _executed(cursor)
        )
    }

    DuckDBClient(db_path=str(tmp_path / "schema.db"))
    connection = duckdb.connect(str(tmp_path / "schema.db"), read_only=True)
    try:
        for table, snowflake_columns in snowflake_ddl.items():
            duckdb_columns = [
                row[0] for row in connection.execute(f"DESCRIBE {table}").fetchall()
            ]
            assert snowflake_columns == duckdb_columns, (
                f"{table} columns differ between warehouses"
            )
    finally:
        connection.close()


def test_snowflake_load_epochs_stages_then_copies(snowflake_client, staging_with_data):
    """The load must PUT the parquet, clear the subject, then COPY, atomically."""
    client, cursor, _ = snowflake_client

    client.load_epochs(staging_with_data, subject_id=7, overwrite=True)

    statements = _executed(cursor)
    joined = "\n".join(statements)
    assert "CREATE TEMPORARY STAGE IF NOT EXISTS STAGE_SLEEP_EPOCHS_7" in joined
    assert any(s.startswith("PUT ") for s in statements)
    assert "DELETE FROM SLEEP_EPOCHS WHERE SUBJECT_ID = %s" in statements

    # DELETE and COPY have to sit inside one transaction
    assert statements.index("BEGIN") < statements.index(
        "DELETE FROM SLEEP_EPOCHS WHERE SUBJECT_ID = %s"
    )
    copy_index = next(i for i, s in enumerate(statements) if "COPY INTO" in s)
    assert statements.index("BEGIN") < copy_index < statements.index("COMMIT")


def test_snowflake_load_epochs_skips_delete_without_overwrite(
    snowflake_client, staging_with_data
):
    """overwrite=False appends, so nothing may be deleted."""
    client, cursor, _ = snowflake_client

    client.load_epochs(staging_with_data, subject_id=7, overwrite=False)

    assert not any("DELETE" in s.upper() for s in _executed(cursor))


def test_snowflake_load_epochs_rolls_back_and_drops_stage(
    snowflake_client, staging_with_data
):
    """A failed COPY must roll back and still clean up the temporary stage."""
    client, cursor, _ = snowflake_client

    def fail_on_copy(sql, *args, **kwargs):
        if "COPY INTO" in sql:
            raise RuntimeError("copy blew up")

    cursor.execute.side_effect = fail_on_copy

    with pytest.raises(RuntimeError, match="copy blew up"):
        client.load_epochs(staging_with_data, subject_id=7)

    statements = _executed(cursor)
    assert "ROLLBACK" in statements
    assert "COMMIT" not in statements
    assert any("DROP STAGE IF EXISTS" in s for s in statements)


def test_snowflake_load_epochs_rejects_missing_path(snowflake_client):
    """Validation happens before a connection is opened."""
    client, _, _ = snowflake_client

    with pytest.raises(FileNotFoundError, match="does not exist"):
        client.load_epochs("/nonexistent/path", subject_id=1)


def test_snowflake_log_ingestion_error_inserts(snowflake_client):
    """Errors are parameterised, never interpolated into the statement."""
    client, cursor, connection = snowflake_client

    client.log_ingestion_error(
        subject_id=3,
        error_type="SchemaError",
        error_message="bad row",
        stack_trace="trace",
    )

    statement, params = cursor.execute.call_args.args
    assert "INSERT INTO INGESTION_ERRORS" in statement
    assert params == (3, "SchemaError", "bad row", "trace")
    connection.close.assert_called_once()
