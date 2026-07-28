import os

import snowflake.connector

from warehouse.base import WarehouseClient


class SnowflakeClient(WarehouseClient):
    """
    Snowflake implementation of the WarehouseClient.
    Relies on standard SNOWFLAKE_* environment variables for connection or explicit arguments.
    """

    def __init__(self):
        # Relies on environment variables or external configuration for connection details
        # Keeps the initialization simple and secure
        self.user = os.getenv("SNOWFLAKE_USER")
        self.password = os.getenv("SNOWFLAKE_PASSWORD")
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        self.database = os.getenv("SNOWFLAKE_DATABASE")
        self.schema = os.getenv("SNOWFLAKE_SCHEMA")
        self.role = os.getenv("SNOWFLAKE_ROLE")

        # Validate required credentials
        missing = []
        if not self.user:
            missing.append("SNOWFLAKE_USER")
        if not self.password:
            missing.append("SNOWFLAKE_PASSWORD")
        if not self.account:
            missing.append("SNOWFLAKE_ACCOUNT")
        if not self.role:
            missing.append("SNOWFLAKE_ROLE")

        if missing:
            raise ValueError(
                f"Missing required Snowflake credentials: {', '.join(missing)}. "
                "Please set these environment variables."
            )

    def _get_connection(self):
        return snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            role=self.role,
        )

    def ensure_tables_exist(self) -> None:
        """
        Creates SLEEP_EPOCHS and INGESTION_ERRORS in the configured database and
        schema if they are not already there.

        Unlike the DuckDB client this is not called from __init__, since opening
        a connection should not imply issuing DDL. The pipeline flow and
        scripts/setup_db.py both call it explicitly. Requires a role with
        CREATE TABLE on the target schema.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # FLOAT is Snowflake's 64-bit double, matching DuckDB's DOUBLE
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS SLEEP_EPOCHS (
                    SUBJECT_ID INTEGER,
                    EPOCH_IDX INTEGER,
                    STAGE VARCHAR,
                    DELTA_POWER FLOAT,
                    THETA_POWER FLOAT,
                    ALPHA_POWER FLOAT,
                    SIGMA_POWER FLOAT,
                    BETA_POWER FLOAT,
                    LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS INGESTION_ERRORS (
                    ERROR_ID VARCHAR(36) DEFAULT UUID_STRING(),
                    SUBJECT_ID INTEGER,
                    ERROR_TYPE VARCHAR,
                    ERROR_MESSAGE VARCHAR,
                    STACK_TRACE VARCHAR,
                    OCCURRED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)
        finally:
            cursor.close()
            conn.close()

    def load_epochs(
        self, staging_path: str, subject_id: int, overwrite: bool = True
    ) -> None:
        """
        Loads subject-level sleep epoch data into the SLEEP_EPOCHS table in Snowflake.
        """
        import re
        from pathlib import Path

        # Validate inputs before opening a connection or touching data
        path_obj = Path(staging_path).resolve()
        if not path_obj.is_dir():
            raise FileNotFoundError(f"Staging path does not exist: {staging_path}")

        parquet_files = sorted(path_obj.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files found in: {staging_path}")

        if not isinstance(subject_id, int) or subject_id < 0:
            raise ValueError(f"Invalid subject_id: {subject_id}")

        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            # Create a temporary internal stage with validated identifier
            stage_name = f"STAGE_SLEEP_EPOCHS_{subject_id}"
            if not re.match(r"^[A-Z_][A-Z0-9_]*$", stage_name):
                raise ValueError(f"Invalid stage name: {stage_name}")
            cursor.execute(f"CREATE TEMPORARY STAGE IF NOT EXISTS {stage_name}")

            try:
                # 1. PUT files into the internal stage
                # Using auto_compress=False because parquet is already compressed
                safe_path = str(path_obj.absolute()).replace("'", "''")
                put_command = f"PUT 'file://{safe_path}/*.parquet' @{stage_name} AUTO_COMPRESS=FALSE"
                cursor.execute(put_command)

                # 2. DELETE + COPY in a transaction for atomicity
                cursor.execute("BEGIN")
                try:
                    if overwrite:
                        cursor.execute(
                            "DELETE FROM SLEEP_EPOCHS WHERE SUBJECT_ID = %s",
                            (subject_id,),
                        )

                    copy_command = f"""
                        COPY INTO SLEEP_EPOCHS
                        FROM @{stage_name}
                        FILE_FORMAT = (TYPE = PARQUET)
                        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                        PURGE = TRUE
                    """
                    cursor.execute(copy_command)
                    cursor.execute("COMMIT")
                except Exception:
                    cursor.execute("ROLLBACK")
                    raise

            finally:
                # Clean up the stage
                cursor.execute(f"DROP STAGE IF EXISTS {stage_name}")

        finally:
            cursor.close()
            conn.close()

    def clear_epochs(self) -> None:
        """Deletes every row from SLEEP_EPOCHS."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM SLEEP_EPOCHS")
        finally:
            cursor.close()
            conn.close()

    def log_ingestion_error(
        self,
        subject_id: int,
        error_type: str,
        error_message: str,
        stack_trace: str | None = None,
    ) -> None:
        """
        Logs an ingestion error into the INGESTION_ERRORS table.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO INGESTION_ERRORS (SUBJECT_ID, ERROR_TYPE, ERROR_MESSAGE, STACK_TRACE)
                VALUES (%s, %s, %s, %s)
                """,
                (subject_id, error_type, error_message, stack_trace),
            )
        finally:
            cursor.close()
            conn.close()
