import os
from typing import Optional
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
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
        self.role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")

        # Validate required credentials
        missing = []
        if not self.user:
            missing.append("SNOWFLAKE_USER")
        if not self.password:
            missing.append("SNOWFLAKE_PASSWORD")
        if not self.account:
            missing.append("SNOWFLAKE_ACCOUNT")

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

    def load_epochs(
        self, staging_path: str, subject_id: int, overwrite: bool = True
    ) -> None:
        """
        Loads subject-level sleep epoch data into the SLEEP_EPOCHS table in Snowflake.
        """
        from pathlib import Path

        conn = self._get_connection()
        try:
            # Clears existing data for this subject (idempotency)
            if overwrite:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM SLEEP_EPOCHS WHERE SUBJECT_ID = %s", (subject_id,)
                )

            path_obj = Path(staging_path)
            if not path_obj.exists():
                return

            parquet_files = sorted(path_obj.glob("*.parquet"))
            if not parquet_files:
                return

            cursor = conn.cursor()

            # Create a temporary internal stage if it doesn't exist
            stage_name = f"STAGE_SLEEP_EPOCHS_{subject_id}"
            cursor.execute(f"CREATE TEMPORARY STAGE IF NOT EXISTS {stage_name}")

            try:
                # 1. PUT files into the internal stage
                # Using auto_compress=False because parquet is already compressed
                # Using Path.absolute() to ensure Snowflake CLI tool finds it correctly
                put_command = f"PUT file://{path_obj.absolute()}/*.parquet @{stage_name} AUTO_COMPRESS=FALSE"
                cursor.execute(put_command)

                # 2. COPY INTO the target table
                # We use MATCH_BY_COLUMN_NAME to map Parquet columns to Snowflake columns automatically
                copy_command = f"""
                    COPY INTO SLEEP_EPOCHS 
                    FROM @{stage_name} 
                    FILE_FORMAT = (TYPE = PARQUET) 
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    PURGE = TRUE
                """
                cursor.execute(copy_command)

            finally:
                # 3. Clean up the stage
                cursor.execute(f"DROP STAGE IF EXISTS {stage_name}")

        finally:
            conn.close()

    def log_ingestion_error(
        self,
        subject_id: int,
        error_type: str,
        error_message: str,
        stack_trace: Optional[str] = None,
    ) -> None:
        """
        Logs an ingestion error into the INGESTION_ERRORS table.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO INGESTION_ERRORS (SUBJECT_ID, ERROR_TYPE, ERROR_MESSAGE, STACK_TRACE)
                VALUES (%s, %s, %s, %s)
                """,
                (subject_id, error_type, error_message, stack_trace),
            )
        finally:
            conn.close()
