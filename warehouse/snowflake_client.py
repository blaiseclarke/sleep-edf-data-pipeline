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

    def load_epochs(self, df: pd.DataFrame, subject_id: int, overwrite: bool = True) -> None:
        """
        Loads subject-level sleep epoch data into the SLEEP_EPOCHS table in Snowflake.
        """
        conn = self._get_connection()
        try:
            # Clears existing data for this subject (idempotency)
            # Prevents duplicate rows if the pipeline is re-run for a subject
            # Only runs if overwrite is True (default)
            if overwrite:
                cursor = conn.cursor()
                cursor.execute(
                    "DELETE FROM SLEEP_EPOCHS WHERE SUBJECT_ID = %s", (subject_id,)
                )

            # Bulk loads new data
            # write_pandas is highly optimized; it transparently uploads the dataframe to a temporary stage
            # and performs a COPY INTO operation, which is much faster than INSERT statements
            if not df.empty:
                # Snowflake expects uppercase column names by default
                df.columns = [c.upper() for c in df.columns]
                success, nchunks, nrows, _ = write_pandas(
                    conn,
                    df,
                    "SLEEP_EPOCHS",
                    database=self.database,
                    schema=self.schema,
                )
                if not success:
                    raise RuntimeError(f"Failed to write pandas DataFrame for subject {subject_id}")
            
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
