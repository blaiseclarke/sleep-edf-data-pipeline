import os
import duckdb
import pandas as pd
from typing import Optional
from warehouse.base import WarehouseClient


class DuckDBClient(WarehouseClient):
    """
    DuckDB implementation of the WarehouseClient for local persistent storage.
    """

    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            from ingest_data import DB_PATH

            self.db_path = DB_PATH
        else:
            self.db_path = db_path
        
        # Ensure parent directory exists
        if self.db_path and os.path.dirname(self.db_path):
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        # Initialize tables if they don't exist
        self._ensure_tables_exist()

    def _ensure_tables_exist(self):
        """Creates required tables if they don't exist."""
        connection = duckdb.connect(self.db_path)
        try:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS SLEEP_EPOCHS (
                    SUBJECT_ID INTEGER,
                    EPOCH_IDX INTEGER,
                    STAGE VARCHAR,
                    DELTA_POWER DOUBLE,
                    THETA_POWER DOUBLE,
                    ALPHA_POWER DOUBLE,
                    SIGMA_POWER DOUBLE,
                    BETA_POWER DOUBLE,
                    LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            connection.execute("""
                CREATE TABLE IF NOT EXISTS INGESTION_ERRORS (
                    ERROR_ID UUID DEFAULT uuid(),
                    SUBJECT_ID INTEGER,
                    ERROR_TYPE VARCHAR,
                    ERROR_MESSAGE VARCHAR,
                    STACK_TRACE VARCHAR,
                    OCCURRED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        finally:
            connection.close()

    def load_epochs(self, df: pd.DataFrame, subject_id: int) -> None:
        """
        Loads subject-level sleep epoch data into the SLEEP_EPOCHS table.
        Clears existing data for the subject before inserting.
        """
        connection = duckdb.connect(self.db_path)
        try:
            # Deletes existing records for subject
            connection.execute(
                "DELETE FROM SLEEP_EPOCHS WHERE SUBJECT_ID = ?", (subject_id,)
            )

            # Defines explicit column mapping
            columns = [
                "SUBJECT_ID",
                "EPOCH_IDX",
                "STAGE",
                "DELTA_POWER",
                "THETA_POWER",
                "ALPHA_POWER",
                "SIGMA_POWER",
                "BETA_POWER",
            ]

            # Inserts new records using explicit column list
            # Leverages DuckDB's ability to query local pandas dataframes directly ('FROM df')
            # for high-performance interactions causing minimal overhead
            query = f"""
                INSERT INTO SLEEP_EPOCHS ({", ".join(columns)})
                SELECT {", ".join(columns)}
                FROM df
            """
            connection.execute(query)
        finally:
            connection.close()

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
        connection = duckdb.connect(self.db_path)
        try:
            connection.execute(
                """
                INSERT INTO INGESTION_ERRORS (SUBJECT_ID, ERROR_TYPE, ERROR_MESSAGE, STACK_TRACE)
                VALUES (?, ?, ?, ?)
                """,
                (subject_id, error_type, error_message, stack_trace),
            )
        finally:
            connection.close()
