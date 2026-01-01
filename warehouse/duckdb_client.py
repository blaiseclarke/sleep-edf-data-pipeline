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

    def load_epochs(self, df: pd.DataFrame, subject_id: int) -> None:
        """
        Loads subject-level sleep epoch data into the SLEEP_EPOCHS table.
        Clears existing data for the subject before inserting.
        """
        connection = duckdb.connect(self.db_path)
        try:
            # Delete existing records for subject
            connection.execute(
                "DELETE FROM SLEEP_EPOCHS WHERE SUBJECT_ID = ?", (subject_id,)
            )

            # Define explicit column mapping
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

            # Insert new records using explicit column list
            # We reference the 'df' variable directly in the SQL string
            # as DuckDB can scan local pandas DataFrames.
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
