from typing import Optional, Protocol
import pandas as pd


class WarehouseClient(Protocol):
    """
    Blueprint that all database clients must follow.
    Ensures the pipeline can interact with DuckDB, Snowflake, or other databases
    using a consistent interface.
    """

    def load_epochs(self, df: pd.DataFrame, subject_id: int) -> None:
        """Saves a batch of sleep data to the database."""
        ...

    def log_ingestion_error(
        self,
        subject_id: int,
        error_type: str,
        error_message: str,
        stack_trace: Optional[str] = None,
    ) -> None:
        """Saves error details to a table to debug them later."""
        ...
