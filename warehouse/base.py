from typing import Optional, Protocol


class WarehouseClient(Protocol):
    """
    Blueprint that all database clients must follow.
    Ensures the pipeline can interact with DuckDB, Snowflake, or other databases
    using a consistent interface.
    """

    def ensure_tables_exist(self) -> None:
        """
        Creates SLEEP_EPOCHS and INGESTION_ERRORS if they are not already there.
        Must be idempotent: callers run it on every setup, not just the first.
        """
        ...

    def load_epochs(
        self, staging_path: str, subject_id: int, overwrite: bool = True
    ) -> None:
        """Saves a batch of sleep data to the database from a Parquet directory."""
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
