import os
import traceback
import pandas as pd
import pandera as pa
from typing import Optional
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger, unmapped

# Loading environment variables
load_dotenv()

from ingest_data import (  # noqa: E402
    process_subject as extract_logic,
    STARTING_SUBJECT,
    ENDING_SUBJECT,
    DB_PATH,
)
from warehouse.duckdb_client import DuckDBClient  # noqa: E402
from validators import SleepSchema  # noqa: E402
from prefect.task_runners import ConcurrentTaskRunner  # noqa: E402


@task(retries=2, retry_delay_seconds=10)
def extract_subject_data(
    subject_id: int, warehouse_client: DuckDBClient
) -> pd.DataFrame:
    """
    Locates and extracts subject data from the local EDF files.
    """
    logger = get_run_logger()
    logger.info(f"Starting extraction for subject {subject_id}")

    try:
        df = extract_logic(subject_id)

        if df is None or df.empty:
            logger.warning(f"No data was returned for subject {subject_id}")
            return pd.DataFrame()

        return df

    except Exception as e:
        error_msg = f"Extraction failed for subject {subject_id}: {str(e)}"
        logger.error(error_msg)
        warehouse_client.log_ingestion_error(
            subject_id=subject_id,
            error_type=type(e).__name__,
            error_message=str(e),
            stack_trace=traceback.format_exc(),
        )
        return pd.DataFrame()


@task
def validate_data(
    df: pd.DataFrame, subject_id: int, warehouse_client: DuckDBClient
) -> Optional[pd.DataFrame]:
    """
    Validates raw DataFrame records against the Pandera SleepSchema.
    """
    logger = get_run_logger()

    try:
        return SleepSchema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as e:
        error_msg = f"Validation failed for subject {subject_id}: {str(e)}"
        logger.error(error_msg)
        warehouse_client.log_ingestion_error(
            subject_id=subject_id,
            error_type="SchemaErrors",
            error_message=str(e),
            stack_trace=traceback.format_exc(),
        )
        return None


@task
def process_subject(
    subject_id: int, warehouse_client: DuckDBClient
) -> Optional[pd.DataFrame]:
    """
    Composite task to handle extraction and validation for a single subject.
    """
    raw_df = extract_subject_data(subject_id, warehouse_client)

    if raw_df is None or raw_df.empty:
        return None

    return validate_data(raw_df, subject_id, warehouse_client)


@task
def load_to_warehouse(client: DuckDBClient, df: pd.DataFrame, subject_id: int):
    """
    Persists subject data to the configured warehouse.
    """
    logger = get_run_logger()
    logger.info(f"Loading data for subject {subject_id} to warehouse...")
    client.load_epochs(df, subject_id)


@flow(
    name="Sleep-EDF Ingestion Pipeline",
    task_runner=ConcurrentTaskRunner(
        max_workers=int(os.getenv("PREFECT_MAX_WORKERS", "3"))
    ),
)
def run_ingestion_pipeline():
    """
    Executes the ingestion pipeline using Prefect mapping for parallelization.
    """
    logger = get_run_logger()
    warehouse_client = DuckDBClient(db_path=DB_PATH)

    subject_ids = list(range(STARTING_SUBJECT, ENDING_SUBJECT + 1))

    # Use mapping for parallel extraction and validation
    processed_results = process_subject.map(subject_ids, unmapped(warehouse_client))

    # Serialized loading to avoid DuckDB locking
    for subject_id, result_future in zip(subject_ids, processed_results):
        try:
            clean_df = result_future.result()

            if clean_df is not None:
                # Ensure columns are uppercase to match warehouse schema
                clean_df.columns = [c.upper() for c in clean_df.columns]
                load_to_warehouse(warehouse_client, clean_df, subject_id)
            else:
                logger.warning(
                    f"Skipping load for subject {subject_id} due to previous failures."
                )

        except Exception as e:
            error_msg = (
                f"Critical failure in load loop for subject {subject_id}: {str(e)}"
            )
            logger.error(error_msg)
            warehouse_client.log_ingestion_error(
                subject_id=subject_id,
                error_type=type(e).__name__,
                error_message=str(e),
                stack_trace=traceback.format_exc(),
            )

    logger.info("Pipeline finished!")


if __name__ == "__main__":
    run_ingestion_pipeline()
