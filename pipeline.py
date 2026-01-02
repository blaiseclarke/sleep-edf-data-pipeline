import os
import traceback
import pandas as pd
from prefect import task, flow, get_run_logger

from ingest_data import (
    process_subject,
    fetch_data,
    STARTING_SUBJECT,
    ENDING_SUBJECT,
    RECORDING,
    STUDY,
)
from warehouse.factory import get_warehouse_client
from warehouse.base import WarehouseClient
from validators import SleepSchema
from pandera.errors import SchemaErrors
from prefect.task_runners import ConcurrentTaskRunner


@task(retries=2, retry_delay_seconds=10)
def extract_subject_data(subject_id: int) -> dict:
    """
    Locates and extracts subject data from local EDF files.
    
    This is kept separate from validation to debug extraction errors 
    (ex. missing files, corrupt headers) independently of data quality errors.
    """
    logger = get_run_logger()
    logger.info(f"Starting extraction for subject {subject_id}")

    try:
        df = process_subject(subject_id)

        if df is None or df.empty:
            return {
                "subject_id": subject_id,
                "data": None,
                "error": {"type": "NoData", "message": "No data returned"},
            }

        return {"subject_id": subject_id, "data": df, "error": None}

    except Exception as e:
        logger.error(f"Extraction failed for subject {subject_id}: {str(e)}")
        return {
            "subject_id": subject_id,
            "data": None,
            "error": {
                "type": type(e).__name__,
                "message": str(e),
                "stack_trace": traceback.format_exc(),
            },
        }


@task
def validate_data(df: pd.DataFrame, subject_id: int) -> dict:
    """
    Validates raw DataFrame records against the Pandera SleepSchema.
    
    This acts as a gatekeeper: if the data contains negative power values or 
    unknown sleep stages, it is rejected *before* it gets to the database.
    This prevents "garbage in, garbage out" in the downstream dashboard.
    """
    logger = get_run_logger()

    try:
        validated_df = SleepSchema.validate(df, lazy=True)
        return {"subject_id": subject_id, "data": validated_df, "error": None}
    except SchemaErrors as e:
        logger.error(f"Validation failed for subject {subject_id}: {str(e)}")
        return {
            "subject_id": subject_id,
            "data": None,
            "error": {
                "type": "SchemaErrors",
                "message": str(e),
                "stack_trace": traceback.format_exc(),
            },
        }


@task
def process_subject_task(subject_id: int) -> dict:
    """Composite task to handle extraction and validation for a single subject."""
    result = extract_subject_data(subject_id)

    if result["error"]:
        return result

    return validate_data(result["data"], subject_id)


@task
def load_to_warehouse(client: WarehouseClient, df: pd.DataFrame, subject_id: int):
    """Persists subject data to the configured warehouse."""
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
    Executes the ingestion pipeline.
    
    1. EXTRACT/TRANSFORM: Run `process_subject_task` in parallel because 
       signal processing is CPU intensive and the subjects are independent.
    2. LOAD: Switch to a sequential loop to write to the database.
       This is done because SQLite/DuckDB (the local DBs) can lock up or corrupt 
       files if multiple processes try to write to them simultaneously.
    """
    logger = get_run_logger()
    warehouse_client = get_warehouse_client()

    subject_ids = list(range(STARTING_SUBJECT, ENDING_SUBJECT + 1))

    # 1. Pre-fetch data
    # logical step: download everything first
    logger.info(
        f"Ensuring data is available for subjects {subject_ids} in study '{STUDY}'"
    )
    fetch_data(subjects=subject_ids, recording=[RECORDING])

    # 2. Processes data in parallel
    # Uses .map() to execute the processing task for all subjects 
    # concurrently, leveraging available CPU cores
    processed_results = process_subject_task.map(subject_ids)

    # 3. Serial write
    # Deliberately write one subject at a time
    # While slower, this guarantees the database file never gets corrupted
    # by concurrent write attempts
    for subject_id, result_future in zip(subject_ids, processed_results):
        try:
            result = result_future.result()

            if result["error"]:
                err = result["error"]
                logger.warning(
                    f"Subject {subject_id} failed {err['type']}: {err['message']}"
                )
                warehouse_client.log_ingestion_error(
                    subject_id=subject_id,
                    error_type=err["type"],
                    error_message=err["message"],
                    stack_trace=err.get("stack_trace"),
                )
                continue

            clean_df = result["data"]
            if clean_df is not None:
                # Ensure columns are uppercase to match warehouse schema
                clean_df.columns = [c.upper() for c in clean_df.columns]
                load_to_warehouse(warehouse_client, clean_df, subject_id)

        except Exception as e:
            error_msg = f"Critical failure in coordination loop for subject {subject_id}: {str(e)}"
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
