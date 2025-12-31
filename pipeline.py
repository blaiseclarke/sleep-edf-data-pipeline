import os
import traceback
import pandas as pd
from prefect import task, flow, get_run_logger
from mne.datasets.sleep_physionet.age import fetch_data as fetch_edf_data

from ingest_data import (
    process_subject as extract_logic,
    STARTING_SUBJECT,
    ENDING_SUBJECT,
    RECORDING,
    DB_PATH,
)
from warehouse.duckdb_client import DuckDBClient
from validators import SleepSchema
from pandera.errors import SchemaErrors
from prefect.task_runners import ConcurrentTaskRunner


@task(retries=2, retry_delay_seconds=10)
def extract_subject_data(subject_id: int) -> dict:
    """
    Locates and extracts subject data from the local EDF files.
    """
    logger = get_run_logger()
    logger.info(f"Starting extraction for subject {subject_id}")

    try:
        df = extract_logic(subject_id)

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
def process_subject(subject_id: int) -> dict:
    """
    Composite task to handle extraction and validation for a single subject.
    """
    result = extract_subject_data(subject_id)

    if result["error"]:
        return result

    return validate_data(result["data"], subject_id)


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

    # Fetch data upfront to avoid parallel download contention
    logger.info(f"Ensuring data is available for subjects {subject_ids}")
    fetch_edf_data(subjects=subject_ids, recording=[RECORDING])

    # Use mapping for parallel extraction and validation
    # Parallel tasks no longer write to the warehouse to avoid locking
    processed_results = process_subject.map(subject_ids)

    # Serialized loading and error logging to avoid DuckDB locking
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
