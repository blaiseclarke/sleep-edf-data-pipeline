from pathlib import Path
from prefect import task, flow, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from pandera.errors import SchemaErrors

import pandas as pd
import shutil
import traceback

from ingest.processing import batch_process_file
from ingest_data import (
    ENDING_SUBJECT,
    RECORDING,
    STARTING_SUBJECT,
    STUDY,
    fetch_data
)
from validators import SleepSchema
from warehouse.base import WarehouseClient
from warehouse.factory import get_warehouse_client


@task(retries=2, retry_delay_seconds=10)
def extract_to_parquet(subject_id: int) -> dict:
    """
    Consumes the generator and writes batches to partitioned Parquet files.
    Returns the directory path where files were saved.
    """
    logger = get_run_logger()
    logger.info(f"Starting extraction for subject {subject_id}")

    # Create a staging directory for this subject
    # ex. data/staging/subject_1/
    staging_dir = Path(f"data/staging/subject_{subject_id}")
    if staging_dir.exists():
        shutil.rmtree(staging_dir)  # Clean start
    staging_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Get the paths
        filepaths = fetch_data(subjects=[subject_id], recording=[RECORDING])
        if not filepaths:
            return {"subject_id": subject_id, "path": None, "error": "No files found"}

        psg_path, hypno_path = filepaths[0]

        # Initialize generator
        record_generator = batch_process_file(
            subject_id=subject_id,
            psg_path=psg_path,
            hypno_path=hypno_path,
            batch_size=100
        )

        total_batches = 0

        # Consume generator
        for i, df_batch in enumerate(record_generator):
            if df_batch.empty:
                continue

            # Validate here so we don't save bad data to disk
            validated_df = SleepSchema.validate(df_batch, lazy=True)

            # Write to Parquet
            # Format: part_0.parquet, part_1.parquet
            file_path = staging_dir / f"part_{i}.parquet"
            validated_df.to_parquet(file_path, index=False)
            total_batches += 1

        if total_batches == 0:
            return {
                "subject_id": subject_id,
                "path": None,
                "error": "No epochs processed",
            }

        return {"subject_id": subject_id, "path": str(staging_dir), "error": None}

    except SchemaErrors as e:
        logger.error(f"Validation failed for subject {subject_id}: {e}")
        return {
            "subject_id": subject_id,
            "path": None,
            "error": {"type": "SchemaError", "message": str(e)},
        }
    except Exception as e:
        logger.error(f"Extraction failed for subject {subject_id}: {e}")
        return {
            "subject_id": subject_id,
            "path": None,
            "error": {
                "type": type(e).__name__,
                "message": str(e),
                "stack_trace": traceback.format_exc(),
            },
        }


@task
def load_parquet_to_warehouse(
    client: WarehouseClient, staging_path: str, subject_id: int
):
    """
    Reads partitioned Parquet files and loads them to the warehouse.
    """
    logger = get_run_logger()
    path_obj = Path(staging_path)

    if not path_obj.exists():
        logger.warning(f"Staging path {staging_path} does not exist.")
        return

    # Gather all partition files
    parquet_files = sorted(path_obj.glob("*.parquet"))

    logger.info(f"Loading {len(parquet_files)} batches for subject {subject_id}...")

    # Load file by file (or bulk load if the warehouse supports it)
    # DuckDB can actually query the whole folder: "SELECT * FROM '.../*.parquet'"
    # But for now, let's keep it explicit to match your Client interface.

    # Optional: Read all parts into one DF if memory allows, OR loop and load.
    # Since we designed this for memory safety, let's loop.
    for i, p_file in enumerate(parquet_files):
        df = pd.read_parquet(p_file)
        # Ensure uppercase for Snowflake compatibility
        df.columns = [c.upper() for c in df.columns]
        
        # Only overwrite on the first batch, append (retain) for subsequent batches
        overwrite = (i == 0)
        client.load_epochs(df, subject_id, overwrite=overwrite)

    # Cleanup (Optional: remove staging files after successful load)
    # shutil.rmtree(path_obj)


@flow(name="Sleep-EDF Ingestion Pipeline")
def run_ingestion_pipeline():
    logger = get_run_logger()
    warehouse_client = get_warehouse_client()

    subject_ids = list(range(STARTING_SUBJECT, ENDING_SUBJECT + 1))

    # Download data
    logger.info("Ensuring data is available...")
    fetch_data(subjects=subject_ids, recording=[RECORDING])

    # Extract & validate (parallel) -> writes to disk
    extraction_results = extract_to_parquet.map(subject_ids)

    # Load to warehouse (serial) -> reads from disk
    for subject_id, result_future in zip(subject_ids, extraction_results):
        try:
            result = result_future.result()

            if result.get("error"):
                err = result["error"]
                # Handle nested error dicts or string errors
                msg = err["message"] if isinstance(err, dict) else str(err)
                logger.warning(f"Skipping subject {subject_id}: {msg}")
                warehouse_client.log_ingestion_error(
                    subject_id=subject_id,
                    error_type="ExtractionFailed",
                    error_message=msg,
                )
                continue

            staging_path = result["path"]
            if staging_path:
                load_parquet_to_warehouse(warehouse_client, staging_path, subject_id)

        except Exception as e:
            logger.error(f"Pipeline loop failed for subject {subject_id}: {e}")

    logger.info("Pipeline finished!")


if __name__ == "__main__":
    run_ingestion_pipeline()
