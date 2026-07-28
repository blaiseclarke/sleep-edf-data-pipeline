import os
import subprocess

from pandera.errors import SchemaErrors
from prefect import flow, get_run_logger, task
from prefect.task_runners import ThreadPoolTaskRunner

from ingest.config import (
    ENDING_SUBJECT,
    MAX_WORKERS,
    RECORDING,
    STAGING_DIR,
    STARTING_SUBJECT,
    fetch_data,
)
from ingest.processing import batch_process_file
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
    staging_dir = STAGING_DIR / f"subject_{subject_id}"
    staging_dir.mkdir(parents=True, exist_ok=True)
    # Remove only parquet files instead of rmtree to avoid race conditions on retry
    for f in staging_dir.glob("*.parquet"):
        f.unlink()

    # Fetch calls (should trigger retry if they fail)
    filepaths = fetch_data(subjects=[subject_id], recording=[RECORDING])
    if not filepaths:
        return {"subject_id": subject_id, "path": None, "error": "No files found"}

    psg_path, hypno_path = filepaths[0]

    try:
        # Processing logic
        record_generator = batch_process_file(
            subject_id=subject_id,
            psg_path=psg_path,
            hypno_path=hypno_path,
            batch_size=100,
        )

        total_batches = 0

        # Consume generator
        for i, df_batch in enumerate(record_generator):
            if df_batch.empty:
                continue

            # Validate here so no bad data is saved to disk
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
        # Catch data quality errors
        logger.error(f"Validation failed for subject {subject_id}: {e}")
        return {
            "subject_id": subject_id,
            "path": None,
            "error": {"type": "SchemaError", "message": str(e)},
        }


@task
def load_parquet_to_warehouse(
    client: WarehouseClient, staging_path: str, subject_id: int
):
    """
    Hands the staging directory to the warehouse client to be natively loaded.
    """
    logger = get_run_logger()
    logger.info(f"Loading data from {staging_path} for subject {subject_id}...")

    # Load data directly using the staging path footprint
    # The warehouse client validates path existence and raises FileNotFoundError if missing
    client.load_epochs(staging_path, subject_id, overwrite=True)


def _run_dbt(arguments: list, logger) -> None:
    """
    Runs one dbt subcommand, relaying its output line by line as it arrives.

    dbt writes progress per model, so capturing the output and logging it in one
    block at the end hides how far a long build has got and, on failure, holds
    back the very lines needed to diagnose it.
    """
    argv = ["dbt", *arguments]
    logger.info("Executing: %s", " ".join(argv))

    with subprocess.Popen(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as process:
        for line in process.stdout or []:
            line = line.rstrip()
            if line:
                logger.info(line)

    if process.returncode != 0:
        raise RuntimeError(
            f"`{' '.join(argv)}` failed with exit code {process.returncode}"
        )


@task
def run_dbt_transformations():
    """
    Executes the dbt models using the local CLI to transform the newly loaded data.
    """
    logger = get_run_logger()

    warehouse_type = os.getenv("WAREHOUSE_TYPE", "duckdb").lower()
    target = "dev_duckdb" if warehouse_type == "duckdb" else "dev"

    # Install package dependencies (dbt_utils) before building
    _run_dbt(["deps", "--profiles-dir", "."], logger)

    # `build` walks the DAG running each model and then its tests, so a model
    # whose tests fail never has dependents built on top of it. Running every
    # model first and testing afterwards materialised the bad data throughout
    # the warehouse before anything noticed.
    _run_dbt(["build", "--profiles-dir", ".", "--target", target], logger)


# Each mapped subject holds a batch of EEG epochs in memory while it computes
# its spectra, so the fan-out is bounded rather than one thread per subject.
@flow(
    name="Sleep-EDF Ingestion Pipeline",
    task_runner=ThreadPoolTaskRunner(max_workers=MAX_WORKERS),
)
def run_ingestion_pipeline():
    logger = get_run_logger()

    if STARTING_SUBJECT > ENDING_SUBJECT:
        raise ValueError(
            f"STARTING_SUBJECT ({STARTING_SUBJECT}) must be <= "
            f"ENDING_SUBJECT ({ENDING_SUBJECT})"
        )

    warehouse_client = get_warehouse_client()

    # `docker compose up` and `python pipeline.py` both run the flow without a
    # separate setup step, so create the tables here. Idempotent on either
    # warehouse; scripts/setup_db.py remains available to run it on its own.
    logger.info("Ensuring warehouse tables exist...")
    warehouse_client.ensure_tables_exist()

    subject_ids = list(range(STARTING_SUBJECT, ENDING_SUBJECT + 1))

    # Download data
    logger.info("Ensuring data is available...")
    fetch_data(subjects=subject_ids, recording=[RECORDING])

    # Extract & validate (parallel) -> writes to disk
    extraction_results = extract_to_parquet.map(subject_ids)

    # Load to warehouse (serial) -> reads from disk
    failed_subjects = []
    # strict: .map() returns exactly one future per subject. If that ever stops
    # holding, silently truncating to the shorter list would drop subjects.
    for subject_id, result_future in zip(subject_ids, extraction_results, strict=True):
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
                failed_subjects.append(subject_id)
                continue

            staging_path = result["path"]
            if staging_path:
                load_parquet_to_warehouse(warehouse_client, staging_path, subject_id)

        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            logger.error(f"Pipeline loop failed for subject {subject_id}: {e}")
            failed_subjects.append(subject_id)

    if failed_subjects:
        logger.warning(
            f"{len(failed_subjects)}/{len(subject_ids)} subjects failed: {failed_subjects}"
        )

    if len(failed_subjects) == len(subject_ids):
        raise RuntimeError("All subjects failed — skipping dbt transformations.")

    logger.info(
        "Pipeline finished extracting and loading data. Starting transformations..."
    )
    run_dbt_transformations()

    logger.info("Pipeline completely finished!")


if __name__ == "__main__":
    run_ingestion_pipeline()
