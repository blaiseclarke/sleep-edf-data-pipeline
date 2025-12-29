import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from prefect import task, flow, get_run_logger
from snowflake.connector.pandas_tools import write_pandas
from ingest_data import process_subject, STARTING_SUBJECT, ENDING_SUBJECT
from schemas import SleepEpoch
from pydantic import ValidationError

# Loading environment variables
load_dotenv()


@task(retries=2, retry_delay_seconds=10)
def extract_subject_data(subject_id: int) -> pd.DataFrame:
    """
    Locates and extracts subject data from the local EDF files.

    Handles signal processing using MNE-Python, channel renaming, and bandpass
    filtering.

    :param subject_id: subject ID for the file.
    :return DataFrame to be sent to Pydantic for validation, then ingestion.
    """
    logger = get_run_logger()
    logger.info(f"Starting extraction for subject {subject_id}")

    df = process_subject(subject_id)

    if df is None or df.empty:
        logger.warning(f"No data was returned for subject {subject_id}")
        return pd.DataFrame()

    return df


@task
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates raw DataFrame records against the Pydantic SleepEpoch constraints.

    Iterates through records, catching validation errors, and logging them
    as warnings.

    :param df: DataFrame containing band power data for a batch of epochs.
    :return: DataFrame containing records that have passed validation.
    """

    logger = get_run_logger()
    records = df.to_dict(orient="records")
    valid_records = []

    for record in records:
        try:
            valid_record = SleepEpoch(**record)
            valid_records.append(valid_record.model_dump())
        except ValidationError as e:
            logger.error(f"Validation failed for epoch {record.get('epoch_idx')}: {e}")
            continue

    return pd.DataFrame(valid_records)


@task
def load_subject_to_snowflake(df, subject_id, table_name="SLEEP_EPOCHS"):
    """
    Loads a pandas DataFrame directly into Snowflake.

    This function handles the connection and makes sure the connection is closed
    even if the upload fails.
    """
    logger = get_run_logger()

    # Get credentials from environment variables
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")

    if not all([user, password, account, warehouse, database, schema]):
        raise ValueError("Missing Snowflake environment variables")

    logger.info(f"Connecting to Snowflake account {account}...")

    # Establish connection
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )

    try:
        cursor = conn.cursor()
        try:
            delete_query = f"DELETE FROM {table_name} WHERE SUBJECT_ID = {subject_id}"
            logger.info(f"Clearing existing data for Subject {subject_id}...")
            cursor.execute(delete_query)
        except Exception as e:
            logger.warning(f"Could not clear data: {e}")

        success, n_chunks, n_rows, _ = write_pandas(
            conn, df, table_name.upper(), auto_create_table=True, overwrite=False
        )

        if success:
            logger.info(f"Subject {subject_id}: Loaded {n_rows} rows.")
        else:
            raise Exception("Snowflake upload failed.")
    finally:
        conn.close()


@flow(name="Sleep-EDF Ingestion Pipeline")
def run_ingestion_pipeline():
    """
    Executes the ingestion pipeline as the Prefect starting point.

    Orchestrates extraction, validation, and loading tasks across all
    subject files.
    """
    logger = get_run_logger()

    # Iterate through subject recordings
    for subject_id in range(STARTING_SUBJECT, ENDING_SUBJECT + 1):
        raw_df = extract_subject_data(subject_id)

        if raw_df.empty:
            continue

        clean_df = validate_data(raw_df)

        clean_df.columns = [c.upper() for c in clean_df.columns]
        load_subject_to_snowflake(clean_df, subject_id)

    logger.info("Pipeline finished!")


if __name__ == "__main__":
    run_ingestion_pipeline()
