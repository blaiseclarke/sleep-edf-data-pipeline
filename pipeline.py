import pandas as pd
from prefect import task, flow, get_run_logger
from ingest_data import process_subject, STARTING_SUBJECT, ENDING_SUBJECT
from schemas import SleepEpoch
from pydantic import ValidationError

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
    records = df.to_dict(orient='records')
    valid_records = []

    for record in records:
        try:
            valid_record = SleepEpoch(**record)
            valid_records.append(valid_record.model_dump())
        except ValidationError as e:
            logger.error(f"Validation failed for epoch {record.get('epoch_idx')}: {e}")
            continue
    
    return pd.DataFrame(valid_records)

@flow(name='Sleep-EDF Ingestion Pipeline')
def run_ingestion_pipeline():
    """
    Executes the ingestion pipeline as the Prefect starting point.

    Orchestrates extraction, validation, and loading tasks across all 
    subject files.
    """
    logger = get_run_logger()
    all_data = []

    # Iterate through subjects
    for subject_id in range(STARTING_SUBJECT, ENDING_SUBJECT + 1):
        raw_df = extract_subject_data(subject_id)

        if not raw_df.empty:
            clean_df = validate_data(raw_df)
            all_data.append(clean_df)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv("sleep_data_validated.csv", index=False)
        logger.info(f"Pipeline finished! Exported {len(final_df)} validated rows.")
    else:
        logger.warning("No data processed.")

if __name__ == "__main__":
    run_ingestion_pipeline()