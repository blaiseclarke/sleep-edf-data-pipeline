"""
Drive one deliberate ingestion failure and confirm it lands in INGESTION_ERRORS.

Run with:  PYTHONPATH=. python scripts/simulate_error.py
"""

import duckdb
from prefect import flow

from ingest.config import DB_PATH
from pipeline import describe_error, extract_to_parquet
from warehouse.factory import get_warehouse_client

MISSING_SUBJECT = 999


@flow
def simulate_ingestion_failure():
    client = get_warehouse_client()
    client.ensure_tables_exist()

    # Subject 999 is outside the dataset, so extraction finds no files.
    result = extract_to_parquet(subject_id=MISSING_SUBJECT)

    if not result["error"]:
        raise RuntimeError(
            f"Expected subject {MISSING_SUBJECT} to fail, but extraction succeeded."
        )

    error_type, error_message, stack_trace = describe_error(result["error"])
    client.log_ingestion_error(
        subject_id=MISSING_SUBJECT,
        error_type=error_type,
        error_message=error_message,
        stack_trace=stack_trace,
    )
    return error_message


if __name__ == "__main__":
    simulate_ingestion_failure()

    connection = duckdb.connect(DB_PATH, read_only=True)
    try:
        errors = connection.execute(
            """
            SELECT SUBJECT_ID, ERROR_TYPE, ERROR_MESSAGE, OCCURRED_AT
            FROM INGESTION_ERRORS
            WHERE SUBJECT_ID = ?
            ORDER BY OCCURRED_AT DESC
            """,
            [MISSING_SUBJECT],
        ).df()
    finally:
        connection.close()

    print("\nCaptured ingestion errors:")
    print(errors.to_string(index=False))
    if errors.empty:
        raise SystemExit("No error was recorded — the error warehouse is not working.")
