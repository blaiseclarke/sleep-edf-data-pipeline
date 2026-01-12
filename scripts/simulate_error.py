import duckdb
from pipeline import extract_subject_data
from warehouse.duckdb_client import DuckDBClient
from ingest_data import DB_PATH
from prefect import flow


@flow
def test_error_logging_flow():
    warehouse_client = DuckDBClient(db_path=DB_PATH)
    # Simulate a failure by trying to process a subject that doesn't exist (999)
    # This forces the pipeline to generate an error, to verify the logging system catches it
    result = extract_subject_data(subject_id=999)

    if result["error"]:
        err = result["error"]
        warehouse_client.log_ingestion_error(
            subject_id=999,
            error_type=err["type"],
            error_message=err["message"],
            stack_trace=err.get("stack_trace"),
        )


if __name__ == "__main__":
    # Ensure DB is set up
    from scripts.setup_db import setup_database

    setup_database()

    # Run the flow
    test_error_logging_flow()

    # Verify the error in DuckDB
    con = duckdb.connect(DB_PATH)
    df = con.execute("SELECT * FROM INGESTION_ERRORS").df()
    print("\nCaptured Ingestion Errors:")
    print(df)
    con.close()
