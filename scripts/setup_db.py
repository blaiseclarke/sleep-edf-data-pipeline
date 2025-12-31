import os
import logging
import duckdb

from ingest_data import DB_PATH


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_database():
    """
    Initializes the DuckDB database and creates the necessary tables.
    """
    # Create directory if it doesn't exist
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        logger.info(f"Creating database directory: {db_dir}")
        os.makedirs(db_dir, exist_ok=True)

    connection = duckdb.connect(DB_PATH)

    try:
        # Create SLEEP_EPOCHS table
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS SLEEP_EPOCHS (
                SUBJECT_ID INTEGER,
                EPOCH_IDX INTEGER,
                STAGE VARCHAR,
                DELTA_POWER DOUBLE,
                THETA_POWER DOUBLE,
                ALPHA_POWER DOUBLE,
                SIGMA_POWER DOUBLE,
                BETA_POWER DOUBLE,
                LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Create INGESTION_ERRORS table
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS INGESTION_ERRORS (
                ERROR_ID UUID DEFAULT uuid(),
                SUBJECT_ID INTEGER,
                ERROR_TYPE VARCHAR,
                ERROR_MESSAGE VARCHAR,
                STACK_TRACE VARCHAR,
                OCCURRED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        logger.info("DuckDB database setup successfully.")

    finally:
        connection.close()


if __name__ == "__main__":
    setup_database()
