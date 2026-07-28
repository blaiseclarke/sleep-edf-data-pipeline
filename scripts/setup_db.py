import logging
import os

from warehouse.factory import get_warehouse_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_database():
    """
    Creates the tables the pipeline writes to, in whichever warehouse
    WAREHOUSE_TYPE selects.

    The DDL lives on the clients rather than here, so that DuckDB and Snowflake
    cannot drift apart and so that this script stays a thin entry point.
    """
    warehouse_type = os.getenv("WAREHOUSE_TYPE", "duckdb").lower()

    logger.info("Setting up %s tables...", warehouse_type)

    # DuckDB creates its parent directory and its tables on construction;
    # Snowflake needs the explicit call below.
    client = get_warehouse_client()
    client.ensure_tables_exist()

    logger.info("%s setup completed successfully.", warehouse_type.capitalize())


if __name__ == "__main__":
    setup_database()
