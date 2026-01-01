import os
from warehouse.base import WarehouseClient
from warehouse.duckdb_client import DuckDBClient
from warehouse.snowflake_client import SnowflakeClient


def get_warehouse_client() -> WarehouseClient:
    """
    Factory function to return the appropriate WarehouseClient based on configuration.
    Defaults to DuckDB if WAREHOUSE_TYPE is not set or set to 'duckdb'.
    """
    warehouse_type = os.getenv("WAREHOUSE_TYPE", "duckdb").lower()

    if warehouse_type == "snowflake":
        return SnowflakeClient()
    elif warehouse_type == "duckdb":
        # DuckDB client handles the default DB_PATH if none is provided
        return DuckDBClient()
    else:
        raise ValueError(f"Unsupported WAREHOUSE_TYPE: {warehouse_type}")
