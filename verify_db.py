import duckdb
import os
import pandas as pd

from ingest.config import DB_PATH

def verify_db():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        # Check tables
        tables = con.execute("SHOW TABLES").df()
        print("Tables found:")
        print(tables)

        if "SLEEP_EPOCHS" in tables["name"].values:
            count = con.execute("SELECT COUNT(*) FROM SLEEP_EPOCHS").fetchone()[0]
            print(f"\nSLEEP_EPOCHS Row Count: {count}")
            
            # Sample data
            df = con.execute("SELECT * FROM SLEEP_EPOCHS LIMIT 5").df()
            print("\nSample Data:")
            print(df)
            
            # Check for invalid stages
            invalid_stages = con.execute(
                "SELECT COUNT(*) FROM SLEEP_EPOCHS WHERE STAGE IN ('MOVE', 'NAN')"
            ).fetchone()[0]
            print(f"\nInvalid Stages (MOVE/NAN) Count: {invalid_stages}")
            
            # Check for negative power values (should be allowed now if dB < 0)
            neg_power = con.execute(
                "SELECT COUNT(*) FROM SLEEP_EPOCHS WHERE DELTA_POWER < 0"
            ).fetchone()[0]
            print(f"\nNegative Power Values Count: {neg_power}")

        else:
            print("\nSLEEP_EPOCHS table not found!")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    verify_db()
