import duckdb

try:
    con = duckdb.connect("data/sleep_data.db")
    count = con.execute("SELECT count(*) FROM SLEEP_EPOCHS").fetchone()[0]
    print(f"Total Rows in SLEEP_EPOCHS: {count}")
except Exception as e:
    print(f"Verification Failed: {e}")
    try:
        print("Existing tables:", con.execute("SHOW TABLES").fetchall())
    except:
        pass
