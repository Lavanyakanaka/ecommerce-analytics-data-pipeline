import json
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values
from sqlalchemy import text

from scripts.db_connection import get_connection, get_engine

RAW_PATH = Path("data/raw")
OUT_PATH = Path("data/staging")
OUT_PATH.mkdir(exist_ok=True)

def bulk_insert_data(df: pd.DataFrame, table_name: str, connection) -> int:
    """Bulk insert a dataframe into a target table."""
    if df.empty:
        return 0
    cols = ",".join(df.columns)
    values = [tuple(x) for x in df.to_numpy()]
    query = f"INSERT INTO {table_name} ({cols}) VALUES %s"
    with connection.cursor() as cur:
        execute_values(cur, query, values)
        connection.commit()
    return len(df)

def load_csv_to_staging(csv_path: str, table_name: str, connection) -> dict:
    """Load a CSV into staging and return a summary."""
    df = pd.read_csv(csv_path)
    rows = bulk_insert_data(df, table_name, connection)
    return {"table": table_name, "rows_loaded": rows}

def validate_staging_load(connection) -> dict:
    """Return row counts for staging tables after load."""
    result = {}
    with connection.cursor() as cur:
        for table in ["customers", "products", "transactions", "transaction_items"]:
            cur.execute(f"SELECT COUNT(*) FROM staging.{table}")
            result[table] = cur.fetchone()[0]
    return result

def main():
    conn = get_connection()
    engine = get_engine()
    
    # Truncate staging tables before loading
    with engine.begin() as db_conn:
        for table in ["customers", "products", "transactions", "transaction_items"]:
            db_conn.execute(text(f"TRUNCATE TABLE staging.{table} CASCADE"))
    
    summary = []

    mapping = {
        "customers.csv": "staging.customers",
        "products.csv": "staging.products",
        "transactions.csv": "staging.transactions",
        "transaction_items.csv": "staging.transaction_items"
    }

    for csv, table in mapping.items():
        summary.append(load_csv_to_staging(RAW_PATH / csv, table, conn))

    summary.append(validate_staging_load(conn))

    with open(OUT_PATH / "ingestion_summary.json", "w") as f:
        json.dump(summary, f, indent=4)

    conn.close()

if __name__ == "__main__":
    main()
