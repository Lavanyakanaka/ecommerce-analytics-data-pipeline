import pandas as pd
from sqlalchemy import text

from scripts.db_connection import get_connection, get_engine


def cleanse_customer_data(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicates and rows missing required customer fields."""
    required = ["customer_id", "first_name", "last_name", "email"]
    return df.drop_duplicates(subset=["customer_id"]).dropna(subset=required)


def cleanse_product_data(df: pd.DataFrame) -> pd.DataFrame:
    """Remove products with invalid pricing or missing keys."""
    required = ["product_id", "product_name", "category", "price"]
    cleaned = df.drop_duplicates(subset=["product_id"]).dropna(subset=required)
    return cleaned[cleaned["price"] > 0]


def apply_business_rules(df: pd.DataFrame, rule_type: str) -> pd.DataFrame:
    """Apply business rules for sales data."""
    if rule_type == "transactions":
        return df[df["total_amount"] > 0]
    if rule_type == "items":
        return df[(df["quantity"] > 0) & (df["unit_price"] > 0)]
    return df


def load_to_production(df: pd.DataFrame, table_name: str, strategy: str) -> dict:
    """Load dataframe into production schema using the chosen strategy."""
    if strategy == "truncate-insert":
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE production.{table_name}")
        conn.commit()
        cur.close()
        conn.close()
    
    engine = get_engine()
    df.to_sql(table_name, engine, schema="production", if_exists="append", index=False)
    return {"table": f"production.{table_name}", "rows_loaded": len(df), "strategy": strategy}


def main() -> dict:
    conn = get_connection()

    customers = pd.read_sql("SELECT * FROM staging.customers", conn)
    products = pd.read_sql("SELECT * FROM staging.products", conn)
    transactions = pd.read_sql("SELECT * FROM staging.transactions", conn)
    items = pd.read_sql("SELECT * FROM staging.transaction_items", conn)

    customers = cleanse_customer_data(customers)
    products = cleanse_product_data(products)
    transactions = apply_business_rules(transactions, "transactions")
    items = apply_business_rules(items, "items")

    summary = []
    summary.append(load_to_production(customers, "customers", "truncate-insert"))
    summary.append(load_to_production(products, "products", "truncate-insert"))
    summary.append(load_to_production(transactions, "transactions", "truncate-insert"))
    summary.append(load_to_production(items, "transaction_items", "truncate-insert"))
    
    conn.close()
    return {"status": "success", "summary": summary}


if __name__ == "__main__":
    result = main()
    print(result)
